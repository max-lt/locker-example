//! V8 Locker with HTTP thread pool
//!
//! Demonstrates V8 isolate pool + HTTP client thread pool working together.

use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use rand::Rng;

fn now() -> String {
  let d = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
  format!("{:02}:{:02}:{:02}.{:03}", (d.as_secs() / 3600) % 24, (d.as_secs() / 60) % 60, d.as_secs() % 60, d.subsec_millis())
}

// === HTTP Thread Pool ===

struct HttpRequest {
  worker_id: usize,
  step: usize,
  response_tx: mpsc::Sender<HttpResult>,
}

struct HttpResult {
  worker_id: usize,
  step: usize,
  data: String,
  thread_name: String,
  duration_ms: u64,
}

struct HttpPool {
  tx: mpsc::Sender<HttpRequest>,
}

impl HttpPool {
  fn new(num_threads: usize) -> Self {
    let (tx, rx) = mpsc::channel::<HttpRequest>();
    let rx = Arc::new(Mutex::new(rx));

    for i in 0..num_threads {
      let rx = Arc::clone(&rx);
      let name = format!("http-{}", i);
      thread::Builder::new().name(name.clone()).spawn(move || {
        loop {
          match rx.lock().unwrap().recv() {
            Ok(req) => {
              let delay = rand::rng().random_range(10..50);
              thread::sleep(Duration::from_millis(delay));
              let _ = req.response_tx.send(HttpResult {
                worker_id: req.worker_id,
                step: req.step,
                data: format!("{{\"w\":{},\"s\":{}}}", req.worker_id, req.step),
                thread_name: name.clone(),
                duration_ms: delay,
              });
            }
            Err(_) => break,
          }
        }
      }).unwrap();
    }
    Self { tx }
  }

  fn fetch(&self, worker_id: usize, step: usize) -> mpsc::Receiver<HttpResult> {
    let (tx, rx) = mpsc::channel();
    let _ = self.tx.send(HttpRequest { worker_id, step, response_tx: tx });
    rx
  }
}

// === V8 Thread Pool ===

struct PooledIsolate {
  id: usize,
  isolate: v8::UnenteredIsolate,
}

struct IsolatePool {
  isolates: Mutex<Vec<PooledIsolate>>,
}

impl IsolatePool {
  fn new(num: usize) -> Self {
    Self {
      isolates: Mutex::new((0..num).map(|i| PooledIsolate {
        id: i,
        isolate: v8::Isolate::new_unentered(Default::default()),
      }).collect()),
    }
  }

  fn acquire(&self) -> Option<PooledIsolate> { self.isolates.lock().unwrap().pop() }
  fn release(&self, iso: PooledIsolate) { self.isolates.lock().unwrap().push(iso); }
}

struct V8Request {
  worker_id: usize,
  step: usize,
  fetch_data: Option<String>,
  response_tx: mpsc::Sender<V8Result>,
}

struct V8Result {
  worker_id: usize,
  step: usize,
  result: String,
  isolate_id: usize,
  thread_name: String,
}

struct V8Pool {
  tx: mpsc::Sender<V8Request>,
}

impl V8Pool {
  fn new(num_threads: usize, isolate_pool: Arc<IsolatePool>) -> Self {
    let (tx, rx) = mpsc::channel::<V8Request>();
    let rx = Arc::new(Mutex::new(rx));

    for i in 0..num_threads {
      let rx = Arc::clone(&rx);
      let pool = Arc::clone(&isolate_pool);
      let name = format!("v8-{}", i);

      thread::Builder::new().name(name.clone()).spawn(move || {
        loop {
          match rx.lock().unwrap().recv() {
            Ok(req) => {
              let mut pooled = loop {
                if let Some(p) = pool.acquire() { break p; }
                thread::sleep(Duration::from_micros(100));
              };

              let mut locker = v8::Locker::new(&mut pooled.isolate);
              let result = {
                let scope = std::pin::pin!(v8::HandleScope::new(&mut *locker));
                let scope = &mut scope.init();
                let context = v8::Context::new(scope, Default::default());
                let scope = &mut v8::ContextScope::new(scope, context);

                let code = if let Some(data) = &req.fetch_data {
                  format!("`w{} s{}: got {}`", req.worker_id, req.step, data)
                } else {
                  format!("`w{} s{}: init`", req.worker_id, req.step)
                };
                let code = v8::String::new(scope, &code).unwrap();
                let script = v8::Script::compile(scope, code, None).unwrap();
                script.run(scope).unwrap().to_rust_string_lossy(scope)
              };

              let iso_id = pooled.id;
              drop(locker);
              pool.release(pooled);

              let _ = req.response_tx.send(V8Result {
                worker_id: req.worker_id,
                step: req.step,
                result,
                isolate_id: iso_id,
                thread_name: name.clone(),
              });
            }
            Err(_) => break,
          }
        }
      }).unwrap();
    }
    Self { tx }
  }

  fn execute(&self, worker_id: usize, step: usize, fetch_data: Option<String>) -> mpsc::Receiver<V8Result> {
    let (tx, rx) = mpsc::channel();
    let _ = self.tx.send(V8Request { worker_id, step, fetch_data, response_tx: tx });
    rx
  }
}

fn main() {
  let num_workers: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(10);
  let num_isolates: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(2);
  let steps = 3;

  let platform = v8::new_default_platform(0, false).make_shared();
  v8::V8::initialize_platform(platform);
  v8::V8::initialize();

  println!("=== V8 + HTTP Pool Example ===\n");
  println!("Config: {} workers, {} isolates, {} steps/worker\n", num_workers, num_isolates, steps);

  let isolate_pool = Arc::new(IsolatePool::new(num_isolates));
  let v8_pool = Arc::new(V8Pool::new(2, Arc::clone(&isolate_pool)));
  let http_pool = Arc::new(HttpPool::new(4));

  let start = Instant::now();

  // Run workers: JS -> fetch -> JS -> fetch -> JS
  thread::scope(|s| {
    for w in 0..num_workers {
      let v8 = Arc::clone(&v8_pool);
      let http = Arc::clone(&http_pool);
      s.spawn(move || {
        for step in 0..steps {
          // Execute JS
          let fetch_data = if step > 0 {
            let rx = http.fetch(w, step);
            let r = rx.recv().unwrap();
            println!("[{}] w{} fetch step {} from {} ({}ms)", now(), w, step, r.thread_name, r.duration_ms);
            Some(r.data)
          } else {
            None
          };

          let rx = v8.execute(w, step, fetch_data);
          let r = rx.recv().unwrap();
          println!("[{}] w{} JS step {}: {} (t={} i={})", now(), w, step, r.result, r.thread_name, r.isolate_id);
        }
        println!("[{}] w{} DONE", now(), w);
      });
    }
  });

  let elapsed = start.elapsed();
  println!("\n=== {} workers completed in {:.2}s ===", num_workers, elapsed.as_secs_f64());
}
