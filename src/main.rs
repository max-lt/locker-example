//! PoC: Async V8 Isolate Pool with TRUE Multi-threaded Execution
//!
//! Demonstrates suspending/resuming V8 isolates on async boundaries
//! with REAL parallelism - multiple V8 threads executing JS simultaneously.
//!
//! Architecture:
//! - V8 Thread Pool: Dedicated threads for JS execution (uses Locker for thread safety)
//! - HTTP Client Pool: Dedicated threads for fetch operations
//! - Main thread: Async orchestration only

use clap::Parser;
use futures::stream::{FuturesUnordered, StreamExt};
use rand::Rng;
use std::future::Future;
use std::pin::Pin;
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot;

#[derive(Parser)]
#[command(name = "openworkers")]
#[command(about = "Multi-threaded V8 isolate pool demo", long_about = None)]
struct Args {
  /// Number of concurrent workers
  #[arg(short, long, default_value_t = 20)]
  workers: usize,

  /// Number of V8 threads for JS execution
  #[arg(short = 't', long = "v8-threads", default_value_t = 2)]
  v8_threads: usize,

  /// Number of V8 isolates (can differ from threads)
  #[arg(short, long, default_value_t = 4)]
  isolates: usize,

  /// Number of HTTP threads for fetch operations
  #[arg(short = 'H', long = "http-threads", default_value_t = 4)]
  http_threads: usize,

  /// Number of steps per worker (JS exec + fetch per step)
  #[arg(short, long, default_value_t = 3)]
  steps: usize,

  /// Timeout in seconds
  #[arg(long, default_value_t = 30)]
  timeout: u64,
}

/// Get ISO timestamp
fn now() -> String {
  let duration = SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap();
  let secs = duration.as_secs();
  let millis = duration.subsec_millis();
  let hours = (secs / 3600) % 24;
  let mins = (secs / 60) % 60;
  let secs = secs % 60;
  format!("{:02}:{:02}:{:02}.{:03}", hours, mins, secs, millis)
}

/// Log with timestamp and worker ID
fn log(worker_id: usize, msg: &str) {
  let thread = thread::current();
  let thread_name = thread.name().unwrap_or("???");
  println!("[{}] [{}] [w={:03}] {}", now(), thread_name, worker_id, msg);
}

/// Log for V8 execution (worker + thread + isolate)
fn log_v8(worker_id: usize, thread_name: &str, isolate_id: usize, msg: &str) {
  println!(
    "[{}] [w={:03},t={},i={}] {}",
    now(),
    worker_id,
    thread_name,
    isolate_id,
    msg
  );
}

/// Log for pool threads
fn log_thread(msg: &str) {
  let thread = thread::current();
  let thread_name = thread.name().unwrap_or("???");
  println!("[{}] [{}] {}", now(), thread_name, msg);
}

// ============================================================================
// HTTP Client Thread Pool
// ============================================================================

struct HttpRequest {
  worker_id: usize,
  step: usize,
  url: String,
  response_tx: oneshot::Sender<FetchResult>,
}

#[derive(Debug)]
struct FetchResult {
  data: String,
  duration_ms: u64,
  thread_name: String,
}

struct HttpClientPool {
  request_tx: mpsc::Sender<HttpRequest>,
}

impl HttpClientPool {
  fn new(num_threads: usize) -> Self {
    let (request_tx, request_rx) = mpsc::channel::<HttpRequest>();
    let request_rx = Arc::new(Mutex::new(request_rx));

    for i in 0..num_threads {
      let rx = Arc::clone(&request_rx);
      let thread_name = format!("http-{}", i);

      thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
          log_thread("STARTED");

          loop {
            let request = {
              let rx = rx.lock().unwrap();
              rx.recv()
            };

            match request {
              Ok(req) => {
                let delay_ms = rand::rng().random_range(10..50);
                log_thread(&format!(
                  "FETCH worker={:03} step={} ({}ms)",
                  req.worker_id, req.step, delay_ms
                ));

                thread::sleep(Duration::from_millis(delay_ms));

                let result = FetchResult {
                  data: format!(
                    "{{\"worker\":{},\"step\":{},\"value\":{}}}",
                    req.worker_id,
                    req.step,
                    req.worker_id * 100 + req.step
                  ),
                  duration_ms: delay_ms,
                  thread_name: thread_name.clone(),
                };

                let _ = req.response_tx.send(result);
              }
              Err(_) => {
                log_thread("SHUTDOWN");
                break;
              }
            }
          }
        })
        .expect("Failed to spawn HTTP thread");
    }

    Self { request_tx }
  }

  fn fetch(&self, worker_id: usize, step: usize, url: String) -> oneshot::Receiver<FetchResult> {
    let (response_tx, response_rx) = oneshot::channel();
    let _ = self.request_tx.send(HttpRequest {
      worker_id,
      step,
      url,
      response_tx,
    });
    response_rx
  }
}

// ============================================================================
// V8 Isolate Pool
// ============================================================================

struct PooledIsolate {
  id: usize,
  isolate: v8::UnenteredIsolate,
}

struct IsolatePool {
  isolates: Mutex<Vec<PooledIsolate>>,
}

impl IsolatePool {
  fn new(num_isolates: usize) -> Self {
    let isolates = (0..num_isolates)
      .map(|i| {
        let isolate = v8::Isolate::new_unentered(v8::CreateParams::default());
        log_thread(&format!("Created isolate #{}", i));
        PooledIsolate { id: i, isolate }
      })
      .collect();
    Self {
      isolates: Mutex::new(isolates),
    }
  }

  fn acquire(&self) -> Option<PooledIsolate> {
    self.isolates.lock().unwrap().pop()
  }

  fn release(&self, isolate: PooledIsolate) {
    self.isolates.lock().unwrap().push(isolate);
  }
}

// ============================================================================
// V8 Thread Pool - TRUE PARALLEL JS EXECUTION
// ============================================================================

/// Request to execute JS on a V8 thread
struct V8Request {
  worker_id: usize,
  step: usize,
  total_steps: usize,
  fetch_result: Option<FetchResult>,
  response_tx: oneshot::Sender<V8Result>,
}

/// Result from V8 execution
struct V8Result {
  worker_id: usize,
  step: usize,
  js_result: String,
  thread_name: String,
  isolate_id: usize,
}

/// Pool of V8 threads that share isolates from IsolatePool
struct V8ThreadPool {
  request_tx: mpsc::Sender<V8Request>,
}

impl V8ThreadPool {
  fn new(num_threads: usize, isolate_pool: Arc<IsolatePool>) -> Self {
    let (request_tx, request_rx) = mpsc::channel::<V8Request>();
    let request_rx = Arc::new(Mutex::new(request_rx));

    for i in 0..num_threads {
      let rx = Arc::clone(&request_rx);
      let pool = Arc::clone(&isolate_pool);
      let thread_name = format!("{}", i);

      thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
          log_thread("STARTED");

          loop {
            let request = {
              let rx = rx.lock().unwrap();
              rx.recv()
            };

            match request {
              Ok(req) => {
                // Acquire isolate from pool
                let mut pooled = loop {
                  if let Some(iso) = pool.acquire() {
                    break iso;
                  }
                  // Spin wait (in real code, use condvar)
                  thread::sleep(Duration::from_micros(100));
                };

                let isolate_id = pooled.id;

                log_v8(req.worker_id, &thread_name, isolate_id, &format!(
                  "EXEC step {}/{}",
                  req.step + 1,
                  req.total_steps
                ));

                // Acquire lock - Locker handles enter/exit automatically
                let mut locker = v8::Locker::new(&mut pooled.isolate);

                // Execute JS - access Isolate through Locker's Deref
                let js_result = {
                  let scope = std::pin::pin!(v8::HandleScope::new(&mut *locker));
                  let scope = &mut scope.init();
                  let context = v8::Context::new(scope, Default::default());
                  let scope = &mut v8::ContextScope::new(scope, context);

                  let code = if let Some(ref fetch) = req.fetch_result {
                    format!(
                      r#"
                      const workerId = {};
                      const step = {};
                      const fetchData = {};
                      const v8Thread = "{}";
                      const isolateId = {};
                      const httpThread = "{}";
                      const fetchTime = {};
                      `[Worker ${{workerId}}] Step ${{step}}: got ${{JSON.stringify(fetchData)}} (thread: ${{v8Thread}}, isolate: ${{isolateId}}, http: ${{httpThread}}, ${{fetchTime}}ms)`
                      "#,
                      req.worker_id,
                      req.step,
                      fetch.data,
                      thread_name,
                      isolate_id,
                      fetch.thread_name,
                      fetch.duration_ms,
                    )
                  } else {
                    format!(
                      r#"
                      const workerId = {};
                      const step = {};
                      const v8Thread = "{}";
                      const isolateId = {};
                      `[Worker ${{workerId}}] Step ${{step}}: initial setup (thread: ${{v8Thread}}, isolate: ${{isolateId}})`
                      "#,
                      req.worker_id, req.step, thread_name, isolate_id,
                    )
                  };

                  let code = v8::String::new(scope, &code).unwrap();
                  let script = v8::Script::compile(scope, code, None).unwrap();
                  let result = script.run(scope).unwrap();
                  result.to_rust_string_lossy(scope)
                };

                // Locker drops automatically, releasing lock
                drop(locker);

                log_v8(req.worker_id, &thread_name, isolate_id, &format!("DONE: {}", js_result.chars().take(50).collect::<String>()));

                // Return isolate to pool
                pool.release(pooled);

                let _ = req.response_tx.send(V8Result {
                  worker_id: req.worker_id,
                  step: req.step,
                  js_result,
                  thread_name: thread_name.clone(),
                  isolate_id,
                });
              }
              Err(_) => {
                log_thread("SHUTDOWN");
                break;
              }
            }
          }
        })
        .expect("Failed to spawn V8 thread");
    }

    Self { request_tx }
  }

  fn execute(
    &self,
    worker_id: usize,
    step: usize,
    total_steps: usize,
    fetch_result: Option<FetchResult>,
  ) -> oneshot::Receiver<V8Result> {
    let (response_tx, response_rx) = oneshot::channel();
    let _ = self.request_tx.send(V8Request {
      worker_id,
      step,
      total_steps,
      fetch_result,
      response_tx,
    });
    response_rx
  }
}

// ============================================================================
// Worker Future
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
enum WorkerState {
  Initial,
  WaitingV8,
  WaitingFetch,
  Done,
}

struct WorkerTask {
  worker_id: usize,
  step: usize,
  total_steps: usize,
  last_fetch_result: Option<FetchResult>,
}

struct PooledExecution {
  v8_pool: Arc<V8ThreadPool>,
  http_pool: Arc<HttpClientPool>,
  task: WorkerTask,
  state: WorkerState,
  pending_v8: Option<oneshot::Receiver<V8Result>>,
  pending_fetch: Option<oneshot::Receiver<FetchResult>>,
  last_js_result: String,
}

impl PooledExecution {
  fn new(
    v8_pool: Arc<V8ThreadPool>,
    http_pool: Arc<HttpClientPool>,
    worker_id: usize,
    total_steps: usize,
  ) -> Self {
    Self {
      v8_pool,
      http_pool,
      task: WorkerTask {
        worker_id,
        step: 0,
        total_steps,
        last_fetch_result: None,
      },
      state: WorkerState::Initial,
      pending_v8: None,
      pending_fetch: None,
      last_js_result: String::new(),
    }
  }
}

impl Future for PooledExecution {
  type Output = (usize, String);

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.as_mut().get_mut();

    loop {
      match this.state {
        WorkerState::Initial => {
          log(this.task.worker_id, "Submitting JS to V8 pool...");

          // Submit to V8 thread pool
          let rx = this.v8_pool.execute(
            this.task.worker_id,
            this.task.step,
            this.task.total_steps,
            this.task.last_fetch_result.take(),
          );
          this.pending_v8 = Some(rx);
          this.state = WorkerState::WaitingV8;
        }

        WorkerState::WaitingV8 => {
          if let Some(ref mut rx) = this.pending_v8 {
            match Pin::new(rx).poll(cx) {
              Poll::Pending => {
                return Poll::Pending;
              }
              Poll::Ready(result) => {
                let v8_result = result.expect("V8 pool dropped");
                this.last_js_result = v8_result.js_result;
                this.pending_v8 = None;
                this.task.step += 1;

                if this.task.step >= this.task.total_steps {
                  log(this.task.worker_id, "ALL STEPS COMPLETE!");
                  this.state = WorkerState::Done;
                  return Poll::Ready((this.task.worker_id, this.last_js_result.clone()));
                } else {
                  // Start fetch
                  log(this.task.worker_id, &format!("Starting fetch for step {}...", this.task.step));
                  let url = format!(
                    "https://api.mock/worker/{}/step/{}",
                    this.task.worker_id, this.task.step
                  );
                  let rx = this.http_pool.fetch(this.task.worker_id, this.task.step, url);
                  this.pending_fetch = Some(rx);
                  this.state = WorkerState::WaitingFetch;
                }
              }
            }
          }
        }

        WorkerState::WaitingFetch => {
          if let Some(ref mut rx) = this.pending_fetch {
            match Pin::new(rx).poll(cx) {
              Poll::Pending => {
                return Poll::Pending;
              }
              Poll::Ready(result) => {
                let fetch_result = result.expect("HTTP pool dropped");
                log(
                  this.task.worker_id,
                  &format!("Fetch done from {}, submitting next JS...", fetch_result.thread_name),
                );
                this.task.last_fetch_result = Some(fetch_result);
                this.pending_fetch = None;

                // Submit next JS execution
                let rx = this.v8_pool.execute(
                  this.task.worker_id,
                  this.task.step,
                  this.task.total_steps,
                  this.task.last_fetch_result.take(),
                );
                this.pending_v8 = Some(rx);
                this.state = WorkerState::WaitingV8;
              }
            }
          }
        }

        WorkerState::Done => {
          panic!("Polling completed future");
        }
      }
    }
  }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() {
  let args = Args::parse();

  // Initialize V8
  let platform = v8::new_default_platform(0, false).make_shared();
  v8::V8::initialize_platform(platform);
  v8::V8::initialize();

  println!("=== OpenWorkers - Multi-threaded V8 Demo ===\n");
  println!("Configuration:");
  println!("  - V8 Thread Pool: {} threads", args.v8_threads);
  println!("  - V8 Isolate Pool: {} isolates (shared across threads)", args.isolates);
  println!("  - HTTP Thread Pool: {} threads", args.http_threads);
  println!("  - Workers: {} concurrent", args.workers);
  println!("  - Steps per worker: {}", args.steps);
  println!();

  // Create isolate pool first
  let isolate_pool = Arc::new(IsolatePool::new(args.isolates));

  // Create thread pools
  let v8_pool = Arc::new(V8ThreadPool::new(args.v8_threads, Arc::clone(&isolate_pool)));
  let http_pool = Arc::new(HttpClientPool::new(args.http_threads));

  println!("--- Starting execution ---\n");
  let start = std::time::Instant::now();

  // Create workers
  let mut futures: FuturesUnordered<PooledExecution> = FuturesUnordered::new();

  for worker_id in 0..args.workers {
    let execution = PooledExecution::new(
      Arc::clone(&v8_pool),
      Arc::clone(&http_pool),
      worker_id,
      args.steps,
    );
    futures.push(execution);
  }

  // Run
  let mut completed = 0;
  let num_workers = args.workers;
  let run_all = async {
    while let Some((worker_id, _result)) = futures.next().await {
      completed += 1;
      println!(
        "\n[{}] >>> WORKER {:03} FINISHED ({}/{}) <<<\n",
        now(),
        worker_id,
        completed,
        num_workers,
      );
    }
  };

  tokio::select! {
    _ = run_all => {
      let elapsed = start.elapsed();
      println!("\n=== All {} workers completed in {:.2}s ===", num_workers, elapsed.as_secs_f64());
      println!("Throughput: {:.1} workers/sec", num_workers as f64 / elapsed.as_secs_f64());
    }
    _ = tokio::time::sleep(Duration::from_secs(args.timeout)) => {
      println!("\n=== TIMEOUT after {}s! {}/{} completed ===", args.timeout, completed, num_workers);
    }
  }
}
