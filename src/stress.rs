//! V8 Locker stress test
//!
//! Multi-threaded stress test with configurable workers and isolates.

use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

fn now() -> String {
    let d = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let (h, m, s, ms) = (
        (d.as_secs() / 3600) % 24,
        (d.as_secs() / 60) % 60,
        d.as_secs() % 60,
        d.subsec_millis(),
    );
    format!("{:02}:{:02}:{:02}.{:03}", h, m, s, ms)
}

struct PooledIsolate {
    id: usize,
    isolate: v8::UnenteredIsolate,
}

struct IsolatePool {
    isolates: Mutex<Vec<PooledIsolate>>,
}

impl IsolatePool {
    fn new(num: usize) -> Self {
        let isolates = (0..num)
            .map(|i| PooledIsolate {
                id: i,
                isolate: v8::Isolate::new_unentered(Default::default()),
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

struct WorkRequest {
    worker_id: usize,
    step: usize,
    total_steps: usize,
    response_tx: mpsc::Sender<WorkResult>,
}

struct WorkResult {
    worker_id: usize,
    step: usize,
    isolate_id: usize,
    thread_name: String,
    result: String,
}

fn main() {
    let num_workers: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);
    let num_isolates: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let num_threads: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2);
    let steps_per_worker: usize = std::env::args()
        .nth(4)
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    let platform = v8::new_default_platform(0, false).make_shared();
    v8::V8::initialize_platform(platform);
    v8::V8::initialize();

    println!("=== V8 Locker Stress Test ===\n");
    println!(
        "Config: {} workers, {} isolates, {} threads, {} steps/worker",
        num_workers, num_isolates, num_threads, steps_per_worker
    );
    println!("Total: {} JS executions\n", num_workers * steps_per_worker);

    let pool = Arc::new(IsolatePool::new(num_isolates));
    let (work_tx, work_rx) = mpsc::channel::<WorkRequest>();
    let work_rx = Arc::new(Mutex::new(work_rx));

    // Spawn V8 worker threads
    for t in 0..num_threads {
        let rx = Arc::clone(&work_rx);
        let pool = Arc::clone(&pool);
        let name = format!("v8-{}", t);

        thread::Builder::new()
            .name(name.clone())
            .spawn(move || {
                loop {
                    let req = { rx.lock().unwrap().recv() };
                    match req {
                        Ok(req) => {
                            let mut pooled = loop {
                                if let Some(p) = pool.acquire() {
                                    break p;
                                }
                                thread::sleep(Duration::from_micros(100));
                            };

                            let mut locker = v8::Locker::new(&mut pooled.isolate);
                            let result = {
                                let scope = std::pin::pin!(v8::HandleScope::new(&mut *locker));
                                let scope = &mut scope.init();
                                let context = v8::Context::new(scope, Default::default());
                                let scope = &mut v8::ContextScope::new(scope, context);

                                let code = format!(
                                    "`w${{{}}} s${{{}}} = ${{{} * 100 + {}}}`",
                                    req.worker_id, req.step, req.worker_id, req.step
                                );
                                let code = v8::String::new(scope, &code).unwrap();
                                let script = v8::Script::compile(scope, code, None).unwrap();
                                script.run(scope).unwrap().to_rust_string_lossy(scope)
                            };

                            let iso_id = pooled.id;
                            drop(locker);
                            pool.release(pooled);

                            let _ = req.response_tx.send(WorkResult {
                                worker_id: req.worker_id,
                                step: req.step,
                                isolate_id: iso_id,
                                thread_name: name.clone(),
                                result,
                            });
                        }
                        Err(_) => break,
                    }
                }
            })
            .unwrap();
    }

    let start = Instant::now();
    let (result_tx, result_rx) = mpsc::channel();

    // Submit all work
    for w in 0..num_workers {
        for s in 0..steps_per_worker {
            let _ = work_tx.send(WorkRequest {
                worker_id: w,
                step: s,
                total_steps: steps_per_worker,
                response_tx: result_tx.clone(),
            });
        }
    }
    drop(result_tx);

    // Collect results
    let mut count = 0;
    let total = num_workers * steps_per_worker;
    while let Ok(r) = result_rx.recv() {
        count += 1;
        if count % 10 == 0 || count == total {
            println!(
                "[{}] {}/{} - {} (t={} i={})",
                now(),
                count,
                total,
                r.result,
                r.thread_name,
                r.isolate_id
            );
        }
    }

    let elapsed = start.elapsed();
    println!(
        "\n=== Completed {} ops in {:.2}s ({:.0} ops/sec) ===",
        total,
        elapsed.as_secs_f64(),
        total as f64 / elapsed.as_secs_f64()
    );
}
