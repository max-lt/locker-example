//! PoC: Async V8 Isolate Pool with Custom Poll
//!
//! Demonstrates suspending/resuming V8 isolates on async boundaries.
//! When a worker hits I/O (Poll::Pending), we unlock the isolate so
//! other workers can use the thread.
//!
//! Architecture:
//! - V8 Isolate Pool: 2 isolates shared by 4 workers
//! - HTTP Client Pool: Dedicated thread pool (like reqwest thread-local clients)
//! - Workers suspend on fetch(), releasing isolate while HTTP runs on another thread

use futures::stream::{FuturesUnordered, StreamExt};
use rand::Rng;
use std::future::Future;
use std::pin::Pin;
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot;

/// Get ISO timestamp
fn now() -> String {
  let duration = SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap();
  let secs = duration.as_secs();
  let millis = duration.subsec_millis();
  // Simple HH:MM:SS.mmm format
  let hours = (secs / 3600) % 24;
  let mins = (secs / 60) % 60;
  let secs = secs % 60;
  format!("{:02}:{:02}:{:02}.{:03}", hours, mins, secs, millis)
}

/// Log with timestamp, thread name, and padded worker ID
fn log(worker_id: usize, msg: &str) {
  let thread = thread::current();
  let thread_name = thread.name().unwrap_or("???");
  println!("[{}] [{}] [Worker {:03}] {}", now(), thread_name, worker_id, msg);
}

/// Log for HTTP thread
fn log_http(thread_name: &str, msg: &str) {
  println!("[{}] [{}] {}", now(), thread_name, msg);
}

// ============================================================================
// HTTP Client Thread Pool (simulates reqwest with thread-local clients)
// ============================================================================

/// Request to the HTTP pool
struct HttpRequest {
  worker_id: usize,
  step: usize,
  url: String,
  response_tx: oneshot::Sender<FetchResult>,
}

/// Result from a "fetch" operation
#[derive(Debug)]
struct FetchResult {
  url: String,
  data: String,
  duration_ms: u64,
  thread_name: String,
}

/// HTTP Client Pool - runs fetch operations on dedicated threads
struct HttpClientPool {
  request_tx: mpsc::Sender<HttpRequest>,
}

impl HttpClientPool {
  fn new(num_threads: usize) -> Self {
    let (request_tx, request_rx) = mpsc::channel::<HttpRequest>();
    let request_rx = Arc::new(Mutex::new(request_rx));

    // Spawn HTTP worker threads
    for i in 0..num_threads {
      let rx = Arc::clone(&request_rx);
      let thread_name = format!("http-{}", i);

      thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
          // Thread-local "HTTP client" (simulated)
          log_http(&thread_name, "HTTP client thread STARTED");

          loop {
            // Get next request (blocking)
            let request = {
              let rx = rx.lock().unwrap();
              rx.recv()
            };

            match request {
              Ok(req) => {
                // Simulate HTTP fetch with random latency (10-50ms for stress test)
                let delay_ms = rand::rng().random_range(10..50);

                log_http(
                  &thread_name,
                  &format!(
                    "FETCH START worker={:03} step={} url={} ({}ms)",
                    req.worker_id, req.step, req.url, delay_ms
                  ),
                );

                // Blocking sleep (we're on a dedicated thread)
                thread::sleep(Duration::from_millis(delay_ms));

                let result = FetchResult {
                  url: req.url.clone(),
                  data: format!(
                    "{{\"worker\":{},\"step\":{},\"value\":{}}}",
                    req.worker_id,
                    req.step,
                    req.worker_id * 100 + req.step
                  ),
                  duration_ms: delay_ms,
                  thread_name: thread_name.clone(),
                };

                log_http(
                  &thread_name,
                  &format!(
                    "FETCH DONE  worker={:03} step={} ({}ms)",
                    req.worker_id, req.step, delay_ms
                  ),
                );

                let _ = req.response_tx.send(result);
              }
              Err(_) => {
                log_http(&thread_name, "HTTP client thread SHUTDOWN");
                break;
              }
            }
          }
        })
        .expect("Failed to spawn HTTP thread");
    }

    Self { request_tx }
  }

  /// Submit a fetch request, returns a receiver for the result
  fn fetch(&self, worker_id: usize, step: usize, url: String) -> oneshot::Receiver<FetchResult> {
    let (response_tx, response_rx) = oneshot::channel();
    let request = HttpRequest {
      worker_id,
      step,
      url,
      response_tx,
    };
    let _ = self.request_tx.send(request);
    response_rx
  }
}

// ============================================================================
// V8 Isolate Pool
// ============================================================================

/// Represents an isolate that can be pooled
struct PooledIsolate {
  isolate: v8::UnenteredIsolate,
}

impl PooledIsolate {
  fn new() -> Self {
    let params = v8::CreateParams::default();
    let isolate = v8::Isolate::new_unentered(params);
    Self { isolate }
  }
}

/// Pool of V8 isolates
struct IsolatePool {
  isolates: Mutex<Vec<PooledIsolate>>,
}

impl IsolatePool {
  fn new(size: usize) -> Self {
    let isolates = (0..size).map(|_| PooledIsolate::new()).collect();
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

/// State of a worker execution
#[derive(Debug, Clone, Copy, PartialEq)]
enum ExecutionState {
  /// Not yet started
  Initial,
  /// Isolate is entered and locked - executing JS
  Locked,
  /// Isolate unlocked, waiting for async I/O
  Suspended,
  /// Execution complete
  Done,
}

/// A worker task that simulates multiple JS operations with async I/O between them
struct WorkerTask {
  worker_id: usize,
  /// Which step we're on (0, 1, 2...)
  step: usize,
  /// Total steps to execute
  total_steps: usize,
  /// Pending fetch (waiting for HTTP pool response)
  pending_fetch: Option<oneshot::Receiver<FetchResult>>,
  /// Last fetch result to use in JS
  last_fetch_result: Option<FetchResult>,
}

impl WorkerTask {
  fn new(worker_id: usize, total_steps: usize) -> Self {
    Self {
      worker_id,
      step: 0,
      total_steps,
      pending_fetch: None,
      last_fetch_result: None,
    }
  }
}

/// Custom Future that handles isolate lock/unlock on poll boundaries
struct PooledExecution {
  pool: Arc<IsolatePool>,
  http_pool: Arc<HttpClientPool>,
  isolate: Option<PooledIsolate>,
  task: WorkerTask,
  state: ExecutionState,
  locker: Option<LockerGuard>,
}

/// RAII guard for Locker (we need to store it across polls)
struct LockerGuard {
  // We store raw pointers because Locker doesn't implement Unpin
  // and we need to manage lifetime manually
  _locker: v8::Locker,
}

impl PooledExecution {
  fn new(pool: Arc<IsolatePool>, http_pool: Arc<HttpClientPool>, task: WorkerTask) -> Self {
    Self {
      pool,
      http_pool,
      isolate: None,
      task,
      state: ExecutionState::Initial,
      locker: None,
    }
  }

  /// Enter and lock the isolate
  fn enter_and_lock(&mut self) {
    let isolate = self.isolate.as_mut().unwrap();
    unsafe {
      (&isolate.isolate as &v8::Isolate).enter();
    }
    let locker = v8::Locker::new(&isolate.isolate);
    self.locker = Some(LockerGuard { _locker: locker });
    self.state = ExecutionState::Locked;
  }

  /// Unlock and exit the isolate
  fn unlock_and_exit(&mut self) {
    // Drop locker first
    self.locker = None;
    // Then exit
    if let Some(isolate) = self.isolate.as_ref() {
      unsafe {
        (&isolate.isolate as &v8::Isolate).exit();
      }
    }
    self.state = ExecutionState::Suspended;
  }

  /// Execute one step of JS, using fetch result if available
  fn execute_js_step(&mut self) -> String {
    let isolate = self.isolate.as_mut().unwrap();
    let step = self.task.step;
    let worker_id = self.task.worker_id;

    // Use pin! macro for HandleScope
    let scope = std::pin::pin!(v8::HandleScope::new(&mut isolate.isolate));
    let scope = &mut scope.init();
    let context = v8::Context::new(scope, Default::default());
    let scope = &mut v8::ContextScope::new(scope, context);

    // Build JS code that uses fetch result if available
    let code = if let Some(ref fetch_result) = self.task.last_fetch_result {
      format!(
        r#"
        const workerId = {worker_id};
        const step = {step};
        const fetchData = {data};
        const httpThread = "{thread}";
        const fetchTime = {duration};
        `[Worker ${{workerId}}] Step ${{step}}: got ${{JSON.stringify(fetchData)}} from ${{httpThread}} in ${{fetchTime}}ms`
        "#,
        worker_id = worker_id,
        step = step,
        data = fetch_result.data,
        thread = fetch_result.thread_name,
        duration = fetch_result.duration_ms,
      )
    } else {
      format!(
        r#"
        const workerId = {worker_id};
        const step = {step};
        `[Worker ${{workerId}}] Step ${{step}}: initial setup`
        "#,
        worker_id = worker_id,
        step = step,
      )
    };

    let code = v8::String::new(scope, &code).unwrap();
    let script = v8::Script::compile(scope, code, None).unwrap();
    let result = script.run(scope).unwrap();
    result.to_rust_string_lossy(scope)
  }
}

impl Future for PooledExecution {
  type Output = (usize, String); // (worker_id, final_result)

  fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.as_mut().get_mut();

    loop {
      match this.state {
        ExecutionState::Initial => {
          // Acquire isolate from pool
          log(this.task.worker_id, "STATE=Initial, trying to acquire isolate...");
          if let Some(isolate) = this.pool.acquire() {
            this.isolate = Some(isolate);
            this.enter_and_lock();
            log(this.task.worker_id, "ISOLATE ACQUIRED, entering + locking");
          } else {
            // No isolate available, wake later
            log(this.task.worker_id, "NO ISOLATE AVAILABLE, waiting...");
            cx.waker().wake_by_ref();
            return Poll::Pending;
          }
        }

        ExecutionState::Suspended => {
          // Check if our fetch is ready
          if let Some(ref mut fetch_rx) = this.task.pending_fetch {
            log(this.task.worker_id, "STATE=Suspended, polling fetch result...");
            match Pin::new(fetch_rx).poll(cx) {
              Poll::Pending => {
                // Still waiting for HTTP response
                log(this.task.worker_id, "FETCH PENDING, still waiting for HTTP response");
                return Poll::Pending;
              }
              Poll::Ready(result) => {
                // HTTP response received!
                let fetch_result = result.expect("HTTP pool dropped");
                log(
                  this.task.worker_id,
                  &format!(
                    "FETCH RECEIVED from {} in {}ms, data={}",
                    fetch_result.thread_name, fetch_result.duration_ms, fetch_result.data
                  ),
                );
                this.task.last_fetch_result = Some(fetch_result);
                this.task.pending_fetch = None;

                // Need to re-acquire an isolate from pool
                log(this.task.worker_id, "Trying to re-acquire isolate...");
                if let Some(isolate) = this.pool.acquire() {
                  this.isolate = Some(isolate);
                  this.enter_and_lock();
                  log(this.task.worker_id, "ISOLATE RE-ACQUIRED, resuming JS execution");
                } else {
                  // No isolate available yet, try again later
                  log(this.task.worker_id, "NO ISOLATE AVAILABLE after fetch, waiting...");
                  cx.waker().wake_by_ref();
                  return Poll::Pending;
                }
              }
            }
          }
        }

        ExecutionState::Locked => {
          // Execute JS step
          log(
            this.task.worker_id,
            &format!(
              "STATE=Locked, executing JS step {}/{}",
              this.task.step + 1,
              this.task.total_steps
            ),
          );
          let result = this.execute_js_step();
          log(
            this.task.worker_id,
            &format!("JS EXECUTED: {}", result.chars().take(80).collect::<String>()),
          );
          this.task.step += 1;

          if this.task.step >= this.task.total_steps {
            // All done!
            log(
              this.task.worker_id,
              "ALL STEPS COMPLETE, unlocking + exiting isolate",
            );
            this.unlock_and_exit();
            let isolate = this.isolate.take().unwrap();
            this.pool.release(isolate);
            log(this.task.worker_id, "ISOLATE RELEASED back to pool, DONE!");
            this.state = ExecutionState::Done;
            return Poll::Ready((this.task.worker_id, result));
          } else {
            // Need to do a fetch - unlock isolate AND RETURN TO POOL!
            let url = format!(
              "https://api.mock/worker/{}/step/{}",
              this.task.worker_id, this.task.step
            );
            log(
              this.task.worker_id,
              &format!("STARTING FETCH for step {}, unlocking isolate...", this.task.step),
            );
            this.unlock_and_exit();

            // Return isolate to pool so others can use it!
            let isolate = this.isolate.take().unwrap();
            this.pool.release(isolate);
            log(
              this.task.worker_id,
              "ISOLATE RETURNED to pool (available for other workers)",
            );

            // Submit fetch request to HTTP thread pool
            log(
              this.task.worker_id,
              &format!("SUBMITTING FETCH to HTTP pool: {}", url),
            );
            let mut fetch_rx = this.http_pool.fetch(this.task.worker_id, this.task.step, url);

            // Poll once to register waker with the oneshot channel
            match Pin::new(&mut fetch_rx).poll(cx) {
              Poll::Ready(result) => {
                // Already complete (unlikely but handle it)
                log(this.task.worker_id, "FETCH ALREADY COMPLETE (fast path)");
                let fetch_result = result.expect("HTTP pool dropped");
                this.task.last_fetch_result = Some(fetch_result);
                // Re-acquire isolate immediately
                if let Some(isolate) = this.pool.acquire() {
                  this.isolate = Some(isolate);
                  this.enter_and_lock();
                  log(this.task.worker_id, "ISOLATE RE-ACQUIRED immediately");
                  continue; // Go back to Locked state
                }
              }
              Poll::Pending => {
                // Normal case - waiting for HTTP response
                log(this.task.worker_id, "FETCH PENDING, suspending worker...");
              }
            }

            this.task.pending_fetch = Some(fetch_rx);
            this.state = ExecutionState::Suspended;

            // Return Pending - isolate is FREE for other workers!
            return Poll::Pending;
          }
        }

        ExecutionState::Done => {
          panic!("Polling completed future");
        }
      }
    }
  }
}

#[tokio::main]
async fn main() {
  // Initialize V8
  let platform = v8::new_default_platform(0, false).make_shared();
  v8::V8::initialize_platform(platform);
  v8::V8::initialize();

  const NUM_HTTP_THREADS: usize = 8;
  const STEPS_PER_WORKER: usize = 3;

  // Parse command line args: [workers] [isolates]
  let num_workers: usize = std::env::args()
    .nth(1)
    .and_then(|s| s.parse().ok())
    .unwrap_or(20);

  let num_isolates: usize = std::env::args()
    .nth(2)
    .and_then(|s| s.parse().ok())
    .unwrap_or(2);

  println!("=== OpenWorkers Stress Test ===\n");
  println!("Configuration:");
  println!("  - V8 Isolate Pool: {} isolates", num_isolates);
  println!("  - HTTP Client Pool: {} threads", NUM_HTTP_THREADS);
  println!("  - Workers: {} concurrent workers", num_workers);
  println!("  - Steps per worker: {} (each step = JS exec + fetch)", STEPS_PER_WORKER);
  println!("  - Total operations: {} JS executions + {} fetches",
    num_workers * STEPS_PER_WORKER,
    num_workers * (STEPS_PER_WORKER - 1));
  println!();

  // Create V8 isolate pool
  let pool = Arc::new(IsolatePool::new(num_isolates));

  // Create HTTP client pool
  let http_pool = Arc::new(HttpClientPool::new(NUM_HTTP_THREADS));

  println!("--- Starting execution ---\n");
  let start = std::time::Instant::now();

  // Create workers
  let mut futures: FuturesUnordered<PooledExecution> = FuturesUnordered::new();

  for worker_id in 0..num_workers {
    println!("[{}] Creating Worker {:03} ({} steps)", now(), worker_id, STEPS_PER_WORKER);
    let task = WorkerTask::new(worker_id, STEPS_PER_WORKER);
    let execution = PooledExecution::new(Arc::clone(&pool), Arc::clone(&http_pool), task);
    futures.push(execution);
  }

  println!("\n[{}] All {} workers queued, starting poll loop...\n", now(), num_workers);

  // Run with 30s timeout
  let mut completed = 0;
  let run_all = async {
    while let Some((worker_id, result)) = futures.next().await {
      completed += 1;
      println!(
        "\n[{}] >>> WORKER {:03} FINISHED ({}/{}) - result: {}\n",
        now(),
        worker_id,
        completed,
        num_workers,
        result.chars().take(60).collect::<String>()
      );
    }
  };

  tokio::select! {
    _ = run_all => {
      let elapsed = start.elapsed();
      println!("\n=== All {} workers completed in {:.2}s ===", num_workers, elapsed.as_secs_f64());
      println!("Throughput: {:.1} workers/sec", num_workers as f64 / elapsed.as_secs_f64());
    }
    _ = tokio::time::sleep(Duration::from_secs(30)) => {
      println!("\n=== TIMEOUT after 30s! {}/{} completed ===", completed, num_workers);
    }
  }
}
