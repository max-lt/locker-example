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
use std::time::Duration;
use tokio::sync::oneshot;

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
          println!("[{}] HTTP client thread started", thread_name);

          loop {
            // Get next request (blocking)
            let request = {
              let rx = rx.lock().unwrap();
              rx.recv()
            };

            match request {
              Ok(req) => {
                // Simulate HTTP fetch with random latency
                let delay_ms = rand::rng().random_range(30..200);
                println!(
                  "[{}] Fetching {} for Worker {} ({}ms)",
                  thread_name, req.url, req.worker_id, delay_ms
                );

                // Blocking sleep (we're on a dedicated thread)
                thread::sleep(Duration::from_millis(delay_ms));

                let result = FetchResult {
                  url: req.url,
                  data: format!(
                    "{{\"worker\":{},\"step\":{},\"value\":{}}}",
                    req.worker_id,
                    req.step,
                    req.worker_id * 100 + req.step
                  ),
                  duration_ms: delay_ms,
                  thread_name: thread_name.clone(),
                };

                println!(
                  "[{}] Completed fetch for Worker {} in {}ms",
                  thread_name, req.worker_id, delay_ms
                );

                let _ = req.response_tx.send(result);
              }
              Err(_) => {
                println!("[{}] HTTP client thread shutting down", thread_name);
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
          if let Some(isolate) = this.pool.acquire() {
            this.isolate = Some(isolate);
            this.enter_and_lock();
            println!(
              "[Worker {}] Acquired isolate, starting execution",
              this.task.worker_id
            );
          } else {
            // No isolate available, wake later
            cx.waker().wake_by_ref();
            return Poll::Pending;
          }
        }

        ExecutionState::Suspended => {
          // Check if our fetch is ready
          if let Some(ref mut fetch_rx) = this.task.pending_fetch {
            match Pin::new(fetch_rx).poll(cx) {
              Poll::Pending => {
                // Still waiting for HTTP response
                return Poll::Pending;
              }
              Poll::Ready(result) => {
                // HTTP response received!
                let fetch_result = result.expect("HTTP pool dropped");
                println!(
                  "[Worker {}] Fetch complete from {} in {}ms",
                  this.task.worker_id, fetch_result.thread_name, fetch_result.duration_ms
                );
                this.task.last_fetch_result = Some(fetch_result);
                this.task.pending_fetch = None;

                // Need to re-acquire an isolate from pool
                if let Some(isolate) = this.pool.acquire() {
                  this.isolate = Some(isolate);
                  this.enter_and_lock();
                  println!(
                    "[Worker {}] Re-acquired isolate, resuming JS",
                    this.task.worker_id
                  );
                } else {
                  println!(
                    "[Worker {}] Fetch done but no isolate available, waiting...",
                    this.task.worker_id
                  );
                  cx.waker().wake_by_ref();
                  return Poll::Pending;
                }
              }
            }
          } else {
            println!(
              "[Worker {}] Suspended but no pending fetch?!",
              this.task.worker_id
            );
          }
        }

        ExecutionState::Locked => {
          // Execute JS step
          let result = this.execute_js_step();
          println!("{}", result);
          this.task.step += 1;

          if this.task.step >= this.task.total_steps {
            // All done!
            this.unlock_and_exit();
            let isolate = this.isolate.take().unwrap();
            this.pool.release(isolate);
            this.state = ExecutionState::Done;
            println!(
              "[Worker {}] Completed all steps, released isolate\n",
              this.task.worker_id
            );
            return Poll::Ready((this.task.worker_id, result));
          } else {
            // Need to do a fetch - unlock isolate AND RETURN TO POOL!
            let url = format!(
              "https://api.example.com/worker/{}/step/{}",
              this.task.worker_id, this.task.step
            );
            println!(
              "[Worker {}] Starting fetch({}), returning isolate to pool...",
              this.task.worker_id, url
            );
            this.unlock_and_exit();

            // Return isolate to pool so others can use it!
            let isolate = this.isolate.take().unwrap();
            this.pool.release(isolate);

            // Submit fetch request to HTTP thread pool
            let mut fetch_rx = this.http_pool.fetch(this.task.worker_id, this.task.step, url);

            // Poll once to register waker with the oneshot channel
            match Pin::new(&mut fetch_rx).poll(cx) {
              Poll::Ready(result) => {
                // Already complete (unlikely but handle it)
                let fetch_result = result.expect("HTTP pool dropped");
                this.task.last_fetch_result = Some(fetch_result);
                // Re-acquire isolate immediately
                if let Some(isolate) = this.pool.acquire() {
                  this.isolate = Some(isolate);
                  this.enter_and_lock();
                  continue; // Go back to Locked state
                }
              }
              Poll::Pending => {
                // Normal case - waiting for HTTP response
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

  println!("=== Async V8 Isolate Pool + HTTP Client Pool Demo ===\n");
  println!("Architecture:");
  println!("  - V8 Isolate Pool: 2 isolates");
  println!("  - HTTP Client Pool: 3 threads (like reqwest thread-local clients)");
  println!("  - Workers: 4 workers, each doing 3 JS steps with fetch() between them");
  println!();
  println!("When a worker calls fetch():");
  println!("  1. Worker releases isolate back to pool");
  println!("  2. HTTP request runs on dedicated HTTP thread");
  println!("  3. When response arrives, worker re-acquires an isolate");
  println!("  4. Worker continues JS execution with fetch result\n");

  // Create V8 isolate pool (2 isolates)
  let pool = Arc::new(IsolatePool::new(2));

  // Create HTTP client pool (3 threads)
  let http_pool = Arc::new(HttpClientPool::new(3));

  println!("--- Starting execution ---\n");

  // Create 4 workers, each doing 3 steps
  let mut futures: FuturesUnordered<PooledExecution> = FuturesUnordered::new();

  for worker_id in 0..4 {
    let task = WorkerTask::new(worker_id, 3);
    let execution = PooledExecution::new(Arc::clone(&pool), Arc::clone(&http_pool), task);
    futures.push(execution);
  }

  // Run with 10s timeout
  let run_all = async {
    while let Some((worker_id, result)) = futures.next().await {
      println!(">>> Worker {} finished with: {}\n", worker_id, result);
    }
  };

  tokio::select! {
    _ = run_all => {
      println!("=== All workers completed! ===");
    }
    _ = tokio::time::sleep(Duration::from_secs(10)) => {
      println!("=== TIMEOUT after 10s! ===");
    }
  }
}
