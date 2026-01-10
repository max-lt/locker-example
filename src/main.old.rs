//! PoC: Async V8 Isolate Pool with Custom Poll (Simple version)
//!
//! Demonstrates suspending/resuming V8 isolates on async boundaries.
//! When a worker hits I/O (Poll::Pending), we unlock the isolate so
//! other workers can use the thread.
//!
//! This is the simpler version using tokio::time::sleep to simulate I/O.

use futures::stream::{FuturesUnordered, StreamExt};
use rand::Rng;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

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
  /// Pending I/O future (simulated with tokio::time::Sleep)
  pending_io: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl WorkerTask {
  fn new(worker_id: usize, total_steps: usize) -> Self {
    Self {
      worker_id,
      step: 0,
      total_steps,
      pending_io: None,
    }
  }
}

/// Custom Future that handles isolate lock/unlock on poll boundaries
struct PooledExecution {
  pool: Arc<IsolatePool>,
  isolate: Option<PooledIsolate>,
  task: WorkerTask,
  state: ExecutionState,
  locker: Option<LockerGuard>,
}

/// RAII guard for Locker (we need to store it across polls)
struct LockerGuard {
  _locker: v8::Locker,
}

impl PooledExecution {
  fn new(pool: Arc<IsolatePool>, task: WorkerTask) -> Self {
    Self {
      pool,
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

  /// Execute one step of JS
  fn execute_js_step(&mut self) -> String {
    let isolate = self.isolate.as_mut().unwrap();
    let step = self.task.step;
    let worker_id = self.task.worker_id;

    // Use pin! macro for HandleScope
    let scope = std::pin::pin!(v8::HandleScope::new(&mut isolate.isolate));
    let scope = &mut scope.init();
    let context = v8::Context::new(scope, Default::default());
    let scope = &mut v8::ContextScope::new(scope, context);

    let code = format!(
      r#"
      const workerId = {worker_id};
      const step = {step};
      `[Worker ${{workerId}}] Step ${{step}}: computed ${{workerId * 10 + step}}`
      "#
    );

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
          // Check if our I/O is ready
          if let Some(ref mut sleep) = this.task.pending_io {
            println!("[Worker {}] Polling sleep...", this.task.worker_id);
            match Pin::new(sleep).poll(cx) {
              Poll::Pending => {
                println!("[Worker {}] Sleep still pending", this.task.worker_id);
                return Poll::Pending;
              }
              Poll::Ready(()) => {
                println!("[Worker {}] Sleep ready!", this.task.worker_id);
                // I/O complete! Need to re-acquire an isolate from pool
                this.task.pending_io = None;

                if let Some(isolate) = this.pool.acquire() {
                  this.isolate = Some(isolate);
                  this.enter_and_lock();
                  println!(
                    "[Worker {}] I/O complete, re-acquired isolate from pool",
                    this.task.worker_id
                  );
                } else {
                  println!(
                    "[Worker {}] No isolate available, waiting...",
                    this.task.worker_id
                  );
                  // No isolate available yet, try again later
                  cx.waker().wake_by_ref();
                  return Poll::Pending;
                }
              }
            }
          } else {
            println!(
              "[Worker {}] Suspended but no pending_io?!",
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
              "[Worker {}] Completed all steps, released isolate",
              this.task.worker_id
            );
            return Poll::Ready((this.task.worker_id, result));
          } else {
            // Simulate async I/O - unlock isolate AND RETURN TO POOL!
            println!(
              "[Worker {}] Starting I/O, returning isolate to pool...",
              this.task.worker_id
            );
            this.unlock_and_exit();

            // Return isolate to pool so others can use it!
            let isolate = this.isolate.take().unwrap();
            this.pool.release(isolate);

            // Create a sleep future to simulate I/O with random delay
            let delay_ms = rand::rng().random_range(20..150);
            println!("[Worker {}] I/O will take {}ms", this.task.worker_id, delay_ms);
            let mut sleep = Box::pin(tokio::time::sleep(Duration::from_millis(delay_ms)));

            // Poll once to register with tokio runtime!
            let _ = Pin::new(&mut sleep).poll(cx);

            this.task.pending_io = Some(sleep);
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

  println!("=== Async V8 Isolate Pool Demo ===\n");
  println!("Pool has 2 isolates, but we'll run 4 workers.");
  println!("Workers will suspend (unlock isolate) on I/O,");
  println!("allowing other workers to use the isolates.\n");

  // Create pool with only 2 isolates
  let pool = Arc::new(IsolatePool::new(2));

  // Create 4 workers, each doing 3 steps
  let mut futures: FuturesUnordered<PooledExecution> = FuturesUnordered::new();

  for worker_id in 0..4 {
    let task = WorkerTask::new(worker_id, 3);
    let execution = PooledExecution::new(Arc::clone(&pool), task);
    futures.push(execution);
  }

  println!("--- Starting execution ---\n");

  // Run with 5s timeout
  let run_all = async {
    while let Some((worker_id, result)) = futures.next().await {
      println!(">>> Worker {} finished with: {}\n", worker_id, result);
    }
  };

  tokio::select! {
    _ = run_all => {
      println!("=== All workers completed! ===");
    }
    _ = tokio::time::sleep(Duration::from_secs(5)) => {
      println!("=== TIMEOUT after 5s! ===");
    }
  }
}
