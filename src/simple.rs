//! Simple V8 Locker example
//!
//! Minimal example showing basic Locker usage with an isolate pool.

use std::sync::{Arc, Mutex};

struct IsolatePool {
  isolates: Mutex<Vec<v8::UnenteredIsolate>>,
}

impl IsolatePool {
  fn new(size: usize) -> Self {
    let isolates = (0..size)
      .map(|_| v8::Isolate::new_unentered(Default::default()))
      .collect();
    Self {
      isolates: Mutex::new(isolates),
    }
  }

  fn acquire(&self) -> Option<v8::UnenteredIsolate> {
    self.isolates.lock().unwrap().pop()
  }

  fn release(&self, isolate: v8::UnenteredIsolate) {
    self.isolates.lock().unwrap().push(isolate);
  }
}

fn run_js(pool: &IsolatePool, worker_id: usize) -> String {
  let mut isolate = pool.acquire().expect("No isolate available");

  // Locker handles enter/exit automatically
  let mut locker = v8::Locker::new(&mut isolate);

  let result = {
    let scope = std::pin::pin!(v8::HandleScope::new(&mut *locker));
    let scope = &mut scope.init();
    let context = v8::Context::new(scope, Default::default());
    let scope = &mut v8::ContextScope::new(scope, context);

    let code = format!("'Worker {} computed: ' + ({} * 10 + 42)", worker_id, worker_id);
    let code = v8::String::new(scope, &code).unwrap();
    let script = v8::Script::compile(scope, code, None).unwrap();
    let result = script.run(scope).unwrap();
    result.to_rust_string_lossy(scope)
  };

  drop(locker);
  pool.release(isolate);

  result
}

fn main() {
  // Initialize V8
  let platform = v8::new_default_platform(0, false).make_shared();
  v8::V8::initialize_platform(platform);
  v8::V8::initialize();

  println!("=== Simple V8 Locker Example ===\n");

  // Create pool with 2 isolates
  let pool = Arc::new(IsolatePool::new(2));

  // Run 4 workers sequentially
  for i in 0..4 {
    let result = run_js(&pool, i);
    println!("[Worker {}] {}", i, result);
  }

  println!("\nDone!");
}
