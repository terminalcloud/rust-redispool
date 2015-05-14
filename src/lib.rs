// Copyright 2015 Terminal.com
// Based on work by The Rust Project Developers.
//
// Copyright 2014 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Abstraction of a thread pool of redis connections for basic parallelism.
extern crate redis;

use std::sync::mpsc::{channel, Sender, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
pub use redis::*;

#[derive(Clone)]
struct ThreadConnectionInfo {
    host: String,
    port: u16,
    db: i64,
    passwd: Option<String>,
}

impl redis::IntoConnectionInfo for ThreadConnectionInfo {
    fn into_connection_info(self) -> redis::RedisResult<redis::ConnectionInfo> {
        Ok(redis::ConnectionInfo { host: self.host, port: self.port, db: self.db, passwd: self.passwd })
    }
}

trait RedisFnBox {
    fn call_box(self: Box<Self>, cx: &mut redis::Connection);
}

impl<F: FnOnce(&mut redis::Connection)> RedisFnBox for F {
    fn call_box(self: Box<F>, cx: &mut redis::Connection) {
        (*self)(cx);
    }
}

type Thunk<'a> = Box<RedisFnBox + Send + 'a>;

struct Sentinel<'a> {
    jobs: &'a Arc<Mutex<Receiver<Thunk<'static>>>>,
    cx_params: ThreadConnectionInfo,
    active: bool
}

impl<'a> Sentinel<'a> {
    fn new(jobs: &'a Arc<Mutex<Receiver<Thunk<'static>>>>, cx_params: ThreadConnectionInfo) -> Sentinel<'a> {
        Sentinel {
            jobs: jobs,
            cx_params: cx_params,
            active: true
        }
    }

    // Cancel and destroy this sentinel.
    fn cancel(mut self) {
        self.active = false;
    }
}

impl<'a> Drop for Sentinel<'a> {
    fn drop(&mut self) {
        if self.active {
            spawn_in_pool(self.jobs.clone(), self.cx_params.clone())
        }
    }
}

/// A thread pool used to query redis connections in parallel.
///
/// Spawns `n` worker threads with active connections
/// and replenishes the pool if any worker threads
/// panic or their connection fails.
///
/// # Example
///
/// ```no-run
/// use redispool::RedisPool;
/// use std::sync::mpsc::channel;
///
/// let pool = RedisPool::new("redis://127.0.0.1/", 4);
///
/// let (tx, rx) = channel();
/// for i in 0..8 {
///     let tx = tx.clone();
///     pool.execute(move|cx: &mut redis::Connection| {
///         // Query the connection and return the result
///         // let result = cx.???;
///         // tx.send(result).unwrap();
///     });
/// }
/// ```
pub struct RedisPool {
    // How the threadpool communicates with subthreads.
    //
    // This is the only such Sender, so when it is dropped all subthreads will
    // quit.
    jobs: Sender<Thunk<'static>>
}

impl RedisPool {
    /// Spawns a new redis thread pool with `threads` threads.
    ///
    /// # Panics
    ///
    /// This function will panic if `threads` is 0.
    pub fn new<T: redis::IntoConnectionInfo>(threads: usize, params: T) -> RedisPool {
        assert!(threads >= 1);

        let (tx, rx) = channel::<Thunk<'static>>();
        let rx = Arc::new(Mutex::new(rx));

        let cx_info = params.into_connection_info().unwrap();
        let cx_thread = ThreadConnectionInfo { host: cx_info.host, port: cx_info.port, db: cx_info.db, passwd: cx_info.passwd };

        // Threadpool threads
        for _ in 0..threads {
            spawn_in_pool(rx.clone(), cx_thread.clone());
        }

        RedisPool { jobs: tx }
    }

    /// Executes the function `job` on a thread in the pool.
    pub fn execute<F>(&self, job: F)
        where F : FnOnce(&mut redis::Connection) + Send + 'static
    {
        self.jobs.send(Box::new(move |cx: &mut redis::Connection| job(cx))).unwrap();
    }
}

fn spawn_in_pool(jobs: Arc<Mutex<Receiver<Thunk<'static>>>>, cx_info: ThreadConnectionInfo) {
    thread::spawn(move || {
        // Will spawn a new thread on panic unless it is cancelled.
        let sentinel = Sentinel::new(&jobs, cx_info.clone());

        let client = match redis::Client::open(cx_info) {
            Ok(client) => client,
            Err(..) => {
                sentinel.cancel();
                return;
            }
        };

        let mut cx = match client.get_connection() {
            Ok(cx) => cx,
            Err(..) => {
                sentinel.cancel();
                return;
            }
        };

        loop {
            let message = {
                // Only lock jobs for the time it takes
                // to get a job, not run it.
                let lock = jobs.lock().unwrap();
                lock.recv()
            };

            match message {
                Ok(job) => job.call_box(&mut cx),

                // The Threadpool was dropped.
                Err(..) => break
            }
        }

        sentinel.cancel();
    });
}

/* Tests require redis, and will be reimplemented at a later time
#[cfg(test)]
mod test {
    use super::RedisPool;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Barrier};

    const TEST_TASKS: usize = 4;

    #[test]
    fn test_works() {
        let pool = RedisPool::new(TEST_TASKS);

        let (tx, rx) = channel();
        for _ in 0..TEST_TASKS {
            let tx = tx.clone();
            pool.execute(move|| {
                tx.send(1).unwrap();
            });
        }

        assert_eq!(rx.iter().take(TEST_TASKS).fold(0, |a, b| a + b), TEST_TASKS);
    }

    #[test]
    #[should_panic]
    fn test_zero_tasks_panic() {
        RedisPool::new(0);
    }

    #[test]
    fn test_recovery_from_subtask_panic() {
        let pool = RedisPool::new(TEST_TASKS);

        // Panic all the existing threads.
        for _ in 0..TEST_TASKS {
            pool.execute(move|| -> () { panic!() });
        }

        // Ensure new threads were spawned to compensate.
        let (tx, rx) = channel();
        for _ in 0..TEST_TASKS {
            let tx = tx.clone();
            pool.execute(move|| {
                tx.send(1).unwrap();
            });
        }

        assert_eq!(rx.iter().take(TEST_TASKS).fold(0, |a, b| a + b), TEST_TASKS);
    }

    #[test]
    fn test_should_not_panic_on_drop_if_subtasks_panic_after_drop() {

        let pool = RedisPool::new(TEST_TASKS);
        let waiter = Arc::new(Barrier::new(TEST_TASKS + 1));

        // Panic all the existing threads in a bit.
        for _ in 0..TEST_TASKS {
            let waiter = waiter.clone();
            pool.execute(move|| {
                waiter.wait();
                panic!();
            });
        }

        drop(pool);

        // Kick off the failure.
        waiter.wait();
    }
}
*/
