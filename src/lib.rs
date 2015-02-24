// Copyright 2014 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Abstraction of a thread pool for basic parallelism.

#![feature(unsafe_destructor)]

use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;

trait FnBox<A, R> {
    fn call_box(self: Box<Self>, a: A) -> R;
}

impl<A, R, F: FnOnce(A) -> R> FnBox<A, R> for F {
    fn call_box(self: Box<F>, a: A) -> R {
        (*self)(a)
    }
}

type Thunk = Box<FnBox<(), ()> + Send + 'static>;

struct Sentinel<'a> {
    jobs: &'a Arc<Mutex<Receiver<Thunk>>>,
    active: bool
}

impl<'a> Sentinel<'a> {
    fn new(jobs: &'a Arc<Mutex<Receiver<Thunk>>>) -> Sentinel<'a> {
        Sentinel {
            jobs: jobs,
            active: true
        }
    }

    // Cancel and destroy this sentinel.
    fn cancel(mut self) {
        self.active = false;
    }
}

#[unsafe_destructor]
impl<'a> Drop for Sentinel<'a> {
    fn drop(&mut self) {
        if self.active {
            spawn_in_pool(self.jobs.clone())
        }
    }
}

/// A thread pool used to execute functions in parallel.
///
/// Spawns `n` worker threads and replenishes the pool if any worker threads
/// panic.
///
/// # Example
///
/// ```rust
/// use threadpool::ThreadPool;
/// use std::iter::AdditiveIterator;
/// use std::sync::mpsc::channel;
///
/// let pool = ThreadPool::new(4);
///
/// let (tx, rx) = channel();
/// for _ in 0..8 {
///     let tx = tx.clone();
///     pool.execute(move|| {
///         tx.send(1_u32).unwrap();
///     });
/// }
///
/// assert_eq!(rx.iter().take(8).sum(), 8);
/// ```
pub struct ThreadPool {
    // How the threadpool communicates with subthreads.
    //
    // This is the only such Sender, so when it is dropped all subthreads will
    // quit.
    jobs: Sender<Thunk>
}

impl ThreadPool {
    /// Spawns a new thread pool with `threads` threads.
    ///
    /// # Panics
    ///
    /// This function will panic if `threads` is 0.
    pub fn new(threads: usize) -> ThreadPool {
        assert!(threads >= 1);

        let (tx, rx) = channel::<Thunk>();
        let rx = Arc::new(Mutex::new(rx));

        // Threadpool threads
        for _ in 0..threads {
            spawn_in_pool(rx.clone());
        }

        ThreadPool { jobs: tx }
    }

    /// Executes the function `job` on a thread in the pool.
    pub fn execute<F>(&self, job: F)
        where F : FnOnce() + Send + 'static
    {
        self.jobs.send(Box::new(move |()| job())).unwrap();
    }
}

fn spawn_in_pool(jobs: Arc<Mutex<Receiver<Thunk>>>) {
    thread::spawn(move || {
        // Will spawn a new thread on panic unless it is cancelled.
        let sentinel = Sentinel::new(&jobs);

        loop {
            let message = {
                // Only lock jobs for the time it takes
                // to get a job, not run it.
                let lock = jobs.lock().unwrap();
                lock.recv()
            };

            match message {
                Ok(job) => job.call_box(()),

                // The Taskpool was dropped.
                Err(..) => break
            }
        }

        sentinel.cancel();
    });
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Barrier};

    const TEST_TASKS: usize = 4;

    #[test]
    fn test_works() {
        let pool = ThreadPool::new(TEST_TASKS);

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
    #[should_fail]
    fn test_zero_tasks_panic() {
        ThreadPool::new(0);
    }

    #[test]
    fn test_recovery_from_subtask_panic() {
        let pool = ThreadPool::new(TEST_TASKS);

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

        let pool = ThreadPool::new(TEST_TASKS);
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