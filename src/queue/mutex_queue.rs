use crate::queue::QueueTrait;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

// simple mutex queue
pub struct MutexQueue<T> {
    inner: Arc<Mutex<VecDeque<T>>>,
}

impl<T> Default for MutexQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> MutexQueue<T> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}

impl<T: Send> QueueTrait<T> for MutexQueue<T> {
    fn push(&self, item: T) {
        let mut guard = self.inner.lock().unwrap();
        guard.push_back(item);
    }

    fn pop(&self) -> Option<T> {
        let mut guard = self.inner.lock().unwrap();
        guard.pop_front()
    }
}
