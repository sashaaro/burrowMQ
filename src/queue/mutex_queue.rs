use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::mem::MaybeUninit;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Arc, Mutex};
use crate::server::QueueTrait;

pub struct MutexQueue<T> {
    inner: Arc<Mutex<VecDeque<T>>>,
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



