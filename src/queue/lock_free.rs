use crate::queue::QueueTrait;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, Ordering};

pub(crate) struct Node<T> {
    value: Option<T>,
    next: AtomicPtr<Node<T>>,
}

pub struct LockFreeQueue<T> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>,
}

unsafe impl<T> Sync for LockFreeQueue<T> {}
unsafe impl<T> Send for LockFreeQueue<T> {}

impl<T> Default for LockFreeQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T> LockFreeQueue<T> {
    fn new() -> Self {
        let dummy = Box::into_raw(Box::new(Node {
            value: None,
            next: AtomicPtr::new(null_mut()),
        }));

        LockFreeQueue {
            head: AtomicPtr::new(dummy),
            tail: AtomicPtr::new(dummy),
        }
    }
}
impl<T> QueueTrait<T> for LockFreeQueue<T> {
    fn push(&self, v: T) {
        let new_node = Box::into_raw(Box::new(Node {
            value: Some(v),
            next: AtomicPtr::new(null_mut()),
        }));

        loop {
            let tail = self.tail.load(Ordering::Acquire);
            let next = unsafe { (*tail).next.load(Ordering::Acquire) };
            if next.is_null() {
                if unsafe {
                    (*tail)
                        .next
                        .compare_exchange(null_mut(), new_node, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                } {
                    let _ = self.tail.compare_exchange(
                        tail,
                        new_node,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    break;
                }
            } else {
                let _ = self
                    .tail
                    .compare_exchange(tail, next, Ordering::AcqRel, Ordering::Acquire);
            }
        }
    }

    fn pop(&self) -> Option<T> {
        loop {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);
            let next = unsafe { (*head).next.load(Ordering::Acquire) };

            if next.is_null() {
                return None;
            }
            if head == tail {
                let _ = self
                    .tail
                    .compare_exchange(tail, next, Ordering::AcqRel, Ordering::Acquire);
                continue;
            }

            if self
                .head
                .compare_exchange(head, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let result = unsafe { (*head).value.take() };
                unsafe {
                    drop(Box::from_raw(head));
                }
                return result;
            }
        }
    }
}

impl<T> Drop for LockFreeQueue<T> {
    fn drop(&mut self) {
        while self.pop().is_some() {}
        let dummy = self.head.load(Ordering::Relaxed);
        unsafe {
            drop(Box::from_raw(dummy));
        }
    }
}
