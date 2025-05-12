pub mod crossbeam;
pub mod lock_free;
pub mod mutex_queue;

pub trait QueueTrait<T>: Send + Sync {
    fn push(&self, item: T);
    fn pop(&self) -> Option<T>;
    // fn len(&self) -> usize;
}
