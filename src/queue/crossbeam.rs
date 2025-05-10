use crate::queue::QueueTrait;

impl<T: Send> QueueTrait<T> for crossbeam_queue::SegQueue<T> {
    fn push(&self, item: T) {
        self.push(item)
    }
    fn pop(&self) -> Option<T> {
        self.pop()
    }
}
