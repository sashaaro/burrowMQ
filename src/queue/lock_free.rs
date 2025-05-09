use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, Ordering};
use crate::server::QueueTrait;

pub(crate) struct Node<T> {
    value: Option<T>,
    next: AtomicPtr<Node<T>>,
}

pub struct LockFreeQueue<T> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>
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
            next: AtomicPtr::new(null_mut())
        }));

        LockFreeQueue {
            head: AtomicPtr::new(dummy),
            tail: AtomicPtr::new(dummy)
        }
    }
}
impl<T> QueueTrait<T> for LockFreeQueue<T> {
    fn push(&self, v: T) {
        let new_node = Box::into_raw(Box::new(Node {
            value: Some(v),
            next: AtomicPtr::new(null_mut())
        }));

        loop {
            let tail = self.tail.load(Ordering::Acquire);
            let next = unsafe {(*tail).next.load(Ordering::Acquire)};
            if next.is_null() {
                if unsafe{
                    (*tail).next.compare_exchange(null_mut(), new_node, Ordering::AcqRel, Ordering::Acquire).is_ok()
                } {
                    let _ = self.tail.compare_exchange(
                        tail,
                        new_node,
                        Ordering::AcqRel,
                        Ordering::Acquire
                    );
                    break;
                }
            } else {
                let _ = self.tail.compare_exchange(
                    tail,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire
                );
            }
        }
    }

    fn pop(&self) -> Option<T> {
        loop {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);
            let next =  unsafe{(*head).next.load(Ordering::Acquire)};

            if next.is_null() {
                return None
            }
            if head == tail {
                let _ = self.tail.compare_exchange(
                    tail,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire
                );
                continue
            }

            if self.head.compare_exchange(
                head,
                next,
                Ordering::AcqRel,
                Ordering::Acquire
            ).is_ok() {
                let result = unsafe{ (*head).value.take() };
                unsafe {
                    drop(Box::from_raw(head));
                }
                return result

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


#[cfg(test)]
mod tests {
    use std::marker::PhantomPinned;
    use std::pin::Pin;
    use std::ptr::null_mut;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicPtr, AtomicU64};
    use std::sync::atomic::Ordering::Acquire;
    use std::thread;
    use crate::queue::lock_free::LockFreeQueue;
    use crate::server::QueueTrait;

    #[test]
    fn node_test() {
        let q = LockFreeQueue::new();

        q.push(10);
        q.push(20);

        println!("{:?}", q.pop()); // Some(10)
        println!("{:?}", q.pop()); // Some(20)
        println!("{:?}", q.pop()); // None




        let q = Arc::new(
            LockFreeQueue::new()
            // crossbeam_queue::SegQueue::new()
        );

        let mut handles = vec![];
        // 4 потока-писателя
        for i in 0..4 {
            let q = Arc::clone(&q);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    q.push(i * 100 + j);
                }
            }));
        }

        let inc = Arc::new(AtomicU64::new(0));
        // 4 потока-чтения
        for _ in 0..4 {
            let q = Arc::clone(&q);


            let inc = Arc::clone(&inc);
            handles.push(thread::spawn(move || {
                let mut local = vec![];
                loop {
                    if let Some(x) = q.pop() {
                        local.push(x);
                        inc.fetch_add(1, Acquire);
                    } else {
                        break;
                    }
                }
                println!("Поток вытащил: {:?}", local);
            }));
        }

        // Ждём завершения всех потоков
        for handle in handles {
            handle.join().unwrap();
        }

        println!("всего {:?}", inc.load(Acquire))
    }

    #[test]
    fn it_works_a() {
        // struct SelfRef {
        //     data: String,
        //     data_ref: *const str,
        // 
        //     // a: PhantomPinned
        // }
        //     impl SelfRef {
        //         fn new(data: String) -> Self {
        //             SelfRef {
        //                 data,
        //                 data_ref: &data,
        //             }
        //         }
        // 
        //         fn init(self: Pin<&mut Self>) {
        //             let self_ptr: *const str = self.data.as_str();
        //             let mut_ref = unsafe { self.get_unchecked_mut() };
        //             mut_ref.data_ref = self_ptr;
        //         }
        //         
        // 
        //         fn print_ref(&self) {
        //             let s = unsafe { &*self.data_ref };
        //             println!("data_ref: {}", s);
        //         }
        //     }
        // 
        //     let mut b = Box::pin(SelfRef::new("hello".to_string()));
        //     b.as_mut().init();
        //     b.print_ref();
    }

    
    fn it_works() {
        let mut v = vec![1,2,3];

        let ptr: *const i32 = &v[0];
        dbg!(ptr);

        let next_el_ptr = (ptr as usize + std::mem::size_of::<i32>()) as *const i32;

        unsafe {
            let v = next_el_ptr.as_ref().unwrap();
            dbg!(v);
            dbg!(*next_el_ptr);
            assert_eq!(*v, 2)
        }

        let next_el_ptr = (ptr as usize + std::mem::size_of::<i16>()) as *const i32;
        unsafe {
            let v = next_el_ptr.as_ref().unwrap();
            dbg!(v);
        }

        let ptr: *mut i32 = &mut v[0];
        unsafe { *ptr = 2; }
        dbg!(v[0]);
        dbg!(ptr);

        dbg!(unsafe{*ptr});
        drop(v);
        dbg!(unsafe{*ptr});



        let i: u16 = 512;
        let ptr: *const u16 = &i;
        let next_el_ptr = (ptr as usize ) as *const u8;


        unsafe {
            let v = next_el_ptr.as_ref().unwrap();
            dbg!(*v); // почему тут 0
        }

        // let ptr: *const i32 = std::ptr::null();
        // assert!(ptr.is_null());
        // unsafe{assert_eq!(*ptr, 1);}

        let ptr = 0x000060edaf804300 as *const u8;
        unsafe {
            let val = *ptr; // <-- здесь почти наверняка упадёт
        }
    }
}