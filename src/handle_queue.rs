use crate::models::InternalQueue;
use crate::server::{BurrowMQServer, QueueTrait};
use crate::utils::gen_random_name;
use amq_protocol::protocol::queue;
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

impl<Q: QueueTrait<Bytes> + Default> BurrowMQServer<Q> {
    pub(crate) async fn handle_queue_method(
        self: Arc<Self>,
        frame: queue::AMQPMethod,
    ) -> anyhow::Result<queue::AMQPMethod> {
        let resp = match frame {
            queue::AMQPMethod::Bind(bind) => {
                if !self
                    .exchanges
                    .lock()
                    .await
                    .contains_key(bind.exchange.as_str())
                {
                    panic!("exchange not found") // todo send error
                };
                if !self.queues.contains_key(bind.queue.as_str()) {
                    panic!("queue not found") // todo send error
                };

                self.queue_bindings.lock().await.push(bind.clone());

                queue::AMQPMethod::BindOk(queue::BindOk {})
            }
            queue::AMQPMethod::Declare(declare) => {
                let mut queue_name = declare.queue.to_string();
                if queue_name.is_empty() {
                    queue_name = gen_random_name();
                }

                self.queues.entry(queue_name.clone()).or_insert_with(|| {
                    Arc::new(InternalQueue {
                        queue_name: queue_name.clone(),
                        ready_vec: Default::default(),
                        consumed: Default::default(),
                        consuming: Default::default(),
                    })
                });

                queue::AMQPMethod::DeclareOk(queue::DeclareOk {
                    queue: queue_name.clone().into(),
                    message_count: 0,  // сколько сообщений уже в очереди
                    consumer_count: 0, // TODO сколько потребителей подписаны на эту очередь
                })
            }
            queue::AMQPMethod::Purge(purge) => {
                let Some(mut queue) = self.queues.get_mut(purge.queue.as_str()) else {
                    panic!("queue not found"); // TODO
                };

                *queue = Arc::new(InternalQueue {
                    queue_name: queue.queue_name.clone(),
                    ready_vec: Default::default(),
                    consumed: Default::default(),
                    consuming: Default::default(),
                });

                queue::AMQPMethod::PurgeOk(queue::PurgeOk {
                    message_count: 1, // TODO
                })
            }
            f => {
                unimplemented!("unimplemented queue method: {f:?}")
            }
        };
        Ok(resp)
    }
}
