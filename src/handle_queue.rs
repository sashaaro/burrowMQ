use crate::models::InternalError::{ExchangeNotFound, QueueNotFound, Unsupported};
use crate::models::InternalQueue;
use crate::queue::QueueTrait;
use crate::server::BurrowMQServer;
use crate::utils::gen_random_name;
use amq_protocol::protocol::queue;
use amq_protocol::protocol::queue::DeleteOk;
use bytes::Bytes;
use std::sync::Arc;

impl<Q: QueueTrait<Bytes> + Default> BurrowMQServer<Q> {
    pub(crate) async fn handle_queue_method(
        self: Arc<Self>,
        channel_id: u16,
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
                    return Err(ExchangeNotFound(bind.exchange.to_string()).into());
                };
                if !self.queues.contains_key(bind.queue.as_str()) {
                    return Err(QueueNotFound(bind.queue.to_string(), channel_id).into());
                };

                self.queue_bindings.lock().await.push(bind.clone());

                queue::AMQPMethod::BindOk(queue::BindOk {})
            }
            queue::AMQPMethod::Declare(declare) => {
                let mut queue_name = declare.queue.to_string();
                if queue_name.is_empty() {
                    queue_name = gen_random_name();
                }

                if !self.queues.contains_key(&queue_name) {
                    let queue = Arc::new(InternalQueue::new(queue_name.clone()));

                    Arc::clone(&self)
                        .listen_queue_ready(Arc::clone(&queue))
                        .await;

                    self.queues.insert(queue_name.clone(), queue);
                }

                // let queue = self.queues.entry(queue_name.clone()).or_insert_with(|| {
                //     let queue = Arc::new(InternalQueue {
                //         queue_name: queue_name.clone(),
                //         ready_vec: Default::default(),
                //         consumed: Default::default(),
                //         consuming: Default::default(),
                //     });
                //     queue
                // });
                //
                // drop(queue);

                queue::AMQPMethod::DeclareOk(queue::DeclareOk {
                    queue: queue_name.clone().into(),
                    message_count: 0,  // сколько сообщений уже в очереди
                    consumer_count: 0, // TODO сколько потребителей подписаны на эту очередь
                })
            }
            queue::AMQPMethod::Purge(purge) => {
                let Some(queue) = self.queues.get(purge.queue.as_str()) else {
                    return Err(QueueNotFound(purge.queue.to_string(), channel_id).into());
                };

                let mut i = 0;
                while queue.store.pop().is_some() {
                    i += 1;
                } // TODO

                queue::AMQPMethod::PurgeOk(queue::PurgeOk { message_count: i })
            }
            queue::AMQPMethod::Delete(delete) => {
                let queue = self.queues.remove(delete.queue.as_str());
                if let Some(queue) = queue {
                    if let Some(handler) = self.handlers.lock().await.remove(&queue.0) {
                        handler.abort();
                    }
                } else {
                    return Err(QueueNotFound(delete.queue.to_string(), channel_id).into());
                }

                queue::AMQPMethod::DeleteOk(DeleteOk {
                    message_count: 0, // TODO
                })
            }
            f => {
                return Err(Unsupported(format!("unsupported method: {f:?}")).into());
            }
        };
        Ok(resp)
    }
}
