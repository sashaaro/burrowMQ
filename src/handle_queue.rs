use crate::models::InternalQueue;
use crate::server::{BurrowMQServer, gen_random_queue_name};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::{AMQPClass, queue};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

impl BurrowMQServer {
    pub(crate) async fn handle_queue_method(
        self: Arc<Self>,
        channel_id: u16,
        socket: Arc<Mutex<OwnedWriteHalf>>,
        frame: queue::AMQPMethod,
    ) -> anyhow::Result<()> {
        match frame {
            queue::AMQPMethod::Bind(bind) => {
                let Some(_) = self.exchanges.lock().await.get_mut(bind.exchange.as_str()) else {
                    panic!("exchange not found") // todo send error
                };
                let Some(_) = self.queues.get_mut(bind.queue.as_str()) else {
                    panic!("queue not found") // todo send error
                };

                self.queue_bindings.lock().await.push(bind.clone());

                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Queue(queue::AMQPMethod::BindOk(queue::BindOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            queue::AMQPMethod::Declare(declare) => {
                let mut queue_name = declare.queue.to_string();
                if queue_name.is_empty() {
                    queue_name = gen_random_queue_name();
                }

                self.queues
                    .entry(queue_name.clone())
                    .or_insert_with(|| InternalQueue {
                        queue_name: queue_name.clone(),
                        ready_vec: Default::default(),
                        unacked_vec: Default::default(),
                    });

                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Queue(queue::AMQPMethod::DeclareOk(queue::DeclareOk {
                        queue: queue_name.clone().into(),
                        message_count: 0,  // сколько сообщений уже в очереди
                        consumer_count: 0, // TODO сколько потребителей подписаны на эту очередь
                    })),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            queue::AMQPMethod::Purge(purge) => {
                let Some(mut queue) = self.queues.get_mut(purge.queue.as_str()) else {
                    panic!("queue not found"); // TODO
                };

                let count = queue.ready_vec.len() as u32;
                queue.ready_vec.clear();
                drop(queue);

                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Queue(queue::AMQPMethod::PurgeOk(queue::PurgeOk {
                        message_count: count,
                    })),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            f => {
                unimplemented!("unimplemented queue method: {f:?}")
            }
        }
        Ok(())
    }
}
