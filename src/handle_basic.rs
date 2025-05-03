use crate::models::{ChannelInfo, ConsumerSubscription, ExchangeKind, InternalError};
use crate::parsing::ParsingContext;
use crate::server::BurrowMQServer;
use amq_protocol::frame::{AMQPFrame, parse_frame};
use amq_protocol::protocol::basic::Publish;
use amq_protocol::protocol::{AMQPClass, basic, channel};
use amq_protocol::types::ShortString;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

impl BurrowMQServer {
    pub(crate) async fn handle_basic_method(
        self: Arc<Self>,
        channel_id: u16,
        session_id: i64,
        socket: Arc<Mutex<OwnedWriteHalf>>,
        frame: basic::AMQPMethod,
        parsing_context: ParsingContext<'_>,
        buf: &[u8],
    ) -> anyhow::Result<()> {
        match frame {
            basic::AMQPMethod::Publish(publish) => {
                let shift = parsing_context.as_ptr() as usize - buf.as_ptr() as usize;

                let message = Self::extract_message(&buf[shift..]).map_err(|err| {
                    log::warn!("fail parse message: {}", err);
                    err
                })?;

                let match_queue_names = Arc::clone(&self).find_queues(&publish).await;

                // TODO if confirm mode then ack
                if let Err(err) = match_queue_names {
                    let amqp_frame = AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Basic(basic::AMQPMethod::Return(basic::Return {
                            reply_code: 312,
                            reply_text: "NO_ROUTE".into(),
                            exchange: publish.exchange.to_string().into(),
                            routing_key: publish.routing_key.to_string().into(),
                        })),
                    );
                    let buffer = Self::make_buffer_from_frame(&amqp_frame);
                    let _ = socket.lock().await.write_all(&buffer).await;
                    return Ok(());
                };
                let queue_names = match_queue_names.unwrap();

                if queue_names.is_empty() && publish.mandatory {
                    let amqp_frame = AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Basic(basic::AMQPMethod::Return(basic::Return {
                            reply_code: 0,
                            reply_text: Default::default(),
                            exchange: Default::default(),
                            routing_key: Default::default(),
                        })),
                    );
                    let buffer = Self::make_buffer_from_frame(&amqp_frame);
                    let _ = socket.lock().await.write_all(&buffer).await;
                    return Ok(());
                }

                for queue_name in queue_names {
                    if let Some(mut queue) = self.queues.get_mut(&queue_name) {
                        queue.ready_vec.push_back(message.clone().into());
                        if queue.ready_vec.len() == 1 {
                            Arc::clone(&self)
                                .queue_process(queue.queue_name.clone())
                                .await;
                        }
                    } else {
                        panic!("not found queue {}", queue_name);
                    }
                }
            }
            basic::AMQPMethod::Consume(consume) => {
                let queue = self.queues.get_mut(&consume.queue.to_string());
                if queue.is_none() {
                    drop(queue);
                    let amqp_frame = AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Channel(channel::AMQPMethod::Close(channel::Close {
                            reply_code: 404,
                            reply_text: "NOT_FOUND".into(),
                            class_id: 50,
                            method_id: 20,
                        })),
                    );
                    let buffer = Self::make_buffer_from_frame(&amqp_frame);
                    let _ = socket.lock().await.write_all(&buffer).await;
                    return Ok(());
                }
                drop(queue);

                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(&session_id).expect("Session not found");

                let consumer_tag = consume.consumer_tag.to_string();
                if consumer_tag.is_empty() {
                    // consumer_tag = Alphanumeric.sample_string(&mut rand::rng(), 8)
                }

                let ch: &mut ChannelInfo = session
                    .channels
                    .iter_mut()
                    .find(|c| c.id == channel_id)
                    .expect("Channel not found");
                if !ch.active_consumers.contains_key(&consumer_tag) {
                    ch.active_consumers.insert(
                        consumer_tag.clone(),
                        ConsumerSubscription {
                            queue: consume.queue.to_string(),
                        },
                    );
                };

                if !consume.nowait {
                    let amqp_frame = AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Basic(basic::AMQPMethod::ConsumeOk(basic::ConsumeOk {
                            consumer_tag: ShortString::from(consumer_tag), // TODO
                        })),
                    );
                    let buffer = Self::make_buffer_from_frame(&amqp_frame);
                    let _ = socket.lock().await.write_all(&buffer).await;
                }
                drop(sessions);

                Arc::clone(&self)
                    .queue_process(consume.queue.to_string())
                    .await;
            }
            basic::AMQPMethod::Qos(qos) => {
                if qos.prefetch_count != 1 {
                    panic!("prefetching unsupported")
                }

                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Basic(basic::AMQPMethod::QosOk(basic::QosOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            basic::AMQPMethod::Cancel(cancel) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(&session_id).expect("Session not found");

                let ch: &mut ChannelInfo = session
                    .channels
                    .iter_mut()
                    .find(|c| c.id == channel_id)
                    .expect("Channel not found");

                let canceled_consumer_tag = cancel.consumer_tag.to_string();
                ch.active_consumers.remove(&canceled_consumer_tag);

                let consumer_tag = cancel.consumer_tag.to_string();

                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Basic(basic::AMQPMethod::CancelOk(basic::CancelOk {
                        consumer_tag: consumer_tag.clone().into(),
                    })),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            basic::AMQPMethod::Ack(ack) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(&session_id).expect("Session not found");

                let channel_info: &mut ChannelInfo = session
                    .channels
                    .iter_mut()
                    .find(|c| c.id == channel_id)
                    .expect("Channel not found");

                let Some(unacked) = channel_info.unacked_messages.remove(&ack.delivery_tag) else {
                    panic!("unacked message not found"); // TODO
                };

                let queue_name = unacked.queue.clone();
                drop(sessions);

                let mut queue = self.queues.get_mut(&queue_name).expect("queue not found");
                if queue.unacked_vec.len() > 1 {
                    panic!("prefetching unsupported");
                }
                if queue.unacked_vec.is_empty() {
                    panic!("unacked message not found");
                }
                _ = queue.unacked_vec.pop_front(); // drop
                drop(queue);

                // let mut sub: Option<&ConsumerSubscription> = None;
                // for s in &ch.active_consumers {
                //     if s.queue == queue_name {
                //         sub = Some(s);
                //         break;
                //     }
                // }
                // if sub.is_none() {
                //     panic!("subscription not found");
                // }
                // let sub = sub.unwrap();
                // sub

                Arc::clone(&self)
                    .queue_process(unacked.queue.to_owned())
                    .await;
            }
            // TODO Добавить обработку basic.reject, basic.cancel
            f => {
                unimplemented!("unimplemented queue method: {f:?}")
            }
        }
        Ok(())
    }

    fn extract_message(next_buf: &[u8]) -> Result<Vec<u8>, InternalError> {
        let (parsing_context, frame) =
            parse_frame(ParsingContext::from(next_buf)).map_err(|_| InternalError::InvalidFrame)?;

        let AMQPFrame::Header(_, _, content_header) = frame else {
            return Err(InternalError::InvalidFrame);
        };

        let body_size = content_header.body_size as usize;

        let shift = parsing_context.as_ptr() as usize - next_buf.as_ptr() as usize;
        let body_buf = &next_buf[shift..];

        let (_, body_frame) = parse_frame(body_buf).map_err(|_| InternalError::InvalidFrame)?;

        let AMQPFrame::Body(_, body) = body_frame else {
            return Err(InternalError::InvalidFrame);
        };

        if body.len() != body_size {
            return Err(InternalError::InvalidFrame);
        }

        Ok(body)
    }

    async fn find_queues(self: Arc<Self>, publish: &Publish) -> anyhow::Result<Vec<String>> {
        let mut matched_queue_names = vec![];

        match publish.exchange.as_str() {
            "" => {
                let queue = self.queues.get(publish.routing_key.as_str());
                if queue.is_some() {
                    matched_queue_names.push(publish.routing_key.to_string());
                } else if publish.mandatory {
                    return Err(anyhow::anyhow!("NO_ROUTE"));
                }
                drop(queue);
            }
            exchange => {
                if let Some(exchange) = self.exchanges.lock().await.get(exchange) {
                    let bindings = self.queue_bindings.lock().await;
                    match exchange.declaration.kind.clone().into() {
                        ExchangeKind::Direct => {
                            for bind in bindings.iter() {
                                if bind.routing_key == publish.routing_key
                                    && self.queues.contains_key(bind.queue.as_str())
                                {
                                    matched_queue_names.push(bind.queue.to_string());
                                }
                            }
                        }
                        ExchangeKind::Fanout => {
                            for bind in bindings.iter() {
                                if publish.exchange == bind.exchange
                                    && self.queues.contains_key(bind.queue.as_str())
                                {
                                    matched_queue_names.push(bind.queue.to_string());
                                }
                            }
                        }
                        ExchangeKind::Topic | ExchangeKind::Headers => {
                            unimplemented!("topic and headers not supported yet");
                        }
                    }
                }
            }
        }

        Ok(matched_queue_names)
    }
}
