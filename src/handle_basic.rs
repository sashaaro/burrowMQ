use crate::models::InternalError::{ChannelNotFound, Unsupported};
use crate::models::{ConsumerSubscription, ExchangeKind, InternalError};
use crate::parsing::ParsingContext;
use crate::queue::QueueTrait;
use crate::server::{BurrowMQServer, Responder};
use crate::utils::gen_random_name;
use amq_protocol::frame::{AMQPFrame, parse_frame};
use amq_protocol::protocol::basic::Publish;
use amq_protocol::protocol::{AMQPClass, basic, channel};
use amq_protocol::types::ShortString;
use bytes::Bytes;
use std::sync::Arc;

impl<Q: QueueTrait<Bytes> + Default> BurrowMQServer<Q> {
    pub(crate) async fn handle_basic_method(
        self: Arc<Self>,
        channel_id: u16,
        session_id: u64,
        responder: Responder,
        frame: basic::AMQPMethod,
        parsing_context: ParsingContext<'_>,
        buf: &[u8],
    ) -> anyhow::Result<Option<basic::AMQPMethod>> {
        let resp = match frame {
            basic::AMQPMethod::Publish(publish) => {
                let shift = parsing_context.as_ptr() as usize - buf.as_ptr() as usize;

                let message = Self::extract_message(&buf[shift..]).map_err(|err| {
                    log::warn!("fail parse message: {}", err);
                    err
                })?;

                let match_queue_names = Arc::clone(&self).find_queues(&publish).await;

                // TODO if confirm mode then ack
                if let Err(_err) = match_queue_names {
                    return Ok(Some(basic::AMQPMethod::Return(basic::Return {
                        reply_code: 312,
                        reply_text: "NO_ROUTE".into(),
                        exchange: publish.exchange.to_string().into(),
                        routing_key: publish.routing_key.to_string().into(),
                    })));
                }

                let queue_names = match match_queue_names {
                    Ok(queue_names) => queue_names,
                    _ => {
                        return Ok(Some(basic::AMQPMethod::Return(basic::Return {
                            reply_code: 312,
                            reply_text: "NO_ROUTE".into(),
                            exchange: publish.exchange.to_string().into(),
                            routing_key: publish.routing_key.to_string().into(),
                        })));
                    }
                };

                if queue_names.is_empty() && publish.mandatory {
                    return Ok(Some(amq_protocol::protocol::basic::AMQPMethod::Return(
                        basic::Return {
                            reply_code: 0,
                            reply_text: Default::default(),
                            exchange: Default::default(),
                            routing_key: Default::default(),
                        },
                    )));
                }

                let Some(queue) = self.queues.get(&queue_names[0]) else {
                    return Ok(Some(basic::AMQPMethod::Return(basic::Return {
                        reply_code: 312,
                        reply_text: "NO_ROUTE".into(),
                        exchange: publish.exchange.to_string().into(),
                        routing_key: publish.routing_key.to_string().into(),
                    })));
                };

                queue.ready_vec.push(Bytes::from(message));
                drop(queue);

                for queue_name in queue_names {
                    Arc::clone(&self).queue_process_loop(&queue_name).await;
                }
                None
            }
            basic::AMQPMethod::Consume(consume) => {
                if !self.queues.contains_key(consume.queue.as_str()) {
                    responder(AMQPClass::Channel(channel::AMQPMethod::Close(
                        channel::Close {
                            reply_code: 404,
                            reply_text: "NOT_FOUND".into(),
                            class_id: 50,
                            method_id: 20,
                        },
                    )))
                    .await;

                    return Ok(None);
                }

                let mut sessions = self.sessions.lock().await;
                let session = match sessions.get_mut(&session_id) {
                    Some(session) => session,
                    None => return Err(InternalError::SessionNotFound.into()),
                };

                let mut consumer_tag = consume.consumer_tag.to_string();
                if consumer_tag.is_empty() {
                    consumer_tag = gen_random_name()
                }

                let Some(ch) = session.channels.iter_mut().find(|c| c.id == channel_id) else {
                    return Err(ChannelNotFound(channel_id).into());
                };

                if !ch.active_consumers.contains_key(&consumer_tag) {
                    ch.active_consumers.insert(
                        consumer_tag.clone(),
                        ConsumerSubscription {
                            queue: consume.queue.to_string(),
                        },
                    );
                };
                drop(sessions);

                if !consume.nowait {
                    Arc::clone(&self)
                        .queue_process_loop(consume.queue.as_str())
                        .await;

                    Some(basic::AMQPMethod::ConsumeOk(basic::ConsumeOk {
                        consumer_tag: ShortString::from(consumer_tag),
                    }))
                } else {
                    Arc::clone(&self)
                        .queue_process_loop(consume.queue.as_str())
                        .await;

                    None
                }
            }
            basic::AMQPMethod::Qos(qos) => {
                if qos.prefetch_count != 1 {
                    return Err(Unsupported("prefetching unsupported".to_owned()).into());
                }

                let mut sessions = self.sessions.lock().await;
                let session = match sessions.get_mut(&session_id) {
                    Some(session) => session,
                    None => return Err(InternalError::SessionNotFound.into()),
                };

                let Some(ch) = session.channels.iter_mut().find(|c| c.id == channel_id) else {
                    return Err(ChannelNotFound(channel_id).into());
                };

                ch.prefetch_count = qos.prefetch_count as u64;

                Some(basic::AMQPMethod::QosOk(basic::QosOk {}))
            }
            basic::AMQPMethod::Cancel(cancel) => {
                let mut sessions = self.sessions.lock().await;
                let session = match sessions.get_mut(&session_id) {
                    Some(session) => session,
                    None => return Err(InternalError::SessionNotFound.into()),
                };

                let Some(ch) = session.channels.iter_mut().find(|c| c.id == channel_id) else {
                    return Err(ChannelNotFound(channel_id).into());
                };

                let canceled_consumer_tag = cancel.consumer_tag.to_string();
                ch.active_consumers.remove(&canceled_consumer_tag);

                let consumer_tag = cancel.consumer_tag.to_string();

                Some(basic::AMQPMethod::CancelOk(basic::CancelOk {
                    consumer_tag: consumer_tag.clone().into(),
                }))
            }
            basic::AMQPMethod::Ack(ack) => {
                let mut sessions = self.sessions.lock().await;
                let session = match sessions.get_mut(&session_id) {
                    Some(session) => session,
                    None => return Err(InternalError::SessionNotFound.into()),
                };

                let Some(channel_info) = session.channels.iter_mut().find(|c| c.id == channel_id)
                else {
                    return Err(ChannelNotFound(channel_id).into());
                };

                let unacked = channel_info.awaiting_acks.remove(&ack.delivery_tag as &u64);
                let Some(unacked) = unacked else {
                    panic!("unacked message not found. msg: {:?}", ack); // TODO
                };

                Arc::clone(&self)
                    .queue_process_loop(unacked.queue.as_str())
                    .await;

                None
            }
            // TODO Добавить обработку basic.reject, basic.cancel
            f => {
                return Err(Unsupported(format!("unsupported method: {f:?}")).into());
            }
        };

        Ok(resp)
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
                if self.queues.contains_key(publish.routing_key.as_str()) {
                    matched_queue_names.push(publish.routing_key.to_string());
                } else if publish.mandatory {
                    return Err(anyhow::anyhow!("NO_ROUTE"));
                }
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
                            return Err(Unsupported(
                                "topic and headers not supported yet".to_owned(),
                            )
                            .into());
                        }
                    }
                }
            }
        }

        Ok(matched_queue_names)
    }
}
