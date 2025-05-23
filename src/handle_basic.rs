use crate::models::InternalError::{ChannelNotFound, UnknownDeliveryTag, Unsupported};
use crate::models::{ExchangeKind, InternalError, Subscription};
use crate::parsing::ParsingContext;
use crate::queue::QueueTrait;
use crate::server::{BurrowMQServer, Responder};
use crate::utils::gen_random_name;
use amq_protocol::frame::{AMQPFrame, parse_frame};
use amq_protocol::protocol::basic::Publish;
use amq_protocol::protocol::{AMQPClass, basic, channel};
use amq_protocol::types::ShortString;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

impl<Q: QueueTrait<Bytes> + Default> BurrowMQServer<Q> {
    pub(crate) async fn handle_basic_method(
        self: Arc<Self>,
        channel_id: u16,
        session_id: u64,
        responder: Responder,
        frame: basic::AMQPMethod,
        parsing_context: ParsingContext<'_>,
    ) -> anyhow::Result<Option<basic::AMQPMethod>> {
        let resp = match frame {
            basic::AMQPMethod::Publish(publish) => {
                let Ok((parsing_context, content_frame)) = parse_frame(parsing_context) else {
                    log::warn!("wrong content header frames");
                    return Ok(None);
                };
                let AMQPFrame::Header(_, _, content_frame) = content_frame else {
                    log::warn!("wrong content header frames");
                    return Ok(None);
                };

                let mut bodies = vec![];

                let mut parsing_context = parsing_context;
                loop {
                    let result = parse_frame(parsing_context);

                    let Ok((local_parsing_context, body_frame)) = result else {
                        break;
                    };
                    parsing_context = local_parsing_context;

                    let AMQPFrame::Body(_, body) = body_frame else {
                        // TODO check & prevent extra mem allocation
                        break;
                    };
                    bodies.push(body);
                }

                let v: Vec<String> = bodies
                    .iter()
                    .map(|v| String::from_utf8_lossy(v).to_string())
                    .into_iter()
                    .collect();

                if bodies.len() == 0 {
                    log::error!("wrong body frames");
                    return Ok(None);
                }

                return self.handle_bodies(publish, bodies).await;
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
                };
                let mut consumer_tag = consume.consumer_tag.to_string();
                if consumer_tag.is_empty() {
                    consumer_tag = gen_random_name()
                }

                let mut session = match self.sessions.get_mut(&session_id) {
                    Some(session) => session,
                    None => return Err(InternalError::SessionNotFound.into()),
                };

                let Some(channel) = session.channels.get_mut(&channel_id) else {
                    return Err(ChannelNotFound(channel_id).into());
                };

                let subscription = Subscription {
                    internal_id: self.consumer_inc.fetch_add(1, Ordering::Acquire) + 1,
                    no_ack: consume.no_ack,
                    total_awaiting_acks_count: channel.total_awaiting_acks_count,
                    awaiting_acks_count: 0,
                    prefetch_count: channel.prefetch_count,
                    consumer_tag: consumer_tag.to_string(),
                    session_id,
                    channel_id,
                };

                let mut consumer_metadata = self.consumer_metadata.lock().await;

                match consumer_metadata
                    .consumer_tags
                    .get_mut(consume.queue.as_str())
                {
                    None => {
                        let mut h = HashMap::new();
                        h.insert(consumer_tag.clone(), subscription);
                        consumer_metadata
                            .consumer_tags
                            .insert(consume.queue.to_string(), h);
                    }
                    Some(h) => {
                        h.insert(consumer_tag.clone(), subscription);
                    }
                };
                consumer_metadata
                    .queues
                    .insert(consumer_tag.clone(), consume.queue.to_string());
                drop(consumer_metadata);

                self.mark_queue_ready(consume.queue.as_str()).await;

                if !consume.nowait {
                    Some(basic::AMQPMethod::ConsumeOk(basic::ConsumeOk {
                        consumer_tag: ShortString::from(consumer_tag),
                    }))
                } else {
                    None
                }
            }
            basic::AMQPMethod::Qos(qos) => {
                if qos.prefetch_count != 1 {
                    return Err(Unsupported("prefetching unsupported".to_owned()).into());
                }

                let mut session = match self.sessions.get_mut(&session_id) {
                    Some(session) => session,
                    None => return Err(InternalError::SessionNotFound.into()),
                };

                let Some(ch) = session.channels.get_mut(&channel_id) else {
                    return Err(ChannelNotFound(channel_id).into());
                };

                ch.prefetch_count = qos.prefetch_count as u64;

                Some(basic::AMQPMethod::QosOk(basic::QosOk {}))
            }
            basic::AMQPMethod::Cancel(cancel) => {
                let mut session = match self.sessions.get_mut(&session_id) {
                    Some(session) => session,
                    None => return Err(InternalError::SessionNotFound.into()),
                };

                let Some(channel) = session.channels.get_mut(&channel_id) else {
                    return Err(ChannelNotFound(channel_id).into());
                };

                let mut consumer_metadata = self.consumer_metadata.lock().await;
                let Some(queue_name) = consumer_metadata
                    .queues
                    .remove(cancel.consumer_tag.as_str())
                else {
                    panic!("todo")
                };
                let Some(subscription) = consumer_metadata
                    .consumer_tags
                    .get_mut(&queue_name)
                    .unwrap()
                    .remove(cancel.consumer_tag.as_str())
                else {
                    panic!("todo")
                };

                channel.total_awaiting_acks_count -= subscription.awaiting_acks_count;

                let consumer_tag = cancel.consumer_tag.to_string();

                Some(basic::AMQPMethod::CancelOk(basic::CancelOk {
                    consumer_tag: consumer_tag.clone().into(),
                }))
            }
            basic::AMQPMethod::Ack(ack) => {
                let mut session = match self.sessions.get_mut(&session_id) {
                    Some(session) => session,
                    None => return Err(InternalError::SessionNotFound.into()),
                };

                let Some(channel_info) = session.channels.get_mut(&channel_id) else {
                    return Err(ChannelNotFound(channel_id).into());
                };

                let unacked = channel_info.awaiting_acks.remove(&ack.delivery_tag as &u64);
                let Some(unacked) = unacked else {
                    return Err(UnknownDeliveryTag(channel_id, ack.delivery_tag).into());
                };

                let mut consumer_metadata = self.consumer_metadata.lock().await;
                let queue_name = match consumer_metadata.queues.get(&unacked.consumer_tag) {
                    Some(queue_name) => queue_name.clone(),
                    None => {
                        return Err(InternalError::QueueNotFound("".to_string(), channel_id).into());
                    }
                };
                let subscription = consumer_metadata
                    .consumer_tags
                    .get_mut(&queue_name)
                    .unwrap()
                    .get_mut(&unacked.consumer_tag)
                    .unwrap();

                subscription.awaiting_acks_count -= 1;
                subscription.total_awaiting_acks_count -= 1;
                channel_info.total_awaiting_acks_count -= 1;
                drop(consumer_metadata);

                self.mark_queue_ready(unacked.queue.as_str()).await;

                None
            }
            // TODO Добавить обработку basic.reject, basic.cancel
            f => {
                return Err(Unsupported(format!("unsupported method: {f:?}")).into());
            }
        };

        Ok(resp)
    }

    async fn handle_bodies(
        self: Arc<Self>,
        publish: Publish,
        bodies: Vec<Vec<u8>>,
    ) -> anyhow::Result<Option<basic::AMQPMethod>> {
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

        for b in bodies {
            queue.store.push(Bytes::from(b));
        }
        drop(queue);

        for queue_name in queue_names {
            self.mark_queue_ready(&queue_name).await;
        }

        Ok(None)
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
