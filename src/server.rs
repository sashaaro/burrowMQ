use amq_protocol::protocol::basic::Publish;
use dashmap;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

use crate::models::{
    ChannelInfo, ConsumerSubscription, InternalExchange, InternalQueue, Session, UnackedMessage,
};
use crate::parsing::ParsingContext;
use amq_protocol::frame::{AMQPContentHeader, AMQPFrame, gen_frame, parse_frame};
use amq_protocol::protocol::connection::{AMQPMethod, OpenOk, Start, Tune};
use amq_protocol::protocol::exchange::DeclareOk;
use amq_protocol::protocol::queue::Bind;
use amq_protocol::protocol::{AMQPClass, basic, channel, exchange, queue};
use amq_protocol::types::{FieldTable, LongString, ShortString};
use bytes::Bytes;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(thiserror::Error, Debug)]
enum InternalError {
    // #[error("exchange not found")]
    // ExchangeNotFound,
    // #[error("queue not found")]
    // QueueNotFound,
    #[error("invalid frame")]
    InvalidFrame,
}

pub struct BurrowMQServer {
    pub(crate) exchanges: Arc<Mutex<HashMap<String, InternalExchange>>>,
    pub(crate) queues: Arc<dashmap::DashMap<String, InternalQueue>>,
    pub(crate) sessions: Arc<Mutex<HashMap<i64, Session>>>,
    pub(crate) queue_bindings: Mutex<Vec<Bind>>, // Очередь ↔ Exchange
}

const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeKind {
    Direct,
    Fanout,
    Headers,
    Topic,
}

impl From<ShortString> for ExchangeKind {
    fn from(val: ShortString) -> Self {
        match val.as_str() {
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            "headers" => ExchangeKind::Headers,
            "topic" => ExchangeKind::Topic,
            _ => ExchangeKind::Direct,
        }
    }
}

impl Default for BurrowMQServer {
    fn default() -> Self {
        Self::new()
    }
}

impl BurrowMQServer {
    pub fn new() -> Self {
        Self {
            exchanges: Arc::new(Mutex::new(HashMap::new())),
            queues: Arc::new(dashmap::DashMap::new()),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            queue_bindings: Mutex::new(Vec::new()),
        }
    }

    pub async fn start_forever(self, port: u16) -> anyhow::Result<()> {
        let addr = format!("0.0.0.0:{}", port);
        let listener = TcpListener::bind(&addr).await?;
        println!("Listening on {}", addr);

        let server = Arc::new(self);

        loop {
            let (socket, addr) = listener.accept().await?;
            log::info!("New client from {:?}", addr);
            let server = Arc::clone(&server);

            _ = tokio::spawn(async move {
                server.handle_session(socket, addr).await;
            });
        }
    }

    pub async fn handle_session(self: Arc<Self>, socket: TcpStream, _: std::net::SocketAddr) {
        let session_id = self.sessions.lock().await.iter().len() as i64 + 1;

        let (read, write) = socket.into_split();

        let w = Arc::new(Mutex::new(write));
        self.sessions.lock().await.insert(
            session_id,
            Session {
                // confirm_mode: false,
                channels: Default::default(),
                read: Arc::new(Mutex::new(read)),
                write: Arc::clone(&w),
            },
        );

        let w = Arc::downgrade(&w);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(500)).await;
                let Some(w) = w.upgrade() else { break };

                let amqp_frame = AMQPFrame::Heartbeat(0);
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                w.lock()
                    .await
                    .write_all(&buffer)
                    .await
                    .expect("failed to send heartbeat");
            }
        });

        let mut buf = [0u8; 1024];
        loop {
            if let Err(err) = Arc::clone(&self)
                .handle_recv_buffer(&session_id, &mut buf)
                .await
            {
                log::error!("error {:?}", err);
                break;
            }
        }
    }

    async fn handle_recv_buffer(
        self: Arc<Self>,
        session_id: &i64,
        buf: &mut [u8; 1024],
    ) -> anyhow::Result<()> {
        let sessions = self.sessions.lock().await;
        let session = sessions.get(session_id).unwrap();
        let r = Arc::clone(&session.read);
        drop(sessions);

        let n = r.lock().await.read(buf).await?;

        // if n == 0 {
        //     return bail!("connection closed");
        // }
        if buf.starts_with(PROTOCOL_HEADER) {
            log::trace!("received: {:?}", &buf[..n]);

            let start = Start {
                version_major: 0,
                version_minor: 9,
                server_properties: FieldTable::default(), // можно добавить info о сервере
                mechanisms: LongString::from("PLAIN"),
                locales: LongString::from("en_US"),
            };

            let amqp_frame = AMQPFrame::Method(0, AMQPClass::Connection(AMQPMethod::Start(start)));

            let buffer = Self::make_buffer_from_frame(&amqp_frame);

            let sessions = self.sessions.lock().await;
            let session = sessions.get(session_id).unwrap();
            let w = Arc::clone(&session.write);
            drop(sessions);
            let _ = w.lock().await.write_all(&buffer).await;
        } else if n > 0 {
            let buf = &buf[..n];

            let (parsing_context, frame) =
                parse_frame(ParsingContext::from(buf)).expect("invalid frame");

            let sessions = self.sessions.lock().await;
            let session = sessions.get(session_id).unwrap();
            let w = Arc::clone(&session.write);
            drop(sessions);

            if let AMQPFrame::Heartbeat(channel_id) = frame {
                println!("Heartbeat: channel {:?} ", channel_id);
                return Ok(());
            } else if let AMQPFrame::Method(channel_id, method) = frame {
                Arc::clone(&self)
                    .handle_frame(
                        session_id,
                        channel_id as u16,
                        w,
                        &buf,
                        parsing_context,
                        method,
                    )
                    .await?;
            } else {
                unimplemented!("unimplemented frame: {frame:?}")
            }
        };
        Ok(())
    }

    async fn handle_frame(
        self: Arc<Self>,
        session_id: &i64,
        channel_id: u16,
        socket: Arc<Mutex<OwnedWriteHalf>>,
        buf: &&[u8],
        parsing_context: ParsingContext<'_>,
        frame: AMQPClass,
    ) -> anyhow::Result<()> {
        // log::warn!(frame:? = frame; "frame received");
        match frame {
            AMQPClass::Connection(AMQPMethod::StartOk(_)) => {
                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Connection(AMQPMethod::Tune(Tune {
                        channel_max: 10,
                        frame_max: 1024,
                        heartbeat: 10,
                    })),
                );

                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPClass::Connection(AMQPMethod::TuneOk(tune_ok)) => {
                println!("TuneOk: {:?}", tune_ok);
            }
            AMQPClass::Connection(AMQPMethod::Open(_)) => {
                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Connection(AMQPMethod::OpenOk(OpenOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPClass::Exchange(exchange::AMQPMethod::Bind(_)) => {
                unimplemented!("exchange bindings unimplemented")
            }
            AMQPClass::Queue(queue_method) => {
                self.handle_queue_method(channel_id, socket, &queue_method)
                    .await?;
            }
            AMQPClass::Exchange(exchange::AMQPMethod::Declare(declare)) => {
                let mut exchanges = self.exchanges.lock().await;

                if !exchanges.contains_key(declare.exchange.as_str()) {
                    exchanges.insert(
                        declare.exchange.to_string(),
                        InternalExchange {
                            declaration: declare.clone(),
                        },
                    );
                }
                drop(exchanges);

                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Exchange(exchange::AMQPMethod::DeclareOk(DeclareOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPClass::Channel(channel::AMQPMethod::Open(_)) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(session_id).expect("Session not found");

                session.channels.push(ChannelInfo {
                    active_consumers: Default::default(),
                    id: channel_id,
                    delivery_tag: 0.into(),
                    unacked_messages: Default::default(),
                });

                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPClass::Basic(basic::AMQPMethod::Publish(publish)) => {
                let shift = parsing_context.as_ptr() as usize - buf.as_ptr() as usize;

                let message = Self::extract_message(&buf[shift..]).map_err(|err| {
                    log::warn!("fail parse message: {}", err);
                    return err;
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
            AMQPClass::Basic(basic::AMQPMethod::Consume(consume)) => {
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
                let session = sessions.get_mut(session_id).expect("Session not found");

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
            AMQPClass::Basic(basic::AMQPMethod::Qos(qos)) => {
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
            AMQPClass::Basic(basic::AMQPMethod::Cancel(cancel)) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(session_id).expect("Session not found");

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
            AMQPClass::Basic(basic::AMQPMethod::Ack(ack)) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(session_id).expect("Session not found");

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
            AMQPClass::Channel(channel::AMQPMethod::Close(close)) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(session_id).expect("Session not found");

                session.channels.retain(|c| c.id != channel_id);
                // let mut sessions = self.sessions.lock().await;
                // let session = sessions.get_mut(session_id).expect("Session not found");
                // session.channels = session.channels.iter().filter(|c| c.id != channel_id).cloned().collect();

                drop(sessions);

                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Channel(channel::AMQPMethod::CloseOk(channel::CloseOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            // TODO Добавить обработку basic.reject, basic.cancel
            _ => {
                panic!("unsupported frame");
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

    async fn queue_process(self: Arc<Self>, queue_name: String) {
        let Some(mut queue) = self.queues.get_mut(&queue_name) else {
            log::info!(queue:? = queue_name; "no queue");
            return;
        };
        if queue.unacked_vec.len() == 1 {
            // prefetching unsupported
            log::info!(queue:? = queue_name; "prefetching unsupported");
            return;
        }
        if queue.ready_vec.is_empty() {
            log::info!(queue:? = queue_name; "queue empty");
            return;
        };

        let message = queue.ready_vec.pop_front().unwrap();
        queue.unacked_vec.push_back(message.clone());
        drop(queue); // release lock explicitly

        let mut suit_subscriptions = vec![];
        let mut sessions = self.sessions.lock().await;
        for (session_id, s) in sessions.iter() {
            for (_, ch) in s.channels.iter().enumerate() {
                for (consumer_tag, _) in ch.active_consumers.iter() {
                    // TODO remove clone
                    suit_subscriptions.push((*session_id, ch.id, consumer_tag.to_string()));
                }
            }
        }

        if suit_subscriptions.is_empty() {
            log::info!(queue:? = queue_name;"no subscriptions");
            return;
        }

        let selected_subscription = suit_subscriptions.get(0).unwrap().to_owned(); // TODO choose consumer round-robin
        let (session_id, channel_id, consumer_tag) = selected_subscription;

        let session = sessions.get_mut(&session_id).unwrap();
        let channel_info: &ChannelInfo = session
            .channels
            .iter()
            .find(|c| c.id == channel_id)
            .unwrap();
        let delivery_tag = channel_info.delivery_tag.fetch_add(1, Ordering::Acquire) + 1;

        let amqp_frame = AMQPFrame::Method(
            channel_info.id,
            AMQPClass::Basic(basic::AMQPMethod::Deliver(basic::Deliver {
                consumer_tag: ShortString::from(consumer_tag), // TODO
                delivery_tag,
                redelivered: false,
                exchange: ShortString::from(""),    // TODO
                routing_key: ShortString::from(""), // TODO
            })),
        );

        session
            .channels
            .get_mut(channel_id as usize)
            .unwrap()
            .unacked_messages
            .insert(
                delivery_tag,
                UnackedMessage {
                    delivery_tag,
                    queue: queue_name,
                    // unacked_index: index,
                    // TODO message: message.clone(),
                },
            );

        let mut buffer = Self::make_buffer_from_frame(&amqp_frame);

        let amqp_frame = AMQPFrame::Header(
            channel_id,
            60_u16,
            Box::new(AMQPContentHeader {
                class_id: 60,
                body_size: message.len() as u64,
                properties: Default::default(),
            }),
        );
        buffer.extend(Self::make_buffer_from_frame(&amqp_frame));

        let amqp_frame = AMQPFrame::Body(channel_id, message.into()); // TODO new msg allocation
        buffer.extend(Self::make_buffer_from_frame(&amqp_frame));

        session.write.lock().await.write_all(&buffer).await.unwrap();
        // self.sessions.lock().await.get(&session_id).unwrap().write.lock().await.write_all(&buffer).await.unwrap();
    }

    pub(crate) fn make_buffer_from_frame(frame: &AMQPFrame) -> Vec<u8> {
        let buffer = Vec::with_capacity(1024);
        gen_frame(frame)(buffer.into())
            .expect("failed to generate frame")
            .write
    }
}

pub(crate) fn gen_random_queue_name() -> String {
    let mut rng = rand::thread_rng();
    let mut queue_name = String::with_capacity(10);
    for _ in 0..10 {
        queue_name.push(rng.gen_range('a'..='z'));
    }
    queue_name
}
