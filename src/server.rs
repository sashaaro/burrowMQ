use amq_protocol::protocol::basic::Publish;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

use crate::parsing::ParsingContext;
use amq_protocol::frame::{AMQPContentHeader, AMQPFrame, gen_frame, parse_frame};
use amq_protocol::protocol::connection::{AMQPMethod, OpenOk, Start, Tune};
use amq_protocol::protocol::exchange::DeclareOk;
use amq_protocol::protocol::queue::Bind;
use amq_protocol::protocol::{AMQPClass, basic, channel, exchange, queue};
use amq_protocol::types::{FieldTable, LongString, ShortString};
use bytes::{Bytes, BytesMut};
use futures_util::TryFutureExt;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

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
    exchanges: Arc<Mutex<HashMap<String, InternalExchange>>>,
    queues: Arc<Mutex<HashMap<String, InternalQueue>>>,
    sessions: Arc<Mutex<HashMap<i64, Session>>>,
    queue_bindings: Mutex<Vec<Bind>>, // Очередь ↔ Exchange
    cancel_token: CancellationToken,
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

struct InternalExchange {
    declaration: exchange::Declare,
}

struct InternalQueue {
    // TODO declaration: queue::Declare,
    queue_name: String,
    ready_vec: VecDeque<Bytes>,
    // TODO messages_ready: u64
    // TODO messages_unacknowledged: u64
    // acked: AtomicU64,
    // acked_markers: [bool; 2048],
    // marker_index: AtomicU32,
}

#[derive(Default, Clone)]
struct ConsumerSubscription {
    queue: String,
    // callback: куда доставлять сообщения
    // TODO no_ack: bool,
    // exclusive: bool,
    unacked: HashMap<i64, Bytes> // i64 - delivery_tag
}

struct UnackedMessage {
    delivery_tag: u64,
    queue: String,
    // TODO message: Bytes,
    // TODO redelivered: bool,
    // TODO properties: MessageProperties,
    consumer_tag: String,
}

struct ChannelInfo {
    id: u16,
    active_consumers: HashMap<String, ConsumerSubscription>, // String - consumer tag // TODO subscriptions list
    unacked_messages: HashMap<u64, UnackedMessage>,          // u64 - delivery tag
    delivery_tag: AtomicU64,                                 // уникален в рамках одного канала
}

struct Session {
    // TODO confirm_mode: bool,
    open_channels: Vec<ChannelInfo>,
    read: Arc<Mutex<OwnedReadHalf>>,
    write: Arc<Mutex<OwnedWriteHalf>>,
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
            queues: Arc::new(Mutex::new(HashMap::new())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            queue_bindings: Mutex::new(Vec::new()),
            cancel_token: CancellationToken::new(),
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
            
            let handle = tokio::spawn(async move {
                server.handle_session(socket, addr).await;
            });
        }
    }

    pub async fn handle_session(self: Arc<Self>, socket: TcpStream, _: std::net::SocketAddr) {
        let session_id = self.sessions.lock().await.iter().len() as i64 + 1;

        let (read, write) = socket.into_split();
        self.sessions.lock().await.insert(
            session_id,
            Session {
                // confirm_mode: false,
                open_channels: Default::default(),
                read: Arc::new(Mutex::new(read)),
                write: Arc::new(Mutex::new(write)),
            },
        );

        let that = Arc::clone(&self);
        tokio::spawn(async move {
            let that = Arc::clone(&that);
            sleep(Duration::from_secs(3)).await;
            loop {
                sleep(Duration::from_secs(2)).await;
                let sessions = that.sessions.lock().await;
                let Some(session) = sessions.get(&session_id) else {
                    drop(sessions);
                    break;
                };
                let w = Arc::clone(&session.write);
                drop(sessions);

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
                .handle_recv_buffer(session_id, &mut buf)
                .await
            {
                log::error!("error {:?}", err);
                break;
            }
        }
    }
    
    
    async fn close_session(self: Arc<Self>, session_id: i64) {
        let session = self.sessions.lock().await.remove(&session_id);
        let Some(session) = session else {
            panic!("session not found"); // TODO
        };

        let mut queues = self.queues.lock().await;
        for mut x in session.open_channels {
            for (delivery_tag, unacked_message) in x.unacked_messages {
                let Some(queue) = queues.get_mut(&unacked_message.queue) else {
                    continue;
                };
                
                let Some(subscription) = x.active_consumers.get_mut(&unacked_message.consumer_tag) else {
                    continue;
                };
                if subscription.unacked.is_empty() {
                    continue;
                }
                if subscription.unacked.len() != 1 {
                    panic!("prefetching unsupported");
                }
                
                queue.ready_vec.push_front(subscription.unacked.pop_front().unwrap())
            }
        }
    }

    async fn handle_recv_buffer(
        self: Arc<Self>,
        session_id: i64,
        buf: &mut [u8; 1024],
    ) -> anyhow::Result<()> {
        let sessions = self.sessions.lock().await;
        let session = sessions.get(&session_id).unwrap();
        let r = Arc::clone(&session.read);
        drop(sessions);

        let n = r.lock().await.read(buf).await?;

        if n == 0 {
            self.close_session(session_id).await;
            return Err(anyhow::anyhow!("connection closed"));
        }
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
            let session = sessions.get(&session_id).unwrap();
            let w = Arc::clone(&session.write);
            drop(sessions);
            let _ = w.lock().await.write_all(&buffer).await;
        } else if n > 0 {
            let buf = &buf[..n];

            let (parsing_context, frame) =
                parse_frame(ParsingContext::from(buf)).expect("invalid frame");

            let sessions = self.sessions.lock().await;
            let session = sessions.get(&session_id).unwrap();
            let w = Arc::clone(&session.write);
            drop(sessions);
            Arc::clone(&self)
                .handle_frame(&session_id, w, &buf, parsing_context, &frame)
                .await?;
        };
        Ok(())
    }

    async fn handle_frame(
        self: Arc<Self>,
        session_id: &i64,
        socket: Arc<Mutex<OwnedWriteHalf>>,
        buf: &&[u8],
        parsing_context: ParsingContext<'_>,
        frame: &AMQPFrame,
    ) -> anyhow::Result<()> {
        // log::warn!(frame:? = frame; "frame received");
        dbg!(frame);
        match &frame {
            AMQPFrame::Method(channel_id, AMQPClass::Connection(AMQPMethod::StartOk(_))) => {
                let amqp_frame = AMQPFrame::Method(
                    *channel_id,
                    AMQPClass::Connection(AMQPMethod::Tune(Tune {
                        channel_max: 10,
                        frame_max: 1024,
                        heartbeat: 10,
                    })),
                );

                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPFrame::Method(_, AMQPClass::Connection(AMQPMethod::TuneOk(tune_ok))) => {
                println!("TuneOk: {:?}", tune_ok);
            }
            AMQPFrame::Method(channel_id, AMQPClass::Connection(AMQPMethod::Open(_))) => {
                let amqp_frame = AMQPFrame::Method(
                    *channel_id,
                    AMQPClass::Connection(AMQPMethod::OpenOk(OpenOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPFrame::Method(_, AMQPClass::Exchange(exchange::AMQPMethod::Bind(_))) => {
                unimplemented!("exchange bindings unimplemented")
            }
            AMQPFrame::Method(channel_id, AMQPClass::Queue(queue::AMQPMethod::Bind(bind))) => {
                let Some(_) = self.exchanges.lock().await.get_mut(bind.exchange.as_str()) else {
                    panic!("exchange not found") // todo send error
                };
                let Some(_) = self.queues.lock().await.get_mut(bind.queue.as_str()) else {
                    panic!("queue not found") // todo send error
                };

                self.queue_bindings.lock().await.push(bind.clone());

                let amqp_frame = AMQPFrame::Method(
                    *channel_id,
                    AMQPClass::Queue(queue::AMQPMethod::BindOk(queue::BindOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPFrame::Method(
                channel_id,
                AMQPClass::Exchange(exchange::AMQPMethod::Declare(declare)),
            ) => {
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
                    *channel_id,
                    AMQPClass::Exchange(exchange::AMQPMethod::DeclareOk(DeclareOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPFrame::Method(channel_id, AMQPClass::Channel(channel::AMQPMethod::Open(_))) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(session_id).expect("Session not found");

                session.open_channels.push(ChannelInfo {
                    active_consumers: Default::default(),
                    id: *channel_id,
                    delivery_tag: 0.into(),
                    unacked_messages: Default::default(),
                });

                let amqp_frame = AMQPFrame::Method(
                    *channel_id,
                    AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPFrame::Heartbeat(channel_id) => {
                println!("Heartbeat: channel {:?} ", channel_id);
            }
            AMQPFrame::Method(
                channel_id,
                AMQPClass::Queue(queue::AMQPMethod::Declare(declare)),
            ) => {
                let mut queue_name = declare.queue.to_string();
                if queue_name.is_empty() {
                    queue_name = "test_queue".to_string();
                }

                let mut queues = self.queues.lock().await;
                if !queues.contains_key(queue_name.as_str()) {
                    queues.insert(
                        queue_name.clone(),
                        InternalQueue {
                            queue_name: queue_name.clone(),
                            ready_vec: Default::default(),
                        },
                    );
                }
                drop(queues);

                let amqp_frame = AMQPFrame::Method(
                    *channel_id,
                    AMQPClass::Queue(queue::AMQPMethod::DeclareOk(queue::DeclareOk {
                        queue: queue_name.clone().into(),
                        message_count: 0,  // сколько сообщений уже в очереди
                        consumer_count: 0, // TODO сколько потребителей подписаны на эту очередь
                    })),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPFrame::Method(
                channel_id,
                AMQPClass::Basic(basic::AMQPMethod::Publish(publish)),
            ) => {
                let shift = parsing_context.as_ptr() as usize - buf.as_ptr() as usize;

                let next_buf = &buf[shift..];

                let message = Self::extract_message(next_buf);

                if let Err(err) = message {
                    log::warn!("fail parse message: {}", err);
                    return Err(err.into());
                }
                let message = message.unwrap();

                let suit_queue_names = Arc::clone(&self).find_queues(publish).await;

                match suit_queue_names {
                    Ok(suit_queue_names) => {
                        if suit_queue_names.is_empty() && publish.mandatory {
                            let amqp_frame = AMQPFrame::Method(
                                *channel_id,
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

                        for queue_name in suit_queue_names {
                            if let Some(queue) = self.queues.lock().await.get_mut(&queue_name) {
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
                    Err(_) => {
                        // TODO
                        let amqp_frame = AMQPFrame::Method(
                            *channel_id,
                            AMQPClass::Basic(basic::AMQPMethod::Return(basic::Return {
                                reply_code: 312,
                                reply_text: "NO_ROUTE".into(),
                                exchange: publish.exchange.to_string().into(),
                                routing_key: publish.routing_key.to_string().into(),
                            })),
                        );
                        let buffer = Self::make_buffer_from_frame(&amqp_frame);
                        let _ = socket.lock().await.write_all(&buffer).await;
                    }
                }
            }
            AMQPFrame::Method(
                channel_id,
                AMQPClass::Basic(basic::AMQPMethod::Consume(consume)),
            ) => {
                let mut queues = self.queues.lock().await;
                let queue = queues.get_mut(&consume.queue.to_string());
                if queue.is_none() {
                    let amqp_frame = AMQPFrame::Method(
                        *channel_id,
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
                drop(queues);

                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(session_id).expect("Session not found");

                let consumer_tag = consume.consumer_tag.to_string();
                if consumer_tag.is_empty() {
                    // consumer_tag = Alphanumeric.sample_string(&mut rand::rng(), 8)
                }

                let ch: &mut ChannelInfo = session
                    .open_channels
                    .get_mut(*channel_id as usize - 1)
                    .expect("Channel not found");
                if !ch.active_consumers.contains_key(&consumer_tag) {
                    ch.active_consumers.insert(
                        consumer_tag.clone(),
                        ConsumerSubscription {
                            queue: consume.queue.to_string(),
                            unacked: Default::default(),
                        },
                    );
                };

                if !consume.nowait {
                    let amqp_frame = AMQPFrame::Method(
                        *channel_id,
                        AMQPClass::Basic(basic::AMQPMethod::ConsumeOk(basic::ConsumeOk {
                            consumer_tag: consumer_tag.clone().into(),
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
            AMQPFrame::Method(channel_id, AMQPClass::Basic(basic::AMQPMethod::Qos(qos))) => {
                if qos.prefetch_count != 1 {
                    panic!("prefetching unsupported")
                }

                let amqp_frame = AMQPFrame::Method(
                    *channel_id,
                    AMQPClass::Basic(basic::AMQPMethod::QosOk(basic::QosOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPFrame::Method(channel_id, AMQPClass::Basic(basic::AMQPMethod::Cancel(cancel))) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(session_id).expect("Session not found");

                let ch: &mut ChannelInfo = session
                    .open_channels
                    .get_mut(*channel_id as usize - 1)
                    .expect("channel not found");

                let canceled_consumer_tag = cancel.consumer_tag.to_string();
                ch.active_consumers.remove(&canceled_consumer_tag);

                let consumer_tag = cancel.consumer_tag.to_string();

                let amqp_frame = AMQPFrame::Method(
                    *channel_id,
                    AMQPClass::Basic(basic::AMQPMethod::CancelOk(basic::CancelOk {
                        consumer_tag: consumer_tag.clone().into(),
                    })),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            AMQPFrame::Method(channel_id, AMQPClass::Basic(basic::AMQPMethod::Ack(ack))) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(session_id).expect("Session not found");

                let ch: &mut ChannelInfo = session
                    .open_channels
                    .get_mut(*channel_id as usize - 1)
                    .expect("Channel not found");

                let Some(unacked) = ch.unacked_messages.remove(&ack.delivery_tag) else {
                    panic!("unacked message not found");
                };

                let queue_name = unacked.queue.clone();
                let mut queues = self.queues.lock().await;
                let queue = queues.get_mut(&queue_name).expect("queue not found");

                
                let sub = ch
                    .active_consumers
                    .values_mut()
                    .find(|s| s.queue == queue_name)
                    .expect("subscription not found");

                
                _ = sub.unacked.remove(ack.delivery_tag); // remove from unacked_vec
                
                drop(queues);
                drop(sessions);
                
                Arc::clone(&self)
                    .queue_process(queue_name)
                    .await;
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
        let queues = self.queues.lock().await;

        match publish.exchange.as_str() {
            "" => {
                if queues.get(publish.routing_key.as_str()).is_some() {
                    matched_queue_names.push(publish.routing_key.to_string());
                } else if publish.mandatory {
                    return Err(anyhow::anyhow!("NO_ROUTE"));
                }
            }
            _ => {
                if let Some(exchange) = self.exchanges.lock().await.get(publish.exchange.as_str()) {
                    let bindings = self.queue_bindings.lock().await;
                    match exchange.declaration.kind.clone().into() {
                        ExchangeKind::Direct => {
                            for bind in bindings.iter() {
                                if bind.routing_key == publish.routing_key
                                    && queues.contains_key(bind.queue.as_str())
                                {
                                    matched_queue_names.push(bind.queue.to_string());
                                }
                            }
                        }
                        ExchangeKind::Fanout => {
                            for bind in bindings.iter() {
                                if publish.exchange == bind.exchange
                                    && queues.contains_key(bind.queue.as_str())
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
        // TODO rework with channels

        let mut suit_subscriptions = vec![];

        let mut sessions = self.sessions.lock().await;
        for (session_id, s) in sessions.iter() {
            for (channel_index, ch) in s.open_channels.iter().enumerate() {
                for (consumer_tag, _) in ch.active_consumers.iter() {
                    // TODO remove clone
                    suit_subscriptions.push((*session_id, channel_index, consumer_tag));
                }
            }
        }

        if suit_subscriptions.is_empty() {
            log::info!(queue:? = queue_name;"no subscriptions");
            return;
        }

        // TODO choose consumer round-robin
        let selected_subscription = suit_subscriptions.first().unwrap();

        let mut queues_lock = self.queues.lock().await;
        let queue = queues_lock.get_mut(&queue_name);
        let Some(queue) = queue else {
            log::info!(queue:? = queue_name; "no queue");

            return;
        };

        let consumer_tag = selected_subscription.2.clone();
        let ch_index = selected_subscription.1;
        let session_id = selected_subscription.0;
        let session = sessions.get_mut(&session_id).unwrap();
        let channel_info: &mut ChannelInfo = session.open_channels.get_mut(ch_index).unwrap();
        let subscription = channel_info
            .active_consumers
            .get_mut(&consumer_tag)
            .expect("subscription not found");

        if subscription.unacked.len() == 1 {
            // prefetching unsupported
            log::info!(queue:? = queue_name; "prefetching unsupported");
            return;
        }
        
        let w = Arc::clone(&session.write);
        

        let message = queue.ready_vec.pop_front();
        let Some(message) = message else {
            log::info!(queue:? = queue_name; "queue empty");

            return;
        };

        subscription.unacked.push_back(message.clone());
        // let v = queue.marker_index.fetch_add(1, Ordering::Acquire);
        // if 2048 == v + 1 {
        //     // TODO stop consume
        // }
        //
        // let index = queue.unacked_vec.len() as u16;

        dbg!(String::from_utf8_lossy(&message.clone()));

        let delivery_tag = channel_info.delivery_tag.fetch_add(1, Ordering::Acquire) + 1;

        let amqp_frame = AMQPFrame::Method(
            channel_info.id,
            AMQPClass::Basic(basic::AMQPMethod::Deliver(basic::Deliver {
                consumer_tag: ShortString::from(consumer_tag.clone()), // TODO
                delivery_tag,
                redelivered: false,
                exchange: ShortString::from(""),    // TODO
                routing_key: ShortString::from(""), // TODO
            })),
        );
        let channel_id = channel_info.id;

        session
            .open_channels
            .get_mut(ch_index)
            .unwrap()
            .unacked_messages
            .insert(
                delivery_tag,
                UnackedMessage {
                    delivery_tag,
                    consumer_tag,
                    queue: queue_name,
                    // unacked_index: index,
                    // TODO message: message.clone(),
                },
            );

        drop(sessions); // release lock
        drop(queues_lock); // release lock explicitly

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

        let amqp_frame = AMQPFrame::Body(channel_id, message.clone().into()); // TODO stop clone
        buffer.extend(Self::make_buffer_from_frame(&amqp_frame));

        w.lock().await.write_all(&buffer).await.unwrap();
    }

    fn make_buffer_from_frame(frame: &AMQPFrame) -> Vec<u8> {
        let mut buffer = vec![0u8; 1024];
        gen_frame(frame)(buffer.as_mut_slice().into()).unwrap();
        Self::trim_right_bytes(&mut buffer);
        buffer
    }

    fn trim_right_bytes(buffer: &mut Vec<u8>) {
        while let Some(&0) = buffer.last() {
            buffer.pop();
        }
    }
}
