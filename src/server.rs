use crate::defer::ScopeCall;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

use crate::defer;
use crate::models::InternalError::{ChannelNotFound, Unsupported};
use crate::models::{
    InternalError, InternalExchange, InternalQueue, Session, Subscription, UnackedMessage,
};
use crate::parsing::ParsingContext;
use crate::queue::QueueTrait;
use crate::utils::make_buffer_from_frame;
use amq_protocol::frame::{AMQPContentHeader, AMQPFrame, parse_frame};
use amq_protocol::protocol::connection::{AMQPMethod, Start};
use amq_protocol::protocol::constants::FRAME_MIN_SIZE;
use amq_protocol::protocol::queue::Bind;
use amq_protocol::protocol::{AMQPClass, basic, channel};
use amq_protocol::types::{FieldTable, LongString, ShortString};
use bytes::Bytes;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Default)]
pub struct ConsumerMetadata {
    pub(crate) consumer_tags: HashMap<String, HashMap<String, Subscription>>, // Queue ↔ ConsumerTag Consumer
    pub(crate) queues: HashMap<String, String>,                               // ConsumerTag ↔ Queue
}

pub struct BurrowMQServer<Q: QueueTrait<Bytes> + Default + 'static> {
    pub(crate) exchanges: Mutex<HashMap<String, InternalExchange>>,
    pub(crate) queues: dashmap::DashMap<String, Arc<InternalQueue<Q>>>,
    pub(crate) sessions: dashmap::DashMap<u64, Session>,
    pub(crate) consumer_metadata: Mutex<ConsumerMetadata>,
    pub(crate) queue_bindings: Mutex<Vec<Bind>>, // Queue ↔ Exchange
    pub session_inc: AtomicU64,
    pub queue_inc: AtomicU64,
    pub consumer_inc: AtomicU64,

    pub handlers: Mutex<HashMap<String, JoinHandle<()>>>, // By Queue
}

const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";

impl<Q: QueueTrait<Bytes> + Default> Default for BurrowMQServer<Q> {
    fn default() -> Self {
        Self::new()
    }
}

pub type Responder = Box<dyn FnOnce(AMQPClass) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

impl<Q: QueueTrait<Bytes> + Default> BurrowMQServer<Q> {
    pub fn new() -> Self {
        Self {
            exchanges: Mutex::new(HashMap::new()),
            queues: dashmap::DashMap::new(),
            sessions: Default::default(),
            queue_bindings: Mutex::new(Vec::new()),
            session_inc: AtomicU64::new(0),
            queue_inc: AtomicU64::new(0),
            consumer_inc: AtomicU64::new(0),
            consumer_metadata: Default::default(),
            handlers: Default::default(),
        }
    }

    // TODO graceful shutdown
    pub async fn start_forever(self, port: u16) -> anyhow::Result<()> {
        let addr = format!("0.0.0.0:{}", port);
        let listener = TcpListener::bind(&addr).await?;
        log::info!("Listening on {}", addr);

        let server = Arc::new(self);

        loop {
            let (socket, addr) = listener.accept().await?;
            log::info!("New client from {:?}", addr);
            let server = Arc::clone(&server);

            tokio::spawn(async move {
                server.handle_session(socket, addr).await;
            });
        }
    }

    pub fn start_heartbeat(self: Arc<Self>, session_id: u64, write: Weak<Mutex<OwnedWriteHalf>>) {
        let this = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(30_000)).await;
                let Some(w) = write.upgrade() else { break };

                let amqp_frame = AMQPFrame::Heartbeat(0);
                let buffer = make_buffer_from_frame(&amqp_frame).expect("failed to make buffer"); // TODO
                if let Err(err) = w.lock().await.write_all(&buffer).await {
                    log::warn!(err:? = err; "failed to send heartbeat");
                    break;
                }
            }

            this.sessions.remove(&session_id);
        });
    }

    pub async fn handle_session(self: Arc<Self>, socket: TcpStream, _: std::net::SocketAddr) {
        let session_id = self.session_inc.fetch_add(1, Ordering::Release) + 1;

        let (mut read, write) = socket.into_split();

        let w = Arc::new(Mutex::new(write));
        Arc::clone(&self).start_heartbeat(session_id, Arc::downgrade(&w));

        self.sessions.insert(
            session_id,
            Session {
                // confirm_mode: false,
                channels: Default::default(),
                write: Arc::clone(&w),
            },
        );

        let mut buf = [0u8; FRAME_MIN_SIZE as usize];

        let this = Arc::clone(&self);
        let close = async || {
            Arc::clone(&w)
                .lock()
                .await
                .shutdown()
                .await
                .expect("fail shutdown");

            if let Some(session) = this.sessions.get(&session_id) {
                let channel_ids: Vec<u16> = session.channels.iter().map(|(_, c)| c.id).collect();
                for channel_id in channel_ids {
                    this.close_channel(session_id, channel_id)
                        .await
                        .expect("fail close channel");
                }
            }

            self.sessions.remove(&session_id);
        };

        loop {
            let n = match read.read(&mut buf).await {
                Ok(n) => n,
                Err(err) => {
                    log::error!("{}", err);
                    break;
                }
            };
            if n == 0 {
                log::info!("session {} closed", session_id);

                self.sessions.remove(&session_id);
                break;
            }
            let result = Arc::clone(&self)
                .handle_recv_buffer(session_id, &buf, n)
                .await;

            if let Err(err) = result {
                // TODO close channels

                match err.downcast_ref::<InternalError>() {
                    None => {
                        _ = close().await;
                        log::error!("error {:?}", err);
                        break;
                    }
                    Some(err) => {
                        match err {
                            InternalError::ChannelNotFound(_) => {
                                // TODO  send
                                // reply_code: 501
                                // reply_text: "FRAME_ERROR - invalid frame received"
                                log::error!("{}", err);
                                _ = close().await;
                            }
                            InternalError::InvalidFrame => {
                                log::error!("{}", err);
                                _ = close().await;
                            }
                            InternalError::UnknownDeliveryTag(channel_id, delivery_tag) => {
                                log::error!("{}", err);
                                let responder = self.create_responder(*channel_id, session_id);
                                responder(self.close_channel_response(
                                    406,
                                    &format!(
                                        "PRECONDITION_FAILED - unknown delivery tag '{}'",
                                        delivery_tag
                                    ),
                                    60,
                                    80,
                                ))
                                .await;
                                let _ = self.close_channel(session_id, *channel_id).await;
                            }
                            InternalError::SessionNotFound | InternalError::Unsupported(_) => {
                                log::warn!("{}", err);
                                _ = close().await;
                            }
                            InternalError::ExchangeNotFound(_) => {
                                log::warn!("{}", err);

                                // TODO not_found response
                                // queue::AMQPMethod::Close(channel::Close {
                                //     reply_code: 404,
                                //     reply_text: ShortString::from("Exchange not found"),
                                //     class_id: 50,    // Queue class
                                //     method_id: 20,   // Bind method
                                // }));
                            }
                            InternalError::QueueNotFound(queue, channel_id) => {
                                let responder = self.create_responder(*channel_id, session_id);
                                responder(self.close_channel_response(
                                    404,
                                    &format!("NOT FOUND - no queue '{}'", queue),
                                    50,
                                    30,
                                ))
                                .await;
                                let _ = self.close_channel(session_id, *channel_id).await;

                                log::warn!("{}", err);
                            }
                        }

                        break;
                    }
                }
            }
        }
    }

    fn close_channel_response(
        &self,
        reply_code: u16,
        reply_text: &str,
        class_id: u16,
        method_id: u16,
    ) -> AMQPClass {
        AMQPClass::Channel(channel::AMQPMethod::Close(channel::Close {
            reply_code,
            reply_text: reply_text.into(),
            class_id,
            method_id,
        }))
    }

    fn get_writer(&self, session_id: u64) -> anyhow::Result<Arc<Mutex<OwnedWriteHalf>>> {
        let session = match self.sessions.get(&session_id) {
            Some(session) => session,
            None => return Err(InternalError::SessionNotFound.into()),
        };
        let w = Arc::clone(&session.write);
        drop(session);

        Ok(w)
    }

    async fn handle_recv_buffer(
        self: Arc<Self>,
        session_id: u64,
        receive_buffer: &[u8; FRAME_MIN_SIZE as usize],
        n: usize,
    ) -> anyhow::Result<()> {
        if receive_buffer.starts_with(PROTOCOL_HEADER) {
            let start = Start {
                version_major: 0,
                version_minor: 9,
                server_properties: FieldTable::default(), // можно добавить info о сервере
                mechanisms: LongString::from("PLAIN"),
                locales: LongString::from("en_US"),
            };

            let amqp_frame = AMQPFrame::Method(0, AMQPClass::Connection(AMQPMethod::Start(start)));

            let buffer = make_buffer_from_frame(&amqp_frame)?;
            let _ = self
                .get_writer(session_id)?
                .lock()
                .await
                .write_all(&buffer)
                .await;
        } else {
            let buf = &receive_buffer[..n];

            let mut parsing_context = ParsingContext::from(buf);

            loop {
                let result = parse_frame(parsing_context);
                let Ok((local_parsing_context, frame)) = result else {
                    //log::info!("parse frame brake");
                    break;
                };

                parsing_context = local_parsing_context.clone();

                match frame {
                    AMQPFrame::Heartbeat(_channel_id) => {
                        log::trace!("→ received heartbeat");
                    }
                    AMQPFrame::Method(channel_id, method) => {
                        log::trace!(frame:? = method, channel_id:? = channel_id, size:? = n; "→ received");

                        // let is_publish = matches!(method, AMQPClass::Basic(basic::AMQPMethod::Publish(_)));

                        Arc::clone(&self)
                            .handle_frame(session_id, channel_id, local_parsing_context, method)
                            .await?;
                    }
                    AMQPFrame::Body(_, _) | AMQPFrame::Header(_, _, _) => {} // skip
                    frame => {
                        return Err(Unsupported(format!("unsupported method: {frame:?}")).into());
                    }
                }
            }
        };
        Ok(())
    }

    async fn handle_frame(
        self: Arc<Self>,
        session_id: u64,
        channel_id: u16,
        parsing_context: ParsingContext<'_>,
        frame: AMQPClass,
    ) -> anyhow::Result<()> {
        let amqp_frame: Option<AMQPFrame> = match frame {
            AMQPClass::Connection(connection_method) => {
                let resp = self.handle_connection_method(connection_method).await?;
                resp.map(|resp| AMQPFrame::Method(channel_id, AMQPClass::Connection(resp)))
            }
            AMQPClass::Basic(basic_method) => {
                let responder = self.create_responder(channel_id, session_id);

                let resp = Arc::clone(&self)
                    .handle_basic_method(
                        channel_id,
                        session_id,
                        responder,
                        basic_method,
                        parsing_context,
                    )
                    .await?;

                resp.map(|resp| AMQPFrame::Method(channel_id, AMQPClass::Basic(resp)))
            }
            AMQPClass::Channel(channel_method) => {
                let resp = self
                    .handle_channel_method(channel_id, session_id, channel_method)
                    .await?;
                Some(AMQPFrame::Method(channel_id, AMQPClass::Channel(resp)))
            }
            AMQPClass::Queue(queue_method) => {
                let resp = Arc::clone(&self)
                    .handle_queue_method(channel_id, queue_method)
                    .await?;
                Some(AMQPFrame::Method(channel_id, AMQPClass::Queue(resp)))
            }
            AMQPClass::Exchange(exchange_method) => {
                let resp = Arc::clone(&self)
                    .handle_exchange_method(exchange_method)
                    .await?;
                Some(AMQPFrame::Method(channel_id, AMQPClass::Exchange(resp)))
            }
            unsupported_frame => {
                return Err(
                    Unsupported(format!("unsupported frame: {:?}", unsupported_frame)).into(),
                );
            }
        };

        if let Some(amqp_frame) = amqp_frame {
            // TODO self.response(amqp_frame).await;

            log::trace!(frame:? = amqp_frame, channel_id:? = channel_id; "← sent response");

            let buffer = make_buffer_from_frame(&amqp_frame)?;
            self.get_writer(session_id)?
                .lock()
                .await
                .write_all(&buffer)
                .await?;
        };
        Ok(())
    }

    fn create_responder(&self, channel_id: u16, session_id: u64) -> Responder {
        let w = self.get_writer(session_id).expect("fail take writer");
        Box::new(move |resp: AMQPClass| {
            Box::pin(async move {
                let amqp_frame = AMQPFrame::Method(channel_id, resp);
                let buffer = make_buffer_from_frame(&amqp_frame).expect("failed to make buffer"); // TODO
                let _ = w.lock().await.write_all(&buffer).await;
            })
        })
    }

    pub(crate) async fn mark_queue_ready(&self, queue_name: &str) {
        let Some(queue) = self.queues.get(queue_name) else {
            log::info!(queue:? = queue_name; "⏹ stopped processing: queue not found");
            return;
        };

        queue.is_ready.store(true, Ordering::Release);
        queue.notify.notify_one();
    }

    pub(crate) async fn listen_queue_ready(self: Arc<Self>, queue: Arc<InternalQueue<Q>>) {
        let queue = Arc::clone(&queue);

        let queue_name = queue.queue_name.clone();
        let this = Arc::clone(&self);
        let handler = tokio::spawn(async move {
            'outer: loop {
                if queue.is_ready.load(Ordering::Acquire) {
                } else {
                    queue.notify.notified().await;
                }

                loop {
                    let res = self.process_queue(&*queue).await;

                    match res {
                        Err(err) => {
                            log::error!(err:? = err, queue:? = queue.queue_name; "fail process queue");
                            break 'outer;
                        }
                        Ok(true) => continue,
                        Ok(false) => {
                            break;
                        }
                    }
                }

                queue.is_ready.store(false, Ordering::Release);
            }
        });

        this.handlers.lock().await.insert(queue_name, handler);
    }

    pub(crate) async fn process_queue(&self, queue: &InternalQueue<Q>) -> anyhow::Result<bool> {
        let queue_name = &queue.queue_name;

        let mut consumer_metadata = self.consumer_metadata.lock().await;
        let Some(subscriptions) = consumer_metadata.consumer_tags.get_mut(queue_name) else {
            log::info!(queue:? = queue_name;"⏹ stopped processing: no consumers");
            return Ok(false);
        };

        if subscriptions.is_empty() {
            log::info!(queue:? = queue_name;"⏹ stopped processing: no consumers");
            return Ok(false);
        }

        let mut list = subscriptions
            .values_mut()
            .collect::<Vec<&mut Subscription>>();

        list.sort_by(|a, b| a.internal_id.cmp(&b.internal_id));
        let len = list.len();

        let subscription = list
            .get_mut(
                num_integer::div_rem(queue.consumed.load(Ordering::Acquire), len as u64).1 as usize,
            ) // распределяет сообщения между консьюмерами, подписанными на одну очередь, по принципу "по кругу" (round-robin)
            .map(|v| v.deref_mut());

        let Some(subscription) = subscription else {
            log::info!(queue:? = queue_name; "⏹ stopped processing: no match consumer");
            return Ok(false);
        };

        let Some(mut session) = self.sessions.get_mut(&subscription.session_id) else {
            return Err(InternalError::SessionNotFound.into());
        };

        let Some(channel_info) = session.channels.get_mut(&subscription.channel_id) else {
            return Err(ChannelNotFound(subscription.channel_id).into());
        };

        let message = queue.store.pop();
        let Some(message) = message else {
            log::info!(queue:? = queue_name; "⏹ stopped processing: queue empty");
            return Ok(false);
        };

        log::trace!(consumer_tag:? = subscription.consumer_tag, channel_id:? = subscription.channel_id; "consumer selected");

        let _ = queue.consumed.fetch_add(1, Ordering::AcqRel) + 1;

        let delivery_tag = channel_info.delivery_tag.fetch_add(1, Ordering::Acquire) + 1;

        if channel_info.awaiting_acks.contains_key(&delivery_tag) {
            panic!("delivery tag already in use");
        }

        let unacked_message = UnackedMessage {
            delivery_tag,
            queue: queue_name.to_owned(),
            message: message.clone(),
            consumer_tag: subscription.consumer_tag.to_owned(),
        };

        if !subscription.no_ack {
            channel_info
                .awaiting_acks
                .insert(delivery_tag, unacked_message); // TODO after write_all
        }

        subscription.awaiting_acks_count += 1;
        subscription.total_awaiting_acks_count += 1;
        channel_info.total_awaiting_acks_count += 1;

        let amqp_frame = AMQPFrame::Method(
            channel_info.id,
            AMQPClass::Basic(basic::AMQPMethod::Deliver(basic::Deliver {
                consumer_tag: ShortString::from(subscription.consumer_tag.as_str()),
                delivery_tag,
                redelivered: false,
                exchange: ShortString::from(""),    // TODO
                routing_key: ShortString::from(""), // TODO
            })),
        );

        let mut buffer = make_buffer_from_frame(&amqp_frame)?;

        let amqp_frame = AMQPFrame::Header(
            subscription.channel_id,
            60_u16,
            Box::new(AMQPContentHeader {
                class_id: 60,
                body_size: message.len() as u64,
                properties: Default::default(),
            }),
        );
        buffer.extend(make_buffer_from_frame(&amqp_frame)?);

        let amqp_frame = AMQPFrame::Body(subscription.channel_id, message.into());
        buffer.extend(make_buffer_from_frame(&amqp_frame)?);

        // TODO remove duplicated code write & log
        session.write.lock().await.write_all(&buffer).await?;

        log::trace!(frame:? = amqp_frame, channel_id:? = subscription.channel_id; "← sent message");

        Ok(true)
    }
}
