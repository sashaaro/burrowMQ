use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

use crate::models::{ChannelInfo, InternalExchange, InternalQueue, Session, UnackedMessage};
use crate::parsing::ParsingContext;
use crate::utils::make_buffer_from_frame;
use amq_protocol::frame::{AMQPContentHeader, AMQPFrame, parse_frame};
use amq_protocol::protocol::connection::{AMQPMethod, Start};
use amq_protocol::protocol::queue::Bind;
use amq_protocol::protocol::{AMQPClass, basic};
use amq_protocol::types::{FieldTable, LongString, ShortString};
use bytes::Bytes;
use futures::executor::block_on;
use tokio::sync::Mutex;

pub trait QueueTrait<T>: Send + Sync {
    fn push(&self, item: T);
    fn pop(&self) -> Option<T>;
}

pub struct BurrowMQServer<Q: QueueTrait<Bytes> + Default + 'static> {
    pub(crate) exchanges: Mutex<HashMap<String, InternalExchange>>,
    pub(crate) queues: dashmap::DashMap<String, Arc<InternalQueue<Q>>>,
    pub(crate) sessions: Mutex<HashMap<u64, Session>>,
    pub(crate) queue_bindings: Mutex<Vec<Bind>>, // Очередь ↔ Exchange
    pub session_inc: AtomicU64,
}

const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";

impl<Q: QueueTrait<Bytes> + Default> Default for BurrowMQServer<Q> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Q: QueueTrait<Bytes> + Default> BurrowMQServer<Q> {
    pub fn new() -> Self {
        Self {
            exchanges: Mutex::new(HashMap::new()),
            queues: dashmap::DashMap::new(),
            sessions: Mutex::new(HashMap::new()),
            queue_bindings: Mutex::new(Vec::new()),
            session_inc: AtomicU64::new(1),
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

    pub fn start_heartbeat(&self, write: Weak<Mutex<OwnedWriteHalf>>) {
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(500)).await;
                let Some(w) = write.upgrade() else { break };

                let amqp_frame = AMQPFrame::Heartbeat(0);
                let buffer = make_buffer_from_frame(&amqp_frame);
                w.lock()
                    .await
                    .write_all(&buffer)
                    .await
                    .expect("failed to send heartbeat");
            }
        });
    }

    pub async fn handle_session(self: Arc<Self>, socket: TcpStream, _: std::net::SocketAddr) {
        let session_id = self.session_inc.fetch_add(1, Ordering::Release) + 1;

        let (read, write) = socket.into_split();

        let w = Arc::new(Mutex::new(write));
        self.start_heartbeat(Arc::downgrade(&w));

        self.sessions.lock().await.insert(
            session_id,
            Session {
                // confirm_mode: false,
                channels: Default::default(),
                read: Arc::new(Mutex::new(read)),
                write: Arc::clone(&w),
            },
        );

        let mut buf = [0u8; 1024];
        loop {
            let result = Arc::clone(&self)
                .handle_recv_buffer(session_id, &mut buf)
                .await;

            if let Err(err) = result {
                log::error!("error {:?}", err);
                break;
            }
        }
    }

    async fn handle_recv_buffer(
        self: Arc<Self>,
        session_id: u64,
        buf: &mut [u8; 1024],
    ) -> anyhow::Result<()> {
        let sessions = self.sessions.lock().await;
        let session = sessions.get(&session_id).unwrap();
        let r = Arc::clone(&session.read);
        drop(sessions);

        let n = r.lock().await.read(buf).await?;

        // if n == 0 {
        //     return bail!("connection closed");
        // }
        if buf.starts_with(PROTOCOL_HEADER) {
            let start = Start {
                version_major: 0,
                version_minor: 9,
                server_properties: FieldTable::default(), // можно добавить info о сервере
                mechanisms: LongString::from("PLAIN"),
                locales: LongString::from("en_US"),
            };

            let amqp_frame = AMQPFrame::Method(0, AMQPClass::Connection(AMQPMethod::Start(start)));

            let buffer = make_buffer_from_frame(&amqp_frame);

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
            let w = Arc::clone(&sessions.get(&session_id).unwrap().write);
            drop(sessions);

            if let AMQPFrame::Heartbeat(_channel_id) = frame {
                log::trace!("→ received heartbeat");
                return Ok(());
            } else if let AMQPFrame::Method(channel_id, method) = frame {
                log::trace!(frame:? = method, channel_id:? = channel_id; "→ received");
                Arc::clone(&self)
                    .handle_frame(session_id, channel_id, w, &buf, parsing_context, method)
                    .await?;
            } else {
                unimplemented!("unimplemented frame: {frame:?}")
            }
        };
        Ok(())
    }

    async fn handle_frame(
        self: Arc<Self>,
        session_id: u64,
        channel_id: u16,
        socket: Arc<Mutex<OwnedWriteHalf>>,
        buf: &&[u8],
        parsing_context: ParsingContext<'_>,
        frame: AMQPClass,
    ) -> anyhow::Result<()> {
        let amqp_frame: Option<AMQPFrame> = match frame {
            AMQPClass::Connection(connection_method) => {
                let resp = self.handle_connection_method(connection_method).await?;
                resp.map(|resp| AMQPFrame::Method(channel_id, AMQPClass::Connection(resp)))
            }
            AMQPClass::Basic(basic_method) => {
                let socket2 = Arc::clone(&socket);
                let responder = (|channel_id: u16| {
                    move |resp: AMQPClass| {
                        block_on(async {
                            let amqp_frame = AMQPFrame::Method(channel_id, resp);
                            let buffer = make_buffer_from_frame(&amqp_frame);
                            let _ = socket2.lock().await.write_all(&buffer).await;
                        })
                    }
                })(channel_id);

                let resp = self
                    .handle_basic_method(
                        channel_id,
                        session_id,
                        Box::new(responder),
                        basic_method,
                        parsing_context,
                        buf,
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
                let resp = self.handle_queue_method(queue_method).await?;
                Some(AMQPFrame::Method(channel_id, AMQPClass::Queue(resp)))
            }
            AMQPClass::Exchange(exchange_method) => {
                let resp = self.handle_exchange_method(exchange_method).await?;
                Some(AMQPFrame::Method(channel_id, AMQPClass::Exchange(resp)))
            }
            _ => {
                panic!("unsupported frame");
            }
        };

        if let Some(amqp_frame) = amqp_frame {
            // TODO self.response(amqp_frame).await;

            let buffer = make_buffer_from_frame(&amqp_frame);
            socket.lock().await.write_all(&buffer).await?;

            log::trace!(frame:? = amqp_frame, channel_id:? = channel_id; "← sent");
        };
        Ok(())
    }

    async fn find_match_subscriptions(&self, queue_name: &str) -> Vec<(u64, u16, String)> {
        let mut match_subscriptions = vec![];
        let sessions = self.sessions.lock().await;
        for (session_id, s) in sessions.iter() {
            for ch in s.channels.iter() {
                for (consumer_tag, sub) in ch.active_consumers.iter() {
                    if sub.queue == queue_name {
                        // TODO remove clone
                        match_subscriptions.push((*session_id, ch.id, consumer_tag.to_string()));
                    }
                }
            }
        }

        match_subscriptions
    }
    pub(crate) async fn queue_process_loop(self: Arc<Self>, queue_name: &str) {
        let Some(mut queue) = self.queues.get_mut(queue_name) else {
            log::info!(queue:? = queue_name; "⏹ stopped processing: queue not found");
            return;
        };

        let queue = queue.deref();

        if queue
            .consuming
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let queue = Arc::downgrade(&queue);

        loop {
            let Some(queue) = queue.upgrade() else {
                break;
            };

            if !Arc::clone(&self).queue_process(Arc::clone(&queue)).await {
                queue
                    .consuming
                    .compare_exchange(true, false, Ordering::Acquire, Ordering::Acquire);
                break;
            }
        }
    }

    pub(crate) async fn queue_process(self: Arc<Self>, queue: Arc<InternalQueue<Q>>) -> bool {
        // prefetching unsupported
        // log::warn!(queue:? = queue_name; "⏹ stopped processing: prefetching unsupported");

        let queue_name = &queue.queue_name;

        let match_subscriptions = self.find_match_subscriptions(queue_name).await;

        if match_subscriptions.is_empty() {
            log::info!(queue:? = queue_name;"⏹ stopped processing: no consumers");
            return false;
        }

        let message = queue.ready_vec.pop();
        let Some(message) = message else {
            log::info!(queue:? = queue_name; "⏹ stopped processing: queue empty");
            return false;
        };
        let consumed = queue.consumed.fetch_add(1, Ordering::AcqRel) + 1;

        //choose consumer round-robin
        let consumer_index = (consumed as usize - 1) % match_subscriptions.len();
        let selected_subscription = match_subscriptions.get(consumer_index).unwrap().to_owned();
        let (session_id, channel_id, consumer_tag) = selected_subscription;

        log::info!(queue:? = queue_name, queue:? = queue_name;"⏹ selected consumer for message");

        let mut sessions = self.sessions.lock().await;
        let session = sessions.get_mut(&session_id).unwrap();
        let channel_info: &mut ChannelInfo = session
            .channels
            .iter_mut()
            .find(|c| c.id == channel_id)
            .unwrap();
        let delivery_tag = channel_info.delivery_tag.fetch_add(1, Ordering::Acquire) + 1;

        channel_info.awaiting_acks.insert(
            delivery_tag,
            UnackedMessage {
                delivery_tag,
                queue: queue_name.to_owned(),
                message: message.clone(),
            },
        );

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

        channel_info.awaiting_acks.insert(
            delivery_tag,
            UnackedMessage {
                delivery_tag,
                queue: queue_name.to_owned(),
                message: message.clone(),
            },
        );

        let mut buffer = make_buffer_from_frame(&amqp_frame);

        let amqp_frame = AMQPFrame::Header(
            channel_id,
            60_u16,
            Box::new(AMQPContentHeader {
                class_id: 60,
                body_size: message.len() as u64,
                properties: Default::default(),
            }),
        );
        buffer.extend(make_buffer_from_frame(&amqp_frame));

        let amqp_frame = AMQPFrame::Body(channel_id, message.into());
        buffer.extend(make_buffer_from_frame(&amqp_frame));

        // TODO remove duplicated code write & log
        session.write.lock().await.write_all(&buffer).await.unwrap();
        log::trace!(frame:? = amqp_frame, channel_id:? = channel_id; "← sent");

        true
    }
}
