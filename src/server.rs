use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

use crate::models::{ChannelInfo, InternalExchange, InternalQueue, Session, UnackedMessage};
use crate::parsing::ParsingContext;
use amq_protocol::frame::{AMQPContentHeader, AMQPFrame, gen_frame, parse_frame};
use amq_protocol::protocol::connection::{AMQPMethod, Start};
use amq_protocol::protocol::queue::Bind;
use amq_protocol::protocol::{AMQPClass, basic};
use amq_protocol::types::{FieldTable, LongString, ShortString};
use rand::Rng;
use tokio::sync::Mutex;

pub struct BurrowMQServer {
    pub(crate) exchanges: Arc<Mutex<HashMap<String, InternalExchange>>>,
    pub(crate) queues: Arc<dashmap::DashMap<String, InternalQueue>>,
    pub(crate) sessions: Arc<Mutex<HashMap<i64, Session>>>,
    pub(crate) queue_bindings: Mutex<Vec<Bind>>, // Очередь ↔ Exchange
}

const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";

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

    // TODO graceful shutdown
    pub async fn start_forever(self, port: u16) -> anyhow::Result<()> {
        let addr = format!("0.0.0.0:{}", port);
        let listener = TcpListener::bind(&addr).await?;
        println!("Listening on {}", addr);

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
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                w.lock()
                    .await
                    .write_all(&buffer)
                    .await
                    .expect("failed to send heartbeat");
            }
        });
    }

    pub async fn handle_session(self: Arc<Self>, socket: TcpStream, _: std::net::SocketAddr) {
        let session_id = self.sessions.lock().await.iter().len() as i64 + 1;

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
        session_id: i64,
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

            if let AMQPFrame::Heartbeat(channel_id) = frame {
                println!("Heartbeat: channel {:?} ", channel_id);
                return Ok(());
            } else if let AMQPFrame::Method(channel_id, method) = frame {
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
        session_id: i64,
        channel_id: u16,
        socket: Arc<Mutex<OwnedWriteHalf>>,
        buf: &&[u8],
        parsing_context: ParsingContext<'_>,
        frame: AMQPClass,
    ) -> anyhow::Result<()> {
        // log::warn!(frame:? = frame; "frame received");
        match frame {
            AMQPClass::Connection(connection_method) => {
                self.handle_connection_method(channel_id, session_id, socket, connection_method)
                    .await?;
            }
            AMQPClass::Basic(basic_method) => {
                self.handle_basic_method(
                    channel_id,
                    session_id,
                    socket,
                    basic_method,
                    parsing_context,
                    buf,
                )
                .await?;
            }
            AMQPClass::Channel(channel_method) => {
                self.handle_channel_method(channel_id, session_id, socket, channel_method)
                    .await?;
            }
            AMQPClass::Queue(queue_method) => {
                self.handle_queue_method(channel_id, socket, queue_method)
                    .await?;
            }
            AMQPClass::Exchange(exchange_method) => {
                self.handle_exchange_method(channel_id, session_id, socket, exchange_method)
                    .await?;
            }
            _ => {
                panic!("unsupported frame");
            }
        }
        Ok(())
    }

    pub(crate) async fn queue_process(self: Arc<Self>, queue_name: String) {
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
            for ch in s.channels.iter() {
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

        let selected_subscription = suit_subscriptions.first().unwrap().to_owned(); // TODO choose consumer round-robin
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
