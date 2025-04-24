use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

use crate::parsing::ParsingContext;
use amq_protocol::frame::{AMQPContentHeader, AMQPFrame, gen_frame, parse_frame};
use amq_protocol::protocol::basic::AMQPMethod::Publish;
use amq_protocol::protocol::connection::{AMQPMethod, OpenOk, Start, Tune};
use amq_protocol::protocol::exchange::DeclareOk;
use amq_protocol::protocol::{AMQPClass, basic, channel, exchange, queue};
use amq_protocol::protocol::queue::Bind;
use amq_protocol::types::{ChannelId, FieldTable, LongString, ShortString};
use bytes::Bytes;
use tokio::sync::Mutex;
use serde::{Deserialize, Serialize};

pub struct BurrowMQServer {
    exchanges: Arc<Mutex<HashMap<String, MyExchange>>>,
    queues: Arc<Mutex<HashMap<String, MyQueue>>>,
    sessions: Arc<Mutex<HashMap<i64, Session>>>,
    queue_bindings: Mutex<Vec<Bind>>, // Очередь ↔ Exchange
}

const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeKind {
    Direct,
    Fanout,
    Headers,
    Topic,
}

impl Into<ExchangeKind> for ShortString {
    fn into(self) -> ExchangeKind {
        match self.as_str() {
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            "headers" => ExchangeKind::Headers,
            "topic" => ExchangeKind::Topic,
            _ => ExchangeKind::Direct
        }
    }
}

struct MyExchange {
    declaration: exchange::Declare,
}

struct MyQueue {
    declaration: queue::Declare,
    queue_name: String,
    inner: VecDeque<Bytes>,
    consumers: Vec<i64>, // session_id
}

struct Session {
    // TODO confirm_mode: bool,
    channels: Vec<ChannelId>,
    tcp_stream: Arc<Mutex<TcpStream>>,
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
        }
    }

    pub async fn start_forever(self) -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:5672").await?;
        println!("Listening on 127.0.0.1:5672");

        let server = Arc::new(self);

        loop {
            let (socket, addr) = listener.accept().await?;
            println!("New client from {:?}", addr);
            let server = Arc::clone(&server);

            tokio::spawn(async move {
                server.handle_session(socket, addr).await;
            });
        }
    }

    pub async fn handle_session(self: Arc<Self>, socket: TcpStream, addr: std::net::SocketAddr) {
        let exchanges = Arc::clone(&self.exchanges);
        let sessions = Arc::clone(&self.sessions);
        let queues = Arc::clone(&self.queues);

        let session_id = self.sessions.lock().await.iter().len() as i64 + 1;
        self.sessions.lock().await.insert(
            session_id,
            Session {
                // confirm_mode: false,
                channels: Default::default(),
                tcp_stream: Arc::new(Mutex::new(socket)),
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
                    break;
                };

                let amqp_frame = AMQPFrame::Heartbeat(0);
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                session
                    .tcp_stream
                    .lock()
                    .await
                    .write_all(&buffer)
                    .await
                    .expect("failed to send heartbeat");
            }
        });

        let mut buf = [0u8; 1024];
        loop {
            let exchanges = Arc::clone(&exchanges);
            let sessions = Arc::clone(&sessions);
            let queues = Arc::clone(&queues);

            if Arc::clone(&self)
                .handle_recv_buffer(&session_id, &mut buf, exchanges, sessions, queues)
                .await
            {
                break;
            }
        }
    }

    async fn handle_recv_buffer(
        self: Arc<Self>,
        session_id: &i64,
        buf: &mut [u8; 1024],
        exchanges: Arc<Mutex<HashMap<String, MyExchange>>>,
        sessions: Arc<Mutex<HashMap<i64, Session>>>,
        queues: Arc<Mutex<HashMap<String, MyQueue>>>,
    ) -> bool {
        let sessions2 = self.sessions.lock().await;
        let session = sessions2.get(session_id).unwrap();

        let socket = Arc::clone(&session.tcp_stream);
        drop(sessions2);

        let r = socket.lock().await.read(buf).await;
        match r {
            Ok(n)
                if n == PROTOCOL_HEADER.len()
                    && buf[..PROTOCOL_HEADER.len()].eq(PROTOCOL_HEADER) =>
            {
                println!("Received!!: {:?}", &buf[..n]);

                let start = Start {
                    version_major: 0,
                    version_minor: 9,
                    server_properties: FieldTable::default(), // можно добавить info о сервере
                    mechanisms: LongString::from("PLAIN"),
                    locales: LongString::from("en_US"),
                };

                let amqp_frame =
                    AMQPFrame::Method(0, AMQPClass::Connection(AMQPMethod::Start(start)));

                let buffer = Self::make_buffer_from_frame(&amqp_frame);

                let _ = socket.lock().await.write_all(&buffer).await;
            }
            Ok(n) if n > 0 => {
                let buf = &buf[..n];

                let (parsing_context, frame) =
                    parse_frame(ParsingContext::from(buf)).expect("invalid frame");
                println!("Received amqp frame: {:?}", frame);

                match &frame {
                    AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Connection(AMQPMethod::StartOk(start_ok)),
                    ) => {
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
                    AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Connection(AMQPMethod::Open(open)),
                    ) => {
                        let amqp_frame = AMQPFrame::Method(
                            *channel_id,
                            AMQPClass::Connection(AMQPMethod::OpenOk(OpenOk {})),
                        );
                        let buffer = Self::make_buffer_from_frame(&amqp_frame);
                        let _ = socket.lock().await.write_all(&buffer).await;
                    }
                    AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Exchange(exchange::AMQPMethod::Bind(bind)),
                    ) => {
                        unimplemented!("exchange bindings unimplemented")
                    },
                    AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Queue(queue::AMQPMethod::Bind(bind)),
                    ) => {
                        let Some(exchange) = exchanges.lock().await.get_mut(bind.exchange.as_str()) else {
                            panic!("exchange not found")// todo send error
                        };
                        let Some(queue) = queues.lock().await.get_mut(bind.queue.as_str()) else {
                            panic!("queue not found")// todo send error
                        };

                        self.queue_bindings.lock().await.push(bind.clone());
                    },
                    AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Exchange(exchange::AMQPMethod::Declare(declare)),
                    ) => {
                        exchanges.lock().await.insert(
                            declare.exchange.to_string(),
                            MyExchange {
                                declaration: declare.clone(),
                            },
                        );

                        let amqp_frame = AMQPFrame::Method(
                            *channel_id,
                            AMQPClass::Exchange(exchange::AMQPMethod::DeclareOk(DeclareOk {})),
                        );
                        let buffer = Self::make_buffer_from_frame(&amqp_frame);
                        let _ = socket.lock().await.write_all(&buffer).await;
                    }
                    AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Channel(channel::AMQPMethod::Open(open)),
                    ) => {
                        let mut sessions = sessions.lock().await;
                        let session = sessions.get_mut(session_id).expect("Session not found");

                        session.channels.push(*channel_id);

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

                        queues.lock().await.insert(
                            queue_name.clone(),
                            MyQueue {
                                queue_name: queue_name.clone(),
                                declaration: declare.clone(),
                                inner: Default::default(),
                                consumers: Default::default(),
                            },
                        );

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
                    AMQPFrame::Method(channel_id, AMQPClass::Basic(Publish(publish))) => {
                        let shift = parsing_context.as_ptr() as usize - buf.as_ptr() as usize;

                        let next_buf = &buf[shift..];

                        let header_frame = parse_frame(ParsingContext::from(next_buf));

                        let message = match header_frame {
                            Ok((parsing_context, header_frame)) => {
                                match header_frame {
                                    AMQPFrame::Header(channel_id, size, content_header) => {
                                        // TODO content_header.properties.content_type();
                                        let body_size = content_header.body_size as usize;

                                        let shift = parsing_context.as_ptr() as usize
                                            - next_buf.as_ptr() as usize;

                                        let body_buf = &next_buf[shift..];

                                        let body_frame = parse_frame(body_buf);

                                        match body_frame {
                                            Ok((_, AMQPFrame::Body(channel_id, body))) => {
                                                if body.len() != body_size {
                                                    panic!("body size mismatch")
                                                }

                                                body
                                            }
                                            _ => {
                                                panic!("unsupported header")
                                            }
                                        }
                                    }
                                    _ => {
                                        panic!("unsupported header")
                                    }
                                }
                            }
                            Err(err) => {
                                panic!("{}", err)
                            }
                        };

                        let mut suit_queues: Vec<&mut MyQueue> = vec![];

                        match publish.exchange.to_string().as_str() {
                            "" => { // default exchange
                                match publish.routing_key.as_str() {
                                    "" => panic!("routing key is empty"), // TODO
                                    routing_key => {
                                        if let Some(queue) = queues.lock().await.get_mut(routing_key) {
                                           suit_queues.push(queue);
                                        } else if publish.mandatory {
                                            // TODO socket.write Ошибка
                                        }
                                    }
                                }
                            },
                            exchange => {
                                if let Some(exchange) = exchanges.lock().await.get(exchange) {
                                    match exchange.declaration.kind.clone().into() {
                                        ExchangeKind::Direct => { // По точному совпадению routing_key == binding_key
                                            for bind in self.queue_bindings.lock().await.iter() {
                                                if bind.routing_key.to_string() == publish.routing_key.to_string() {
                                                    if let Some(queue) = queues.lock().await.get_mut(bind.queue.as_str()) {
                                                        suit_queues.push(queue);
                                                    } else if publish.mandatory {
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
                                                        break
                                                    }
                                                }
                                            }
                                        },
                                        ExchangeKind::Fanout => { // Игнорирует routing_key, отправляет всем связанным очередям
                                            for bind in self.queue_bindings.lock().await.iter() {
                                                if let Some(queue) = queues.lock().await.get_mut(bind.queue.as_str()) {
                                                    suit_queues.push(queue);
                                                } else if publish.mandatory {
                                                    // TODO socket.write Ошибка
                                                }
                                            }
                                        }
                                        ExchangeKind::Topic => { // По шаблону: binding_key может содержать * (1 слово) и # (0+ слов)
                                            unimplemented!("exchange kind topic unimplemented")
                                        },
                                        ExchangeKind::Headers => { // По arguments, routing_key игнорируется
                                            unimplemented!("exchange kind headers unimplemented")
                                        },
                                    }
                                } else if publish.mandatory {
                                    // TODO socket.write basic.return
                                }
                            }
                        }

                        for queue in suit_queues {
                            queue.inner.push_back(message.clone().into());
                            if queue.inner.len() == 1 {
                                Arc::clone(&self).trigger_consumers(queue.queue_name.clone());
                            }   
                        }
                        drop(queues);
                        
                        let amqp_frame = AMQPFrame::Method(
                            *channel_id,
                            AMQPClass::Basic(basic::AMQPMethod::Ack(basic::Ack {
                                delivery_tag: 0,
                                multiple: false,
                            })),
                        );
                        let buffer = Self::make_buffer_from_frame(&amqp_frame);
                        let _ = socket.lock().await.write_all(&buffer).await;
                    }
                    AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Basic(basic::AMQPMethod::Consume(consume)),
                    ) => {
                        let mut queues = self.queues.lock().await;
                        let queue = queues.get_mut(&consume.queue.to_string());
                        if queue.is_none() {
                            panic!("queue not found")
                        }
                        let queue = queue.unwrap();
                        queue.consumers.push(*session_id);

                        let amqp_frame = AMQPFrame::Method(
                            *channel_id,
                            AMQPClass::Basic(basic::AMQPMethod::ConsumeOk(basic::ConsumeOk {
                                consumer_tag: "".into(), // TODO
                            })),
                        );
                        let buffer = Self::make_buffer_from_frame(&amqp_frame);
                        let _ = socket.lock().await.write_all(&buffer).await;

                        Arc::clone(&self).trigger_consumers(consume.queue.to_string());
                    }
                    _ => {
                        panic!("unsupported frame");
                    }
                }
            }
            _ => {
                println!("Connection closed or failed");
                return true;
            }
        }
        false
    }

    fn trigger_consumers(self: Arc<Self>, queue_name: String) {
        tokio::spawn(async move {
            sleep(Duration::from_secs(1)).await;
            loop {
                let mut queues_lock = self.queues.lock().await;

                let queue = queues_lock.get_mut(&queue_name);
                if queue.is_none() {
                    break;
                }
                let queue = queue.unwrap();
                let message = queue.inner.pop_front();

                if queue.consumers.is_empty() {
                    break;
                }
                let consumers = queue.consumers.clone();
                drop(queues_lock); // release lock explicitly

                if message.is_none() {
                    break;
                }
                let message = message.unwrap();

                let sessions = self.sessions.lock().await;
                let mut filtered_sessions = vec![];
                for session_id in consumers {
                    let Some(session) = sessions.get(&session_id) else {
                        continue;
                    };

                    filtered_sessions.push(session)
                }

                for session in filtered_sessions {
                    let channel_id = 1; // TODO
                    let amqp_frame = AMQPFrame::Method(
                        channel_id,
                        AMQPClass::Basic(basic::AMQPMethod::Deliver(basic::Deliver {
                            consumer_tag: ShortString::from(""),
                            delivery_tag: 0,
                            redelivered: false,
                            exchange: ShortString::from(""),
                            routing_key: ShortString::from(""),
                        })),
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

                    let amqp_frame = AMQPFrame::Body(channel_id, message.clone().into()); // TODO stop clone
                    buffer.extend(Self::make_buffer_from_frame(&amqp_frame));

                    session
                        .tcp_stream
                        .lock()
                        .await
                        .write_all(&buffer)
                        .await
                        .unwrap();
                }
            }
        });
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
