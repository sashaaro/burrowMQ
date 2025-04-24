use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::sleep;

use amq_protocol::frame::{AMQPFrame, gen_frame, parse_frame, AMQPContentHeader};
use amq_protocol::frame::parsing::parse_content_header;
use amq_protocol::protocol::{basic, channel, exchange, queue, AMQPClass};
use amq_protocol::protocol::basic::AMQPMethod::Publish;
use amq_protocol::protocol::connection::{gen_start, AMQPMethod, OpenOk, Start, Tune};
use amq_protocol::protocol::exchange::DeclareOk;
use amq_protocol::types::{ChannelId, FieldTable, LongString, ShortString};
use bytes::Bytes;
use nom::{InputLength, InputTake};
use tokio::sync::Mutex;
use crate::parsing::ParsingContext;

pub struct BurrowMQServer {
    pub exchanges: Arc<Mutex<HashMap<String, MyExchange>>>,
    pub queues: Arc<Mutex<HashMap<String, MyQueue>>>,
    pub sessions: Arc<Mutex<HashMap<i64, Session>>>,
}

const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";

struct MyExchange {
    declaration: exchange::Declare,
    bindings: Vec<exchange::Bind>
}

struct MyQueue {
    declaration: queue::Declare,
    queue_name: String,
    inner: VecDeque<Bytes>,
    consumers: Vec<i64> // session_id
}

struct Session {
    id: i64,
    confirm_mode: bool,
    channels: Vec<ChannelId>,
    tcp_stream: Arc<Mutex<TcpStream>>
}

impl BurrowMQServer {
    pub fn new() -> Self {
        Self {
            exchanges: Arc::new(Mutex::new(HashMap::new())),
            queues: Arc::new(Mutex::new(HashMap::new())),
            sessions: Arc::new(Mutex::new(HashMap::new())),
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

    pub async fn handle_session(self: Arc<Self>, mut socket: TcpStream, addr: std::net::SocketAddr) {
        let exchanges = Arc::clone(&self.exchanges);
        let sessions = Arc::clone(&self.sessions);
        let queues = Arc::clone(&self.queues);

        let session_id = self.sessions.lock().await.iter().len() as i64 + 1;
        self.sessions.lock().await.insert(session_id, Session {
            id: session_id,
            confirm_mode: false,
            channels: Default::default(),
            tcp_stream: Arc::new(Mutex::new(socket)),
        });

        let that = Arc::clone(&self);
        tokio::spawn(async move {
            let that = Arc::clone(&that);
            sleep(Duration::from_secs(3)).await;
            loop {
                sleep(Duration::from_secs(2)).await;
                let sessions = that.sessions.lock().await;
                let Some(session) = sessions.get(&session_id) else {
                    break
                };
                
                let amqp_frame = AMQPFrame::Heartbeat(0);
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                session.tcp_stream.lock().await.write_all(&buffer).await.expect("failed to send heartbeat");
            }
        });
        
        let mut buf = [0u8; 1024];
        loop {
            let exchanges = Arc::clone(&exchanges);
            let sessions = Arc::clone(&sessions);
            let queues = Arc::clone(&queues);

            if Arc::clone(&self).handle_recv_buffer(&session_id, &mut buf, exchanges, sessions, queues).await { break; }
        }
    }

    async fn handle_recv_buffer(self: Arc<Self>, session_id: &i64, buf: &mut [u8; 1024], exchanges: Arc<Mutex<HashMap<String, MyExchange>>>, sessions: Arc<Mutex<HashMap<i64, Session>>>, queues: Arc<Mutex<HashMap<String, MyQueue>>>) -> bool {
        let sessions2 = self.sessions.lock().await;
        let session = sessions2.get(session_id).unwrap();

        let socket = Arc::clone(&session.tcp_stream);
        drop(sessions2);

        let r = socket.lock().await.read(buf).await;
        match r {
            Ok(n) if n == PROTOCOL_HEADER.len() && buf[..PROTOCOL_HEADER.len()].eq(PROTOCOL_HEADER) => {
                println!("Received!!: {:?}", &buf[..n]);

                let start = Start {
                    version_major: 0,
                    version_minor: 9,
                    server_properties: FieldTable::default(), // можно добавить info о сервере
                    mechanisms: LongString::from("PLAIN"),
                    locales: LongString::from("en_US"),
                };

                let amqp_frame = AMQPFrame::Method(0, AMQPClass::Connection(AMQPMethod::Start(start)));

                let buffer = Self::make_buffer_from_frame(&amqp_frame);

                let _ = socket.lock().await.write_all(&buffer).await;
            }
            Ok(n) if n > 0 => {
                let buf = &buf[..n];

                let (parsing_context, frame) = parse_frame(ParsingContext::from(buf)).expect("invalid frame");
                println!("Received amqp frame: {:?}", frame);

                match &frame {
                    AMQPFrame::Method(channel_id, AMQPClass::Connection(AMQPMethod::StartOk(startOk))) => {
                        let amqp_frame = AMQPFrame::Method(*channel_id, AMQPClass::Connection(AMQPMethod::Tune(Tune {
                            channel_max: 10,
                            frame_max: 1024,
                            heartbeat: 10,
                        })));

                        let buffer = Self::make_buffer_from_frame(&amqp_frame);
                        let _ = socket.lock().await.write_all(&buffer).await;
                    }
                    AMQPFrame::Method(_, AMQPClass::Connection(AMQPMethod::TuneOk(tuneOk))) => {
                        println!("TuneOk: {:?}", tuneOk);
                    }
                    AMQPFrame::Method(channel_id, AMQPClass::Connection(AMQPMethod::Open(open))) => {
                        let amqp_frame = AMQPFrame::Method(*channel_id, AMQPClass::Connection(AMQPMethod::OpenOk(OpenOk {})));
                        let buffer = Self::make_buffer_from_frame(&amqp_frame);
                        let _ = socket.lock().await.write_all(&buffer).await;
                    }
                    AMQPFrame::Method(channel_id, AMQPClass::Exchange(exchange::AMQPMethod::Declare(declare))) => {
                        exchanges.lock().await.insert(declare.exchange.to_string(), MyExchange { declaration: declare.clone(), bindings: Default::default() });

                        let amqp_frame = AMQPFrame::Method(*channel_id, AMQPClass::Exchange(exchange::AMQPMethod::DeclareOk(DeclareOk {})));
                        let buffer = Self::make_buffer_from_frame(&amqp_frame);
                        let _ = socket.lock().await.write_all(&buffer).await;
                    }
                    AMQPFrame::Method(channel_id, AMQPClass::Channel(channel::AMQPMethod::Open(open))) => {
                        // TODO check channel_max
                        // let class_id = open.get_amqp_class_id();
                        // let method_id = open.get_amqp_method_id();

                        let mut sessions = sessions.lock().await;
                        let mut session = sessions.get_mut(&session_id).expect("Session not found");

                        session.channels.push(*channel_id);

                        let amqp_frame = AMQPFrame::Method(*channel_id, AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk {})));
                        let buffer = Self::make_buffer_from_frame(&amqp_frame);
                        let _ = socket.lock().await.write_all(&buffer).await;
                    },
                    AMQPFrame::Heartbeat(channel_id) => {
                        println!("Heartbeat: channel {:?} ", channel_id);
                    }
                    AMQPFrame::Method(channel_id, AMQPClass::Queue(queue::AMQPMethod::Declare(declare))) => {
                        let mut queue_name = declare.queue.to_string();
                        if queue_name.is_empty() {
                            queue_name = "test_queue".to_string();
                        }

                        queues.lock().await.insert(queue_name.clone(), MyQueue {
                            queue_name: queue_name.clone(),
                            declaration: declare.clone(),
                            inner: Default::default(),
                            consumers: Default::default(),
                        });

                        let amqp_frame = AMQPFrame::Method(*channel_id, AMQPClass::Queue(queue::AMQPMethod::DeclareOk(queue::DeclareOk {
                            queue: queue_name.clone().into(),
                            message_count: 0, // сколько сообщений уже в очереди
                            consumer_count: 1, // TODO сколько потребителей подписаны на эту очередь
                        })));
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

                                        let shift = parsing_context.as_ptr() as usize - next_buf.as_ptr() as usize;

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


                        let mut queue_name: Option<String> = None;

                        if !publish.exchange.to_string().is_empty() {
                            let exchanges = exchanges.lock().await;
                            let exchange = exchanges
                                .get(&publish.exchange.to_string())
                                .expect("Exchange not found");

                            // TODO
                            // for bind in exchange.bindings.iter() {
                            //     if bind.routing_key.to_string() == publish.routing_key.to_string() { // Найти очереди, соответствующие routing_key
                            //         queue_name = Some(bind.destination.to_string());
                            //         break
                            //     }
                            // }
                        } else if !publish.routing_key.to_string().is_empty() {
                            queue_name = Some(publish.routing_key.to_string());
                        }

                        if queue_name.is_none() {
                            panic!("queue not found")
                        }

                        let mut queues = queues.lock().await;
                        let found_queue: Option<&mut MyQueue> = queues.get_mut(&queue_name.unwrap());

                        if found_queue.is_none() {
                            if publish.mandatory {
                                let amqp_frame = AMQPFrame::Method(*channel_id, AMQPClass::Basic(basic::AMQPMethod::Return(basic::Return {
                                    reply_code: 312,
                                    reply_text: "NO_ROUTE".into(),
                                    exchange: publish.exchange.to_string().into(),
                                    routing_key: publish.routing_key.to_string().into(),
                                })));
                                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                                let _ = socket.lock().await.write_all(&buffer).await;
                            }
                            println!("queue not found");
                            return true
                        }

                        let found_queue = found_queue.unwrap();

                        found_queue.inner.push_back(message.into());
                        if found_queue.inner.len() == 1 {
                            Arc::clone(&self).trigger_consumers(found_queue.queue_name.clone());
                        }

                        drop(queues);
                        let amqp_frame = AMQPFrame::Method(*channel_id, AMQPClass::Basic(basic::AMQPMethod::Ack(basic::Ack {
                            delivery_tag: 0,
                            multiple: false,
                        })));
                        let buffer = Self::make_buffer_from_frame(&amqp_frame);
                        let _ = socket.lock().await.write_all(&buffer).await;
                    }
                    AMQPFrame::Method(channel_id, AMQPClass::Basic(basic::AMQPMethod::Consume(consume))) => {
                        let mut queues =  self.queues.lock().await;
                        let queue = queues.get_mut(&consume.queue.to_string());
                        if queue.is_none() {
                            panic!("queue not found")
                        }
                        let queue = queue.unwrap();
                        queue.consumers.push(*session_id);
                        
                        let amqp_frame = AMQPFrame::Method(*channel_id, AMQPClass::Basic(basic::AMQPMethod::ConsumeOk(basic::ConsumeOk {
                            consumer_tag: "".into(), // TODO
                        })));
                        let buffer = Self::make_buffer_from_frame(&amqp_frame);
                        let _ = socket.lock().await.write_all(&buffer).await;

                        Arc::clone(&self).trigger_consumers(consume.queue.to_string());
                        
                        // let that = Arc::clone(&self);
                        // tokio::spawn(async { // TODO
                        //     sleep(Duration::from_secs(1)).await;
                        // 
                        //     let mut queues =  self.queues.lock().await;
                        //     let queue = queues.get_mut(&consume.queue.to_string());
                        //     if queue.is_none() {
                        //         panic!("queue not found")
                        //     }
                        //     let queue = queue.unwrap();
                        // 
                        //     that.trigger_consumers(queue.queue_name.clone());
                        // });
                    }
                    _ => {
                        panic!("unsupported frame");
                    }
                }
            }
            _ => {
                println!("Connection closed or failed");
                return true
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
                    break
                }
                let message = message.unwrap();

                let sessions = self.sessions.lock().await;
                let mut filtered_sessions = vec![];
                for session_id in consumers {
                    let Some(session) = sessions.get(&session_id) else {
                        continue
                    };

                    filtered_sessions.push(session)
                }

                for session in filtered_sessions {
                    let channel_id = 1; // TODO
                    let amqp_frame = AMQPFrame::Method(channel_id, AMQPClass::Basic(basic::AMQPMethod::Deliver(basic::Deliver {
                        consumer_tag: ShortString::from(""),
                        delivery_tag: 0,
                        redelivered: false,
                        exchange: ShortString::from(""),
                        routing_key: ShortString::from(""),
                    })));
                    let mut buffer = Self::make_buffer_from_frame(&amqp_frame);


                    let amqp_frame = AMQPFrame::Header(channel_id, 60 as u16, Box::new(AMQPContentHeader {
                        class_id: 60,
                        body_size: message.len() as u64,
                        properties: Default::default(),
                    }));
                    buffer.extend(Self::make_buffer_from_frame(&amqp_frame));

                    let amqp_frame = AMQPFrame::Body(channel_id, message.clone().into()); // TODO stop clone
                    buffer.extend(Self::make_buffer_from_frame(&amqp_frame));

                    session.tcp_stream.lock().await.write_all(&buffer).await.unwrap();
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

    fn trim_right_bytes(mut buffer: &mut Vec<u8>) {
        while let Some(&0) = buffer.last() {
            buffer.pop();
        }
    }
}
