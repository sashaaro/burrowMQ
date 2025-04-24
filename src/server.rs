use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::sleep;

use amq_protocol::frame::{AMQPFrame, gen_frame, parse_frame};
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
    // ...
}

const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";


struct MyExchange {
    declaration: exchange::Declare,
    bindings: Vec<exchange::Bind>
    // bindings: Vec<(String, String)>, // (routing_key, queue_name)
}

struct MyQueue {
    declaration: queue::Declare,
    queue_name: String,
    inner: VecDeque<Bytes>
}

struct Session {
    id: i64,
    confirm_mode: bool,
    channels: Vec<ChannelId>
}

impl BurrowMQServer {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn start_forever(&mut self) -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:5672").await?;
        println!("Listening on 127.0.0.1:5672");


        let exchanges: HashMap<String, MyExchange> = HashMap::new();
        let exchanges = Arc::new(Mutex::new(exchanges));

        let queues: HashMap<String, MyQueue> = HashMap::new();
        let queues = Arc::new(Mutex::new(queues));


        let sessions: HashMap<i64, Session> = HashMap::new();
        let sessions = Arc::new(Mutex::new(sessions));

        loop {
            let (mut socket, addr) = listener.accept().await?;
            println!("New client from {:?}", addr);
            let exchanges = Arc::clone(&exchanges);
            let sessions = Arc::clone(&sessions);
            let queues = Arc::clone(&queues);

            let session_id = sessions.lock().await.iter().len() as i64 + 1;
            sessions.lock().await.insert(session_id, Session {
                id: session_id,
                confirm_mode: false,
                channels: Default::default(),
            });

            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let exchanges = Arc::clone(&exchanges);
                let sessions = Arc::clone(&sessions);
                let queues = Arc::clone(&queues);

                // TODO start heartbeat interval

                loop {
                    let exchanges = Arc::clone(&exchanges);
                    let sessions = Arc::clone(&sessions);

                    match socket.read(&mut buf).await {
                        Ok(n) if n == PROTOCOL_HEADER.len() && buf[..PROTOCOL_HEADER.len()].eq(PROTOCOL_HEADER) => {
                            println!("Received!!: {:?}", &buf[..n]);

                            let start = Start {
                                version_major: 0,
                                version_minor: 9,
                                server_properties: FieldTable::default(), // можно добавить info о сервере
                                mechanisms: LongString::from("PLAIN"),
                                locales: LongString::from("en_US"),
                            };

                            let amqp_frame  = AMQPFrame::Method(0, AMQPClass::Connection(AMQPMethod::Start(start)));

                            let buffer = Self::make_buffer_from_frame(&amqp_frame);

                            let _ = socket.write_all(&buffer).await;
                        }
                        Ok(n) if n > 0 => {
                            let buf = &buf[..n];

                            let (parsing_context, frame) = parse_frame(ParsingContext::from(buf)).expect("invalid frame");
                            println!("Received amqp frame: {:?}", frame);

                            match &frame {
                                AMQPFrame::Method(channel_id, AMQPClass::Connection(AMQPMethod::StartOk(startOk))) => {
                                    let amqp_frame  = AMQPFrame::Method(*channel_id, AMQPClass::Connection(AMQPMethod::Tune(Tune{
                                        channel_max: 10,
                                        frame_max: 1024,
                                        heartbeat: 10,
                                    })));

                                    let buffer = Self::make_buffer_from_frame(&amqp_frame);
                                    let _ = socket.write_all(&buffer).await;
                                }
                                AMQPFrame::Method(_, AMQPClass::Connection(AMQPMethod::TuneOk(tuneOk))) => {
                                    println!("TuneOk: {:?}", tuneOk);
                                }
                                AMQPFrame::Method(channel_id, AMQPClass::Connection(AMQPMethod::Open(open))) => {
                                    let amqp_frame  = AMQPFrame::Method(*channel_id, AMQPClass::Connection(AMQPMethod::OpenOk(OpenOk{})));
                                    let buffer = Self::make_buffer_from_frame(&amqp_frame);
                                    let _ = socket.write_all(&buffer).await;
                                }
                                AMQPFrame::Method(channel_id, AMQPClass::Exchange(exchange::AMQPMethod::Declare(declare))) => {
                                    exchanges.lock().await.insert(declare.exchange.to_string(), MyExchange{declaration: declare.clone(), bindings: Default::default()});

                                    let amqp_frame  = AMQPFrame::Method(*channel_id, AMQPClass::Exchange(exchange::AMQPMethod::DeclareOk(DeclareOk{})));
                                    let buffer = Self::make_buffer_from_frame(&amqp_frame);
                                    let _ = socket.write_all(&buffer).await;
                                }
                                AMQPFrame::Method(channel_id, AMQPClass::Channel(channel::AMQPMethod::Open(open))) => {
                                    // TODO check channel_max
                                    // let class_id = open.get_amqp_class_id();
                                    // let method_id = open.get_amqp_method_id();

                                    let mut sessions = sessions.lock().await;
                                    let mut session = sessions.get_mut(&session_id).expect("Session not found");

                                    session.channels.push(*channel_id);

                                    let amqp_frame  = AMQPFrame::Method(*channel_id, AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk{})));
                                    let buffer = Self::make_buffer_from_frame(&amqp_frame);
                                    let _ = socket.write_all(&buffer).await;
                                },
                                AMQPFrame::Heartbeat(channel_id) => {
                                    println!("Heartbeat: channel {:?} ", channel_id);
                                }
                                AMQPFrame::Method(channel_id, AMQPClass::Queue(queue::AMQPMethod::Declare(declare))) => {
                                    let mut queue_name = declare.queue.to_string();
                                    if queue_name.is_empty() {
                                        queue_name = "test_queue".to_string();
                                    }

                                    queues.lock().await.insert(queue_name.clone(), MyQueue{
                                        queue_name: queue_name.clone(),
                                        declaration: declare.clone(),
                                        inner: Default::default(),
                                    });

                                    let amqp_frame  = AMQPFrame::Method(*channel_id, AMQPClass::Queue(queue::AMQPMethod::DeclareOk(queue::DeclareOk{
                                        queue: queue_name.clone().into(),
                                        message_count: 0, // сколько сообщений уже в очереди
                                        consumer_count: 1, // TODO сколько потребителей подписаны на эту очередь
                                    })));
                                    let buffer = Self::make_buffer_from_frame(&amqp_frame);
                                    let _ = socket.write_all(&buffer).await;
                                }
                                AMQPFrame::Method(channel_id, AMQPClass::Basic(Publish(publish))) => {
                                    // TODO content_header.properties.content_type();

                                    // parsing_context.(content_header.body_size as usize);



                                    let shift = parsing_context.as_ptr()  as usize - buf.as_ptr() as usize;

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

                                        for bind in exchange.bindings.iter() {
                                            if bind.routing_key.to_string() == publish.routing_key.to_string() { // Найти очереди, соответствующие routing_key
                                                queue_name = Some(bind.destination.to_string());
                                                break
                                           }
                                       }
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
                                            let _ = socket.write_all(&buffer).await;
                                        }
                                        break
                                    }

                                    let found_queue = found_queue.unwrap();
                                    
                                    found_queue.inner.push_back(message.into());
                                    if found_queue.inner.len() == 1 {
                                        trigger_consumers(found_queue)
                                    }
                                    
                                    drop(queues);
                                    let amqp_frame  = AMQPFrame::Method(*channel_id, AMQPClass::Basic(basic::AMQPMethod::Ack(basic::Ack{
                                        delivery_tag: 0,
                                        multiple: false,
                                    })));
                                    let buffer = Self::make_buffer_from_frame(&amqp_frame);
                                    let _ = socket.write_all(&buffer).await;
                                }
                                AMQPFrame::Method(channel_id, AMQPClass::Basic(basic::AMQPMethod::Consume(consume))) => {
                                    let _ = 1;

                                    // TODO register consumer

                                    let amqp_frame  = AMQPFrame::Method(*channel_id, AMQPClass::Basic(basic::AMQPMethod::ConsumeOk(basic::ConsumeOk{
                                        consumer_tag: "".into(), // TODO
                                    })));
                                    let buffer = Self::make_buffer_from_frame(&amqp_frame);
                                    let _ = socket.write_all(&buffer).await;
                                }
                                AMQPFrame::Body(_, data) => {
                                    println!("{:?}", data);
                                    let _ = 1;
                                    panic!("11");
                                }
                                _ => {
                                    let _ = 1;
                                    panic!("12");
                                }
                            }
                        }
                        _ => {
                            println!("Connection closed or failed");
                            break
                        }
                    }
                }


                println!("12312321");
                sleep(std::time::Duration::from_millis(20)).await;
            });
        }
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
