use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::sleep;

use amq_protocol::frame::{AMQPFrame, gen_frame, parse_frame};
use amq_protocol::frame::parsing::traits::{Compare, Needed};
use amq_protocol::protocol::{exchange, AMQPClass};
use amq_protocol::protocol::AMQPClass::Exchange;
use amq_protocol::protocol::connection::{gen_start, AMQPMethod, OpenOk, Start, Tune};
use amq_protocol::protocol::exchange::DeclareOk;
use amq_protocol::types::{FieldTable, LongString};
use amq_protocol::types::parsing::traits::ParsableInput;
use bytes::BytesMut;
use tokio::sync::Mutex;
use crate::parsing::ParsingContext;

pub struct BurrowMQServer {
    // ...
}

const PROTOCOL_HEADER: &[u8] = b"AMQP\x00\x00\x09\x01";


struct Exchange {
    declaration: exchange::Declare,
}

impl BurrowMQServer {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn start_forever(&mut self) -> anyhow::Result<()> {
        let listener = TcpListener::bind("127.0.0.1:5672").await?;
        println!("Listening on 127.0.0.1:5672");


        let exchanges: HashMap<String, Exchange> = HashMap::new();
        let exchanges = Arc::new(Mutex::new(exchanges));

        loop {
            let (mut socket, addr) = listener.accept().await?;
            println!("New client from {:?}", addr);

            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let exchanges = Arc::clone(&exchanges);
                loop {
                    let exchanges = Arc::clone(&exchanges);
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

                            let mut buffer = vec![0u8; 1024];
                            gen_frame(&amqp_frame)(buffer.as_mut_slice().into()).unwrap();
                            Self::trim_right_bytes(&mut buffer);

                            let _ = socket.write_all(&buffer).await;
                        }
                        Ok(n) if n > 0 => {

                            let result = parse_frame(ParsingContext::from(&buf[..n])).expect("invalid request");

                            println!("Received: {:?}", &buf[..n]);

                            println!("Received amqp frame: {:?}", result.1);

                            match result.1 {
                                AMQPFrame::Method(_, AMQPClass::Connection(AMQPMethod::StartOk(startOk))) => {
                                    let amqp_frame  = AMQPFrame::Method(0, AMQPClass::Connection(AMQPMethod::Tune(Tune{
                                        channel_max: 10,
                                        frame_max: 1024,
                                        heartbeat: 10,
                                    })));

                                    let mut buffer = vec![0u8; 1024];
                                    gen_frame(&amqp_frame)(buffer.as_mut_slice().into()).unwrap();
                                    Self::trim_right_bytes(&mut buffer);
                                    let _ = socket.write_all(&buffer).await;
                                }
                                AMQPFrame::Method(_, AMQPClass::Connection(AMQPMethod::TuneOk(tuneOk))) => {
                                    let _ = 1;
                                    unimplemented!();
                                }
                                AMQPFrame::Method(_, AMQPClass::Connection(AMQPMethod::Open(open))) => {
                                    let amqp_frame  = AMQPFrame::Method(0, AMQPClass::Connection(AMQPMethod::OpenOk(OpenOk{})));

                                    let mut buffer = vec![0u8; 1024];
                                    gen_frame(&amqp_frame)(buffer.as_mut_slice().into()).unwrap();
                                    Self::trim_right_bytes(&mut buffer);
                                    let _ = socket.write_all(&buffer).await;
                                }
                                AMQPFrame::Method(_, AMQPClass::Exchange(exchange::AMQPMethod::Declare(declare))) => {

                                    exchanges.lock().await.insert(declare.exchange.to_string(), Exchange{declaration: declare});


                                    let amqp_frame  = AMQPFrame::Method(0, AMQPClass::Exchange(exchange::AMQPMethod::DeclareOk(DeclareOk{})));

                                    let mut buffer = vec![0u8; 1024];
                                    gen_frame(&amqp_frame)(buffer.as_mut_slice().into()).unwrap();
                                    Self::trim_right_bytes(&mut buffer);
                                    let _ = socket.write_all(&buffer).await;
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

    fn trim_right_bytes(mut buffer: &mut Vec<u8>) {
        while let Some(&0) = buffer.last() {
            buffer.pop();
        }
    }
}
