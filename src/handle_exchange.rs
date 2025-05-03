use crate::models::InternalExchange;
use crate::server::{BurrowMQServer, gen_random_queue_name};
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::connection::{AMQPMethod, OpenOk, Tune};
use amq_protocol::protocol::exchange::DeclareOk;
use amq_protocol::protocol::{AMQPClass, connection, exchange};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

impl BurrowMQServer {
    pub(crate) async fn handle_exchange_method(
        self: Arc<Self>,
        channel_id: u16,
        session_id: i64,
        socket: Arc<Mutex<OwnedWriteHalf>>,
        frame: exchange::AMQPMethod,
    ) -> anyhow::Result<()> {
        match frame {
            exchange::AMQPMethod::Declare(declare) => {
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
            exchange::AMQPMethod::Bind(_) => {
                todo!()
            }
            f => {
                unimplemented!("unimplemented queue method: {f:?}")
            }
        }
        Ok(())
    }
}
