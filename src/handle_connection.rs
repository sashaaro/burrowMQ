use crate::server::BurrowMQServer;
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::connection::{AMQPMethod, OpenOk, Tune};
use amq_protocol::protocol::{AMQPClass, connection};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

impl BurrowMQServer {
    pub(crate) async fn handle_connection_method(
        self: Arc<Self>,
        channel_id: u16,
        session_id: i64,
        socket: Arc<Mutex<OwnedWriteHalf>>,
        frame: connection::AMQPMethod,
    ) -> anyhow::Result<()> {
        match frame {
            AMQPMethod::StartOk(_) => {
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
            AMQPMethod::TuneOk(tune_ok) => {
                println!("TuneOk: {:?}", tune_ok);
            }
            AMQPMethod::Open(_) => {
                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Connection(AMQPMethod::OpenOk(OpenOk {})),
                );
                let buffer = Self::make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            f => {
                unimplemented!("unimplemented queue method: {f:?}")
            }
        }
        Ok(())
    }
}
