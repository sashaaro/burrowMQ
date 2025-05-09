use crate::server::{BurrowMQServer, QueueTrait};
use amq_protocol::protocol::connection;
use amq_protocol::protocol::connection::{AMQPMethod, OpenOk, Tune};
use bytes::Bytes;
use std::sync::Arc;

impl<Q: QueueTrait<Bytes> + Default> BurrowMQServer<Q> {
    pub(crate) async fn handle_connection_method(
        self: Arc<Self>,
        frame: connection::AMQPMethod,
    ) -> anyhow::Result<Option<AMQPMethod>> {
        let resp = match frame {
            AMQPMethod::StartOk(_) => Some(AMQPMethod::Tune(Tune {
                channel_max: 10,
                frame_max: 1024,
                heartbeat: 10,
            })),
            AMQPMethod::TuneOk(tune_ok) => {
                println!("TuneOk: {:?}", tune_ok);
                None
            }
            AMQPMethod::Open(_) => Some(AMQPMethod::OpenOk(OpenOk {})),
            f => {
                unimplemented!("unimplemented queue method: {f:?}")
            }
        };
        Ok(resp)
    }
}
