use crate::models::InternalError::Unsupported;
use crate::queue::QueueTrait;
use crate::server::BurrowMQServer;
use amq_protocol::protocol::connection::{AMQPMethod, OpenOk, Tune};
use bytes::Bytes;

impl<Q: QueueTrait<Bytes> + Default> BurrowMQServer<Q> {
    pub(crate) async fn handle_connection_method(
        &self,
        frame: AMQPMethod,
    ) -> anyhow::Result<Option<AMQPMethod>> {
        let resp = match frame {
            AMQPMethod::StartOk(_) => Some(AMQPMethod::Tune(Tune {
                channel_max: 10,
                frame_max: 1024,
                heartbeat: 10,
            })),
            AMQPMethod::TuneOk(_) => None,
            AMQPMethod::Open(_) => Some(AMQPMethod::OpenOk(OpenOk {})),
            f => {
                return Err(Unsupported(format!("unsupported method: {f:?}")).into());
            }
        };
        Ok(resp)
    }
}
