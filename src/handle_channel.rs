use crate::models::ChannelInfo;
use crate::queue::QueueTrait;
use crate::server::BurrowMQServer;
use amq_protocol::protocol::channel;
use bytes::Bytes;
use std::sync::Arc;

impl<Q: QueueTrait<Bytes> + Default> BurrowMQServer<Q> {
    pub(crate) async fn handle_channel_method(
        self: Arc<Self>,
        channel_id: u16,
        session_id: u64,
        frame: channel::AMQPMethod,
    ) -> anyhow::Result<channel::AMQPMethod> {
        let resp = match frame {
            channel::AMQPMethod::Open(_open) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(&session_id).expect("Session not found");

                if session
                    .channels
                    .iter()
                    .filter(|c| c.id == channel_id)
                    .count()
                    > 0
                {
                    return Ok(channel::AMQPMethod::Close(channel::Close {
                        method_id: 10,
                        class_id: 20,
                        reply_code: 504,
                        reply_text: "channel already open".into(),
                    }));
                }

                session.channels.push(ChannelInfo {
                    id: channel_id,
                    active_consumers: Default::default(),
                    delivery_tag: 0.into(),
                    awaiting_acks: Default::default(),
                    prefetch_count: 1,
                });

                channel::AMQPMethod::OpenOk(channel::OpenOk {})
            }
            channel::AMQPMethod::Close(_close) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(&session_id).expect("Session not found");

                session.channels.retain(|c| c.id != channel_id);
                drop(sessions);

                channel::AMQPMethod::CloseOk(channel::CloseOk {})
            }
            channel::AMQPMethod::CloseOk(_) => {
                unimplemented!("unimplemented queue method: channel.close_ok")
            }
            f => {
                unimplemented!("unimplemented queue method: {f:?}")
            }
        };
        Ok(resp)
    }
}
