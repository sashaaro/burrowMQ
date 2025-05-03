use crate::models::ChannelInfo;
use crate::server::BurrowMQServer;
use amq_protocol::protocol::channel;
use std::sync::Arc;

impl BurrowMQServer {
    pub(crate) async fn handle_channel_method(
        self: Arc<Self>,
        channel_id: u16,
        session_id: u64,
        frame: channel::AMQPMethod,
    ) -> anyhow::Result<channel::AMQPMethod> {
        let resp = match frame {
            channel::AMQPMethod::Open(_) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(&session_id).expect("Session not found");

                session.channels.push(ChannelInfo {
                    active_consumers: Default::default(),
                    id: channel_id,
                    delivery_tag: 0.into(),
                    unacked_messages: Default::default(),
                });

                channel::AMQPMethod::OpenOk(channel::OpenOk {})
            }
            channel::AMQPMethod::Close(close) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(&session_id).expect("Session not found");

                session.channels.retain(|c| c.id != channel_id);
                // let mut sessions = self.sessions.lock().await;
                // let session = sessions.get_mut(session_id).expect("Session not found");
                // session.channels = session.channels.iter().filter(|c| c.id != channel_id).cloned().collect();

                drop(sessions);

                channel::AMQPMethod::CloseOk(channel::CloseOk {})
            }

            f => {
                unimplemented!("unimplemented queue method: {f:?}")
            }
        };
        Ok(resp)
    }
}
