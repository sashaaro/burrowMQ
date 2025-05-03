use crate::models::ChannelInfo;
use crate::server::BurrowMQServer;
use crate::utils::make_buffer_from_frame;
use amq_protocol::frame::AMQPFrame;
use amq_protocol::protocol::{AMQPClass, channel};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

impl BurrowMQServer {
    pub(crate) async fn handle_channel_method(
        self: Arc<Self>,
        channel_id: u16,
        session_id: u64,
        socket: Arc<Mutex<OwnedWriteHalf>>,
        frame: channel::AMQPMethod,
    ) -> anyhow::Result<()> {
        match frame {
            channel::AMQPMethod::Open(_) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(&session_id).expect("Session not found");

                session.channels.push(ChannelInfo {
                    active_consumers: Default::default(),
                    id: channel_id,
                    delivery_tag: 0.into(),
                    unacked_messages: Default::default(),
                });

                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Channel(channel::AMQPMethod::OpenOk(channel::OpenOk {})),
                );
                let buffer = make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }
            channel::AMQPMethod::Close(close) => {
                let mut sessions = self.sessions.lock().await;
                let session = sessions.get_mut(&session_id).expect("Session not found");

                session.channels.retain(|c| c.id != channel_id);
                // let mut sessions = self.sessions.lock().await;
                // let session = sessions.get_mut(session_id).expect("Session not found");
                // session.channels = session.channels.iter().filter(|c| c.id != channel_id).cloned().collect();

                drop(sessions);

                let amqp_frame = AMQPFrame::Method(
                    channel_id,
                    AMQPClass::Channel(channel::AMQPMethod::CloseOk(channel::CloseOk {})),
                );
                let buffer = make_buffer_from_frame(&amqp_frame);
                let _ = socket.lock().await.write_all(&buffer).await;
            }

            f => {
                unimplemented!("unimplemented queue method: {f:?}")
            }
        }
        Ok(())
    }
}
