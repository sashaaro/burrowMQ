use crate::models::InternalError::Unsupported;
use crate::models::{ChannelInfo, InternalError};
use crate::queue::QueueTrait;
use crate::server::BurrowMQServer;
use amq_protocol::protocol::channel;
use bytes::Bytes;
use std::sync::Arc;

impl<Q: QueueTrait<Bytes> + Default> BurrowMQServer<Q> {
    pub(crate) async fn handle_channel_method(
        &self,
        channel_id: u16,
        session_id: u64,
        frame: channel::AMQPMethod,
    ) -> anyhow::Result<channel::AMQPMethod> {
        let resp = match frame {
            channel::AMQPMethod::Open(_open) => {
                let mut session = match self.sessions.get_mut(&session_id) {
                    Some(session) => session,
                    None => return Err(InternalError::SessionNotFound.into()),
                };

                if session.channels.get(&channel_id).is_some() {
                    return Ok(channel::AMQPMethod::Close(channel::Close {
                        method_id: 10,
                        class_id: 20,
                        reply_code: 504,
                        reply_text: "channel already open".into(),
                    }));
                }

                session.channels.insert(
                    channel_id,
                    ChannelInfo {
                        id: channel_id,
                        delivery_tag: 0.into(),
                        awaiting_acks: Default::default(),
                        prefetch_count: 1,
                        total_awaiting_acks_count: 0,
                    },
                );

                channel::AMQPMethod::OpenOk(channel::OpenOk {})
            }
            channel::AMQPMethod::Close(_close) => {
                let mut session = match self.sessions.get_mut(&session_id) {
                    Some(session) => session,
                    None => return Err(InternalError::SessionNotFound.into()),
                };

                let Some(channel) = session.channels.remove(&channel_id) else {
                    return Err(InternalError::ChannelNotFound(channel_id).into());
                };

                let mut consumer_metadata = self.consumer_metadata.lock().await;
                for x in channel.awaiting_acks {
                    // TODO pushfront awaiting acks messages to queue
                    // consumer_metadata.consumer_tags.
                }

                let mut tags = vec![];
                for x in consumer_metadata.consumer_tags.iter() {
                    for sub in x.1 {
                        if sub.1.channel_id == channel.id {
                            tags.push(sub.1.consumer_tag.clone());
                        }
                    }
                }
                for tag in tags {
                    let queue = consumer_metadata.queues.get(&tag);
                    let Some(queue) = queue else {
                        continue;
                    };
                    let queue = queue.to_string();

                    let mut subscription = consumer_metadata.consumer_tags.get_mut(&queue);
                    if let Some(mut subscriptions) = subscription {
                        if let Some(subscription) = subscriptions.remove(&tag) {
                            // subscription.
                        }
                    }
                }

                channel::AMQPMethod::CloseOk(channel::CloseOk {})
            }
            channel::AMQPMethod::CloseOk(f) => {
                return Err(Unsupported(format!("unsupported method: {f:?}")).into());
            }
            f => {
                return Err(Unsupported(format!("unsupported method: {f:?}")).into());
            }
        };
        Ok(resp)
    }
}
