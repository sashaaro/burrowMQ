use crate::models::InternalError::Unsupported;
use crate::models::InternalExchange;
use crate::queue::QueueTrait;
use crate::server::BurrowMQServer;
use amq_protocol::protocol::exchange;
use amq_protocol::protocol::exchange::DeclareOk;
use amq_protocol::protocol::exchange::DeleteOk;
use bytes::Bytes;
use std::sync::Arc;

impl<Q: QueueTrait<Bytes> + Default> BurrowMQServer<Q> {
    pub(crate) async fn handle_exchange_method(
        self: Arc<Self>,
        frame: exchange::AMQPMethod,
    ) -> anyhow::Result<exchange::AMQPMethod> {
        let resp: exchange::AMQPMethod = match frame {
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

                exchange::AMQPMethod::DeclareOk(DeclareOk {})
            }
            exchange::AMQPMethod::Bind(_) => {
                todo!()
            }
            exchange::AMQPMethod::Delete(delete) => {
                let exchange = self.exchanges.lock().await.remove(delete.exchange.as_str());
                if let Some(exchange) = exchange {
                    // TODO
                }

                exchange::AMQPMethod::DeleteOk(DeleteOk {})
            }
            f => {
                return Err(Unsupported(format!("unsupported method: {f:?}")).into());
            }
        };
        Ok(resp)
    }
}
