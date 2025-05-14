use crate::queue::QueueTrait;
use amq_protocol::protocol::exchange;
use amq_protocol::types::ShortString;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Mutex, Notify};

pub(crate) struct InternalExchange {
    pub(crate) declaration: exchange::Declare,
}

pub(crate) struct InternalQueue<Q: QueueTrait<Bytes> + Default> {
    pub(crate) queue_name: String,
    pub(crate) store: Q,
    pub(crate) consumed: AtomicU64,
    pub(crate) notify: Notify,
    pub(crate) is_ready: AtomicBool,
}

impl<Q: QueueTrait<Bytes> + Default> InternalQueue<Q> {
    pub fn new(queue_name: String) -> Self {
        Self {
            queue_name,
            store: Default::default(),
            consumed: Default::default(),
            notify: Default::default(),
            is_ready: false.into(),
        }
    }
}

#[derive(Default, Clone)]
pub(crate) struct Subscription {
    pub(crate) internal_id: u64,
    pub(crate) session_id: u64,
    pub(crate) channel_id: u16,
    pub(crate) consumer_tag: String,
    // TODO no_ack: bool,
    // exclusive: bool,
    pub(crate) no_ack: bool,

    pub(crate) awaiting_acks_count: u64,
    pub(crate) total_awaiting_acks_count: u64, // per channel
    pub(crate) prefetch_count: u64,            // per channel
}
pub(crate) struct UnackedMessage {
    #[allow(dead_code)] // FIXME: is never read
    pub(crate) delivery_tag: u64,
    pub(crate) queue: String,
    pub(crate) message: Bytes,
    pub(crate) consumer_tag: String,
    // TODO redelivered: bool,
    // TODO properties: MessageProperties,
    // unacked_index: u16,
}

pub(crate) struct ChannelInfo {
    pub(crate) id: u16,
    pub(crate) delivery_tag: AtomicU64, // уникален в рамках одного канала
    pub(crate) awaiting_acks: HashMap<u64, UnackedMessage>, // - delivery tag
    pub(crate) prefetch_count: u64,
    pub(crate) total_awaiting_acks_count: u64,
    // pub(crate) consumers: dashmap::DashMap<String, Subscription>, // String - consumer tag consumer tag unqiue per channel
}

pub(crate) struct Session {
    // TODO confirm_mode: bool,
    pub(crate) channels: HashMap<u16, ChannelInfo>,
    pub(crate) write: Arc<Mutex<OwnedWriteHalf>>,
}

#[derive(thiserror::Error, Debug)]
pub(crate) enum InternalError {
    #[error("session not found")]
    SessionNotFound,
    #[error("channel {0} not found")]
    ChannelNotFound(u16),
    #[error("exchange {0} not found")]
    ExchangeNotFound(String),
    #[error("queue {0} not found")]
    QueueNotFound(String, u16),
    #[error("unsupported feature cause: {0}")]
    Unsupported(String),
    #[error("invalid frame")]
    InvalidFrame,
    #[error("unknown delivery tag")]
    UnknownDeliveryTag(u16, u64),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ExchangeKind {
    Direct,
    Fanout,
    Headers,
    Topic,
}

impl From<ShortString> for ExchangeKind {
    fn from(val: ShortString) -> Self {
        match val.as_str() {
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            "headers" => ExchangeKind::Headers,
            "topic" => ExchangeKind::Topic,
            _ => ExchangeKind::Direct,
        }
    }
}
