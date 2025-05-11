use crate::queue::QueueTrait;
use amq_protocol::protocol::exchange;
use amq_protocol::types::ShortString;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender, channel};

pub(crate) struct InternalExchange {
    pub(crate) declaration: exchange::Declare,
}

pub(crate) struct InternalQueue<Q: QueueTrait<Bytes> + Default> {
    pub(crate) queue_name: String,
    pub(crate) ready_vec: Q,
    pub(crate) consuming_rev: Mutex<Receiver<bool>>,
    pub(crate) consuming: AtomicBool,
    // TODO messages_ready: u64
    // TODO messages_unacknowledged: u64
    // acked: AtomicU64,
    // acked_markers: [bool; 2048],
    // marker_index: AtomicU32,
    pub consumed: AtomicU64,
    pub(crate) consuming_send: Sender<bool>,
}

impl<Q: QueueTrait<Bytes> + Default> InternalQueue<Q> {
    pub fn new(queue_name: String) -> Self {
        let (consuming_send, consuming_rev) = channel(1);
        Self {
            queue_name,
            ready_vec: Default::default(),
            consumed: Default::default(),
            consuming: Default::default(),
            consuming_rev: Mutex::new(consuming_rev),
            consuming_send,
        }
    }
}

#[derive(Default, Clone)]
pub(crate) struct ConsumerSubscription {
    #[allow(dead_code)] // FIXME: is never read
    pub(crate) queue: String,
    // callback: куда доставлять сообщения
    // TODO no_ack: bool,
    // exclusive: bool,
}
pub(crate) struct UnackedMessage {
    #[allow(dead_code)] // FIXME: is never read
    pub(crate) delivery_tag: u64,
    pub(crate) queue: String,
    pub(crate) message: Bytes,
    // TODO redelivered: bool,
    // TODO properties: MessageProperties,
    // unacked_index: u16,
}

pub(crate) struct ChannelInfo {
    pub(crate) id: u16,
    pub(crate) active_consumers: HashMap<String, ConsumerSubscription>, // String - consumer tag // TODO subscriptions list
    pub(crate) delivery_tag: AtomicU64, // уникален в рамках одного канала
    pub(crate) awaiting_acks: HashMap<u64, UnackedMessage>, // - delivery tag
    pub(crate) prefetch_count: u64,
}

pub(crate) struct Session {
    // TODO confirm_mode: bool,
    pub(crate) channels: Vec<ChannelInfo>,
    pub(crate) read: Arc<Mutex<OwnedReadHalf>>,
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
    QueueNotFound(String),
    #[error("unsupported feature cause: {0}")]
    Unsupported(String),
    #[error("invalid frame")]
    InvalidFrame,
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
