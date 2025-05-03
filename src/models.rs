use amq_protocol::protocol::exchange;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

pub(crate) struct InternalExchange {
    pub(crate) declaration: exchange::Declare,
}

pub(crate) struct InternalQueue {
    // TODO declaration: queue::Declare,
    pub(crate) queue_name: String,
    pub(crate) ready_vec: VecDeque<Bytes>,
    pub(crate) unacked_vec: VecDeque<Bytes>,
    // TODO messages_ready: u64
    // TODO messages_unacknowledged: u64
    // acked: AtomicU64,
    // acked_markers: [bool; 2048],
    // marker_index: AtomicU32,
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
    // TODO message: Bytes,
    // TODO redelivered: bool,
    // TODO properties: MessageProperties,
    // unacked_index: u16,
}

pub(crate) struct ChannelInfo {
    pub(crate) id: u16,
    pub(crate) active_consumers: HashMap<String, ConsumerSubscription>, // String - consumer tag // TODO subscriptions list
    pub(crate) unacked_messages: HashMap<u64, UnackedMessage>,          // u64 - delivery tag
    pub(crate) delivery_tag: AtomicU64, // уникален в рамках одного канала
}

pub(crate) struct Session {
    // TODO confirm_mode: bool,
    pub(crate) channels: Vec<ChannelInfo>,
    pub(crate) read: Arc<Mutex<OwnedReadHalf>>,
    pub(crate) write: Arc<Mutex<OwnedWriteHalf>>,
}
