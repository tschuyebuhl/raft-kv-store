use std::sync::mpsc::{Receiver, Sender};

use raft::RawNode;

use crate::storage::SqliteBackend;

struct Node<T> {
    raft_group: RawNode<SqliteBackend>,
    incoming: Receiver<T>,
    outgoing: Sender<T>,
}
