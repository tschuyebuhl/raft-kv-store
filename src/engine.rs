use std::marker::PhantomData;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use std::time::{Duration, Instant};

use bincode::config;
use log::{debug, info, trace, warn};
use raft::prelude::{Config, Entry, EntryType, Message};
use raft::{RawNode, Ready};
use rusqlite::{ToSql, types::FromSql};

use crate::storage::{SharedSqliteStorage, SqliteBackend};
use crate::transport::{RaftMessage, TcpTransport};

pub type EngineResult<T> = Result<T, EngineError>;

#[derive(Debug)]
pub enum EngineError {
    Raft(raft::Error),
    Codec(String),
    ChannelClosed,
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Raft(e) => write!(f, "raft error: {e}"),
            EngineError::Codec(e) => write!(f, "codec error: {e}"),
            EngineError::ChannelClosed => write!(f, "engine input channel closed"),
        }
    }
}

impl std::error::Error for EngineError {}

impl From<raft::Error> for EngineError {
    fn from(err: raft::Error) -> Self {
        EngineError::Raft(err)
    }
}

pub struct SynchronousRaftNode<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + 'static + ToSql + FromSql,
{
    raw_node: RawNode<SharedSqliteStorage>,
    storage: SharedSqliteStorage,
    transport: TcpTransport,
    inbound_tx: Sender<RaftMessage>,
    inbound_rx: Receiver<RaftMessage>,
    tick_interval: Duration,
    last_tick: Instant,
    _marker: PhantomData<T>,
}

impl<T> SynchronousRaftNode<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + 'static + ToSql + FromSql,
{
    pub fn new(
        mut storage: SqliteBackend,
        transport: TcpTransport,
        mut cfg: Config,
        voters: Vec<u64>,
        tick_interval: Duration,
        logger: slog::Logger,
    ) -> EngineResult<Self> {
        storage.initialize();
        storage.ensure_defaults(&voters)?;
        let hard_state = storage.hard_state()?;
        cfg.applied = hard_state.commit;
        cfg.validate().map_err(EngineError::from)?;

        let shared_storage = storage.shared();
        let (inbound_tx, inbound_rx) = std::sync::mpsc::channel();
        transport.start_listener(inbound_tx.clone());

        info!("initialising raft raw node {}", cfg.id);
        let raw_node =
            RawNode::new(&cfg, shared_storage.clone(), &logger).map_err(EngineError::from)?;
        info!("raft raw node {} ready", cfg.id);

        Ok(Self {
            raw_node,
            storage: shared_storage,
            transport,
            inbound_tx,
            inbound_rx,
            tick_interval,
            last_tick: Instant::now(),
            _marker: PhantomData,
        })
    }

    pub fn inbound_sender(&self) -> Sender<RaftMessage> {
        self.inbound_tx.clone()
    }

    pub fn leader_id(&self) -> u64 {
        self.raw_node.raft.leader_id
    }

    pub fn poll<F>(&mut self, timeout: Duration, mut on_entry: F) -> EngineResult<()>
    where
        F: FnMut(T, Vec<u8>),
    {
        match self.inbound_rx.recv_timeout(timeout) {
            Ok(msg) => self.consume_message(msg)?,
            Err(RecvTimeoutError::Timeout) => trace!("poll timeout expired, ticking"),
            Err(RecvTimeoutError::Disconnected) => return Err(EngineError::ChannelClosed),
        }

        loop {
            match self.inbound_rx.try_recv() {
                Ok(msg) => self.consume_message(msg)?,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Err(EngineError::ChannelClosed),
            }
        }

        self.maybe_tick();
        self.handle_ready(&mut on_entry)?;
        Ok(())
    }

    pub fn propose(&mut self, data: &T) -> EngineResult<()> {
        let payload = bincode::serde::encode_to_vec(data, config::standard())
            .map_err(|e| EngineError::Codec(e.to_string()))?;
        self.raw_node
            .propose(Vec::new(), payload)
            .map_err(EngineError::from)
    }

    pub fn campaign(&mut self) -> EngineResult<()> {
        self.raw_node.campaign().map_err(EngineError::from)
    }

    fn consume_message(&mut self, msg: RaftMessage) -> EngineResult<()> {
        match msg {
            RaftMessage::Msg(m) => {
                debug!("dispatching raft message to state machine");
                self.raw_node.step(m).map_err(EngineError::from)
            }
            RaftMessage::Propose { data, context } => {
                debug!(
                    "processing external proposal ({} bytes, context {})",
                    data.len(),
                    context.len()
                );
                self.raw_node
                    .propose(context, data)
                    .map_err(EngineError::from)
            }
        }
    }

    fn maybe_tick(&mut self) {
        if self.last_tick.elapsed() >= self.tick_interval {
            trace!("advancing raft tick");
            self.raw_node.tick();
            self.last_tick = Instant::now();
        }
    }

    fn handle_ready<F>(&mut self, on_entry: &mut F) -> EngineResult<()>
    where
        F: FnMut(T, Vec<u8>),
    {
        if !self.raw_node.has_ready() {
            return Ok(());
        }

        let mut ready = self.raw_node.ready();
        self.send_messages(ready.take_messages());

        if !ready.snapshot().is_empty() {
            info!("snapshot received in ready");
            let snapshot = ready.snapshot().clone();
            let handle = self.storage.handle();
            let mut storage = handle.lock().expect("storage lock poisoned");
            storage
                .apply_snapshot(snapshot)
                .map_err(EngineError::from)?;
        }

        let committed = ready.take_committed_entries();
        self.apply_committed(committed, on_entry);

        self.persist_ready(&mut ready)?;
        self.send_messages(ready.take_persisted_messages());

        let mut light_ready = self.raw_node.advance(ready);
        if let Some(commit) = light_ready.commit_index() {
            debug!("advancing commit index to {}", commit);
            let handle = self.storage.handle();
            let mut storage = handle.lock().expect("storage lock poisoned");
            storage.update_commit(commit).map_err(EngineError::from)?;
        }
        self.send_messages(light_ready.take_messages());
        let committed_after_advance = light_ready.take_committed_entries();
        self.apply_committed(committed_after_advance, on_entry);
        self.raw_node.advance_apply();
        Ok(())
    }

    fn persist_ready(&mut self, ready: &mut Ready) -> EngineResult<()> {
        let entries = ready.take_entries();
        let handle = self.storage.handle();
        let mut storage = handle.lock().expect("storage lock poisoned");
        if !entries.is_empty() {
            storage
                .append_entries(&entries)
                .map_err(EngineError::from)?;
        }
        if let Some(hs) = ready.hs() {
            storage.set_hard_state(hs.clone());
        }
        Ok(())
    }

    fn send_messages(&mut self, msgs: Vec<Message>) {
        for msg in msgs {
            if msg.to == 0 {
                continue;
            }
            debug!(
                "sending raft message to {} of type {:?}",
                msg.to,
                msg.get_msg_type()
            );
            self.transport.send(&msg);
        }
    }

    fn apply_committed<F>(&self, entries: Vec<Entry>, on_entry: &mut F)
    where
        F: FnMut(T, Vec<u8>),
    {
        for entry in entries {
            if entry.data.is_empty() {
                continue;
            }
            if entry.get_entry_type() != EntryType::EntryNormal {
                continue;
            }
            match bincode::serde::decode_from_slice::<T, _>(&entry.data, config::standard()) {
                Ok((value, _)) => {
                    debug!("delivering committed entry to state machine");
                    on_entry(value, entry.get_context().to_vec())
                }
                Err(e) => warn!("failed to decode committed entry: {}", e),
            }
        }
    }
}
