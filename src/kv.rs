use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use bincode::config;
use log::{debug, error, info, warn};
use rusqlite::ToSql;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::config::NodeConfig;
use crate::engine::{EngineError, SynchronousRaftNode};
use crate::logging::stdlog_to_slog;
use crate::storage::SqliteBackend;
use crate::transport::{RaftMessage, TcpTransport};

#[derive(Debug, Error)]
pub enum KvError {
    #[error("raft engine error: {0}")]
    Engine(#[from] EngineError),
    #[error("failed to send proposal to raft")]
    ChannelClosed,
    #[error("failed to encode/decode command: {0}")]
    Codec(String),
    #[error("shared state poisoned")]
    StatePoisoned,
    #[error("raft proposal dropped before application")]
    ApplyDropped,
}

#[derive(Clone)]
pub struct KvHandle<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    state: Arc<RwLock<HashMap<String, T>>>,
    raft_tx: Sender<RaftMessage>,
    pending: Arc<Mutex<HashMap<u64, mpsc::Sender<()>>>>,
    next_proposal_id: Arc<AtomicU64>,
    leader_id: Arc<AtomicU64>,
    node_id: u64,
    linearizable: bool,
}

impl<T> KvHandle<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    pub fn put(&self, key: String, value: T) -> Result<(), KvError> {
        debug!("queueing PUT for key '{}'", key);
        let cmd = KvCommand::Put { key, value };
        let payload = bincode::serde::encode_to_vec(&cmd, config::standard())
            .map_err(|e| KvError::Codec(e.to_string()))?;
        if !self.linearizable {
            self.send_proposal(Vec::new(), payload)?;
            info!(
                "node {} accepted non-linearizable proposal (ack not awaited)",
                self.node_id
            );
            return Ok(());
        }

        let proposal_id = self.next_proposal_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel();
        self.pending
            .lock()
            .map_err(|_| KvError::StatePoisoned)?
            .insert(proposal_id, tx);

        let mut context = Vec::with_capacity(8);
        context.extend_from_slice(&proposal_id.to_le_bytes());

        if let Err(err) = self.send_proposal(context, payload) {
            if let Ok(mut map) = self.pending.lock() {
                map.remove(&proposal_id);
            }
            return Err(err);
        }

        match rx.recv() {
            Ok(()) => {
                info!(
                    "node {} observed majority commit for proposal {}; replying to client",
                    self.node_id, proposal_id
                );
                Ok(())
            }
            Err(_) => {
                warn!(
                    "node {} proposal {} dropped before application",
                    self.node_id, proposal_id
                );
                Err(KvError::ApplyDropped)
            }
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<T>, KvError> {
        debug!("GET query for key '{}'", key);
        let guard = self.state.read().map_err(|_| KvError::StatePoisoned)?;
        Ok(guard.get(key).cloned())
    }

    pub fn snapshot(&self) -> Result<Vec<(String, T)>, KvError> {
        debug!("snapshot requested");
        let guard = self.state.read().map_err(|_| KvError::StatePoisoned)?;
        Ok(guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    fn send_proposal(&self, context: Vec<u8>, payload: Vec<u8>) -> Result<(), KvError> {
        self.raft_tx
            .send(RaftMessage::Propose {
                data: payload,
                context,
            })
            .map_err(|_| KvError::ChannelClosed)
    }

    pub(crate) fn notify_applied(&self, context: &[u8]) {
        if !self.linearizable || context.len() != 8 {
            return;
        }
        let mut id_bytes = [0u8; 8];
        id_bytes.copy_from_slice(&context[..8]);
        let id = u64::from_le_bytes(id_bytes);
        let tx_opt = self.pending.lock().ok().and_then(|mut map| map.remove(&id));
        match tx_opt {
            Some(tx) => {
                let _ = tx.send(());
                info!(
                    "node {} acknowledged proposal {}; leader currently {}",
                    self.node_id,
                    id,
                    self.leader_id.load(Ordering::Relaxed)
                );
            }
            None => warn!("no pending proposal found for id {}", id),
        }
    }

    pub(crate) fn fail_pending(&self) {
        if !self.linearizable {
            return;
        }
        if let Ok(mut map) = self.pending.lock() {
            if !map.is_empty() {
                warn!(
                    "clearing {} pending proposals without acknowledgement",
                    map.len()
                );
                for (_, tx) in map.drain() {
                    let _ = tx.send(());
                }
            }
        }
    }

    pub fn leader_id(&self) -> u64 {
        self.leader_id.load(Ordering::Relaxed)
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn is_linearizable(&self) -> bool {
        self.linearizable
    }
}

pub struct KvApp<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    handle: KvHandle<T>,
    _raft_thread: thread::JoinHandle<()>,
}

impl<T> KvApp<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    pub fn handle(&self) -> KvHandle<T> {
        self.handle.clone()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: DeserializeOwned"))]
pub enum KvCommand<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    Put { key: String, value: T },
}

impl<T> ToSql for KvCommand<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let bytes = bincode::serde::encode_to_vec(self, config::standard())
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(ToSqlOutput::from(bytes))
    }
}

impl<T> FromSql for KvCommand<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let bytes = value.as_blob()?;
        let (cmd, _): (KvCommand<T>, usize) =
            bincode::serde::decode_from_slice(bytes, config::standard())
                .map_err(|e| FromSqlError::Other(Box::new(e)))?;
        Ok(cmd)
    }
}

pub fn start_kv_node<T>(config: NodeConfig) -> Result<KvApp<T>, KvError>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    let linearizable = config.linearizable;
    start_kv_node_with_mode(config, linearizable)
}

pub fn start_kv_node_with_mode<T>(
    config: NodeConfig,
    linearizable: bool,
) -> Result<KvApp<T>, KvError>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + ToSql + FromSql + 'static,
{
    info!(
        "bootstrapping raft node {} (raft: {}, http: {})",
        config.node_id, config.bind_addr, config.http_addr
    );
    if linearizable {
        info!("linearizable proposals enabled");
    } else {
        warn!("linearizable proposals disabled; writes return before commit");
    }

    let storage = SqliteBackend::new(PathBuf::from(&config.storage_path));
    let transport = TcpTransport::new(
        config.node_id,
        config.bind_addr.clone(),
        config.peer_addresses(),
    );
    let raft_cfg = raft::prelude::Config {
        id: config.node_id,
        heartbeat_tick: config.heartbeat_tick,
        election_tick: config.election_tick,
        ..Default::default()
    };
    let tick_interval = Duration::from_millis(config.tick_interval_ms);
    let voters = config.voter_ids();

    let logger = stdlog_to_slog();
    let mut node = SynchronousRaftNode::<KvCommand<T>>::new(
        storage,
        transport,
        raft_cfg,
        voters,
        tick_interval,
        logger,
    )?;
    let raft_tx = node.inbound_sender();

    let state = Arc::new(RwLock::new(HashMap::<String, T>::new()));
    let pending = Arc::new(Mutex::new(HashMap::<u64, mpsc::Sender<()>>::new()));
    let next_proposal_id = Arc::new(AtomicU64::new(1));
    let leader_id = Arc::new(AtomicU64::new(0));

    let handle = KvHandle {
        state: Arc::clone(&state),
        raft_tx: raft_tx.clone(),
        pending: Arc::clone(&pending),
        next_proposal_id: Arc::clone(&next_proposal_id),
        leader_id: Arc::clone(&leader_id),
        node_id: config.node_id,
        linearizable,
    };

    let apply_state = Arc::clone(&state);
    let handle_for_thread = handle.clone();
    let leader_for_thread = Arc::clone(&leader_id);

    let node_id = config.node_id;
    let raft_handle = thread::Builder::new()
        .name(format!("raft-node-{}", node_id))
        .spawn(move || {
            info!("raft worker {} entering main loop", node_id);
            if let Err(e) = node.campaign() {
                warn!("node {} failed to campaign at startup: {e}", node_id);
            }
            loop {
                let state_clone = Arc::clone(&apply_state);
                let handle_for_apply = handle_for_thread.clone();
                let leader_for_loop = Arc::clone(&leader_for_thread);
                match node.poll(tick_interval, move |cmd: KvCommand<T>, context: Vec<u8>| {
                    debug!("node {} applying committed command", node_id);
                    match cmd {
                        KvCommand::Put { key, value } => match state_clone.write() {
                            Ok(mut guard) => {
                                guard.insert(key, value);
                            }
                            Err(_) => {
                                error!("node {} state lock poisoned while applying entry", node_id)
                            }
                        },
                    }
                    handle_for_apply.notify_applied(&context);
                }) {
                    Ok(()) => {
                        let leader = node.leader_id();
                        leader_for_loop.store(leader, Ordering::Relaxed);
                    }
                    Err(e) => {
                        error!("raft loop for node {} exited with error: {e}", node_id);
                        handle_for_thread.fail_pending();
                        leader_for_thread.store(0, Ordering::Relaxed);
                        break;
                    }
                }
            }
            handle_for_thread.fail_pending();
            leader_for_thread.store(0, Ordering::Relaxed);
            info!("raft worker {} terminated", node_id);
        })
        .expect("failed to spawn raft thread");

    Ok(KvApp {
        handle,
        _raft_thread: raft_handle,
    })
}
