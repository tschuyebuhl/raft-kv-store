use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::{debug, info, warn};
use protobuf::Message as ProtobufMessage;
use raft::prelude::Message as RaftProtoMessage;
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

/// Unified TCP transport for Raft messages.
pub struct TcpTransport {
    bind_addr: String,
    node_id: u64,
    peers: HashMap<u64, (String, Option<BufWriter<TcpStream>>)>,
}

pub enum RaftMessage {
    Propose { data: Vec<u8>, context: Vec<u8> },
    Msg(RaftProtoMessage),
}

impl TcpTransport {
    pub fn new(node_id: u64, bind_addr: String, peers: Vec<(u64, String)>) -> Self {
        let peers = peers
            .into_iter()
            .filter(|(id, _)| *id != node_id)
            .map(|(id, addr)| (id, (addr, None)))
            .collect();
        Self {
            bind_addr,
            node_id,
            peers,
        }
    }

    /// Start listener thread. Pushes all received Raft messages into `raft_tx`.
    pub fn start_listener(&self, raft_tx: Sender<RaftMessage>) {
        let bind_addr = self.bind_addr.clone();
        let listener = TcpListener::bind(&bind_addr)
            .unwrap_or_else(|_| panic!("failed to bind transport on {}", bind_addr));
        info!("node {} listening on {}", self.node_id, bind_addr);

        thread::spawn(move || {
            for conn in listener.incoming() {
                if let Ok(stream) = conn {
                    debug!("accepted inbound raft connection on {bind_addr}");
                    let tx = raft_tx.clone();
                    thread::spawn(move || handle_connection(stream, tx));
                } else if let Err(e) = conn {
                    warn!("error accepting connection: {e}");
                }
            }
        });
    }

    /// Send one Raft message to its target peer.
    pub fn send(&mut self, msg: &RaftProtoMessage) {
        let to_id = msg.to;
        let (addr, writer_opt) = match self.peers.get_mut(&to_id) {
            Some(p) => p,
            None => {
                warn!("unknown peer {}, dropping outbound msg", to_id);
                return;
            }
        };
        let addr_nonmut = addr.clone();

        // reconnect if needed
        if writer_opt.is_none() {
            match TcpStream::connect(addr_nonmut) {
                Ok(stream) => {
                    stream.set_write_timeout(Some(Duration::from_secs(1))).ok();
                    info!("{} connected to peer {} ({})", self.node_id, to_id, addr);
                    *writer_opt = Some(BufWriter::new(stream));
                }
                Err(e) => {
                    warn!("connect {} -> {} failed: {}", self.node_id, addr, e);
                    return;
                }
            }
        }

        let w = writer_opt.as_mut().unwrap();
        match msg.write_to_bytes() {
            Ok(bytes) => {
                if w.write_u32::<BigEndian>(bytes.len() as u32).is_err()
                    || w.write_all(&bytes).is_err()
                    || w.flush().is_err()
                {
                    warn!("broken connection to {}; will reconnect", to_id);
                    *writer_opt = None;
                } else {
                    debug!("sent raft message to {} ({} bytes)", to_id, bytes.len());
                }
            }
            Err(e) => warn!("serialization error: {:?}", e),
        }
    }

    /// Broadcast message to all peers except self.
    pub fn broadcast(&mut self, msg: &RaftProtoMessage) {
        for peer_id in self.peers.keys().cloned().collect::<Vec<_>>() {
            if peer_id == self.node_id {
                continue;
            }
            let mut clone = msg.clone();
            clone.to = peer_id;
            self.send(&clone);
        }
    }
}

/// Handle one inbound TCP stream: continuously parse framed Raft messages.
fn handle_connection(mut stream: TcpStream, raft_tx: Sender<RaftMessage>) {
    let mut reader = BufReader::new(&mut stream);

    while let Ok(l) = reader.read_u32::<BigEndian>() {
        let mut buf = vec![0; l as usize];
        if reader.read_exact(&mut buf).is_err() {
            break;
        }
        match RaftProtoMessage::parse_from_bytes(&buf) {
            Ok(msg) => {
                debug!("received raft message {:?}", msg.get_msg_type());
                if raft_tx.send(RaftMessage::Msg(msg)).is_err() {
                    warn!("raft channel closed; stopping connection handler");
                    break;
                }
            }
            Err(e) => warn!("decode error: {:?}", e),
        }
    }
}
