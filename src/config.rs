use std::fs;
use std::path::Path;

use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config file: {0}")]
    Parse(#[from] toml::de::Error),
}

#[derive(Debug, Clone, Deserialize)]
pub struct PeerConfig {
    pub id: u64,
    pub addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeConfig {
    pub node_id: u64,
    pub bind_addr: String,
    pub storage_path: String,
    pub http_addr: String,
    #[serde(default = "default_linearizable")]
    pub linearizable: bool,
    #[serde(default = "default_heartbeat_tick")]
    pub heartbeat_tick: usize,
    #[serde(default = "default_election_tick")]
    pub election_tick: usize,
    #[serde(default = "default_tick_interval_ms")]
    pub tick_interval_ms: u64,
    pub peers: Vec<PeerConfig>,
}

impl NodeConfig {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let contents = fs::read_to_string(path)?;
        Ok(toml::from_str(&contents)?)
    }

    pub fn peer_addresses(&self) -> Vec<(u64, String)> {
        self.peers.iter().map(|p| (p.id, p.addr.clone())).collect()
    }

    pub fn voter_ids(&self) -> Vec<u64> {
        let mut voters: Vec<u64> = self.peers.iter().map(|p| p.id).collect();
        if !voters.contains(&self.node_id) {
            voters.push(self.node_id);
        }
        voters
    }
}

fn default_heartbeat_tick() -> usize {
    2
}

fn default_election_tick() -> usize {
    10
}

fn default_tick_interval_ms() -> u64 {
    200
}

fn default_linearizable() -> bool {
    true
}
