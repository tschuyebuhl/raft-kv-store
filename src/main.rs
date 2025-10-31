use std::{env, error::Error, thread, time::Duration};

use bincode::config;
use log::info;
use raft_kv_store::{NodeConfig, start_http_service, start_kv_node};
use rusqlite::ToSql;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct ConsensusData {
    height: u64,
    round: u64,
    step: u64,
}

impl ToSql for ConsensusData {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let bytes = bincode::serde::encode_to_vec(self, config::standard())
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(ToSqlOutput::from(bytes))
    }
}

impl FromSql for ConsensusData {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let bytes = value.as_blob()?;
        let (data, _): (ConsensusData, usize) =
            bincode::serde::decode_from_slice(bytes, config::standard())
                .map_err(|e| FromSqlError::Other(Box::new(e)))?;
        Ok(data)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .init();

    let config_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "config/node1.toml".to_string());
    info!("loading config from {}", config_path);
    let config = NodeConfig::from_path(&config_path)?;
    info!(
        "linearizable writes: {}",
        if config.linearizable {
            "enabled"
        } else {
            "disabled"
        }
    );

    let kv_app = start_kv_node::<ConsensusData>(config.clone())?;
    let http_handle = start_http_service::<ConsensusData>(&config.http_addr, kv_app.handle())?;

    info!(
        "node {} ready; http endpoint {}",
        config.node_id, config.http_addr
    );

    let _kv_guard = kv_app;
    let _http_guard = http_handle;

    loop {
        thread::sleep(Duration::from_secs(60));
    }
}
