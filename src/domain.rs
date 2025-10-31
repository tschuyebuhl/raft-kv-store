use bincode::config;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct HardState {
    pub term: u64,
    pub vote: u64,
    pub commit_index: u64,
}

impl From<raft::eraftpb::HardState> for HardState {
    fn from(pb: raft::eraftpb::HardState) -> Self {
        Self {
            term: pb.term,
            vote: pb.vote,
            commit_index: pb.commit,
        }
    }
}

impl From<HardState> for raft::eraftpb::HardState {
    fn from(hs: HardState) -> Self {
        raft::eraftpb::HardState {
            term: hs.term,
            vote: hs.vote,
            commit: hs.commit_index,
            ..Default::default()
        }
    }
}

impl HardState {
    pub fn new(term: u64, vote: u64, commit_index: u64) -> Self {
        Self {
            term,
            vote,
            commit_index,
        }
    }
}

impl FromSql for HardState {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let bytes = value.as_blob()?;
        let cfg = config::standard();
        let (val, _len): (HardState, usize) = bincode::serde::decode_from_slice(bytes, cfg)
            .map_err(|e| FromSqlError::Other(Box::new(e)))?;
        Ok(val)
    }
}

impl ToSql for HardState {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let cfg = config::standard();
        let bytes = bincode::serde::encode_to_vec(self, cfg)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(ToSqlOutput::from(bytes))
    }
}

impl From<raft::eraftpb::ConfState> for ConfState {
    fn from(pb: raft::eraftpb::ConfState) -> Self {
        Self {
            voters: pb.voters,
            learners: pb.learners,
            voters_outgoing: pb.voters_outgoing,
            learners_next: pb.learners_next,
            auto_leave: pb.auto_leave,
        }
    }
}

impl From<ConfState> for raft::eraftpb::ConfState {
    fn from(hs: ConfState) -> Self {
        let mut pb = raft::eraftpb::ConfState::default();
        pb.set_learners(hs.learners);
        pb.set_learners_next(hs.learners_next);
        pb.set_auto_leave(hs.auto_leave);
        pb.set_voters(hs.voters);
        pb.set_voters_outgoing(hs.voters_outgoing);
        pb
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ConfState {
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub voters_outgoing: Vec<u64>,
    pub learners_next: Vec<u64>,
    pub auto_leave: bool,
}

impl FromSql for ConfState {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let bytes = value.as_blob()?;
        let cfg = config::standard();
        let (val, _len): (ConfState, usize) = bincode::serde::decode_from_slice(bytes, cfg)
            .map_err(|e| FromSqlError::Other(Box::new(e)))?;
        Ok(val)
    }
}

impl ToSql for ConfState {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let cfg = config::standard();
        let bytes = bincode::serde::encode_to_vec(self, cfg)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(ToSqlOutput::from(bytes))
    }
}

/*
*     enum EntryType {
    EntryNormal = 0;
    EntryConfChange = 1;
    EntryConfChangeV2 = 2;
}

// The entry is a type of change that needs to be applied. It contains two data fields.
// While the fields are built into the model; their usage is determined by the entry_type.
//
// For normal entries, the data field should contain the data change that should be applied.
// The context field can be used for any contextual data that might be relevant to the
// application of the data.
//
// For configuration changes, the data will contain the ConfChange message and the
// context will provide anything needed to assist the configuration change. The context
// if for the user to set and use in this case.
message Entry {
    EntryType entry_type = 1;
    uint64 term = 2;
    uint64 index = 3;
    bytes data = 4;
    bytes context = 6;

    // Deprecated! It is kept for backward compatibility.
    // TODO: remove it in the next major release.
    bool sync_log = 5;
}
*/

#[derive(Serialize, Deserialize, Debug)]
pub enum EntryType {
    Normal,
    ConfChange,
    ConfChangeV2,
}

impl From<EntryType> for raft::prelude::EntryType {
    fn from(e: EntryType) -> Self {
        match e {
            EntryType::Normal => raft::prelude::EntryType::EntryNormal,
            EntryType::ConfChange => raft::prelude::EntryType::EntryConfChange,
            EntryType::ConfChangeV2 => raft::prelude::EntryType::EntryConfChangeV2,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DbEntry {
    pub entry_type: EntryType,
    pub term: u64,
    pub index: u64,
    pub data: Vec<u8>,
    pub context: Vec<u8>,
}

impl From<DbEntry> for raft::prelude::Entry {
    fn from(e: DbEntry) -> Self {
        raft::prelude::Entry {
            entry_type: e.entry_type.into(),
            term: e.term,
            index: e.index,
            data: e.data.into(),
            context: e.context.into(),
            ..Default::default()
        }
    }
}

impl FromSql for EntryType {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value.as_i64()? {
            0 => Ok(EntryType::Normal),
            1 => Ok(EntryType::ConfChange),
            2 => Ok(EntryType::ConfChangeV2),
            // _ => Err(StorageError::UnknownEntryType),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl ToSql for EntryType {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            EntryType::Normal => Ok(0.into()),
            EntryType::ConfChange => Ok(1.into()),
            EntryType::ConfChangeV2 => Ok(2.into()),
        }
    }
}

impl FromSql for DbEntry {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let bytes = value.as_blob()?;
        let cfg = config::standard();
        let (val, _len): (DbEntry, usize) = bincode::serde::decode_from_slice(bytes, cfg)
            .map_err(|e| FromSqlError::Other(Box::new(e)))?;
        Ok(val)
    }
}

impl ToSql for DbEntry {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        let cfg = config::standard();
        let bytes = bincode::serde::encode_to_vec(self, cfg)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(ToSqlOutput::from(bytes))
    }
}
