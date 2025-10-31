use std::path::PathBuf;

use raft::Storage;
use rusqlite::params;

use crate::domain::Entry;

pub struct SqliteBackend {
    db: rusqlite::Connection,
}

impl SqliteBackend {
    pub fn new(path: PathBuf) -> Self {
        let db = match rusqlite::Connection::open(path) {
            Ok(db) => db,
            Err(err) => panic!("Failed to open database: {}", err),
        };
        Self { db }
    }
    /*
     * entry data: term index data context
     * conf state data: voters learners voters_outgoing learners_next, auto_leave
     * hard state data: term vote commit
     */
    pub fn initialize(&mut self) {
        self.db
            .execute_batch(
                r#"
          create table if not exists hard_state (
            id integer primary key check (id = 1),
            data blob not null
          );

          create table if not exists conf_state (
            id integer primary key check (id = 1),
            data blob not null
          );

          create table if not exists entries (
            entry_type integer not null,
            term integer not null,
            log_index integer primary key,
            context blob not null,
            data blob not null
          );
        "#,
            )
            .unwrap();
    }

    pub fn set_hard_state(&mut self, hs: raft::eraftpb::HardState) {
        let local: crate::domain::HardState = hs.into();
        self.db
            .execute(
                "insert into hard_state (id, data)
                     values (1, ?1)
                     on conflict(id) do update set data = excluded.data;",
                params![local],
            )
            .expect("writing to db should succeed");
    }

    pub fn hard_state(&self) -> raft::Result<raft::eraftpb::HardState> {
        let mut stmt = self
            .db
            .prepare("select data from hard_state where id = 1")
            .unwrap();
        let row: crate::domain::HardState = stmt.query_row([], |row| row.get(0)).unwrap();
        Ok(row.into())
    }

    pub fn set_conf_state(&mut self, cs: raft::eraftpb::ConfState) {
        let local: crate::domain::ConfState = cs.into();
        self.db
            .execute(
                "insert into conf_state (id, data)
                     values (1, ?1)
                     on conflict(id) do update set data = excluded.data;",
                params![local],
            )
            .expect("writing to db should succeed");
    }

    pub fn conf_state(&self) -> raft::Result<raft::eraftpb::ConfState> {
        let mut stmt = self
            .db
            .prepare("select data from conf_state where id = 1")
            .unwrap();
        let row: crate::domain::ConfState = stmt.query_row([], |row| row.get(0)).unwrap();
        Ok(row.into())
    }
}

impl Storage for SqliteBackend {
    fn initial_state(&self) -> raft::Result<raft::RaftState> {
        Ok(raft::RaftState {
            hard_state: self.hard_state()?,
            conf_state: self.conf_state()?,
        })
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        _max_size: impl Into<Option<u64>>,
        _context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<raft::prelude::Entry>> {
        let mut stmt = self
            .db
            .prepare(
                "select entry_type, term, log_index, context, data
                 from entries
                 where log_index >= ?1 and log_index < ?2",
            )
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(Box::new(e))))?;

        let domain_rows = stmt
            .query_map(params![low, high], |row| {
                Ok(Entry {
                    entry_type: row.get("entry_type")?,
                    term: row.get("term")?,
                    index: row.get("log_index")?,
                    context: row.get("context")?,
                    data: row.get("data")?,
                })
            })
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(Box::new(e))))?;

        let mut result = Vec::new();
        for r in domain_rows {
            let domain_entry =
                r.map_err(|e| raft::Error::Store(raft::StorageError::Other(Box::new(e))))?;
            result.push(domain_entry.into());
        }

        Ok(result)
    }

    // Term of entry AT index `idx`.
    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == 0 {
            return Ok(0);
        }
        let mut stmt = self
            .db
            .prepare("select term from entries where log_index = ?")
            .unwrap();
        let term: u64 = stmt.query_row([idx], |row| row.get(0)).unwrap();
        Ok(term)
    }

    // Index of first entry. Need to query SQLite for the minimum index.
    fn first_index(&self) -> raft::Result<u64> {
        let mut stmt = self
            .db
            .prepare("select min(log_index) from entries")
            .unwrap();
        let first_index: Option<u64> = stmt.query_row([], |row| row.get(0)).unwrap();
        Ok(first_index.unwrap_or(0))
    }

    // Index of last entry. Need to query SQLite for the maximum index.
    fn last_index(&self) -> raft::Result<u64> {
        let mut stmt = self
            .db
            .prepare("select max(log_index) from entries")
            .unwrap();
        let last_index: Option<u64> = stmt.query_row([], |row| row.get(0)).unwrap();
        Ok(last_index.unwrap_or(0))
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<raft::prelude::Snapshot> {
        let last_index = self.last_index()?;
        let hard_state = self.hard_state()?;
        if request_index > last_index {
            return Err(raft::Error::Store(raft::StorageError::SnapshotOutOfDate));
        }
        let mut snapshot = raft::prelude::Snapshot::new();
        snapshot.mut_metadata().set_conf_state(self.conf_state()?);
        snapshot.mut_metadata().set_index(hard_state.commit);
        snapshot.mut_metadata().set_term(hard_state.term);

        let mut stmt = self
            .db
            .prepare("select data from entries where log_index = ?")
            .unwrap();
        let row: Vec<u8> = stmt.query_row([request_index], |row| row.get(0)).unwrap();
        snapshot.set_data(row.into());
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::{GetEntriesContext, prelude::*};

    fn setup_backend() -> SqliteBackend {
        let mut backend = SqliteBackend::new(":memory:".into());
        backend.initialize();
        backend
    }

    #[test]
    fn test_initialize_creates_tables() {
        let backend = setup_backend();
        let tables: Vec<String> = backend
            .db
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(|x| x.unwrap())
            .collect();

        assert!(tables.contains(&"hard_state".to_string()));
        assert!(tables.contains(&"conf_state".to_string()));
        assert!(tables.contains(&"entries".to_string()));
    }

    #[test]
    fn test_set_and_get_hard_state() {
        let mut backend = setup_backend();
        let mut hs = HardState::default();
        hs.term = 5;
        hs.vote = 10;
        hs.commit = 20;

        backend.set_hard_state(hs.clone());
        let loaded = backend.hard_state().unwrap();

        assert_eq!(loaded.term, 5);
        assert_eq!(loaded.vote, 10);
        assert_eq!(loaded.commit, 20);
    }

    #[test]
    fn test_set_and_get_conf_state() {
        let mut backend = setup_backend();
        let mut cs = ConfState::default();
        cs.voters = vec![1, 2, 3];

        backend.set_conf_state(cs.clone());
        let loaded = backend.conf_state().unwrap();

        assert_eq!(loaded.voters, vec![1, 2, 3]);
    }

    #[test]
    fn test_first_and_last_index_empty() {
        let backend = setup_backend();
        assert_eq!(backend.first_index().unwrap(), 0);
        assert_eq!(backend.last_index().unwrap(), 0);
    }

    #[test]
    fn test_insert_and_query_entries() {
        let backend = setup_backend();

        backend
            .db
            .execute(
                "insert into entries (entry_type, term, log_index, context, data)
             values (?1, ?2, ?3, ?4, ?5)",
                params![1i32, 2i64, 3i64, vec![1u8, 2, 3], vec![9u8, 8, 7]],
            )
            .unwrap();

        backend
            .db
            .execute(
                "insert into entries (entry_type, term, log_index, context, data)
             values (?1, ?2, ?3, ?4, ?5)",
                params![1i32, 4i64, 5i64, vec![0u8], vec![10u8]],
            )
            .unwrap();

        let entries = backend
            .entries(1, 10, None, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].term, 2);
        assert_eq!(entries[1].term, 4);

        assert_eq!(backend.first_index().unwrap(), 3);
        assert_eq!(backend.last_index().unwrap(), 5);
        assert_eq!(backend.term(5).unwrap(), 4);
    }

    #[test]
    fn test_snapshot_valid_and_out_of_date() {
        let mut backend = setup_backend();

        let mut hs = HardState::default();
        hs.term = 1;
        hs.commit = 3;
        backend.set_hard_state(hs);

        let mut cs = ConfState::default();
        cs.voters = vec![1];
        backend.set_conf_state(cs);

        backend
            .db
            .execute(
                "insert into entries (entry_type, term, log_index, context, data)
             values (?1, ?2, ?3, ?4, ?5)",
                params![1i32, 1i64, 3i64, vec![], vec![42u8]],
            )
            .unwrap();

        let snapshot = backend.snapshot(3, 0).unwrap();
        assert_eq!(snapshot.get_metadata().index, 3);
        assert_eq!(snapshot.get_metadata().term, 1);
        assert_eq!(snapshot.get_data(), &[42u8]);

        let err = backend.snapshot(10, 0).unwrap_err();
        match err {
            raft::Error::Store(raft::StorageError::SnapshotOutOfDate) => {}
            _ => panic!("Expected SnapshotOutOfDate"),
        }
    }
}
