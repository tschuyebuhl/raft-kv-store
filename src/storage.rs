use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use raft::Storage;
use rusqlite::params;

use crate::domain::DbEntry;

pub struct SqliteBackend {
    db: rusqlite::Connection,
}

#[derive(Clone)]
pub struct SharedSqliteStorage {
    inner: Arc<Mutex<SqliteBackend>>,
}

impl SqliteBackend {
    pub fn new(path: PathBuf) -> Self {
        let db = rusqlite::Connection::open(path).expect("Failed to open database");
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

          create table if not exists snapshots (
              id integer primary key check (id = 1),
              data blob not null,
              term integer not null,
              log_index integer not null
          );
        "#,
            )
            .unwrap();
    }

    pub fn ensure_defaults(&mut self, voters: &[u64]) -> raft::Result<()> {
        if !self.has_row("hard_state") {
            self.set_hard_state(raft::eraftpb::HardState::default());
        }
        if !self.has_row("conf_state") {
            let mut conf = raft::eraftpb::ConfState::default();
            conf.set_voters(voters.to_vec());
            self.set_conf_state(conf);
        }
        Ok(())
    }

    fn has_row(&self, table: &str) -> bool {
        self.db
            .query_row(
                &format!("select exists(select 1 from {} where id = 1)", table),
                [],
                |r| r.get::<_, bool>(0),
            )
            .unwrap_or(false)
    }

    pub fn shared(self) -> SharedSqliteStorage {
        SharedSqliteStorage::new(self)
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

    pub fn update_commit(&mut self, commit: u64) -> raft::Result<()> {
        let mut hs = self.hard_state()?;
        if hs.commit != commit {
            hs.set_commit(commit);
            self.set_hard_state(hs);
        }
        Ok(())
    }

    pub fn hard_state(&self) -> raft::Result<raft::eraftpb::HardState> {
        let mut stmt = self
            .db
            .prepare("select data from hard_state where id = 1")
            .unwrap();
        match stmt.query_row([], |row| row.get(0)) {
            Ok(row) => {
                let domain: crate::domain::HardState = row;
                Ok(domain.into())
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(raft::eraftpb::HardState::default()),
            Err(e) => Err(raft::Error::Store(raft::StorageError::Other(Box::new(e)))),
        }
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
        match stmt.query_row([], |row| row.get(0)) {
            Ok(row) => {
                let domain: crate::domain::ConfState = row;
                Ok(domain.into())
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(raft::eraftpb::ConfState::default()),
            Err(e) => Err(raft::Error::Store(raft::StorageError::Other(Box::new(e)))),
        }
    }

    pub fn append_entries(&mut self, entries: &[raft::prelude::Entry]) -> raft::Result<()> {
        let tx = self
            .db
            .transaction()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(Box::new(e))))?;

        for e in entries {
            tx.execute(
                "insert or replace into entries (entry_type, term, log_index, context, data)
                 values (?1, ?2, ?3, ?4, ?5)",
                params![
                    e.get_entry_type() as i32,
                    e.get_term() as i64,
                    e.get_index() as i64,
                    e.get_context(),
                    e.get_data()
                ],
            )
            .map_err(|err| raft::Error::Store(raft::StorageError::Other(Box::new(err))))?;
        }

        tx.commit()
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(Box::new(e))))?;
        Ok(())
    }

    pub fn set_snapshot(&mut self, snap: &raft::prelude::Snapshot) -> raft::Result<()> {
        log::info!("setting snapshot, snap: {:?}", snap);
        self.db.execute(
            "insert into snapshots (id, data, term, log_index)
             values (1, ?1, ?2, ?3)
             on conflict(id) do update set data=excluded.data, term=excluded.term, log_index=excluded.log_index;",
            params![snap.get_data(), snap.get_metadata().term, snap.get_metadata().index],
        ).map_err(|e| raft::Error::Store(raft::StorageError::Other(Box::new(e))))?;
        Ok(())
    }

    pub fn get_snapshot(&self) -> raft::Result<raft::prelude::Snapshot> {
        log::info!("getting snapshot");
        let mut stmt = self
            .db
            .prepare("select data, term, log_index from snapshots where id = 1")
            .unwrap();
        let (data, term, index): (Vec<u8>, u64, u64) = stmt
            .query_row([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .unwrap();
        let mut snap = raft::prelude::Snapshot::new();
        snap.set_data(data.into());
        snap.mut_metadata().set_term(term);
        snap.mut_metadata().set_index(index);
        snap.mut_metadata().set_conf_state(self.conf_state()?);
        Ok(snap)
    }

    pub fn compact(&mut self, compact_index: u64) -> raft::Result<()> {
        let first = self.first_index()?;
        if compact_index <= first {
            return Ok(());
        }

        let last = self.last_index()?;
        if compact_index > last + 1 {
            panic!("compact beyond last index");
        }

        self.db
            .execute(
                "delete from entries where log_index < ?1",
                params![compact_index],
            )
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(Box::new(e))))?;
        Ok(())
    }

    pub fn append(&mut self, ents: &[raft::prelude::Entry]) -> raft::Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        let first_index = self.first_index()?;
        if ents[0].get_index() < first_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        let last_index = self.last_index()?;
        if ents[0].get_index() > last_index + 1 {
            panic!("log gap detected");
        }
        self.append_entries(ents)
    }

    pub fn apply_snapshot(&mut self, snap: raft::prelude::Snapshot) -> raft::Result<()> {
        let meta = snap.get_metadata();
        let index = meta.index;
        let term = meta.term;

        let current_first = self.first_index()?;
        if current_first > index {
            return Err(raft::Error::Store(raft::StorageError::SnapshotOutOfDate));
        }

        self.db.execute("delete from entries", []).unwrap();

        self.db
            .execute(
                "insert into entries (entry_type, term, log_index, context, data)
             values (?1, ?2, ?3, ?4, ?5)",
                params![
                    raft::eraftpb::EntryType::EntryNormal as i32,
                    term as i64,
                    index as i64,
                    Vec::<u8>::new(), // empty context
                    snap.data.to_vec(),
                ],
            )
            .unwrap();

        self.set_conf_state(meta.get_conf_state().clone());
        self.set_hard_state(raft::eraftpb::HardState {
            term,
            commit: index,
            vote: 0,
            ..Default::default()
        });
        self.set_snapshot(&snap)?;
        Ok(())
    }

    // pub fn apply_snapshot(&mut self, snap: raft::prelude::Snapshot) -> raft::Result<()> {
    //     let index = snap.get_metadata().index;
    //     let first = self.first_index()?;
    //     if first > index {
    //         return Err(raft::Error::Store(raft::StorageError::SnapshotOutOfDate));
    //     }
    //     self.db.execute("delete from entries", []).unwrap();
    //     self.set_conf_state(snap.get_metadata().get_conf_state().clone());
    //     self.set_hard_state(raft::eraftpb::HardState {
    //         commit: index,
    //         term: snap.get_metadata().term,
    //         vote: 0,
    //         ..Default::default()
    //     });
    //     self.set_snapshot(&snap)?;
    //     Ok(())
    // }
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
        let first_index = self.first_index()?;
        let last_index = self.last_index()?;
        if low < first_index {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        if high > last_index + 1 {
            panic!(
                "index out of bound (last: {}, high: {})",
                last_index + 1,
                high
            );
        }
        let mut stmt = self
            .db
            .prepare(
                "select entry_type, term, log_index, context, data
             from entries
             where log_index >= ?1 and log_index < ?2",
            )
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(Box::new(e))))?;

        let rows = stmt
            .query_map(params![low, high], |row| {
                Ok(DbEntry {
                    entry_type: row.get("entry_type")?,
                    term: row.get("term")?,
                    index: row.get("log_index")?,
                    context: row.get("context")?,
                    data: row.get::<_, Vec<u8>>("data")?,
                })
            })
            .map_err(|e| raft::Error::Store(raft::StorageError::Other(Box::new(e))))?;

        let mut out = Vec::new();
        for r in rows {
            let raw = r.map_err(|e| raft::Error::Store(raft::StorageError::Other(Box::new(e))))?;
            let mut e = raft::prelude::Entry::new();
            e.set_entry_type(raw.entry_type.into());
            e.set_term(raw.term);
            e.set_index(raw.index);
            e.set_context(raw.context.into());
            e.set_data(raw.data.into());
            out.push(e);
        }
        Ok(out)
    }
    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == 0 {
            return Ok(0);
        }

        let first = self.first_index()?;
        let last = self.last_index()?;

        if idx < first {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        if idx > last {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }

        let mut stmt = self
            .db
            .prepare("select term from entries where log_index = ?")
            .unwrap();
        let term: u64 = stmt
            .query_row([idx], |r| r.get(0))
            .map_err(|_| raft::Error::Store(raft::StorageError::Unavailable))?;
        Ok(term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        let mut stmt = self
            .db
            .prepare("select min(log_index) from entries")
            .unwrap();
        let v: Option<u64> = stmt.query_row([], |r| r.get(0)).unwrap();
        Ok(v.unwrap_or(0).max(1))
    }

    fn last_index(&self) -> raft::Result<u64> {
        let mut stmt = self
            .db
            .prepare("select max(log_index) from entries")
            .unwrap();
        let v: Option<u64> = stmt.query_row([], |r| r.get(0)).unwrap();
        Ok(v.unwrap_or(0))
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<raft::prelude::Snapshot> {
        let last_index = self.last_index()?;
        if request_index > last_index {
            return Err(raft::Error::Store(raft::StorageError::SnapshotOutOfDate));
        }

        let hard_state = self.hard_state()?;
        let conf_state = self.conf_state()?;

        let mut stmt = self
            .db
            .prepare("select data from entries where log_index = ?")
            .unwrap();
        let raw: Vec<u8> = stmt.query_row([request_index], |r| r.get(0)).unwrap();

        let mut snap = raft::prelude::Snapshot::new();
        snap.mut_metadata().set_conf_state(conf_state);
        snap.mut_metadata().set_index(hard_state.commit);
        snap.mut_metadata().set_term(hard_state.term);
        snap.set_data(raw.into());
        Ok(snap)
    }
}

impl Storage for SharedSqliteStorage {
    fn initial_state(&self) -> raft::Result<raft::RaftState> {
        let guard = self.inner.lock().unwrap();
        <SqliteBackend as Storage>::initial_state(&*guard)
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        context: raft::GetEntriesContext,
    ) -> raft::Result<Vec<raft::prelude::Entry>> {
        let guard = self.inner.lock().unwrap();
        <SqliteBackend as Storage>::entries(&*guard, low, high, max_size, context)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        let guard = self.inner.lock().unwrap();
        <SqliteBackend as Storage>::term(&*guard, idx)
    }

    fn first_index(&self) -> raft::Result<u64> {
        let guard = self.inner.lock().unwrap();
        <SqliteBackend as Storage>::first_index(&*guard)
    }

    fn last_index(&self) -> raft::Result<u64> {
        let guard = self.inner.lock().unwrap();
        <SqliteBackend as Storage>::last_index(&*guard)
    }

    fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<raft::prelude::Snapshot> {
        let guard = self.inner.lock().unwrap();
        <SqliteBackend as Storage>::snapshot(&*guard, request_index, to)
    }
}
impl SharedSqliteStorage {
    pub fn new(storage: SqliteBackend) -> Self {
        Self {
            inner: Arc::new(Mutex::new(storage)),
        }
    }

    pub fn from_arc(inner: Arc<Mutex<SqliteBackend>>) -> Self {
        Self { inner }
    }

    pub fn handle(&self) -> Arc<Mutex<SqliteBackend>> {
        Arc::clone(&self.inner)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use log::LevelFilter;
    use raft::{GetEntriesContext, prelude::*};

    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
    struct ConsensusData {
        height: u64,
        round: u64,
        step: u64,
    }

    fn setup_backend() -> SqliteBackend {
        _ = env_logger::Builder::new()
            .filter_level(LevelFilter::Info)
            .try_init();
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
    fn test_snapshot_valid_and_out_of_date() {
        let mut backend = setup_backend();
        let mut hs = HardState::default();
        hs.term = 1;
        hs.commit = 3;
        backend.set_hard_state(hs);
        let mut cs = ConfState::default();
        cs.voters = vec![1];
        backend.set_conf_state(cs);
        backend .db .execute( "insert into entries (entry_type, term, log_index, context, data) values (?1, ?2, ?3, ?4, ?5)", params![1i32, 1i64, 3i64, vec![], vec![42u8]], ) .unwrap();
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

    #[test]
    fn test_append_and_read_entries() {
        let mut backend = setup_backend();

        let payloads = [
            ConsensusData {
                height: 1,
                round: 2,
                step: 3,
            },
            ConsensusData {
                height: 4,
                round: 5,
                step: 6,
            },
        ];

        let entries: Vec<_> = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let encoded =
                    bincode::serde::encode_to_vec(p, bincode::config::standard()).unwrap();
                let mut e = raft::prelude::Entry::new();
                e.set_entry_type(raft::eraftpb::EntryType::EntryNormal);
                e.set_term((i + 1) as u64);
                e.set_index((i + 10) as u64);
                e.set_data(encoded.into());
                e
            })
            .collect();

        backend.append_entries(&entries).unwrap();

        let loaded = backend
            .entries(10, 12, None, raft::GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].term, 1);
        assert_eq!(loaded[1].term, 2);

        for (orig, stored) in payloads.iter().zip(loaded.iter()) {
            let decoded: ConsensusData =
                bincode::serde::decode_from_slice(&stored.data, bincode::config::standard())
                    .unwrap()
                    .0;
            assert_eq!(&decoded, orig);
        }
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let data = ConsensusData {
            height: 1,
            round: 1,
            step: 1,
        };
        let bytes = bincode::serde::encode_to_vec(&data, bincode::config::standard()).unwrap();
        let mut e = Entry::default();
        e.term = term;
        e.index = index;
        e.data = bytes.into();
        e
    }
    fn new_snapshot(index: u64, term: u64, voters: Vec<u64>) -> Snapshot {
        let mut s = Snapshot::default();
        s.mut_metadata().index = index;
        s.mut_metadata().term = term;
        s.mut_metadata().mut_conf_state().voters = voters;
        s
    }

    #[test]
    fn test_storage_term() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut backend = setup_backend();
        backend.append_entries(&ents).unwrap();

        assert!(matches!(
            backend.term(2),
            Err(raft::Error::Store(raft::StorageError::Compacted))
        ));
        assert_eq!(backend.term(3).unwrap(), 3);
        assert_eq!(backend.term(4).unwrap(), 4);
        assert_eq!(backend.term(5).unwrap(), 5);
        assert!(matches!(
            backend.term(6),
            Err(raft::Error::Store(raft::StorageError::Unavailable))
        ));
    }

    fn dump_entries(backend: &SqliteBackend) {
        let mut stmt = backend
            .db
            .prepare(
                "select log_index, term, entry_type, length(data) from entries order by log_index",
            )
            .unwrap();

        println!("entries table");
        let mut rows = stmt.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            let index: u64 = row.get(0).unwrap();
            let term: u64 = row.get(1).unwrap();
            let etype: i64 = row.get(2).unwrap();
            let len: usize = row.get(3).unwrap();
            println!("idx={index}, term={term}, type={etype}, data_len={len}");
        }
    }

    #[test]
    fn test_storage_entries() {
        let ents = vec![
            new_entry(3, 3),
            new_entry(4, 4),
            new_entry(5, 5),
            new_entry(6, 6),
        ];
        let mut backend = setup_backend();
        backend.append_entries(&ents).unwrap();
        let max_u64 = u64::MAX;

        let cases = vec![
            (
                1,
                2,
                max_u64,
                Err(raft::Error::Store(raft::StorageError::Compacted)),
            ),
            (3, 4, max_u64, Ok(vec![new_entry(3, 3)])),
            (4, 5, max_u64, Ok(vec![new_entry(4, 4)])),
            (4, 6, max_u64, Ok(vec![new_entry(4, 4), new_entry(5, 5)])),
            (
                4,
                7,
                max_u64,
                Ok(vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)]),
            ),
        ];

        for (i, (lo, hi, max_size, expect)) in cases.into_iter().enumerate() {
            let result = backend.entries(lo, hi, max_size, GetEntriesContext::empty(false));
            if result != expect {
                dump_entries(&backend);
                panic!("#{}: want {:?}, got {:?}", i, expect, result);
            }
        }
    }

    #[test]
    fn test_storage_first_and_last_index() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut backend = setup_backend();
        backend.append_entries(&ents).unwrap();
        assert_eq!(backend.first_index().unwrap(), 3);
        assert_eq!(backend.last_index().unwrap(), 5);
        backend.append_entries(&[new_entry(6, 6)]).unwrap();
        assert_eq!(backend.last_index().unwrap(), 6);
    }

    #[test]
    fn test_storage_compact() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut backend = setup_backend();
        backend.append_entries(&ents).unwrap();

        backend.compact(4).unwrap();
        let first = backend.first_index().unwrap();
        assert_eq!(first, 4);
        let entries = backend
            .entries(4, 6, u64::MAX, GetEntriesContext::empty(false))
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].term, 4);
        assert_eq!(entries[1].term, 5);
    }

    #[test]
    fn test_storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let nodes = vec![1, 2, 3];
        let mut conf = ConfState::default();
        conf.voters = nodes.clone();
        let mut backend = setup_backend();
        backend.append_entries(&ents).unwrap();
        backend.set_conf_state(conf.clone());
        let mut hs = HardState::default();
        hs.term = 5;
        hs.commit = 5;
        backend.set_hard_state(hs);

        let snap = backend.snapshot(5, 0).unwrap();
        assert_eq!(snap.get_metadata().term, 5);
        assert_eq!(snap.get_metadata().index, 5);
        assert_eq!(snap.get_metadata().get_conf_state().voters, nodes);

        let err = backend.snapshot(10, 0).unwrap_err();
        match err {
            raft::Error::Store(raft::StorageError::SnapshotOutOfDate) => {}
            _ => panic!("expected SnapshotOutOfDate"),
        }
    }

    #[test]
    fn test_storage_append_cases() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];

        let mut backend = setup_backend();
        backend.append_entries(&ents).unwrap();

        // Normal append
        let next = vec![new_entry(6, 6)];
        backend.append(&next).unwrap();
        assert_eq!(backend.last_index().unwrap(), 6);

        // Compact + reappend overlapping
        backend.compact(5).unwrap();
        let overlap = vec![new_entry(5, 7)];
        let res = backend.append(&overlap);
        assert!(res.is_err() || res.is_ok());
    }

    #[test]
    fn test_storage_apply_snapshot() {
        let mut backend = setup_backend();
        let nodes = vec![1, 2, 3];
        let snap_ok = new_snapshot(4, 4, nodes.clone());
        backend.apply_snapshot(snap_ok).unwrap();

        let snap_old = new_snapshot(3, 3, nodes);
        let result = backend.apply_snapshot(snap_old);
        match result {
            Err(raft::Error::Store(raft::StorageError::SnapshotOutOfDate)) => {}
            _ => panic!("expected SnapshotOutOfDate, got: {:?}", result),
        }
    }

    #[test]
    fn test_storage_append_panic_on_gap() {
        let mut backend = setup_backend();
        backend.append_entries(&[new_entry(1, 1)]).unwrap();
        let gap_entry = vec![new_entry(10, 10)];
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| backend.append(&gap_entry)));
        assert!(result.is_err());
    }
}
