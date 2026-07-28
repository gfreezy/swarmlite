// OpenRaft fixes `StorageError` as the error type in its storage traits.
#![allow(clippy::result_large_err)]

use std::fmt;
use std::fmt::Debug;
use std::io::{self, Cursor};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    AnyError, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend, RaftLogReader,
    RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::RwLock;

use crate::types::{
    ApplicationState, CommandOutcome, CommandResponse, Entry, ManagerNode, NodeId, ReplicatedState,
    TypeConfig,
};

const META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");
const LOGS: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_logs");

const LAST_PURGED: &str = "last_purged";
const COMMITTED: &str = "committed";
const VOTE: &str = "vote";
const STATE_MACHINE: &str = "state_machine";
const SNAPSHOT: &str = "snapshot";

type StorageResult<T> = Result<T, StorageError<NodeId>>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct DurableStateMachine {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, ManagerNode>,
    application: ApplicationState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, ManagerNode>,
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct RedbLogStore {
    database: Arc<Database>,
}

#[derive(Debug, Clone)]
pub struct RedbStateMachine {
    database: Arc<Database>,
    inner: Arc<RwLock<DurableStateMachine>>,
    snapshot_sequence: Arc<AtomicU64>,
}

pub async fn open_storage(
    data_dir: impl AsRef<Path>,
) -> StorageResult<(RedbLogStore, RedbStateMachine)> {
    let data_dir = data_dir.as_ref();
    tokio::fs::create_dir_all(data_dir)
        .await
        .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
    let database = Arc::new(
        Database::create(data_dir.join("raft.redb"))
            .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Write, error))?,
    );

    let transaction = database
        .begin_write()
        .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
    {
        transaction
            .open_table(META)
            .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
        transaction
            .open_table(LOGS)
            .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
    }
    transaction
        .commit()
        .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Write, error))?;

    let durable = match read_meta::<DurableStateMachine>(&database, STATE_MACHINE)? {
        Some(durable) => durable,
        None => match read_meta::<StoredSnapshot>(&database, SNAPSHOT)? {
            Some(snapshot) => serde_json::from_slice(&snapshot.data).map_err(|error| {
                store_error(
                    ErrorSubject::Snapshot(Some(snapshot.meta.signature())),
                    ErrorVerb::Read,
                    error,
                )
            })?,
            None => DurableStateMachine::default(),
        },
    };

    let sequence = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    Ok((
        RedbLogStore {
            database: database.clone(),
        },
        RedbStateMachine {
            database,
            inner: Arc::new(RwLock::new(durable)),
            snapshot_sequence: Arc::new(AtomicU64::new(sequence)),
        },
    ))
}

impl RedbStateMachine {
    pub async fn state(&self) -> ReplicatedState {
        self.inner.read().await.application.current.clone()
    }

    fn persist(&self, durable: &DurableStateMachine) -> StorageResult<()> {
        write_meta(
            &self.database,
            STATE_MACHINE,
            durable,
            ErrorSubject::StateMachine,
        )
    }

    fn current_snapshot(&self) -> StorageResult<Option<StoredSnapshot>> {
        read_meta(&self.database, SNAPSHOT)
    }

    fn persist_snapshot(&self, snapshot: &StoredSnapshot) -> StorageResult<()> {
        write_meta(
            &self.database,
            SNAPSHOT,
            snapshot,
            ErrorSubject::Snapshot(Some(snapshot.meta.signature())),
        )
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RedbStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let durable = self.inner.read().await.clone();
        let data = serde_json::to_vec(&durable)
            .map_err(|error| store_error(ErrorSubject::StateMachine, ErrorVerb::Read, error))?;
        let sequence = self.snapshot_sequence.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = durable.last_applied_log.map_or_else(
            || format!("empty-{sequence}"),
            |last| format!("{}-{}-{sequence}", last.leader_id, last.index),
        );
        let meta = SnapshotMeta {
            last_log_id: durable.last_applied_log,
            last_membership: durable.last_membership,
            snapshot_id,
        };
        self.persist_snapshot(&StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        })?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for RedbStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> StorageResult<(Option<LogId<NodeId>>, StoredMembership<NodeId, ManagerNode>)> {
        let durable = self.inner.read().await;
        Ok((durable.last_applied_log, durable.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> StorageResult<Vec<CommandResponse>>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut durable = self.inner.read().await.clone();
        let mut responses = Vec::new();
        for entry in entries {
            durable.last_applied_log = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => ignored_response(&durable),
                EntryPayload::Normal(command) => durable.application.apply(command),
                EntryPayload::Membership(membership) => {
                    durable.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    ignored_response(&durable)
                }
            };
            responses.push(response);
        }

        self.persist(&durable)?;
        *self.inner.write().await = durable;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> StorageResult<Box<<TypeConfig as openraft::RaftTypeConfig>::SnapshotData>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, ManagerNode>,
        snapshot: Box<<TypeConfig as openraft::RaftTypeConfig>::SnapshotData>,
    ) -> StorageResult<()> {
        let data = snapshot.into_inner();
        let mut durable: DurableStateMachine = serde_json::from_slice(&data).map_err(|error| {
            store_error(
                ErrorSubject::Snapshot(Some(meta.signature())),
                ErrorVerb::Read,
                error,
            )
        })?;
        durable.last_applied_log = meta.last_log_id;
        durable.last_membership = meta.last_membership.clone();

        let transaction = self
            .database
            .begin_write()
            .map_err(|error| store_error(ErrorSubject::StateMachine, ErrorVerb::Write, error))?;
        {
            let mut table = transaction.open_table(META).map_err(|error| {
                store_error(ErrorSubject::StateMachine, ErrorVerb::Write, error)
            })?;
            let durable_bytes = serde_json::to_vec(&durable).map_err(|error| {
                store_error(ErrorSubject::StateMachine, ErrorVerb::Write, error)
            })?;
            table
                .insert(STATE_MACHINE, durable_bytes.as_slice())
                .map_err(|error| {
                    store_error(ErrorSubject::StateMachine, ErrorVerb::Write, error)
                })?;
            let stored = StoredSnapshot {
                meta: meta.clone(),
                data,
            };
            let snapshot_bytes = serde_json::to_vec(&stored).map_err(|error| {
                store_error(
                    ErrorSubject::Snapshot(Some(meta.signature())),
                    ErrorVerb::Write,
                    error,
                )
            })?;
            table
                .insert(SNAPSHOT, snapshot_bytes.as_slice())
                .map_err(|error| {
                    store_error(
                        ErrorSubject::Snapshot(Some(meta.signature())),
                        ErrorVerb::Write,
                        error,
                    )
                })?;
        }
        transaction
            .commit()
            .map_err(|error| store_error(ErrorSubject::StateMachine, ErrorVerb::Write, error))?;
        *self.inner.write().await = durable;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> StorageResult<Option<Snapshot<TypeConfig>>> {
        Ok(self.current_snapshot()?.map(|snapshot| Snapshot {
            meta: snapshot.meta,
            snapshot: Box::new(Cursor::new(snapshot.data)),
        }))
    }
}

impl RaftLogReader<TypeConfig> for RedbLogStore {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> StorageResult<Vec<Entry>>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
        let table = transaction
            .open_table(LOGS)
            .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
        let iter = table
            .range(range)
            .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
        iter.map(|item| {
            let (_, value) =
                item.map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
            serde_json::from_slice(value.value())
                .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Read, error))
        })
        .collect()
    }
}

impl RaftLogStorage<TypeConfig> for RedbLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StorageResult<LogState<TypeConfig>> {
        let last_purged_log_id = read_meta(&self.database, LAST_PURGED)?;
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
        let table = transaction
            .open_table(LOGS)
            .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Read, error))?;
        let last = table
            .last()
            .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Read, error))?
            .map(|(_, value)| {
                serde_json::from_slice::<Entry>(value.value())
                    .map(|entry| entry.log_id)
                    .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Read, error))
            })
            .transpose()?;
        Ok(LogState {
            last_purged_log_id,
            last_log_id: last.or(last_purged_log_id),
        })
    }

    async fn save_committed(&mut self, committed: Option<LogId<NodeId>>) -> StorageResult<()> {
        write_meta(&self.database, COMMITTED, &committed, ErrorSubject::Store)
    }

    async fn read_committed(&mut self) -> StorageResult<Option<LogId<NodeId>>> {
        Ok(read_meta(&self.database, COMMITTED)?.flatten())
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> StorageResult<()> {
        write_meta(&self.database, VOTE, vote, ErrorSubject::Vote)
    }

    async fn read_vote(&mut self) -> StorageResult<Option<Vote<NodeId>>> {
        read_meta(&self.database, VOTE)
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> StorageResult<()>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let encoded = entries
            .into_iter()
            .map(|entry| {
                serde_json::to_vec(&entry)
                    .map(|value| (entry.log_id.index, value))
                    .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Write, error))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let result: StorageResult<()> = (|| {
            let transaction = self
                .database
                .begin_write()
                .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
            {
                let mut table = transaction
                    .open_table(LOGS)
                    .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Write, error))?;
                for (index, value) in encoded {
                    table.insert(index, value.as_slice()).map_err(|error| {
                        store_error(ErrorSubject::Logs, ErrorVerb::Write, error)
                    })?;
                }
            }
            transaction
                .commit()
                .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Write, error))
        })();
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                callback.log_io_completed(Err(io::Error::other(message.clone())));
                Err(error)
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        delete_log_range(&self.database, log_id.index..)
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> StorageResult<()> {
        let transaction = self
            .database
            .begin_write()
            .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;
        {
            let mut meta = transaction
                .open_table(META)
                .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
            let value = serde_json::to_vec(&log_id)
                .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
            meta.insert(LAST_PURGED, value.as_slice())
                .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
        }
        {
            let mut logs = transaction
                .open_table(LOGS)
                .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;
            let keys = log_keys(&logs, ..=log_id.index)?;
            for key in keys {
                logs.remove(key)
                    .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;
            }
        }
        transaction
            .commit()
            .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Delete, error))
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

fn ignored_response(durable: &DurableStateMachine) -> CommandResponse {
    CommandResponse {
        request_id: String::new(),
        generation: durable.application.current.generation,
        outcome: CommandOutcome::Ignored,
    }
}

fn read_meta<T: DeserializeOwned>(database: &Database, key: &str) -> StorageResult<Option<T>> {
    let transaction = database
        .begin_read()
        .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Read, error))?;
    let table = transaction
        .open_table(META)
        .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Read, error))?;
    let value = table
        .get(key)
        .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Read, error))?;
    value
        .map(|value| {
            serde_json::from_slice(value.value())
                .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Read, error))
        })
        .transpose()
}

fn write_meta<T: Serialize>(
    database: &Database,
    key: &str,
    value: &T,
    subject: ErrorSubject<NodeId>,
) -> StorageResult<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| store_error(subject.clone(), ErrorVerb::Write, error))?;
    let transaction = database
        .begin_write()
        .map_err(|error| store_error(subject.clone(), ErrorVerb::Write, error))?;
    {
        let mut table = transaction
            .open_table(META)
            .map_err(|error| store_error(subject.clone(), ErrorVerb::Write, error))?;
        table
            .insert(key, bytes.as_slice())
            .map_err(|error| store_error(subject.clone(), ErrorVerb::Write, error))?;
    }
    transaction
        .commit()
        .map_err(|error| store_error(subject, ErrorVerb::Write, error))
}

fn delete_log_range<R>(database: &Database, range: R) -> StorageResult<()>
where
    R: RangeBounds<u64>,
{
    let transaction = database
        .begin_write()
        .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;
    {
        let mut logs = transaction
            .open_table(LOGS)
            .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;
        let keys = log_keys(&logs, range)?;
        for key in keys {
            logs.remove(key)
                .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Delete, error))?;
        }
    }
    transaction
        .commit()
        .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Delete, error))
}

fn log_keys<R>(table: &redb::Table<'_, u64, &[u8]>, range: R) -> StorageResult<Vec<u64>>
where
    R: RangeBounds<u64>,
{
    table
        .range(range)
        .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Read, error))?
        .map(|item| {
            item.map(|(key, _)| key.value())
                .map_err(|error| store_error(ErrorSubject::Logs, ErrorVerb::Read, error))
        })
        .collect()
}

fn store_error(
    subject: ErrorSubject<NodeId>,
    verb: ErrorVerb,
    error: impl fmt::Display,
) -> StorageError<NodeId> {
    let error = io::Error::other(error.to_string());
    StorageIOError::new(subject, verb, AnyError::new(&error)).into()
}

#[cfg(test)]
mod tests {
    use openraft::testing::{StoreBuilder, Suite};

    use super::*;

    struct Builder;

    impl StoreBuilder<TypeConfig, RedbLogStore, RedbStateMachine, tempfile::TempDir> for Builder {
        async fn build(
            &self,
        ) -> StorageResult<(tempfile::TempDir, RedbLogStore, RedbStateMachine)> {
            let directory = tempfile::tempdir()
                .map_err(|error| store_error(ErrorSubject::Store, ErrorVerb::Write, error))?;
            let (log, state_machine) = open_storage(directory.path()).await?;
            Ok((directory, log, state_machine))
        }
    }

    #[test]
    fn passes_openraft_storage_suite() -> StorageResult<()> {
        Suite::test_all(Builder)
    }

    #[tokio::test]
    async fn persists_application_state() {
        let directory = tempfile::tempdir().unwrap();
        {
            let (_, mut state_machine) = open_storage(directory.path()).await.unwrap();
            state_machine
                .apply([Entry {
                    log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
                    payload: EntryPayload::Normal(crate::types::Command::Replace {
                        request_id: "one".into(),
                        expected_generation: 0,
                        value: b"state".to_vec(),
                    }),
                }])
                .await
                .unwrap();
        }
        let (_, state_machine) = open_storage(directory.path()).await.unwrap();
        assert_eq!(state_machine.state().await.value, b"state");
    }
}
