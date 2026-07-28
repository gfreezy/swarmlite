use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    thread,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use redb::{Database, DatabaseError, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub(crate) const LOCAL_STATE_FILE: &str = "local.redb";
pub(crate) const NODE_KEY: &str = "node";
pub(crate) const FENCE_KEY: &str = "agent_fence";

const STATE: TableDefinition<&str, &[u8]> = TableDefinition::new("local_state");
const OPEN_RETRIES: usize = 100;
const OPEN_RETRY_DELAY: Duration = Duration::from_millis(10);

static LOCAL_DATABASE_ACCESS: OnceLock<Mutex<()>> = OnceLock::new();

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AgentFence {
    pub term: u64,
    pub generation: u64,
}

/// A cheap path handle. Every operation opens redb for one short transaction and closes it
/// again so another Swarmlite process can read CLI defaults while `serve` is running.
#[derive(Clone)]
pub(crate) struct LocalState {
    path: Arc<PathBuf>,
}

impl LocalState {
    pub(crate) fn open(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("failed to create {}", data_dir.display()))?;
        let state = Self {
            path: Arc::new(data_dir.join(LOCAL_STATE_FILE)),
        };
        state.with_database(|database| {
            let transaction = database
                .begin_write()
                .context("failed to initialize local state transaction")?;
            {
                transaction
                    .open_table(STATE)
                    .context("failed to initialize local state table")?;
            }
            transaction
                .commit()
                .context("failed to initialize local state")
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(state.path.as_ref(), std::fs::Permissions::from_mode(0o600))
                .with_context(|| format!("failed to protect {}", state.path.display()))?;
        }
        Ok(state)
    }

    pub(crate) fn open_existing(data_dir: &Path) -> Result<Option<Self>> {
        data_dir
            .join(LOCAL_STATE_FILE)
            .exists()
            .then(|| Self::open(data_dir))
            .transpose()
    }

    pub(crate) fn get_read_only<T: DeserializeOwned>(
        data_dir: &Path,
        key: &str,
    ) -> Result<Option<T>> {
        let Some(state) = Self::open_existing(data_dir)? else {
            return Ok(None);
        };
        state.get(key)
    }

    pub(crate) fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.with_database(|database| read_value(database, key))
    }

    pub(crate) fn put<T: Serialize>(&self, key: &str, value: &T) -> Result<()> {
        let value = serde_json::to_vec(value)?;
        self.put_encoded([(key, value.as_slice())])
    }

    pub(crate) fn put_pair<A: Serialize, B: Serialize>(
        &self,
        first: (&str, &A),
        second: (&str, &B),
    ) -> Result<()> {
        let first_value = serde_json::to_vec(first.1)?;
        let second_value = serde_json::to_vec(second.1)?;
        self.put_encoded([
            (first.0, first_value.as_slice()),
            (second.0, second_value.as_slice()),
        ])
    }

    fn put_encoded<'a>(&self, values: impl IntoIterator<Item = (&'a str, &'a [u8])>) -> Result<()> {
        let values = values
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value.to_vec()))
            .collect::<Vec<_>>();
        self.with_database(|database| {
            let transaction = database
                .begin_write()
                .context("failed to update local state")?;
            {
                let mut table = transaction
                    .open_table(STATE)
                    .context("failed to open local state table")?;
                for (key, value) in &values {
                    table
                        .insert(key.as_str(), value.as_slice())
                        .with_context(|| format!("failed to update local state key {key}"))?;
                }
            }
            transaction.commit().context("failed to commit local state")
        })
    }

    fn with_database<T>(&self, operation: impl FnOnce(&Database) -> Result<T>) -> Result<T> {
        let access = LOCAL_DATABASE_ACCESS.get_or_init(|| Mutex::new(()));
        let _guard = access
            .lock()
            .map_err(|_| anyhow::anyhow!("local database access lock is poisoned"))?;
        let mut database = None;
        for attempt in 0..OPEN_RETRIES {
            match Database::create(self.path.as_ref()) {
                Ok(opened) => {
                    database = Some(opened);
                    break;
                }
                Err(DatabaseError::DatabaseAlreadyOpen) if attempt + 1 < OPEN_RETRIES => {
                    thread::sleep(OPEN_RETRY_DELAY);
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to open {}", self.path.display()));
                }
            }
        }
        let Some(database) = database else {
            bail!(
                "timed out waiting to open {}; another Swarmlite process is holding local state",
                self.path.display()
            );
        };
        operation(&database)
    }
}

fn read_value<T: DeserializeOwned>(database: &Database, key: &str) -> Result<Option<T>> {
    let transaction = database
        .begin_read()
        .context("failed to read local state")?;
    let table = transaction
        .open_table(STATE)
        .context("failed to open local state table")?;
    let value = table
        .get(key)
        .with_context(|| format!("failed to read local state key {key}"))?;
    value
        .map(|value| {
            serde_json::from_slice(value.value())
                .with_context(|| format!("invalid local state key {key}"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_node_values_and_fence_in_one_database() {
        let directory = tempfile::tempdir().unwrap();
        let state = LocalState::open(directory.path()).unwrap();
        state.put(NODE_KEY, &"node-a").unwrap();
        state
            .put(
                FENCE_KEY,
                &AgentFence {
                    term: 3,
                    generation: 7,
                },
            )
            .unwrap();

        let second_handle = LocalState::open_existing(directory.path())
            .unwrap()
            .unwrap();
        assert_eq!(
            second_handle.get::<String>(NODE_KEY).unwrap().as_deref(),
            Some("node-a")
        );
        assert_eq!(
            second_handle.get::<AgentFence>(FENCE_KEY).unwrap().unwrap(),
            AgentFence {
                term: 3,
                generation: 7
            }
        );
    }
}
