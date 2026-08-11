use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub(crate) const DATABASE_FILE: &str = "swarmlite.sqlite";
pub(crate) const NODE_KEY: &str = "node";
pub(crate) const FENCE_KEY: &str = "agent_fence";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct AgentFence {
    pub generation: u64,
}

/// A cheap path handle. Each operation uses a short SQLite transaction so the
/// CLI can read node defaults while `serve` is running.
#[derive(Clone)]
pub(crate) struct LocalState {
    path: Arc<PathBuf>,
}

impl LocalState {
    pub(crate) fn open(data_dir: &Path) -> Result<Self> {
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("failed to create {}", data_dir.display()))?;
        let state = Self {
            path: Arc::new(data_dir.join(DATABASE_FILE)),
        };
        state.with_connection(|connection| {
            connection.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = FULL;
                 CREATE TABLE IF NOT EXISTS local_state (
                    key TEXT PRIMARY KEY,
                    value BLOB NOT NULL
                ) STRICT;",
            )?;
            Ok(())
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
            .join(DATABASE_FILE)
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
        self.with_connection(|connection| {
            let value = connection
                .query_row(
                    "SELECT value FROM local_state WHERE key = ?1",
                    [key],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .optional()
                .with_context(|| format!("failed to read local state key {key}"))?;
            value
                .map(|value| {
                    serde_json::from_slice(&value)
                        .with_context(|| format!("invalid local state key {key}"))
                })
                .transpose()
        })
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
        self.with_connection(|connection| {
            let transaction = connection.unchecked_transaction()?;
            for (key, value) in values {
                transaction.execute(
                    "INSERT INTO local_state(key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    params![key, value],
                )?;
            }
            transaction.commit().context("failed to commit local state")
        })
    }

    fn with_connection<T>(&self, operation: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let connection = Connection::open(self.path.as_ref())
            .with_context(|| format!("failed to open {}", self.path.display()))?;
        connection.busy_timeout(Duration::from_secs(5))?;
        operation(&connection)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Fence {
        generation: u64,
    }

    #[test]
    fn stores_node_values_and_fence_in_one_sqlite_database() {
        let directory = tempfile::tempdir().unwrap();
        let state = LocalState::open(directory.path()).unwrap();
        state
            .put_pair((NODE_KEY, &"node-a"), (FENCE_KEY, &Fence { generation: 7 }))
            .unwrap();

        let second_handle = LocalState::open_existing(directory.path())
            .unwrap()
            .unwrap();
        assert_eq!(
            second_handle.get::<String>(NODE_KEY).unwrap().as_deref(),
            Some("node-a")
        );
        assert_eq!(
            second_handle.get::<Fence>(FENCE_KEY).unwrap(),
            Some(Fence { generation: 7 })
        );
        assert!(directory.path().join(DATABASE_FILE).exists());
    }
}
