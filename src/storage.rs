use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    local_state::DATABASE_FILE,
    model::{
        ClusterSettings, ClusterState, DesiredTaskState, KvState, NodeMember, ObservedTaskState,
        PortBinding, ServiceRecord, StackRecord, TaskRecord,
    },
};

const PERSISTED_SCHEMA_VERSION: u32 = 7;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("control-plane state was modified concurrently")]
    Conflict,
    #[error("SQLite storage error: {0}")]
    Backend(String),
    #[error("invalid persisted data: {0}")]
    InvalidData(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone)]
pub struct VersionedState {
    pub generation: u64,
    pub cluster: ClusterSettings,
    pub state: ClusterState,
    pub kv: KvState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedControlPlane {
    schema_version: u32,
    cluster_id: String,
    cluster: ClusterSettings,
    state: PersistedClusterState,
    kv: KvState,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedClusterState {
    stacks: BTreeMap<String, StackRecord>,
    services: BTreeMap<String, ServiceRecord>,
    tasks: BTreeMap<String, PersistedTaskRecord>,
    members: BTreeMap<String, NodeMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedTaskRecord {
    id: String,
    service_id: String,
    revision: u64,
    slot: u32,
    node_id: String,
    desired: DesiredTaskState,
    ports: Vec<PortBinding>,
    drain_until_unix_ms: Option<i64>,
}

/// SQLite-backed desired-state repository. Runtime heartbeat observations are
/// intentionally excluded and rebuilt after a controller restart.
#[derive(Clone)]
pub struct StateRepository {
    path: Arc<PathBuf>,
    cluster: ClusterSettings,
}

impl StateRepository {
    pub fn open(data_dir: &Path, cluster: ClusterSettings) -> StorageResult<Self> {
        std::fs::create_dir_all(data_dir).map_err(backend)?;
        let repository = Self {
            path: Arc::new(data_dir.join(DATABASE_FILE)),
            cluster,
        };
        repository.with_connection(|connection| {
            connection
                .execute_batch(
                    "PRAGMA journal_mode = WAL;
                     PRAGMA synchronous = FULL;
                     CREATE TABLE IF NOT EXISTS control_plane (
                         singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                         generation INTEGER NOT NULL CHECK (generation >= 0),
                         schema_version INTEGER NOT NULL,
                         cluster_id TEXT NOT NULL,
                         document BLOB NOT NULL
                     ) STRICT;",
                )
                .map_err(backend)?;
            Ok(())
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                repository.path.as_ref(),
                std::fs::Permissions::from_mode(0o600),
            )
            .map_err(backend)?;
        }
        Ok(repository)
    }

    pub async fn initialize_with_cluster(
        &self,
        cluster: &ClusterSettings,
    ) -> StorageResult<VersionedState> {
        if cluster != &self.cluster {
            return Err(StorageError::InvalidData(
                "repository was opened with different cluster settings".to_owned(),
            ));
        }
        self.with_connection(|connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(backend)?;
            if let Some(versioned) = read_versioned(&transaction, &self.cluster)? {
                transaction.commit().map_err(backend)?;
                return Ok(versioned);
            }
            let value = PersistedControlPlane::new(cluster.clone(), ClusterState::default(), KvState::default());
            let document = serde_json::to_vec(&value).map_err(invalid)?;
            transaction
                .execute(
                    "INSERT INTO control_plane(singleton, generation, schema_version, cluster_id, document)
                     VALUES (1, 1, ?1, ?2, ?3)",
                    params![PERSISTED_SCHEMA_VERSION, cluster.cluster_id, document],
                )
                .map_err(backend)?;
            transaction.commit().map_err(backend)?;
            Ok(VersionedState {
                generation: 1,
                cluster: cluster.clone(),
                state: ClusterState::default(),
                kv: KvState::default(),
            })
        })
    }

    pub async fn load(&self) -> StorageResult<VersionedState> {
        self.with_connection(|connection| {
            read_versioned(connection, &self.cluster)?.ok_or_else(|| {
                StorageError::InvalidData("control-plane state is not initialized".to_owned())
            })
        })
    }

    pub async fn replace(
        &self,
        expected_generation: u64,
        cluster: &ClusterSettings,
        state: &ClusterState,
        kv: &KvState,
    ) -> StorageResult<u64> {
        if !same_cluster_identity(cluster, &self.cluster) {
            return Err(StorageError::InvalidData(
                "cluster identity cannot be changed".to_owned(),
            ));
        }
        let value = PersistedControlPlane::new(cluster.clone(), state.clone(), kv.clone());
        let document = serde_json::to_vec(&value).map_err(invalid)?;
        let next_generation = expected_generation
            .checked_add(1)
            .ok_or_else(|| StorageError::Backend("generation overflow".to_owned()))?;
        let expected_generation_sql = i64::try_from(expected_generation).map_err(|_| {
            StorageError::Backend("generation exceeds SQLite integer range".to_owned())
        })?;
        let next_generation_sql = i64::try_from(next_generation).map_err(|_| {
            StorageError::Backend("generation exceeds SQLite integer range".to_owned())
        })?;
        self.with_connection(|connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(backend)?;
            let changed = transaction
                .execute(
                    "UPDATE control_plane
                     SET generation = ?1, schema_version = ?2, document = ?3
                     WHERE singleton = 1 AND generation = ?4 AND cluster_id = ?5",
                    params![
                        next_generation_sql,
                        PERSISTED_SCHEMA_VERSION,
                        document,
                        expected_generation_sql,
                        cluster.cluster_id
                    ],
                )
                .map_err(backend)?;
            if changed != 1 {
                return Err(StorageError::Conflict);
            }
            transaction.commit().map_err(backend)?;
            Ok(next_generation)
        })
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut Connection) -> StorageResult<T>,
    ) -> StorageResult<T> {
        let mut connection = Connection::open(self.path.as_ref()).map_err(backend)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(backend)?;
        operation(&mut connection)
    }
}

pub(crate) fn control_plane_state_exists(data_dir: &Path) -> StorageResult<bool> {
    let path = data_dir.join(DATABASE_FILE);
    if !path.exists() {
        return Ok(false);
    }
    let connection =
        Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(backend)?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .map_err(backend)?;
    let table_exists = connection
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM sqlite_schema
                 WHERE type = 'table' AND name = 'control_plane'
             )",
            [],
            |row| row.get::<_, bool>(0),
        )
        .map_err(backend)?;
    if !table_exists {
        return Ok(false);
    }
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM control_plane WHERE singleton = 1)",
            [],
            |row| row.get(0),
        )
        .map_err(backend)
}

fn read_versioned(
    connection: &Connection,
    expected_cluster: &ClusterSettings,
) -> StorageResult<Option<VersionedState>> {
    let row = connection
        .query_row(
            "SELECT generation, schema_version, cluster_id, document
             FROM control_plane WHERE singleton = 1",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, u32>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            },
        )
        .optional()
        .map_err(backend)?;
    let Some((generation, schema_version, cluster_id, document)) = row else {
        return Ok(None);
    };
    let generation = u64::try_from(generation)
        .map_err(|_| StorageError::InvalidData("negative SQLite generation".to_owned()))?;
    if schema_version != PERSISTED_SCHEMA_VERSION || cluster_id != expected_cluster.cluster_id {
        return Err(StorageError::InvalidData(
            "persisted SQLite state belongs to a different or unsupported cluster".to_owned(),
        ));
    }
    let value: PersistedControlPlane = serde_json::from_slice(&document).map_err(invalid)?;
    if value.schema_version != PERSISTED_SCHEMA_VERSION
        || value.cluster_id != expected_cluster.cluster_id
        || !same_cluster_identity(&value.cluster, expected_cluster)
    {
        return Err(StorageError::InvalidData(
            "persisted SQLite document belongs to a different or unsupported cluster".to_owned(),
        ));
    }
    Ok(Some(VersionedState {
        generation,
        cluster: value.cluster,
        state: value.state.into_runtime(),
        kv: value.kv,
    }))
}

fn same_cluster_identity(left: &ClusterSettings, right: &ClusterSettings) -> bool {
    left.schema_version == right.schema_version
        && left.cluster_id == right.cluster_id
        && left.controller_id == right.controller_id
        && left.controller_port == right.controller_port
}

impl PersistedControlPlane {
    fn new(cluster: ClusterSettings, state: ClusterState, kv: KvState) -> Self {
        Self {
            schema_version: PERSISTED_SCHEMA_VERSION,
            cluster_id: cluster.cluster_id.clone(),
            cluster,
            state: PersistedClusterState::from_runtime(&state),
            kv,
        }
    }
}

impl PersistedClusterState {
    fn from_runtime(state: &ClusterState) -> Self {
        Self {
            stacks: state.stacks.clone(),
            services: state.services.clone(),
            tasks: state
                .tasks
                .iter()
                .map(|(id, task)| (id.clone(), PersistedTaskRecord::from_runtime(task)))
                .collect(),
            members: state.members.clone(),
        }
    }

    fn into_runtime(self) -> ClusterState {
        ClusterState {
            stacks: self.stacks,
            services: self.services,
            nodes: BTreeMap::new(),
            tasks: self
                .tasks
                .into_iter()
                .map(|(id, task)| (id, task.into_runtime()))
                .collect(),
            members: self.members,
            unclaimed_tasks: BTreeMap::new(),
        }
    }
}

impl PersistedTaskRecord {
    fn from_runtime(task: &TaskRecord) -> Self {
        Self {
            id: task.id.clone(),
            service_id: task.service_id.clone(),
            revision: task.revision,
            slot: task.slot,
            node_id: task.node_id.clone(),
            desired: task.desired.clone(),
            ports: task.ports.clone(),
            drain_until_unix_ms: task.drain_until_unix_ms,
        }
    }

    fn into_runtime(self) -> TaskRecord {
        TaskRecord {
            id: self.id,
            service_id: self.service_id,
            revision: self.revision,
            slot: self.slot,
            node_id: self.node_id,
            desired: self.desired,
            observed: ObservedTaskState::Pending,
            ports: self.ports,
            container_id: None,
            drain_until_unix_ms: self.drain_until_unix_ms,
        }
    }
}

fn backend(error: impl std::fmt::Display) -> StorageError {
    StorageError::Backend(error.to_string())
}

fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidData(error.to_string())
}

#[cfg(test)]
mod tests {
    use crate::local_state::{LocalState, NODE_KEY};
    use crate::model::{ClusterGatewayConfig, NodeRecord, ServiceSpec};

    use super::*;

    fn cluster() -> ClusterSettings {
        ClusterSettings {
            schema_version: crate::model::CLUSTER_SCHEMA_VERSION,
            cluster_id: "storage-test".into(),
            controller_id: "controller-node".into(),
            controller_port: 19090,
            gateway: ClusterGatewayConfig::default(),
        }
    }

    #[tokio::test]
    async fn persists_only_durable_state_with_sqlite_cas() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = cluster();
        let local_state = LocalState::open(directory.path()).unwrap();
        local_state.put(NODE_KEY, &"controller-node").unwrap();
        let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
        let first = repository.initialize_with_cluster(&cluster).await.unwrap();
        let mut state = ClusterState::default();
        state.nodes.insert(
            "soft-node".into(),
            NodeRecord {
                id: "soft-node".into(),
                address: "10.0.0.2".into(),
                labels: Default::default(),
                cpu_millis: 1000,
                memory_bytes: 1024,
                port_range_start: 20_000,
                port_range_end: 29_999,
                gateway_enabled: false,
            },
        );
        state.services.insert(
            "demo.web".into(),
            ServiceRecord {
                id: "demo.web".into(),
                stack: "demo".into(),
                name: "web".into(),
                revision: 1,
                spec: ServiceSpec {
                    image: "nginx".into(),
                    command: Vec::new(),
                    entrypoint: Vec::new(),
                    environment: Vec::new(),
                    ports: Vec::new(),
                    volumes: Vec::new(),
                    container_labels: Default::default(),
                    service_labels: Default::default(),
                    healthcheck: None,
                    replicas: 1,
                    constraints: Vec::new(),
                    max_surge: 0,
                    stop_grace_period_seconds: 10,
                },
                deleted: false,
            },
        );
        state.tasks.insert(
            "task-1".into(),
            TaskRecord {
                id: "task-1".into(),
                service_id: "demo.web".into(),
                revision: 1,
                slot: 0,
                node_id: "soft-node".into(),
                desired: DesiredTaskState::Running,
                observed: ObservedTaskState::Healthy,
                ports: Vec::new(),
                container_id: Some("container-1".into()),
                drain_until_unix_ms: None,
            },
        );

        let generation = repository
            .replace(first.generation, &cluster, &state, &KvState::default())
            .await
            .unwrap();
        assert_eq!(generation, first.generation + 1);
        assert!(matches!(
            repository
                .replace(first.generation, &cluster, &state, &KvState::default())
                .await,
            Err(StorageError::Conflict)
        ));
        let loaded = repository.load().await.unwrap();
        assert!(loaded.state.nodes.is_empty());
        assert_eq!(loaded.state.services.len(), 1);
        assert_eq!(
            loaded.state.tasks["task-1"].observed,
            ObservedTaskState::Pending
        );
        assert!(loaded.state.tasks["task-1"].container_id.is_none());
        assert!(directory.path().join(DATABASE_FILE).exists());
        assert!(control_plane_state_exists(directory.path()).unwrap());
        assert_eq!(
            local_state.get::<String>(NODE_KEY).unwrap().as_deref(),
            Some("controller-node")
        );
    }
}
