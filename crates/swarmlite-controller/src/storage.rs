use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
};

use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    database::{DATABASE_FILE, Database},
    kv::KvRepository,
    model::{
        ClusterSettings, ClusterState, DesiredTaskState, GatewayRecoverySnapshot, NodeMember,
        ObservedTaskState, PortBinding, RecoveredStackGateway, RegistryCredential, ServiceRecord,
        StackRecord, TaskRecord,
    },
};
use swarmlite_stack::config_digest;

const PERSISTED_SCHEMA_VERSION: u32 = 12;
// COMPAT(11 -> 12): remove this module and its startup call in the next release.
mod migrations;

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedControlPlane {
    schema_version: u32,
    cluster_id: String,
    cluster: ClusterSettings,
    state: PersistedClusterState,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedClusterState {
    stacks: BTreeMap<String, StackRecord>,
    services: BTreeMap<String, ServiceRecord>,
    tasks: BTreeMap<String, PersistedTaskRecord>,
    members: BTreeMap<String, NodeMember>,
    gateway_routes: BTreeMap<String, RecoveredStackGateway>,
    gateway_generation: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    registry_credentials: BTreeMap<String, RegistryCredential>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedTaskRecord {
    #[serde(default)]
    job: Option<crate::model::JobExecution>,
    #[serde(default)]
    job_observed: Option<ObservedTaskState>,
    id: String,
    service_id: String,
    revision: u64,
    slot: u32,
    node_id: String,
    desired: DesiredTaskState,
    ports: Vec<PortBinding>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    config_digests: Vec<String>,
    drain_until_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConfigBlobGcStats {
    pub referenced: usize,
    pub marked: usize,
    pub retained_for_grace: usize,
    pub deleted: usize,
}

/// SQLite-backed desired-state repository. Runtime heartbeat observations are
/// intentionally excluded and rebuilt after a controller restart, except for
/// one-shot job execution evidence, which must survive to prevent replay.
#[derive(Clone)]
pub struct StateRepository {
    database: Database,
    kv_repository: KvRepository,
    cluster: ClusterSettings,
}

impl StateRepository {
    pub fn open(data_dir: &Path, cluster: ClusterSettings) -> StorageResult<Self> {
        let database = Database::open(data_dir).map_err(backend)?;
        let repository = Self {
            kv_repository: KvRepository::open(database.clone())?,
            database,
            cluster,
        };
        repository.with_connection(|connection| {
            connection
                .execute_batch(
                    "CREATE TABLE IF NOT EXISTS control_plane (
                         singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                         generation INTEGER NOT NULL CHECK (generation >= 0),
                         schema_version INTEGER NOT NULL,
                         cluster_id TEXT NOT NULL,
                         document BLOB NOT NULL
                     ) STRICT;
                     CREATE TABLE IF NOT EXISTS stack_config_blobs (
                         cluster_id TEXT NOT NULL,
                         digest TEXT NOT NULL CHECK (length(digest) = 64),
                         content BLOB NOT NULL,
                         created_at_unix_ms INTEGER NOT NULL DEFAULT (unixepoch() * 1000),
                         unreferenced_since_unix_ms INTEGER,
                         PRIMARY KEY (cluster_id, digest)
                     ) STRICT;",
                )
                .map_err(backend)?;
            Ok(())
        })?;
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
        let loaded = self.with_connection(|connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(backend)?;
            // COMPAT(11 -> 12): remove startup migration in the next release.
            migrations::upgrade_11_to_12(&transaction, &self.cluster)?;
            if let Some(versioned) = read_versioned(&transaction, &self.cluster)? {
                transaction.commit().map_err(backend)?;
                return Ok(Some(versioned));
            }
            Ok(None)
        })?;
        if let Some(loaded) = loaded {
            return Ok(loaded);
        }

        self.with_connection(|connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(backend)?;
            let value = PersistedControlPlane::new(cluster.clone(), ClusterState::default());
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
            })
        })
    }

    /// Initializes a new Controller database from one Gateway snapshot in a
    /// single SQLite transaction. No empty desired state is visible between
    /// database creation and recovery import.
    pub fn initialize_from_gateway_recovery(
        &self,
        snapshot: &GatewayRecoverySnapshot,
    ) -> StorageResult<VersionedState> {
        snapshot
            .validate_for_cluster(&self.cluster.cluster_id)
            .map_err(StorageError::InvalidData)?;
        let state = ClusterState {
            gateway_routes: snapshot.stacks.clone(),
            gateway_generation: snapshot.generation,
            ..ClusterState::default()
        };
        self.with_connection(|connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(backend)?;
            if read_versioned(&transaction, &self.cluster)?.is_some() {
                return Err(StorageError::InvalidData(
                    "control-plane state is already initialized".to_owned(),
                ));
            }
            let value = PersistedControlPlane::new(self.cluster.clone(), state.clone());
            let document = serde_json::to_vec(&value).map_err(invalid)?;
            transaction
                .execute(
                    "INSERT INTO control_plane(singleton, generation, schema_version, cluster_id, document)
                     VALUES (1, 1, ?1, ?2, ?3)",
                    params![PERSISTED_SCHEMA_VERSION, self.cluster.cluster_id, document],
                )
                .map_err(backend)?;
            transaction.commit().map_err(backend)?;
            Ok(VersionedState {
                generation: 1,
                cluster: self.cluster.clone(),
                state,
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
    ) -> StorageResult<u64> {
        if !same_cluster_identity(cluster, &self.cluster) {
            return Err(StorageError::InvalidData(
                "cluster identity cannot be changed".to_owned(),
            ));
        }
        let value = PersistedControlPlane::new(cluster.clone(), state.clone());
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

    pub(crate) fn kv_repository(&self) -> KvRepository {
        self.kv_repository.clone()
    }

    pub fn put_config_blobs(&self, blobs: &BTreeMap<String, Vec<u8>>) -> StorageResult<()> {
        self.with_connection(|connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(backend)?;
            for (digest, content) in blobs {
                if config_digest(content) != *digest {
                    return Err(StorageError::InvalidData(format!(
                        "config blob {digest:?} does not match its SHA-256 digest"
                    )));
                }
                let existing = transaction
                    .query_row(
                        "SELECT content FROM stack_config_blobs
                         WHERE cluster_id = ?1 AND digest = ?2",
                        params![self.cluster.cluster_id, digest],
                        |row| row.get::<_, Vec<u8>>(0),
                    )
                    .optional()
                    .map_err(backend)?;
                match existing {
                    Some(existing) if existing != *content => {
                        return Err(StorageError::InvalidData(format!(
                            "config blob digest collision for {digest}"
                        )));
                    }
                    Some(_) => {
                        transaction
                            .execute(
                                "UPDATE stack_config_blobs
                                 SET unreferenced_since_unix_ms = NULL
                                 WHERE cluster_id = ?1 AND digest = ?2",
                                params![self.cluster.cluster_id, digest],
                            )
                            .map_err(backend)?;
                    }
                    None => {
                        transaction
                            .execute(
                                "INSERT INTO stack_config_blobs(cluster_id, digest, content)
                                 VALUES (?1, ?2, ?3)",
                                params![self.cluster.cluster_id, digest, content],
                            )
                            .map_err(backend)?;
                    }
                }
            }
            transaction.commit().map_err(backend)?;
            Ok(())
        })
    }

    pub fn get_config_blob(&self, digest: &str) -> StorageResult<Option<Vec<u8>>> {
        self.with_connection(|connection| {
            connection
                .query_row(
                    "SELECT content FROM stack_config_blobs
                     WHERE cluster_id = ?1 AND digest = ?2",
                    params![self.cluster.cluster_id, digest],
                    |row| row.get(0),
                )
                .optional()
                .map_err(backend)
        })
    }

    pub fn pin_config_blobs(&self, digests: &BTreeSet<String>) -> StorageResult<()> {
        self.with_connection(|connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(backend)?;
            for digest in digests {
                let changed = transaction
                    .execute(
                        "UPDATE stack_config_blobs
                         SET unreferenced_since_unix_ms = NULL
                         WHERE cluster_id = ?1 AND digest = ?2",
                        params![self.cluster.cluster_id, digest],
                    )
                    .map_err(backend)?;
                if changed != 1 {
                    return Err(StorageError::InvalidData(format!(
                        "config blob {digest:?} disappeared before the Stack was applied"
                    )));
                }
            }
            transaction.commit().map_err(backend)?;
            Ok(())
        })
    }

    pub fn config_blob_size(&self, digest: &str) -> StorageResult<Option<usize>> {
        self.with_connection(|connection| {
            let size = connection
                .query_row(
                    "SELECT length(content) FROM stack_config_blobs
                     WHERE cluster_id = ?1 AND digest = ?2",
                    params![self.cluster.cluster_id, digest],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(backend)?;
            size.map(|size| {
                usize::try_from(size).map_err(|_| {
                    StorageError::InvalidData(format!(
                        "config blob {digest:?} has an invalid stored size"
                    ))
                })
            })
            .transpose()
        })
    }

    pub fn gc_config_blobs(
        &self,
        referenced: &BTreeSet<String>,
        now_unix_ms: i64,
        grace_period_ms: i64,
    ) -> StorageResult<ConfigBlobGcStats> {
        if grace_period_ms < 0 {
            return Err(StorageError::InvalidData(
                "config blob GC grace period must not be negative".into(),
            ));
        }
        self.with_connection(|connection| {
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(backend)?;
            let entries = {
                let mut statement = transaction
                    .prepare(
                        "SELECT digest, unreferenced_since_unix_ms
                         FROM stack_config_blobs WHERE cluster_id = ?1",
                    )
                    .map_err(backend)?;
                statement
                    .query_map(params![self.cluster.cluster_id], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
                    })
                    .map_err(backend)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(backend)?
            };
            let mut stats = ConfigBlobGcStats::default();
            for (digest, unreferenced_since) in entries {
                if referenced.contains(&digest) {
                    stats.referenced += 1;
                    if unreferenced_since.is_some() {
                        transaction
                            .execute(
                                "UPDATE stack_config_blobs
                                 SET unreferenced_since_unix_ms = NULL
                                 WHERE cluster_id = ?1 AND digest = ?2",
                                params![self.cluster.cluster_id, digest],
                            )
                            .map_err(backend)?;
                    }
                    continue;
                }
                match unreferenced_since {
                    None => {
                        transaction
                            .execute(
                                "UPDATE stack_config_blobs
                                 SET unreferenced_since_unix_ms = ?3
                                 WHERE cluster_id = ?1 AND digest = ?2",
                                params![self.cluster.cluster_id, digest, now_unix_ms],
                            )
                            .map_err(backend)?;
                        stats.marked += 1;
                    }
                    Some(since) if now_unix_ms.saturating_sub(since) >= grace_period_ms => {
                        transaction
                            .execute(
                                "DELETE FROM stack_config_blobs
                                 WHERE cluster_id = ?1 AND digest = ?2",
                                params![self.cluster.cluster_id, digest],
                            )
                            .map_err(backend)?;
                        stats.deleted += 1;
                    }
                    Some(_) => stats.retained_for_grace += 1,
                }
            }
            transaction.commit().map_err(backend)?;
            Ok(stats)
        })
    }

    fn with_connection<T>(
        &self,
        operation: impl FnOnce(&mut Connection) -> StorageResult<T>,
    ) -> StorageResult<T> {
        let mut connection = self.database.connect().map_err(backend)?;
        operation(&mut connection)
    }
}

pub fn control_plane_state_exists(data_dir: &Path) -> StorageResult<bool> {
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
    if value.schema_version != schema_version
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
    }))
}

fn same_cluster_identity(left: &ClusterSettings, right: &ClusterSettings) -> bool {
    left.schema_version == right.schema_version
        && left.cluster_id == right.cluster_id
        && left.controller_id == right.controller_id
        && left.controller_port == right.controller_port
}

impl PersistedControlPlane {
    fn new(cluster: ClusterSettings, state: ClusterState) -> Self {
        Self {
            schema_version: PERSISTED_SCHEMA_VERSION,
            cluster_id: cluster.cluster_id.clone(),
            cluster,
            state: PersistedClusterState::from_runtime(&state),
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
            gateway_routes: state.gateway_routes.clone(),
            gateway_generation: state.gateway_generation,
            registry_credentials: state.registry_credentials.clone(),
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
            gateway_routes: self.gateway_routes,
            gateway_generation: self.gateway_generation,
            registry_credentials: self.registry_credentials,
        }
    }
}

impl PersistedTaskRecord {
    fn from_runtime(task: &TaskRecord) -> Self {
        Self {
            job: task.job.clone(),
            job_observed: task.job.as_ref().map(|_| task.observed.clone()),
            id: task.id.clone(),
            service_id: task.service_id.clone(),
            revision: task.revision,
            slot: task.slot,
            node_id: task.node_id.clone(),
            desired: task.desired.clone(),
            ports: task.ports.clone(),
            config_digests: task.config_digests.clone(),
            drain_until_unix_ms: task.drain_until_unix_ms,
        }
    }

    fn into_runtime(self) -> TaskRecord {
        TaskRecord {
            job: self.job,
            id: self.id,
            service_id: self.service_id,
            revision: self.revision,
            slot: self.slot,
            node_id: self.node_id,
            desired: self.desired,
            observed: self.job_observed.unwrap_or(ObservedTaskState::Pending),
            ports: self.ports,
            config_digests: self.config_digests,
            container_id: None,
            drain_until_unix_ms: self.drain_until_unix_ms,
            applied_generation: None,
            reconcile_error: None,
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
    use crate::model::{
        ClusterGatewayConfig, DeploymentImageResolutionNodeRecord, DeploymentImageResolutionRecord,
        GatewayRecoverySnapshot, HttpBackendProtocol, ImageResolutionStatus, NodeRecord,
        RecoveredStackGateway, RegistryCredential, ServicePortKey, ServiceSpec,
        StackDeploymentRecord, StackDeploymentStatus,
    };

    use super::*;

    fn cluster() -> ClusterSettings {
        ClusterSettings {
            schema_version: crate::model::CLUSTER_SCHEMA_VERSION,
            cluster_id: "storage-test".into(),
            controller_id: "controller-node".into(),
            controller_port: 19090,
            proxy: Default::default(),
            agent: Default::default(),
            gateway: ClusterGatewayConfig::default(),
            deployment: Default::default(),
        }
    }

    #[tokio::test]
    async fn atomically_imports_gateway_recovery_routes_into_a_new_controller() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = cluster();
        let gateway = swarmlite_stack::parse_stack(
            r#"
services:
  web:
    image: nginx
    expose: [80]
x-swarmlite:
  http_routes:
    - hostnames: [recovered.example.com]
      rules:
        - backend: { service: web, port: 80 }
"#,
        )
        .unwrap()
        .gateway;
        let snapshot = GatewayRecoverySnapshot::new(
            cluster.cluster_id.clone(),
            87,
            BTreeMap::from([(
                "demo".into(),
                RecoveredStackGateway {
                    gateway,
                    upstreams: BTreeMap::from([(
                        ServicePortKey::new("web", 80, HttpBackendProtocol::Http),
                        vec!["10.0.0.8:32080".into()],
                    )]),
                },
            )]),
        );
        let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();

        let imported = repository
            .initialize_from_gateway_recovery(&snapshot)
            .unwrap();

        assert_eq!(imported.generation, 1);
        assert_eq!(imported.state.gateway_generation, 87);
        assert_eq!(imported.state.gateway_routes, snapshot.stacks);
        let reopened = StateRepository::open(directory.path(), cluster).unwrap();
        let loaded = reopened.load().await.unwrap();
        assert_eq!(loaded.state.gateway_generation, 87);
        assert_eq!(loaded.state.gateway_routes, snapshot.stacks);
        assert!(
            reopened
                .initialize_from_gateway_recovery(&snapshot)
                .is_err()
        );
    }

    #[test]
    fn persists_content_addressed_stack_configs() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = cluster();
        let contents = b"production: true\n".to_vec();
        let digest = crate::model::config_digest(&contents);
        let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
        repository
            .put_config_blobs(&BTreeMap::from([(digest.clone(), contents.clone())]))
            .unwrap();
        repository
            .put_config_blobs(&BTreeMap::from([(digest.clone(), contents.clone())]))
            .unwrap();
        assert_eq!(
            repository.get_config_blob(&digest).unwrap(),
            Some(contents.clone())
        );
        assert_eq!(
            repository.config_blob_size(&digest).unwrap(),
            Some(contents.len())
        );

        let reopened = StateRepository::open(directory.path(), cluster).unwrap();
        assert_eq!(reopened.get_config_blob(&digest).unwrap(), Some(contents));
        assert!(matches!(
            reopened.put_config_blobs(&BTreeMap::from([(digest, b"corrupt".to_vec())])),
            Err(StorageError::InvalidData(_))
        ));
    }

    #[test]
    fn config_blob_gc_persists_grace_period_and_cancels_deletion_when_referenced_again() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = cluster();
        let contents_a = b"config-a".to_vec();
        let contents_b = b"config-b".to_vec();
        let contents_c = b"config-c".to_vec();
        let digest_a = crate::model::config_digest(&contents_a);
        let digest_b = crate::model::config_digest(&contents_b);
        let digest_c = crate::model::config_digest(&contents_c);
        let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
        repository
            .put_config_blobs(&BTreeMap::from([
                (digest_a.clone(), contents_a),
                (digest_b.clone(), contents_b),
                (digest_c.clone(), contents_c),
            ]))
            .unwrap();

        let stats = repository
            .gc_config_blobs(
                &BTreeSet::from([digest_a.clone(), digest_b.clone()]),
                1_000,
                100,
            )
            .unwrap();
        assert_eq!(stats.referenced, 2);
        assert_eq!(stats.marked, 1);
        assert_eq!(stats.deleted, 0);

        let stats = repository
            .gc_config_blobs(&BTreeSet::from([digest_a.clone()]), 1_050, 100)
            .unwrap();
        assert_eq!(stats.marked, 1);
        assert_eq!(stats.retained_for_grace, 1);
        assert!(repository.get_config_blob(&digest_c).unwrap().is_some());
        drop(repository);

        let reopened = StateRepository::open(directory.path(), cluster).unwrap();
        let stats = reopened
            .gc_config_blobs(
                &BTreeSet::from([digest_a.clone(), digest_b.clone()]),
                1_099,
                100,
            )
            .unwrap();
        assert_eq!(stats.retained_for_grace, 1);
        assert!(reopened.get_config_blob(&digest_b).unwrap().is_some());
        let stats = reopened
            .gc_config_blobs(
                &BTreeSet::from([digest_a.clone(), digest_b.clone()]),
                1_100,
                100,
            )
            .unwrap();
        assert_eq!(stats.deleted, 1);
        assert!(reopened.get_config_blob(&digest_c).unwrap().is_none());

        reopened
            .gc_config_blobs(&BTreeSet::from([digest_a.clone()]), 1_200, 100)
            .unwrap();
        let stats = reopened
            .gc_config_blobs(&BTreeSet::from([digest_a]), 1_300, 100)
            .unwrap();
        assert_eq!(stats.deleted, 1);
        assert!(reopened.get_config_blob(&digest_b).unwrap().is_none());
    }

    #[test]
    fn pinning_a_blob_cancels_an_expired_gc_candidate_before_apply() {
        let directory = tempfile::tempdir().unwrap();
        let repository = StateRepository::open(directory.path(), cluster()).unwrap();
        let contents = b"rollback-config".to_vec();
        let digest = crate::model::config_digest(&contents);
        repository
            .put_config_blobs(&BTreeMap::from([(digest.clone(), contents)]))
            .unwrap();
        repository
            .gc_config_blobs(&BTreeSet::new(), 1_000, 100)
            .unwrap();

        repository
            .pin_config_blobs(&BTreeSet::from([digest.clone()]))
            .unwrap();
        let stats = repository
            .gc_config_blobs(&BTreeSet::new(), 2_000, 100)
            .unwrap();

        assert_eq!(stats.marked, 1);
        assert_eq!(stats.deleted, 0);
        assert!(repository.get_config_blob(&digest).unwrap().is_some());
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
                supports_jobs: true,
                id: "soft-node".into(),
                address: "10.0.0.2".into(),
                swarmlite_version: None,
                labels: Default::default(),
                cpu_millis: 1000,
                memory_bytes: 1024,
                port_range_start: 20_000,
                port_range_end: 29_999,
                gateway_enabled: false,
            },
        );
        state.stacks.insert(
            "demo".into(),
            StackRecord {
                name: "demo".into(),
                applied_at_unix_ms: 1,
                services: vec!["demo.web".into()],
                gateway: Default::default(),
                deployment: Some(StackDeploymentRecord {
                    generation: 2,
                    status: StackDeploymentStatus::Healthy,
                    started_at_unix_ms: 1,
                    last_progress_at_unix_ms: 2,
                    progress_deadline_seconds: 300,
                    wait_for_gateway: false,
                    finished_at_unix_ms: Some(2),
                    superseded_by: None,
                    retry_revision: 0,
                    errors: Vec::new(),
                    image_resolutions: BTreeMap::from([(
                        "demo.web".into(),
                        DeploymentImageResolutionRecord {
                            service_id: "demo.web".into(),
                            service: "web".into(),
                            image: "nginx:latest".into(),
                            baseline_revision: 1,
                            status: ImageResolutionStatus::Unchanged,
                            nodes: BTreeMap::from([(
                                "soft-node".into(),
                                DeploymentImageResolutionNodeRecord {
                                    task_ids: vec!["task-1".into()],
                                    status: ImageResolutionStatus::Unchanged,
                                    old_image_ids: BTreeMap::from([(
                                        "task-1".into(),
                                        "sha256:current".into(),
                                    )]),
                                    resolved_image_id: Some("sha256:current".into()),
                                    error: None,
                                },
                            )]),
                        },
                    )]),
                    conditions: Vec::new(),
                    snapshot: Default::default(),
                }),
                deployment_history: BTreeMap::new(),
            },
        );
        state.services.insert(
            "demo.web".into(),
            ServiceRecord {
                job_cursor: None,
                id: "demo.web".into(),
                stack: "demo".into(),
                name: "web".into(),
                revision: 1,
                spec: ServiceSpec {
                    image: "nginx".into(),
                    pull_policy: Default::default(),
                    command: Vec::new(),
                    entrypoint: Vec::new(),
                    environment: Vec::new(),
                    expose: Vec::new(),
                    ports: Vec::new(),
                    volumes: Vec::new(),
                    configs: Vec::new(),
                    container_labels: Default::default(),
                    service_labels: Default::default(),
                    healthcheck: None,
                    replicas: 1,
                    constraints: Vec::new(),
                    max_replicas_per_node: None,
                    max_surge: 0,
                    stop_grace_period_seconds: 10,
                    stop_signal: None,
                    job: None,
                },
                deleted: false,
            },
        );
        state.tasks.insert(
            "task-1".into(),
            TaskRecord {
                job: None,
                id: "task-1".into(),
                service_id: "demo.web".into(),
                revision: 1,
                slot: 0,
                node_id: "soft-node".into(),
                desired: DesiredTaskState::Running,
                observed: ObservedTaskState::Healthy,
                ports: Vec::new(),
                config_digests: vec!["a".repeat(64)],
                container_id: Some("container-1".into()),
                drain_until_unix_ms: None,
                applied_generation: Some(2),
                reconcile_error: None,
            },
        );
        state.registry_credentials.insert(
            "ghcr.io".into(),
            RegistryCredential {
                username: "octocat".into(),
                password: "private-token".into(),
            },
        );

        let generation = repository
            .replace(first.generation, &cluster, &state)
            .await
            .unwrap();
        assert_eq!(generation, first.generation + 1);
        assert!(matches!(
            repository.replace(first.generation, &cluster, &state).await,
            Err(StorageError::Conflict)
        ));
        let loaded = repository.load().await.unwrap();
        assert!(loaded.state.nodes.is_empty());
        assert_eq!(loaded.state.services.len(), 1);
        let image_resolution = &loaded.state.stacks["demo"]
            .deployment
            .as_ref()
            .unwrap()
            .image_resolutions["demo.web"]
            .nodes["soft-node"];
        assert_eq!(
            image_resolution.resolved_image_id.as_deref(),
            Some("sha256:current")
        );
        assert_eq!(
            loaded.state.tasks["task-1"].observed,
            ObservedTaskState::Pending
        );
        assert!(loaded.state.tasks["task-1"].container_id.is_none());
        assert_eq!(
            loaded.state.tasks["task-1"].config_digests,
            vec!["a".repeat(64)]
        );
        assert_eq!(
            loaded.state.registry_credentials["ghcr.io"].password,
            "private-token"
        );
        assert!(directory.path().join(DATABASE_FILE).exists());
        assert!(control_plane_state_exists(directory.path()).unwrap());
        assert_eq!(
            local_state.get::<String>(NODE_KEY).unwrap().as_deref(),
            Some("controller-node")
        );
    }

    #[tokio::test]
    async fn migrates_schema_11_once_during_initialization() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = cluster();
        let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
        let mut document = serde_json::to_value(PersistedControlPlane {
            schema_version: 11,
            cluster_id: cluster.cluster_id.clone(),
            cluster: cluster.clone(),
            state: PersistedClusterState::default(),
        })
        .unwrap();
        let spec = swarmlite_stack::parse_stack("services:\n  web:\n    image: nginx\n")
            .unwrap()
            .services
            .remove("web")
            .unwrap();
        let mut legacy_spec = serde_json::to_value(spec).unwrap();
        legacy_spec.as_object_mut().unwrap().remove("job");
        legacy_spec.as_object_mut().unwrap().remove("stop_signal");
        document["state"]["services"] = serde_json::json!({
            "demo.web": {"id": "demo.web", "stack": "demo", "name": "web", "revision": 1, "spec": legacy_spec, "deleted": false}
        });
        document["state"]["tasks"] = serde_json::json!({
            "task-11": {"id": "task-11", "service_id": "demo.web", "revision": 1,
                "slot": 0, "node_id": "node-a", "desired": "running", "ports": [],
                "config_digests": [], "drain_until_unix_ms": null}
        });
        let gateway = swarmlite_stack::parse_stack("services:\n  web:\n    image: nginx\n    expose: [80]\nx-swarmlite:\n  http_routes:\n    - rules:\n        - cache: {key: {headers: [accept-language]}}\n          backend: {service: web, port: 80}\n      hostnames: [example.com]\n").unwrap().gateway;
        document["state"]["gateway_routes"] = serde_json::json!({"demo": RecoveredStackGateway {gateway, upstreams: Default::default()}});
        document["state"]["gateway_routes"]["demo"]["gateway"]["http_routes"][0]["rules"][0]["cache"]
            ["key"]["hash"] = serde_json::json!(true);
        repository.with_connection(|connection| {
            connection.execute(
                "INSERT INTO control_plane(singleton, generation, schema_version, cluster_id, document) VALUES (1, 5, 11, ?1, ?2)",
                params![cluster.cluster_id, serde_json::to_vec(&document).unwrap()],
            ).map_err(backend)?;
            Ok(())
        }).unwrap();
        assert!(repository.load().await.is_err()); // ordinary reads accept only schema 12
        let loaded = repository.initialize_with_cluster(&cluster).await.unwrap();
        assert_eq!(loaded.generation, 6);
        assert!(loaded.state.tasks["task-11"].job.is_none());
        let cache = loaded.state.gateway_routes["demo"].gateway.http_routes[0].rules[0]
            .cache
            .as_ref()
            .unwrap();
        assert_eq!(
            serde_json::to_value(cache).unwrap(),
            serde_json::json!({"key": {"headers": ["accept-language"]}})
        );
        assert!(loaded.state.services["demo.web"].spec.job.is_none());
        assert!(loaded.state.services["demo.web"].job_cursor.is_none());
        let second = repository.initialize_with_cluster(&cluster).await.unwrap();
        assert_eq!(second.generation, loaded.generation);
        let version: u32 = repository
            .with_connection(|connection| {
                connection
                    .query_row("SELECT schema_version FROM control_plane", [], |row| {
                        row.get(0)
                    })
                    .map_err(backend)
            })
            .unwrap();
        assert_eq!(version, PERSISTED_SCHEMA_VERSION);
        assert_eq!(
            repository.load().await.unwrap().state.services["demo.web"]
                .spec
                .image,
            "nginx"
        );
    }

    #[tokio::test]
    async fn failed_schema_11_migration_preserves_original_data() {
        let directory = tempfile::tempdir().unwrap();
        let cluster = cluster();
        let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
        let mut document = serde_json::to_value(PersistedControlPlane::new(
            cluster.clone(),
            ClusterState::default(),
        ))
        .unwrap();
        document["schema_version"] = serde_json::json!(11);
        document["kv"] = serde_json::json!({"objects": {}}); // removed historical format
        let bytes = serde_json::to_vec(&document).unwrap();
        repository
            .with_connection(|connection| {
                connection
                    .execute(
                        "INSERT INTO control_plane VALUES (1, 5, 11, ?1, ?2)",
                        params![cluster.cluster_id, bytes],
                    )
                    .map_err(backend)?;
                Ok(())
            })
            .unwrap();
        assert!(repository.initialize_with_cluster(&cluster).await.is_err());
        let stored = repository
            .with_connection(|connection| {
                connection
                    .query_row(
                        "SELECT schema_version, generation, document FROM control_plane",
                        [],
                        |row| {
                            Ok((
                                row.get::<_, u32>(0)?,
                                row.get::<_, i64>(1)?,
                                row.get::<_, Vec<u8>>(2)?,
                            ))
                        },
                    )
                    .map_err(backend)
            })
            .unwrap();
        assert_eq!(stored, (11, 5, bytes));
    }

    #[tokio::test]
    async fn rejects_unsupported_control_plane_schema() {
        for version in [7, 8, 9, 10, 13] {
            let directory = tempfile::tempdir().unwrap();
            let cluster = cluster();
            let repository = StateRepository::open(directory.path(), cluster.clone()).unwrap();
            let document =
                serde_json::to_vec(&serde_json::json!({"schema_version": version})).unwrap();
            repository
                .with_connection(|connection| {
                    connection
                        .execute(
                            "INSERT INTO control_plane VALUES (1, 5, ?1, ?2, ?3)",
                            params![version, cluster.cluster_id, document],
                        )
                        .map_err(backend)?;
                    Ok(())
                })
                .unwrap();
            assert!(
                matches!(
                    repository.initialize_with_cluster(&cluster).await,
                    Err(StorageError::InvalidData(_))
                ),
                "version {version}"
            );
        }
    }
}
