use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};
use swarmlite_raft::{
    CommandOutcome, ControllerNode, NodeId, RaftNode, ReplicatedState, SubmitError,
};
use thiserror::Error;
use uuid::Uuid;

use crate::model::{
    ClusterSettings, ClusterState, ControllerRecord, DesiredTaskState, KvState, NodeMember,
    ObservedTaskState, PortBinding, ServiceRecord, StackRecord, TaskRecord,
};

const PERSISTED_SCHEMA_VERSION: u32 = 4;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("control-plane state was modified concurrently")]
    Conflict,
    #[error("this controller is not the Raft leader")]
    NotLeader(Option<String>),
    #[error("Raft storage error: {0}")]
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
    controllers: BTreeMap<String, ControllerRecord>,
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

/// Adapts Swarmlite's durable desired state to the opaque CAS value replicated
/// by `swarmlite-raft`. Node heartbeats and task observations are deliberately
/// removed before persistence and are rebuilt after leadership changes.
#[derive(Clone)]
pub struct StateRepository {
    raft: Arc<RaftNode>,
    cluster: ClusterSettings,
}

impl StateRepository {
    pub fn new(raft: Arc<RaftNode>, cluster: ClusterSettings) -> Self {
        Self { raft, cluster }
    }

    pub fn raft(&self) -> &Arc<RaftNode> {
        &self.raft
    }

    pub fn is_leader(&self) -> bool {
        self.raft.is_leader()
    }

    pub fn current_term(&self) -> u64 {
        self.raft.current_term()
    }

    pub fn leader_url(&self) -> Option<String> {
        self.raft.leader().map(|(_, node)| node.api_url)
    }

    pub fn voter_ids(&self) -> std::collections::BTreeSet<NodeId> {
        self.raft.voter_ids()
    }

    pub fn is_voter(&self, node_id: NodeId) -> bool {
        self.raft.voter_ids().contains(&node_id)
    }

    pub async fn ensure_voter(&self, node_id: NodeId, node: ControllerNode) -> StorageResult<()> {
        if let Some(existing) = self.raft.member(node_id) {
            if self.is_voter(node_id) {
                if existing != node {
                    self.raft
                        .add_learner(node_id, node)
                        .await
                        .map_err(map_raft_error)?;
                }
                return Ok(());
            }
            if existing != node {
                self.raft
                    .add_learner(node_id, node.clone())
                    .await
                    .map_err(map_raft_error)?;
            }
        } else {
            self.raft
                .add_learner(node_id, node)
                .await
                .map_err(map_raft_error)?;
        }
        self.raft.promote(node_id).await.map_err(map_raft_error)
    }

    pub async fn remove_voter(&self, node_id: NodeId) -> StorageResult<()> {
        self.raft
            .remove_voter(node_id, false)
            .await
            .map_err(map_raft_error)
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
        let replica = self.raft.local_state().await;
        if let Some(value) = self.decode_replica(&replica)? {
            return Ok(VersionedState {
                generation: replica.generation,
                cluster: value.cluster,
                state: value.state.into_runtime(),
                kv: value.kv,
            });
        }

        let state = ClusterState::default();
        if !self.raft.is_leader() {
            return Ok(VersionedState {
                generation: replica.generation,
                cluster: cluster.clone(),
                state,
                kv: KvState::default(),
            });
        }
        match self
            .replace(replica.generation, cluster, &state, &KvState::default())
            .await
        {
            Ok(generation) => Ok(VersionedState {
                generation,
                cluster: cluster.clone(),
                state,
                kv: KvState::default(),
            }),
            Err(StorageError::Conflict) => self.load_local().await,
            Err(error) => Err(error),
        }
    }

    pub async fn load_local(&self) -> StorageResult<VersionedState> {
        let replica = self.raft.local_state().await;
        self.versioned_state(replica)
    }

    pub async fn load_consistent(&self) -> StorageResult<VersionedState> {
        let replica = self
            .raft
            .consistent_state()
            .await
            .map_err(map_check_is_leader_error)?;
        self.versioned_state(replica)
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
        let value = PersistedControlPlane {
            schema_version: PERSISTED_SCHEMA_VERSION,
            cluster_id: cluster.cluster_id.clone(),
            cluster: cluster.clone(),
            state: PersistedClusterState::from_runtime(state),
            kv: kv.clone(),
        };
        let body = serde_json::to_vec(&value)
            .map_err(|error| StorageError::InvalidData(error.to_string()))?;
        let response = self
            .raft
            .replace(
                format!("control-plane-{}", Uuid::new_v4().simple()),
                expected_generation,
                body,
            )
            .await
            .map_err(map_submit_error)?;
        match response.outcome {
            CommandOutcome::Applied => Ok(response.generation),
            CommandOutcome::Conflict => Err(StorageError::Conflict),
            CommandOutcome::Ignored => Err(StorageError::Backend(
                "Raft ignored a control-plane command".to_owned(),
            )),
        }
    }

    fn versioned_state(&self, replica: ReplicatedState) -> StorageResult<VersionedState> {
        let (cluster, state, kv) = self.decode_replica(&replica)?.map_or_else(
            || {
                (
                    self.cluster.clone(),
                    ClusterState::default(),
                    KvState::default(),
                )
            },
            |value| (value.cluster, value.state.into_runtime(), value.kv),
        );
        Ok(VersionedState {
            generation: replica.generation,
            cluster,
            state,
            kv,
        })
    }

    fn decode_replica(
        &self,
        replica: &ReplicatedState,
    ) -> StorageResult<Option<PersistedControlPlane>> {
        if replica.value.is_empty() {
            return Ok(None);
        }
        let value: PersistedControlPlane = serde_json::from_slice(&replica.value)
            .map_err(|error| StorageError::InvalidData(error.to_string()))?;
        if value.schema_version != PERSISTED_SCHEMA_VERSION
            || value.cluster_id != self.cluster.cluster_id
            || !same_cluster_identity(&value.cluster, &self.cluster)
        {
            return Err(StorageError::InvalidData(
                "persisted Raft state belongs to a different or unsupported cluster".to_owned(),
            ));
        }
        Ok(Some(value))
    }
}

fn same_cluster_identity(left: &ClusterSettings, right: &ClusterSettings) -> bool {
    left.schema_version == right.schema_version
        && left.cluster_id == right.cluster_id
        && left.controller_port == right.controller_port
        && left.gateway == right.gateway
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
            controllers: state.controllers.clone(),
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
            controllers: self.controllers,
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

fn map_submit_error(error: SubmitError) -> StorageError {
    match error {
        SubmitError::EmptyRequestId => StorageError::Backend(error.to_string()),
        SubmitError::Raft(error) => {
            if let Some(forward) = error.forward_to_leader::<ControllerNode>() {
                StorageError::NotLeader(
                    forward
                        .leader_node
                        .as_ref()
                        .map(|node| node.api_url.clone()),
                )
            } else {
                StorageError::Backend(error.to_string())
            }
        }
    }
}

fn map_raft_error(error: swarmlite_raft::ClientWriteRaftError) -> StorageError {
    if let Some(forward) = error.forward_to_leader::<ControllerNode>() {
        StorageError::NotLeader(
            forward
                .leader_node
                .as_ref()
                .map(|node| node.api_url.clone()),
        )
    } else {
        StorageError::Backend(error.to_string())
    }
}

fn map_check_is_leader_error(error: swarmlite_raft::CheckIsLeaderRaftError) -> StorageError {
    if let Some(forward) = error.forward_to_leader::<ControllerNode>() {
        StorageError::NotLeader(
            forward
                .leader_node
                .as_ref()
                .map(|node| node.api_url.clone()),
        )
    } else {
        StorageError::Backend(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use swarmlite_raft::{ControllerNode, NodeConfig};

    use crate::model::{ClusterGatewayConfig, NodeRecord, ServiceRecord, ServiceSpec, TaskRecord};

    use super::*;

    #[tokio::test]
    async fn persists_only_durable_state_through_raft_cas() {
        let directory = tempfile::tempdir().unwrap();
        let raft = RaftNode::open(NodeConfig::new(
            1,
            ControllerNode {
                raft_url: "http://127.0.0.1:19090/internal/raft".into(),
                api_url: "http://127.0.0.1:19090".into(),
            },
            directory.path(),
            "storage-test",
            "0123456789abcdef0123456789abcdef",
        ))
        .await
        .unwrap();
        raft.initialize().await.unwrap();
        raft.raft()
            .wait(Some(Duration::from_secs(5)))
            .current_leader(1, "test node becomes leader")
            .await
            .unwrap();
        let cluster = ClusterSettings {
            schema_version: 2,
            cluster_id: "storage-test".into(),
            mode: crate::model::ClusterMode::Standalone,
            controller_port: 19090,
            gateway: ClusterGatewayConfig::default(),
        };
        let repository = StateRepository::new(raft.clone(), cluster.clone());
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
                roles: crate::model::agent_roles(),
                controller_url: String::new(),
                raft_id: 1,
                raft_url: String::new(),
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
                desired: crate::model::DesiredTaskState::Running,
                observed: ObservedTaskState::Healthy,
                ports: Vec::new(),
                container_id: Some("container-1".into()),
                drain_until_unix_ms: None,
            },
        );

        repository
            .replace(first.generation, &cluster, &state, &KvState::default())
            .await
            .unwrap();
        let raw = raft.local_state().await;
        let json = std::str::from_utf8(&raw.value).unwrap();
        assert!(!json.contains("\"nodes\""));
        assert!(!json.contains("\"observed\""));
        assert!(!json.contains("\"container_id\""));
        let loaded = repository.load_consistent().await.unwrap();
        assert_eq!(loaded.cluster, cluster);
        assert!(loaded.state.nodes.is_empty());
        assert_eq!(loaded.state.services.len(), 1);
        assert_eq!(
            loaded.state.tasks["task-1"].observed,
            ObservedTaskState::Pending
        );
        assert!(loaded.state.tasks["task-1"].container_id.is_none());
        raft.shutdown().await.unwrap();
    }
}
