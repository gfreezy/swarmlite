//! Embedded, durable OpenRaft storage for the Swarmlite control plane.
//!
//! The crate deliberately replicates an opaque byte value with a generation
//! compare-and-swap. Swarmlite owns the `ClusterState` schema and serializes it
//! before submitting a deterministic [`Command`]. This keeps the Raft crate
//! independent from scheduler and API model changes.

mod network;
mod storage;
mod types;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;
use openraft::{Config, RaftMetrics, SnapshotPolicy};
use thiserror::Error;

pub use network::{HttpNetwork, rpc_router};
pub use storage::{RedbLogStore, RedbStateMachine, open_storage};
pub use types::{
    CheckIsLeaderError, CheckIsLeaderRaftError, ClientWriteError, ClientWriteRaftError, Command,
    CommandOutcome, CommandResponse, ControllerNode, InitializeError, InitializeRaftError, NodeId,
    Raft, ReplicatedState, RpcError, TypeConfig,
};

#[derive(Clone)]
pub struct NodeConfig {
    pub node_id: NodeId,
    pub node: ControllerNode,
    pub data_dir: PathBuf,
    pub cluster_name: String,
    pub token: String,
    pub raft_config: Config,
}

impl NodeConfig {
    pub fn new(
        node_id: NodeId,
        node: ControllerNode,
        data_dir: impl Into<PathBuf>,
        cluster_name: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        let cluster_name = cluster_name.into();
        let raft_config = Config {
            cluster_name: cluster_name.clone(),
            heartbeat_interval: 500,
            election_timeout_min: 1_500,
            election_timeout_max: 3_000,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(1_000),
            max_in_snapshot_log_to_keep: 100,
            ..Config::default()
        };
        Self {
            node_id,
            node,
            data_dir: data_dir.into(),
            cluster_name,
            token: token.into(),
            raft_config,
        }
    }

    fn validate(&self) -> Result<(), Error> {
        if self.cluster_name.trim().is_empty() {
            return Err(Error::Configuration(
                "cluster_name must not be empty".to_owned(),
            ));
        }
        if self.token.len() < 16 {
            return Err(Error::Configuration(
                "the internal Raft token must contain at least 16 bytes".to_owned(),
            ));
        }
        if !is_http_url(&self.node.raft_url) || !is_http_url(&self.node.api_url) {
            return Err(Error::Configuration(
                "raft_url and api_url must be absolute HTTP(S) URLs".to_owned(),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for NodeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeConfig")
            .field("node_id", &self.node_id)
            .field("node", &self.node)
            .field("data_dir", &self.data_dir)
            .field("cluster_name", &self.cluster_name)
            .field("token", &"[redacted]")
            .field("raft_config", &self.raft_config)
            .finish()
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid Raft configuration: {0}")]
    Configuration(String),
    #[error("failed to open Raft storage: {0}")]
    Storage(String),
    #[error("failed to start Raft: {0}")]
    Start(String),
    #[error("failed to shut down Raft: {0}")]
    Shutdown(String),
}

#[derive(Debug, Error)]
pub enum SubmitError {
    #[error("request_id must not be empty")]
    EmptyRequestId,
    #[error(transparent)]
    Raft(Box<ClientWriteRaftError>),
}

impl From<ClientWriteRaftError> for SubmitError {
    fn from(error: ClientWriteRaftError) -> Self {
        Self::Raft(Box::new(error))
    }
}

/// One embedded controller node. Agent-only nodes do not construct this type.
pub struct RaftNode {
    node_id: NodeId,
    node: ControllerNode,
    token: String,
    raft: Raft,
    state_machine: RedbStateMachine,
}

impl RaftNode {
    pub async fn open(mut config: NodeConfig) -> Result<Arc<Self>, Error> {
        config.validate()?;
        config.raft_config.cluster_name = config.cluster_name.clone();
        let raft_config = Arc::new(
            config
                .raft_config
                .validate()
                .map_err(|error| Error::Configuration(error.to_string()))?,
        );
        let (log_store, state_machine) = open_storage(config.data_dir)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let network = HttpNetwork::new(config.token.clone());
        let raft = Raft::new(
            config.node_id,
            raft_config,
            network,
            log_store,
            state_machine.clone(),
        )
        .await
        .map_err(|error| Error::Start(error.to_string()))?;
        Ok(Arc::new(Self {
            node_id: config.node_id,
            node: config.node,
            token: config.token,
            raft,
            state_machine,
        }))
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn local_node(&self) -> &ControllerNode {
        &self.node
    }

    pub fn raft(&self) -> &Raft {
        &self.raft
    }

    /// Router for peer-only Raft RPC. It must be mounted at `node.raft_url`.
    pub fn rpc_router(&self) -> Router {
        rpc_router(self.raft.clone(), self.token.clone())
    }

    /// Initialize this node as the only voter in a new cluster.
    pub async fn initialize(&self) -> Result<(), InitializeRaftError> {
        self.raft
            .initialize(BTreeMap::from([(self.node_id, self.node.clone())]))
            .await
    }

    /// Submit a generation-CAS replacement through the Raft leader.
    pub async fn replace(
        &self,
        request_id: impl Into<String>,
        expected_generation: u64,
        value: Vec<u8>,
    ) -> Result<CommandResponse, SubmitError> {
        let request_id = request_id.into();
        if request_id.trim().is_empty() {
            return Err(SubmitError::EmptyRequestId);
        }
        let response = self
            .raft
            .client_write(Command::Replace {
                request_id,
                expected_generation,
                value,
            })
            .await?;
        Ok(response.data)
    }

    /// Fast local read. Followers may be behind the leader.
    pub async fn local_state(&self) -> ReplicatedState {
        self.state_machine.state().await
    }

    /// Linearizable read; returns ForwardToLeader on a follower.
    pub async fn consistent_state(&self) -> Result<ReplicatedState, CheckIsLeaderRaftError> {
        self.raft.ensure_linearizable().await?;
        Ok(self.state_machine.state().await)
    }

    /// Add a controller as a learner and wait until it catches up.
    pub async fn add_learner(
        &self,
        node_id: NodeId,
        node: ControllerNode,
    ) -> Result<(), ClientWriteRaftError> {
        self.raft.add_learner(node_id, node, true).await?;
        Ok(())
    }

    /// Promote an already caught-up learner without removing existing voters.
    pub async fn promote(&self, node_id: NodeId) -> Result<(), ClientWriteRaftError> {
        let mut voters = self.voter_ids();
        voters.insert(node_id);
        self.raft.change_membership(voters, false).await?;
        Ok(())
    }

    /// Remove a voter. Callers must enforce their controller-count policy.
    pub async fn remove_voter(
        &self,
        node_id: NodeId,
        retain_as_learner: bool,
    ) -> Result<(), ClientWriteRaftError> {
        let mut voters = self.voter_ids();
        voters.remove(&node_id);
        self.raft
            .change_membership(voters, retain_as_learner)
            .await?;
        Ok(())
    }

    pub fn voter_ids(&self) -> BTreeSet<NodeId> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect()
    }

    pub fn member_ids(&self) -> BTreeSet<NodeId> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .nodes()
            .map(|(node_id, _)| *node_id)
            .collect()
    }

    pub fn member(&self, node_id: NodeId) -> Option<ControllerNode> {
        self.raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .get_node(&node_id)
            .cloned()
    }

    pub fn current_term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    pub fn metrics(&self) -> RaftMetrics<NodeId, ControllerNode> {
        self.raft.metrics().borrow().clone()
    }

    pub fn leader(&self) -> Option<(NodeId, ControllerNode)> {
        let metrics = self.metrics();
        let leader_id = metrics.current_leader?;
        let node = metrics
            .membership_config
            .membership()
            .get_node(&leader_id)?
            .clone();
        Some((leader_id, node))
    }

    pub fn is_leader(&self) -> bool {
        self.metrics().current_leader == Some(self.node_id)
    }

    pub async fn trigger_snapshot(&self) -> Result<(), Error> {
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|error| Error::Start(error.to_string()))
    }

    pub async fn shutdown(&self) -> Result<(), Error> {
        self.raft
            .shutdown()
            .await
            .map_err(|error| Error::Shutdown(format!("{error:?}")))
    }
}

fn is_http_url(value: &str) -> bool {
    reqwest::Url::parse(value)
        .is_ok_and(|url| matches!(url.scheme(), "http" | "https") && url.has_host())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_node_commits_and_recovers_state() {
        let directory = tempfile::tempdir().unwrap();
        let node = ControllerNode {
            raft_url: "http://127.0.0.1:19090/internal/raft".into(),
            api_url: "http://127.0.0.1:19090".into(),
        };
        let raft = RaftNode::open(NodeConfig::new(
            1,
            node,
            directory.path(),
            "test-cluster",
            "0123456789abcdef",
        ))
        .await
        .unwrap();
        raft.initialize().await.unwrap();
        raft.raft
            .wait(None)
            .current_leader(1, "single node elects itself")
            .await
            .unwrap();

        let response = raft
            .replace("request-1", 0, b"state".to_vec())
            .await
            .unwrap();
        assert_eq!(response.outcome, CommandOutcome::Applied);
        assert_eq!(raft.consistent_state().await.unwrap().value, b"state");
        raft.shutdown().await.unwrap();
    }
}
