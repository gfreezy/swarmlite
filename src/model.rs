use std::collections::{BTreeMap, BTreeSet};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
pub use swarmlite_stack::{
    GatewayHttpMode, GatewayTlsMode, HealthcheckSpec, HttpBackend, HttpBackendProtocol,
    HttpPathMatch, HttpPathMatchType, HttpPathRewrite, HttpRouteRule, HttpRouteSpec, ServicePort,
    ServiceSpec, StackGatewaySpec, service_spec_hash,
};

pub const CLUSTER_SCHEMA_VERSION: u32 = 5;
pub const DEFAULT_GATEWAY_IMAGE: &str = "ghcr.io/swarmlite/swarmlite-caddy:latest";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterGatewayConfig {
    pub listen: Vec<String>,
    pub image: String,
}

impl Default for ClusterGatewayConfig {
    fn default() -> Self {
        Self {
            listen: vec![":80".to_owned(), ":443".to_owned()],
            image: DEFAULT_GATEWAY_IMAGE.to_owned(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvState {
    pub objects: BTreeMap<String, KvObject>,
    pub prefix_tombstones: BTreeMap<String, KvVersion>,
    pub locks: BTreeMap<String, KvLock>,
    pub next_fencing_token: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvObject {
    pub value_base64: String,
    pub version: KvVersion,
    pub modified_at_unix_ms: i64,
    pub tombstone: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct KvVersion {
    pub physical_unix_ms: i64,
    pub logical: u64,
    pub replica_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvLock {
    pub owner_id: String,
    pub fencing_token: u64,
    pub lease_until_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KvPutRequest {
    pub key: String,
    pub value_base64: String,
    pub version: KvVersion,
    pub modified_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KvDeleteRequest {
    pub key: String,
    pub version: KvVersion,
    pub modified_at_unix_ms: i64,
    /// Deletes the key and all keys below it when true.
    #[serde(default)]
    pub recursive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvPutResponse {
    pub applied: bool,
    pub version: KvVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvObjectResponse {
    pub key: String,
    pub value_base64: String,
    pub version: KvVersion,
    pub modified_at_unix_ms: i64,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvListResponse {
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvStatResponse {
    pub key: String,
    pub modified_at_unix_ms: i64,
    pub size: u64,
    pub is_value: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KvLockAcquireRequest {
    pub name: String,
    pub owner_id: String,
    pub lease_millis: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KvLockStatus {
    Acquired,
    Busy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvLockAcquireResponse {
    pub status: KvLockStatus,
    pub fencing_token: Option<u64>,
    pub lease_until_unix_ms: Option<i64>,
    pub retry_after_millis: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KvLockMutationRequest {
    pub name: String,
    pub owner_id: String,
    pub fencing_token: u64,
    pub lease_millis: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSettings {
    pub schema_version: u32,
    pub cluster_id: String,
    pub mode: ClusterMode,
    pub controller_port: u16,
    pub gateway: ClusterGatewayConfig,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ClusterMode {
    #[default]
    Standalone,
    Ha,
}

impl ClusterMode {
    pub const fn controller_limit(self) -> usize {
        match self {
            Self::Standalone => 1,
            Self::Ha => 3,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfigResponse {
    pub generation: u64,
    pub config: ClusterSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfigUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<ClusterMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_image: Option<String>,
}

pub fn valid_gateway_image(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && value.trim() == value
        && !value.chars().any(char::is_whitespace)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Controller,
    Agent,
    Gateway,
}

pub type NodeRoles = BTreeSet<NodeRole>;

pub fn agent_roles() -> NodeRoles {
    BTreeSet::from([NodeRole::Agent])
}

pub fn initial_roles() -> NodeRoles {
    BTreeSet::from([NodeRole::Controller, NodeRole::Agent, NodeRole::Gateway])
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeMember {
    pub id: String,
    pub address: String,
    pub roles: NodeRoles,
    pub labels: BTreeMap<String, String>,
    pub automatic_roles: bool,
    pub controller_url: String,
    pub raft_id: u64,
    pub raft_url: String,
    pub joined_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerRecord {
    pub node_id: String,
    pub advertise_url: String,
    pub raft_id: u64,
    pub raft_url: String,
    pub reserved_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRecord {
    pub id: String,
    pub stack: String,
    pub name: String,
    pub revision: u64,
    pub spec: ServiceSpec,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackRecord {
    pub name: String,
    pub applied_at_unix_ms: i64,
    pub services: Vec<String>,
    #[serde(default)]
    pub gateway: StackGatewaySpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: String,
    pub address: String,
    pub labels: BTreeMap<String, String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub port_range_start: u16,
    pub port_range_end: u16,
    pub roles: NodeRoles,
    pub controller_url: String,
    pub raft_id: u64,
    pub raft_url: String,
    /// Raft membership generation of the Controller URL set applied by this node.
    #[serde(default)]
    pub controller_set_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DesiredTaskState {
    Running,
    Draining,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObservedTaskState {
    Pending,
    Starting,
    Running,
    Healthy,
    Failed,
    Lost,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PortBinding {
    pub target: u16,
    pub published: u16,
    pub protocol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: String,
    pub service_id: String,
    pub revision: u64,
    pub slot: u32,
    pub node_id: String,
    pub desired: DesiredTaskState,
    pub observed: ObservedTaskState,
    pub ports: Vec<PortBinding>,
    pub container_id: Option<String>,
    pub drain_until_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterState {
    pub stacks: BTreeMap<String, StackRecord>,
    pub services: BTreeMap<String, ServiceRecord>,
    pub nodes: BTreeMap<String, NodeRecord>,
    pub tasks: BTreeMap<String, TaskRecord>,
    pub members: BTreeMap<String, NodeMember>,
    pub controllers: BTreeMap<String, ControllerRecord>,
    pub unclaimed_tasks: BTreeMap<String, UnclaimedTask>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnclaimedTask {
    pub id: String,
    pub stack: String,
    pub service: String,
    pub slot: u32,
    pub revision: u64,
    pub spec_hash: String,
    pub node_id: String,
    pub observed: ObservedTaskState,
    pub ports: Vec<PortBinding>,
    pub container_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderRecord {
    pub id: String,
    pub term: u64,
    pub advertise_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHeartbeat {
    pub node: NodeRecord,
    pub tasks: Vec<TaskReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskReport {
    pub id: String,
    pub observed: ObservedTaskState,
    pub container_id: Option<String>,
    pub cluster_id: Option<String>,
    pub stack: Option<String>,
    pub service: Option<String>,
    pub slot: Option<u32>,
    pub revision: Option<u64>,
    pub spec_hash: Option<String>,
    pub ports: Vec<PortBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub leader_term: u64,
    pub generation: u64,
    #[serde(default)]
    pub controller_set_generation: u64,
    pub cluster: ClusterSettings,
    pub assignments: Vec<TaskAssignment>,
    pub roles: NodeRoles,
    pub labels: BTreeMap<String, String>,
    pub controllers: Vec<String>,
    pub remove_tasks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeControl {
    pub cluster: ClusterSettings,
    pub roles: NodeRoles,
    pub labels: BTreeMap<String, String>,
    pub controllers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapResponse {
    pub cluster: ClusterSettings,
    pub controllers: Vec<String>,
    #[serde(default)]
    pub controller_set_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: String,
    pub address: String,
    pub requested_roles: Option<NodeRoles>,
    pub recovered_roles: NodeRoles,
    pub controller_url: String,
    pub raft_id: u64,
    pub raft_url: String,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    pub cluster: ClusterSettings,
    pub roles: NodeRoles,
    pub labels: BTreeMap<String, String>,
    pub controllers: Vec<String>,
    #[serde(default)]
    pub controller_set_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NodeRolesUpdate {
    pub roles: NodeRoles,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRolesResponse {
    pub node_id: String,
    pub roles: NodeRoles,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeLabelsResponse {
    pub node_id: String,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NodeLabelSetRequest {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NodeLabelRemoveRequest {
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub id: String,
    pub cluster_id: String,
    pub stack: String,
    pub service: String,
    pub service_id: String,
    pub revision: u64,
    pub slot: u32,
    pub spec: ServiceSpec,
    pub ports: Vec<PortBinding>,
    pub leader_term: u64,
    pub generation: u64,
    pub spec_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub cluster_id: String,
    pub generation: u64,
    #[serde(default)]
    pub controller_set_generation: u64,
    pub leader: Option<LeaderRecord>,
    pub is_leader: bool,
    pub gateway: GatewayStatus,
    pub recovery: RecoveryStatus,
    pub state: ClusterState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecoveryStatus {
    pub awaiting_adoption: usize,
    pub conflicting_slots: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GatewayStatus {
    pub enabled: bool,
    pub desired_generation: u64,
    pub applied_generation: Option<u64>,
    #[serde(default)]
    pub desired_controller_set_generation: u64,
    #[serde(default)]
    pub applied_controller_set_generations: BTreeMap<String, u64>,
    pub endpoint_errors: BTreeMap<String, String>,
}
