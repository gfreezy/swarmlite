use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterCaddyConfig {
    pub admin_endpoints: Vec<String>,
    pub listen: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSettings {
    pub schema_version: u32,
    pub cluster_id: String,
    pub controllers: u16,
    pub controller_port: u16,
    pub caddy: ClusterCaddyConfig,
}

pub fn valid_controller_count(controllers: u16) -> bool {
    controllers == 1 || (controllers >= 3 && !controllers.is_multiple_of(2))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfigResponse {
    pub generation: u64,
    pub config: ClusterSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfigUpdate {
    pub controllers: u16,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Controller,
    #[default]
    Worker,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControllerRecord {
    pub node_id: String,
    pub advertise_url: String,
    pub raft_id: u64,
    pub raft_url: String,
    pub reserved_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceSpec {
    pub image: String,
    pub command: Vec<String>,
    pub entrypoint: Vec<String>,
    pub environment: Vec<String>,
    pub ports: Vec<ServicePort>,
    pub volumes: Vec<String>,
    pub container_labels: BTreeMap<String, String>,
    pub service_labels: BTreeMap<String, String>,
    pub healthcheck: Option<HealthcheckSpec>,
    pub replicas: u32,
    pub constraints: Vec<String>,
    pub max_surge: u32,
    pub stop_grace_period_seconds: u64,
}

pub fn service_spec_hash(spec: &ServiceSpec) -> String {
    let encoded = serde_json::to_vec(spec).expect("ServiceSpec serialization cannot fail");
    let digest = Sha256::digest(encoded);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthcheckSpec {
    pub test: Vec<String>,
    pub interval_nanos: Option<i64>,
    pub timeout_nanos: Option<i64>,
    pub retries: Option<i64>,
    pub start_period_nanos: Option<i64>,
    pub start_interval_nanos: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServicePort {
    pub target: u16,
    pub published: Option<u16>,
    pub protocol: String,
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
    pub controller_capable: bool,
    pub controller_url: Option<String>,
    pub raft_id: Option<u64>,
    pub raft_url: Option<String>,
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
    pub assignments: Vec<TaskAssignment>,
    pub node_role: NodeRole,
    pub controllers: Vec<String>,
    pub remove_tasks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeControl {
    pub role: NodeRole,
    pub controllers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapResponse {
    pub cluster: ClusterSettings,
    pub controllers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: String,
    pub address: String,
    pub controller_capable: bool,
    pub controller_url: Option<String>,
    pub raft_id: Option<u64>,
    pub raft_url: Option<String>,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    pub cluster: ClusterSettings,
    pub role: NodeRole,
    pub controllers: Vec<String>,
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
    pub leader: Option<LeaderRecord>,
    pub is_leader: bool,
    pub caddy: CaddyStatus,
    pub recovery: RecoveryStatus,
    pub state: ClusterState,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RecoveryStatus {
    pub awaiting_adoption: usize,
    pub conflicting_slots: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CaddyStatus {
    pub enabled: bool,
    pub desired_generation: u64,
    pub applied_generation: Option<u64>,
    pub endpoint_errors: BTreeMap<String, String>,
}
