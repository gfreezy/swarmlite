use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceSpec {
    pub image: String,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub entrypoint: Vec<String>,
    #[serde(default)]
    pub environment: Vec<String>,
    #[serde(default)]
    pub ports: Vec<ServicePort>,
    #[serde(default)]
    pub volumes: Vec<String>,
    #[serde(default)]
    pub container_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub service_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub healthcheck: Option<HealthcheckSpec>,
    pub replicas: u32,
    #[serde(default)]
    pub constraints: Vec<String>,
    pub max_surge: u32,
    pub stop_grace_period_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthcheckSpec {
    pub test: Vec<String>,
    #[serde(default)]
    pub interval_nanos: Option<i64>,
    #[serde(default)]
    pub timeout_nanos: Option<i64>,
    #[serde(default)]
    pub retries: Option<i64>,
    #[serde(default)]
    pub start_period_nanos: Option<i64>,
    #[serde(default)]
    pub start_interval_nanos: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServicePort {
    pub target: u16,
    #[serde(default)]
    pub published: Option<u16>,
    #[serde(default = "default_protocol")]
    pub protocol: String,
}

fn default_protocol() -> String {
    "tcp".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceRecord {
    pub id: String,
    pub stack: String,
    pub name: String,
    pub revision: u64,
    pub spec: ServiceSpec,
    #[serde(default)]
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
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub port_range_start: u16,
    pub port_range_end: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DesiredTaskState {
    Running,
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
    #[serde(default = "default_protocol")]
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
    #[serde(default)]
    pub ports: Vec<PortBinding>,
    #[serde(default)]
    pub container_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterState {
    #[serde(default)]
    pub stacks: BTreeMap<String, StackRecord>,
    #[serde(default)]
    pub services: BTreeMap<String, ServiceRecord>,
    #[serde(default)]
    pub nodes: BTreeMap<String, NodeRecord>,
    #[serde(default)]
    pub tasks: BTreeMap<String, TaskRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderLease {
    pub id: String,
    pub term: u64,
    pub advertise_url: String,
    pub lease_until_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMeta {
    pub schema_version: u32,
    pub cluster_id: String,
    #[serde(default)]
    pub leader: Option<LeaderLease>,
    pub generation: u64,
    pub snapshot_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHeartbeat {
    pub node: NodeRecord,
    #[serde(default)]
    pub tasks: Vec<TaskReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskReport {
    pub id: String,
    pub observed: ObservedTaskState,
    #[serde(default)]
    pub container_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub leader_term: u64,
    pub generation: u64,
    pub assignments: Vec<TaskAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskAssignment {
    pub id: String,
    pub service_id: String,
    pub revision: u64,
    pub slot: u32,
    pub spec: ServiceSpec,
    pub ports: Vec<PortBinding>,
    pub leader_term: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub cluster_id: String,
    pub generation: u64,
    pub leader: Option<LeaderLease>,
    pub is_leader: bool,
    pub state: ClusterState,
}
