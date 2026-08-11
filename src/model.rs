use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
pub use swarmlite_stack::{
    GatewayHttpMode, GatewayTlsMode, HealthcheckSpec, HttpBackend, HttpBackendProtocol,
    HttpPathMatch, HttpPathMatchType, HttpPathRewrite, HttpRouteRule, HttpRouteSpec, ServicePort,
    ServiceSpec, StackGatewaySpec, service_spec_hash,
};

pub const CLUSTER_SCHEMA_VERSION: u32 = 7;
pub const DEFAULT_GATEWAY_IMAGE: &str = "ghcr.io/gfreezy/swarmlite-caddy:latest";

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KvPutRequest {
    pub key: String,
    pub value_base64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct KvDeleteRequest {
    pub key: String,
    /// Deletes the key and all keys below it when true.
    #[serde(default)]
    pub recursive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KvObjectResponse {
    pub key: String,
    pub value_base64: String,
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
    pub controller_id: String,
    pub controller_port: u16,
    pub gateway: ClusterGatewayConfig,
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
    pub gateway_image: Option<String>,
}

pub fn valid_gateway_image(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && value.trim() == value
        && !value.chars().any(char::is_whitespace)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeMember {
    pub id: String,
    pub address: String,
    pub gateway_enabled: bool,
    pub labels: BTreeMap<String, String>,
    pub joined_at_unix_ms: i64,
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
    pub gateway_enabled: bool,
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
pub struct NodeHeartbeat {
    pub node: NodeRecord,
    pub tasks: Vec<TaskReport>,
    #[serde(default)]
    pub gateway: GatewayReport,
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
    pub generation: u64,
    pub cluster: ClusterSettings,
    pub assignments: Vec<TaskAssignment>,
    pub gateway_enabled: bool,
    pub labels: BTreeMap<String, String>,
    pub remove_tasks: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_config: Option<GatewayAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeControl {
    pub cluster: ClusterSettings,
    pub gateway_enabled: bool,
    pub labels: BTreeMap<String, String>,
    pub gateway_config: Option<GatewayAssignment>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayReport {
    pub applied_generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayAssignment {
    pub generation: u64,
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapResponse {
    pub cluster: ClusterSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: String,
    pub address: String,
    pub gateway_enabled: bool,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    pub cluster: ClusterSettings,
    pub gateway_enabled: bool,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NodeGatewayUpdate {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeGatewayResponse {
    pub node_id: String,
    pub enabled: bool,
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
    pub generation: u64,
    pub spec_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub cluster_id: String,
    pub generation: u64,
    pub controller_id: String,
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
    pub endpoint_errors: BTreeMap<String, String>,
}
