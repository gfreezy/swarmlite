use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use serde::{Deserialize, Serialize};
pub use swarmlite_stack::{
    GatewayHttpMode, GatewayTlsMode, HealthcheckSpec, HttpBackend, HttpBackendProtocol,
    HttpPathMatch, HttpPathMatchType, HttpPathRewrite, HttpRouteRule, HttpRouteSpec, PullPolicy,
    ServiceConfigMount, ServicePort, ServiceSpec, StackGatewaySpec, config_digest,
    service_spec_hash,
};

pub const CLUSTER_SCHEMA_VERSION: u32 = 9;
pub const GATEWAY_RECOVERY_FORMAT_VERSION: u32 = 1;
pub const DEFAULT_GATEWAY_IMAGE: &str = "ghcr.io/gfreezy/swarmlite-caddy:latest";
pub const DEFAULT_DEPLOYMENT_PROGRESS_DEADLINE_SECONDS: u64 = 300;
pub const DEFAULT_IMAGE_PULL_IDLE_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_IMAGE_PULL_MAX_ATTEMPTS: u32 = 5;
pub const DEFAULT_IMAGE_PULL_INITIAL_BACKOFF_SECONDS: u64 = 2;
pub const DEFAULT_IMAGE_PULL_MAX_BACKOFF_SECONDS: u64 = 60;
pub const MAX_CONFIG_FILE_BYTES: usize = 1024 * 1024;
pub const MAX_STACK_CONFIG_BYTES: usize = 8 * 1024 * 1024;
pub const CONFIG_GC_GRACE_PERIOD_SECONDS: u64 = 7 * 24 * 60 * 60;

pub fn service_config_digests(spec: &ServiceSpec) -> Vec<String> {
    spec.configs
        .iter()
        .map(|config| config.digest.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StackApplyRequest {
    pub yaml: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub configs: BTreeMap<String, StackConfigPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StackConfigPayload {
    pub digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_base64: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfigBlobCheckRequest {
    pub digests: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConfigBlobCheckResponse {
    pub missing: BTreeSet<String>,
}

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
    pub deployment: DeploymentPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentPolicy {
    pub progress_deadline_seconds: u64,
    pub image_pull_idle_timeout_seconds: u64,
    pub image_pull_max_attempts: u32,
    pub image_pull_initial_backoff_seconds: u64,
    pub image_pull_max_backoff_seconds: u64,
}

impl Default for DeploymentPolicy {
    fn default() -> Self {
        Self {
            progress_deadline_seconds: DEFAULT_DEPLOYMENT_PROGRESS_DEADLINE_SECONDS,
            image_pull_idle_timeout_seconds: DEFAULT_IMAGE_PULL_IDLE_TIMEOUT_SECONDS,
            image_pull_max_attempts: DEFAULT_IMAGE_PULL_MAX_ATTEMPTS,
            image_pull_initial_backoff_seconds: DEFAULT_IMAGE_PULL_INITIAL_BACKOFF_SECONDS,
            image_pull_max_backoff_seconds: DEFAULT_IMAGE_PULL_MAX_BACKOFF_SECONDS,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfigResponse {
    pub generation: u64,
    pub config: ClusterSettings,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfigUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_image: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_progress_deadline_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_pull_idle_timeout_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_pull_max_attempts: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_pull_initial_backoff_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_pull_max_backoff_seconds: Option<u64>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment: Option<StackDeploymentRecord>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub deployment_history: BTreeMap<u64, StackDeploymentRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServicePortKey {
    pub service: String,
    pub target_port: u16,
    pub protocol: String,
}

impl ServicePortKey {
    pub fn new(
        service: impl Into<String>,
        target_port: u16,
        protocol: HttpBackendProtocol,
    ) -> Self {
        let protocol = match protocol {
            HttpBackendProtocol::Http => "http",
            HttpBackendProtocol::Https => "https",
            HttpBackendProtocol::H2c => "h2c",
        };
        Self {
            service: service.into(),
            target_port,
            protocol: protocol.to_owned(),
        }
    }
}

impl fmt::Display for ServicePortKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}:{}:{}",
            self.service, self.target_port, self.protocol
        )
    }
}

impl FromStr for ServicePortKey {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (service_and_port, protocol) = value
            .rsplit_once(':')
            .ok_or_else(|| "expected service:target_port:protocol".to_owned())?;
        let (service, target_port) = service_and_port
            .rsplit_once(':')
            .ok_or_else(|| "expected service:target_port:protocol".to_owned())?;
        let target_port = target_port
            .parse::<u16>()
            .map_err(|_| "target_port must be between 1 and 65535".to_owned())?;
        if service.is_empty() || target_port == 0 {
            return Err("service and target_port must be non-empty".to_owned());
        }
        if !matches!(protocol, "http" | "https" | "h2c") {
            return Err(format!("unsupported backend protocol {protocol:?}"));
        }
        Ok(Self {
            service: service.to_owned(),
            target_port,
            protocol: protocol.to_owned(),
        })
    }
}

impl Serialize for ServicePortKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ServicePortKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecoveredStackGateway {
    pub gateway: StackGatewaySpec,
    pub upstreams: BTreeMap<ServicePortKey, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GatewayRecoverySnapshot {
    pub format_version: u32,
    pub cluster_id: String,
    pub generation: u64,
    pub stacks: BTreeMap<String, RecoveredStackGateway>,
}

impl GatewayRecoverySnapshot {
    pub fn new(
        cluster_id: impl Into<String>,
        generation: u64,
        stacks: BTreeMap<String, RecoveredStackGateway>,
    ) -> Self {
        Self {
            format_version: GATEWAY_RECOVERY_FORMAT_VERSION,
            cluster_id: cluster_id.into(),
            generation,
            stacks,
        }
    }

    pub fn validate_for_cluster(&self, cluster_id: &str) -> Result<(), String> {
        if self.format_version != GATEWAY_RECOVERY_FORMAT_VERSION {
            return Err(format!(
                "unsupported gateway recovery snapshot format {}; expected {}",
                self.format_version, GATEWAY_RECOVERY_FORMAT_VERSION
            ));
        }
        if self.cluster_id != cluster_id {
            return Err(format!(
                "gateway recovery snapshot belongs to cluster {}, not {cluster_id}",
                self.cluster_id
            ));
        }
        if self.generation == 0 {
            return Err("gateway recovery snapshot generation must be positive".to_owned());
        }
        for (stack_key, stack) in &self.stacks {
            swarmlite_stack::validate_stack_name(stack_key)
                .map_err(|error| format!("invalid recovered stack {stack_key:?}: {error}"))?;
            let expected_keys = stack
                .gateway
                .http_routes
                .iter()
                .flat_map(|route| &route.rules)
                .filter_map(|rule| {
                    rule.backend.service.as_deref().map(|service| {
                        ServicePortKey::new(service, rule.backend.port, rule.backend.protocol)
                    })
                })
                .collect::<BTreeSet<_>>();
            let actual_keys = stack.upstreams.keys().cloned().collect::<BTreeSet<_>>();
            if actual_keys != expected_keys {
                return Err(format!(
                    "recovered stack {stack_key:?} upstream keys do not match its service backends"
                ));
            }
            for (key, upstreams) in &stack.upstreams {
                if upstreams.windows(2).any(|pair| pair[0] >= pair[1]) {
                    return Err(format!(
                        "recovered stack {stack_key:?} upstreams for {key} are not sorted and unique"
                    ));
                }
                if upstreams.iter().any(|upstream| {
                    upstream
                        .rsplit_once(':')
                        .filter(|(address, _)| !address.is_empty())
                        .and_then(|(_, port)| port.parse::<u16>().ok())
                        .is_none_or(|port| port == 0)
                }) {
                    return Err(format!(
                        "recovered stack {stack_key:?} has an invalid address:published_port upstream for {key}"
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StackDeploymentRecord {
    pub generation: u64,
    pub status: StackDeploymentStatus,
    pub started_at_unix_ms: i64,
    pub last_progress_at_unix_ms: i64,
    pub progress_deadline_seconds: u64,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub wait_for_gateway: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<u64>,
    #[serde(default)]
    pub retry_revision: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<StackDeploymentError>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub image_resolutions: BTreeMap<String, DeploymentImageResolutionRecord>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<StackDeploymentCondition>,
    pub snapshot: StackDeploymentSnapshot,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StackDeploymentSnapshot {
    pub services: BTreeMap<String, ServiceSpec>,
    pub gateway: StackGatewaySpec,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StackDeploymentCondition {
    pub kind: StackDeploymentConditionKind,
    pub observed_at_unix_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_at_unix_ms: Option<i64>,
    pub reason: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StackDeploymentConditionKind {
    ProgressDeadlineExceeded,
    UserActionRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentImageResolutionRecord {
    pub service_id: String,
    pub service: String,
    pub image: String,
    pub baseline_revision: u64,
    pub status: ImageResolutionStatus,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub nodes: BTreeMap<String, DeploymentImageResolutionNodeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeploymentImageResolutionNodeRecord {
    pub task_ids: Vec<String>,
    pub status: ImageResolutionStatus,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub old_image_ids: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_image_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ImageResolutionStatus {
    Checking,
    Pulling,
    Comparing,
    Unchanged,
    Changed,
    Skipped,
    Failed,
}

impl ImageResolutionStatus {
    pub fn is_complete(self) -> bool {
        matches!(self, Self::Unchanged | Self::Changed | Self::Skipped)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StackDeploymentStatus {
    Reconciling,
    Stalled,
    Blocked,
    Healthy,
    Failed,
    Superseded,
}

impl StackDeploymentStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Healthy | Self::Failed | Self::Superseded)
    }

    pub fn is_active(self) -> bool {
        !self.is_terminal()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StackDeploymentError {
    pub task_id: String,
    pub service: String,
    pub node_id: String,
    pub phase: TaskReconcilePhase,
    pub message: String,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published: Option<u16>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config_digests: Vec<String>,
    pub container_id: Option<String>,
    pub drain_until_unix_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconcile_error: Option<TaskReconcileError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterState {
    pub stacks: BTreeMap<String, StackRecord>,
    pub services: BTreeMap<String, ServiceRecord>,
    pub nodes: BTreeMap<String, NodeRecord>,
    pub tasks: BTreeMap<String, TaskRecord>,
    pub members: BTreeMap<String, NodeMember>,
    pub unclaimed_tasks: BTreeMap<String, UnclaimedTask>,
    #[serde(default)]
    pub gateway_routes: BTreeMap<String, RecoveredStackGateway>,
    #[serde(default)]
    pub gateway_generation: u64,
    #[serde(default)]
    pub registry_credentials: BTreeMap<String, RegistryCredential>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RegistryCredential {
    pub username: String,
    pub password: String,
}

impl fmt::Debug for RegistryCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistryCredential")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryLoginRequest {
    pub registry: String,
    pub username: String,
    pub password: String,
}

impl fmt::Debug for RegistryLoginRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegistryLoginRequest")
            .field("registry", &self.registry)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegistryLoginResponse {
    pub registry: String,
    pub username: String,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config_digests: Vec<String>,
    pub container_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHeartbeat {
    pub node: NodeRecord,
    pub tasks: Vec<TaskReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_inventory_error: Option<String>,
    #[serde(default)]
    pub task_results: Vec<TaskReconcileReport>,
    #[serde(default)]
    pub task_progress: Vec<TaskReconcileProgress>,
    #[serde(default)]
    pub image_results: Vec<ImageResolutionReport>,
    #[serde(default)]
    pub image_progress: Vec<ImageResolutionProgress>,
    #[serde(default)]
    pub gateway: GatewayReport,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskReconcileProgress {
    pub task_id: String,
    pub desired_generation: u64,
    pub phase: TaskReconcilePhase,
    #[serde(default)]
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskReconcileReport {
    pub task_id: String,
    pub desired_generation: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_generation: Option<u64>,
    pub phase: TaskReconcilePhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum TaskReconcilePhase {
    Inspect,
    Config,
    Pull,
    Create,
    Replace,
    Start,
    Stop,
    Remove,
    Verify,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskReconcileError {
    pub phase: TaskReconcilePhase,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskReport {
    pub id: String,
    pub observed: ObservedTaskState,
    pub container_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_id: Option<String>,
    pub cluster_id: Option<String>,
    pub stack: Option<String>,
    pub service: Option<String>,
    pub slot: Option<u32>,
    pub revision: Option<u64>,
    pub spec_hash: Option<String>,
    pub ports: Vec<PortBinding>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config_digests: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartbeatResponse {
    pub generation: u64,
    pub cluster: ClusterSettings,
    pub assignments: Vec<TaskAssignment>,
    #[serde(default)]
    pub image_assignments: Vec<ImageResolutionAssignment>,
    pub gateway_enabled: bool,
    pub labels: BTreeMap<String, String>,
    pub remove_tasks: Vec<TaskRemovalAssignment>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_config: Option<GatewayAssignment>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub registry_credentials: BTreeMap<String, RegistryCredential>,
    #[serde(default)]
    pub registry_credentials_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskRemovalAssignment {
    pub id: String,
    pub deployment_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeControl {
    pub cluster: ClusterSettings,
    pub gateway_enabled: bool,
    pub labels: BTreeMap<String, String>,
    pub gateway_config: Option<GatewayAssignment>,
    pub registry_credentials_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayReport {
    pub applied_generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default = "default_gateway_retryable")]
    pub retryable: bool,
}

const fn default_gateway_retryable() -> bool {
    true
}

impl Default for GatewayReport {
    fn default() -> Self {
        Self {
            applied_generation: None,
            error: None,
            retryable: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayAssignment {
    pub generation: u64,
    pub config: serde_json::Value,
    pub recovery_snapshot: GatewayRecoverySnapshot,
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
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub registry_credentials: BTreeMap<String, RegistryCredential>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskAssignment {
    pub id: String,
    pub cluster_id: String,
    pub stack: String,
    pub service: String,
    pub service_id: String,
    pub revision: u64,
    pub slot: u32,
    pub desired: DesiredTaskState,
    pub spec: ServiceSpec,
    pub ports: Vec<PortBinding>,
    pub generation: u64,
    pub deployment_generation: u64,
    #[serde(default)]
    pub deployment_retry_revision: u64,
    pub spec_hash: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub image_resolved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageResolutionAssignment {
    pub deployment_generation: u64,
    pub image: String,
    pub services: Vec<ImageResolutionServiceAssignment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageResolutionServiceAssignment {
    pub service_id: String,
    pub task_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageResolutionProgress {
    pub deployment_generation: u64,
    pub image: String,
    pub status: ImageResolutionStatus,
    #[serde(default)]
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_bytes: Option<u64>,
    pub updated_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageResolutionReport {
    pub deployment_generation: u64,
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_image_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub services: Vec<ImageResolutionServiceReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImageResolutionServiceReport {
    pub service_id: String,
    pub old_image_ids: BTreeMap<String, String>,
    pub changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StackDeploymentResponse {
    pub stack: String,
    pub generation: u64,
    pub revision: u64,
    pub status: StackDeploymentStatus,
    pub started_at_unix_ms: i64,
    pub last_progress_at_unix_ms: i64,
    pub progress_deadline_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<u64>,
    #[serde(default)]
    pub retry_revision: u64,
    pub services: Vec<StackDeploymentServiceProgress>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub pending_removals: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_phases: Vec<StackDeploymentTaskPhaseProgress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub image_resolutions: Vec<StackDeploymentImageProgress>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway: Option<StackDeploymentGatewayProgress>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<StackDeploymentError>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<StackDeploymentCondition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackDeploymentListResponse {
    pub stack: String,
    pub current: Option<StackDeploymentResponse>,
    pub history: Vec<StackDeploymentSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StackDeploymentSummary {
    pub generation: u64,
    pub status: StackDeploymentStatus,
    pub started_at_unix_ms: i64,
    pub last_progress_at_unix_ms: i64,
    pub progress_deadline_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<u64>,
    pub retry_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StackRollbackRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StackValidationResponse {
    pub stack: String,
    pub valid: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StackDeploymentServiceProgress {
    pub service: String,
    pub replicas: u32,
    pub applied: u32,
    pub healthy: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StackDeploymentTaskPhaseProgress {
    pub phase: TaskReconcilePhase,
    pub tasks: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StackDeploymentImageProgress {
    pub service: String,
    pub image: String,
    pub status: ImageResolutionStatus,
    pub completed_nodes: u32,
    pub total_nodes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StackDeploymentGatewayProgress {
    pub generation: u64,
    pub applied_nodes: u32,
    pub total_nodes: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub errors: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskListResponse {
    pub tasks: Vec<TaskSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackListResponse {
    pub stacks: Vec<StackSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StackSummary {
    pub name: String,
    pub services: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<StackDeploymentStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceListResponse {
    pub services: Vec<ServiceSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSummary {
    pub id: String,
    pub stack: String,
    pub name: String,
    pub image: String,
    pub replicas: u32,
    pub running_replicas: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSummary {
    pub id: String,
    pub stack: String,
    pub service: String,
    pub slot: u32,
    pub node_id: String,
    pub desired: DesiredTaskState,
    pub observed: ObservedTaskState,
    pub image: String,
    pub ports: Vec<PortBinding>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInspectResponse {
    pub service: ServiceRecord,
    pub stack: StackRecord,
    pub tasks: Vec<TaskRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum DataSessionOperation {
    Logs {
        target: String,
        tail: u32,
        #[serde(default)]
        follow: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSessionCreateResponse {
    pub session_id: String,
    pub attach_token: String,
    pub streams: Vec<DataSessionStream>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSessionStream {
    pub stream_id: u32,
    pub task_id: String,
    pub node_id: String,
    pub stack: String,
    pub service: String,
    pub slot: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDataStream {
    pub stream_id: u32,
    pub task_id: String,
    #[serde(flatten)]
    pub operation: AgentDataStreamOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "stream_operation", rename_all = "snake_case")]
pub enum AgentDataStreamOperation {
    Logs {
        tail: u32,
        #[serde(default)]
        follow: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCommand {
    pub id: String,
    #[serde(flatten)]
    pub operation: AgentCommandOperation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum AgentCommandOperation {
    OpenDataSession {
        session_id: String,
        upload_token: String,
        streams: Vec<AgentDataStream>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCommandPollResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<AgentCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentCommandResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCommandAck {
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceScaleRequest {
    pub replicas: u32,
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

#[cfg(test)]
mod gateway_recovery_tests {
    use super::*;

    #[test]
    fn snapshot_round_trip_uses_stable_service_port_map_keys() {
        let gateway = swarmlite_stack::parse_stack(
            r#"
services:
  web: { image: nginx, expose: [80] }
x-swarmlite:
  http_routes:
    - hostnames: [example.com]
      rules: [{ backend: { service: web, port: 80, protocol: h2c } }]
"#,
        )
        .unwrap()
        .gateway;
        let snapshot = GatewayRecoverySnapshot::new(
            "cluster-a",
            9,
            BTreeMap::from([(
                "demo".into(),
                RecoveredStackGateway {
                    gateway,
                    upstreams: BTreeMap::from([(
                        ServicePortKey::new("web", 80, HttpBackendProtocol::H2c),
                        vec!["10.0.0.8:32080".into()],
                    )]),
                },
            )]),
        );

        let encoded = serde_json::to_string(&snapshot).unwrap();
        assert!(encoded.contains(r#""web:80:h2c""#));
        let decoded: GatewayRecoverySnapshot = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, snapshot);
        decoded.validate_for_cluster("cluster-a").unwrap();
    }
}
