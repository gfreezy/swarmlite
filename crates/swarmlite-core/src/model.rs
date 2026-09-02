use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use serde::{Deserialize, Serialize};
pub use swarmlite_stack::{
    GatewayHttpMode, GatewayTlsMode, HealthcheckSpec, HttpBackend, HttpBackendProtocol,
    HttpPathMatch, HttpPathMatchType, HttpPathRewrite, HttpRouteRule, HttpRouteSpec, PullPolicy,
    ServiceConfigMount, ServicePort, ServiceSpec, StackGatewaySpec, TemplateContext, TemplateNode,
    config_digest, service_spec_hash,
};

pub const CLUSTER_SCHEMA_VERSION: u32 = 9;
pub const GATEWAY_RECOVERY_FORMAT_VERSION: u32 = 1;
pub const LEGACY_DEFAULT_GATEWAY_IMAGE: &str = "ghcr.io/gfreezy/swarmlite-caddy:latest";
pub const DEFAULT_GATEWAY_IMAGE: &str = concat!(
    "ghcr.io/gfreezy/swarmlite-caddy:v",
    env!("CARGO_PKG_VERSION")
);
pub const DEFAULT_DEPLOYMENT_PROGRESS_DEADLINE_SECONDS: u64 = 300;
pub const DEFAULT_IMAGE_PULL_IDLE_TIMEOUT_SECONDS: u64 = 60;
pub const DEFAULT_IMAGE_PULL_MAX_ATTEMPTS: u32 = 5;
pub const DEFAULT_IMAGE_PULL_INITIAL_BACKOFF_SECONDS: u64 = 2;
pub const DEFAULT_IMAGE_PULL_MAX_BACKOFF_SECONDS: u64 = 60;
pub const DEFAULT_AGENT_IMAGE_PRUNE_INTERVAL_SECONDS: u64 = 7 * 24 * 60 * 60;
pub const MAX_CADDY_DURATION_SECONDS: u64 = i64::MAX as u64 / 1_000_000_000;
pub const MAX_CONFIG_FILE_BYTES: usize = 1024 * 1024;
pub const MAX_STACK_CONFIG_BYTES: usize = 8 * 1024 * 1024;
pub const CONFIG_GC_GRACE_PERIOD_SECONDS: u64 = 7 * 24 * 60 * 60;
pub const DEFAULT_GATEWAY_CACHE_MAX_SIZE_BYTES: u64 = 1 << 30;
pub const DEFAULT_GATEWAY_CACHE_LOW_WATER_PERCENT: u8 = 90;
pub const DEFAULT_GATEWAY_CACHE_ADMISSION_WINDOW_SECONDS: u64 = 5 * 60;
pub const DEFAULT_GATEWAY_CACHE_AFTER_REQUESTS: u8 = 3;
pub const MAX_GATEWAY_CACHE_AFTER_REQUESTS: u8 = 8;
pub const DEFAULT_GATEWAY_CACHE_SQLITE_TOUCH_WINDOW_SECONDS: u64 = 5 * 60;
pub const DEFAULT_GATEWAY_CACHE_SQLITE_MMAP_SIZE_BYTES: u64 = 256 << 20;
pub const DEFAULT_GATEWAY_CACHE_SQLITE_READ_CONNECTIONS: u8 = 4;
pub const DEFAULT_GATEWAY_CACHE_SQLITE_BUSY_TIMEOUT_SECONDS: u64 = 5;
pub const DEFAULT_GATEWAY_CACHE_SQLITE_CLEANUP_INTERVAL_SECONDS: u64 = 5 * 60;
pub const DEFAULT_GATEWAY_CACHE_SQLITE_JOURNAL_SIZE_LIMIT_BYTES: u64 = 64 << 20;
pub const MAX_GATEWAY_CACHE_SIGNED_SIZE: u64 = i64::MAX as u64;
pub const MAX_GATEWAY_CACHE_SQLITE_READ_CONNECTIONS: u8 = 16;

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
    #[serde(default)]
    pub managed_image: bool,
    #[serde(default, skip_serializing_if = "GatewayMetricsConfig::is_empty")]
    pub metrics: GatewayMetricsConfig,
    #[serde(default, skip_serializing_if = "GatewayCacheConfig::is_empty")]
    pub cache: GatewayCacheConfig,
    #[serde(default, skip_serializing_if = "GatewayLoggingConfig::is_empty")]
    pub logging: GatewayLoggingConfig,
    #[serde(default, skip_serializing_if = "GatewayShutdownConfig::is_empty")]
    pub shutdown: GatewayShutdownConfig,
    #[serde(default, skip_serializing_if = "GatewayHttpConfig::is_empty")]
    pub http: GatewayHttpConfig,
}

impl Default for ClusterGatewayConfig {
    fn default() -> Self {
        Self {
            listen: vec![":80".to_owned(), ":443".to_owned()],
            image: DEFAULT_GATEWAY_IMAGE.to_owned(),
            managed_image: true,
            metrics: GatewayMetricsConfig::default(),
            cache: GatewayCacheConfig::default(),
            logging: GatewayLoggingConfig::default(),
            shutdown: GatewayShutdownConfig::default(),
            http: GatewayHttpConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayCacheConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub low_water_percent: Option<u8>,
    #[serde(default, skip_serializing_if = "GatewayCacheAdmissionConfig::is_empty")]
    pub admission: GatewayCacheAdmissionConfig,
    #[serde(default, skip_serializing_if = "GatewayCacheSqliteConfig::is_empty")]
    pub sqlite: GatewayCacheSqliteConfig,
}

impl GatewayCacheConfig {
    fn is_empty(&self) -> bool {
        self.max_size_bytes.is_none()
            && self.low_water_percent.is_none()
            && self.admission.is_empty()
            && self.sqlite.is_empty()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayCacheAdmissionConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_after_requests: Option<u8>,
}

impl GatewayCacheAdmissionConfig {
    fn is_empty(&self) -> bool {
        self.window_seconds.is_none() && self.cache_after_requests.is_none()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayCacheSqliteConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub touch_window_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_size_kib: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mmap_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_connections: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub busy_timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cleanup_interval_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journal_size_limit_bytes: Option<u64>,
}

impl GatewayCacheSqliteConfig {
    fn is_empty(&self) -> bool {
        self.touch_window_seconds.is_none()
            && self.cache_size_kib.is_none()
            && self.mmap_size_bytes.is_none()
            && self.read_connections.is_none()
            && self.busy_timeout_seconds.is_none()
            && self.cleanup_interval_seconds.is_none()
            && self.journal_size_limit_bytes.is_none()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayMetricsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_host: Option<bool>,
}

impl GatewayMetricsConfig {
    fn is_empty(&self) -> bool {
        self.enabled.is_none() && self.per_host.is_none()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayLoggingConfig {
    #[serde(default, skip_serializing_if = "GatewayRuntimeLogConfig::is_empty")]
    pub runtime: GatewayRuntimeLogConfig,
    #[serde(default, skip_serializing_if = "GatewayAccessLogConfig::is_empty")]
    pub access: GatewayAccessLogConfig,
}

impl GatewayLoggingConfig {
    fn is_empty(&self) -> bool {
        self.runtime.is_empty() && self.access.is_empty()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayRuntimeLogConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<GatewayLogLevel>,
}

impl GatewayRuntimeLogConfig {
    fn is_empty(&self) -> bool {
        self.level.is_none()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayAccessLogConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<GatewayAccessLogFormat>,
    #[serde(
        default,
        skip_serializing_if = "GatewayAccessLogSamplingConfig::is_empty"
    )]
    pub sampling: GatewayAccessLogSamplingConfig,
}

impl GatewayAccessLogConfig {
    fn is_empty(&self) -> bool {
        self.enabled.is_none() && self.format.is_none() && self.sampling.is_empty()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum GatewayAccessLogFormat {
    Json,
    Console,
}

impl GatewayAccessLogFormat {
    pub fn as_caddy_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Console => "console",
        }
    }
}

impl FromStr for GatewayAccessLogFormat {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "json" => Ok(Self::Json),
            "console" => Ok(Self::Console),
            _ => Err("must be json or console".to_owned()),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayAccessLogSamplingConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thereafter: Option<u32>,
}

impl GatewayAccessLogSamplingConfig {
    fn is_empty(&self) -> bool {
        self.enabled.is_none() && self.first.is_none() && self.thereafter.is_none()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum GatewayLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl GatewayLogLevel {
    pub fn as_caddy_str(self) -> &'static str {
        match self {
            Self::Debug => "DEBUG",
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
        }
    }
}

impl FromStr for GatewayLogLevel {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "debug" => Ok(Self::Debug),
            "info" => Ok(Self::Info),
            "warn" => Ok(Self::Warn),
            "error" => Ok(Self::Error),
            _ => Err("must be one of debug, info, warn, or error".to_owned()),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayShutdownConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grace_period_seconds: Option<u64>,
}

impl GatewayShutdownConfig {
    fn is_empty(&self) -> bool {
        self.grace_period_seconds.is_none()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayHttpConfig {
    #[serde(default, skip_serializing_if = "GatewayHttpTimeoutsConfig::is_empty")]
    pub timeouts: GatewayHttpTimeoutsConfig,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_header_bytes: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http3_enabled: Option<bool>,
}

impl GatewayHttpConfig {
    fn is_empty(&self) -> bool {
        self.timeouts.is_empty() && self.max_header_bytes.is_none() && self.http3_enabled.is_none()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayHttpTimeoutsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_header_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_body_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_seconds: Option<u64>,
}

impl GatewayHttpTimeoutsConfig {
    fn is_empty(&self) -> bool {
        self.read_header_seconds.is_none()
            && self.read_body_seconds.is_none()
            && self.write_seconds.is_none()
            && self.idle_seconds.is_none()
    }
}

pub fn refresh_managed_gateway_image(config: &mut ClusterGatewayConfig) -> bool {
    if config.image == LEGACY_DEFAULT_GATEWAY_IMAGE {
        config.image = DEFAULT_GATEWAY_IMAGE.to_owned();
        config.managed_image = true;
        return true;
    }
    if config.managed_image && config.image != DEFAULT_GATEWAY_IMAGE {
        config.image = DEFAULT_GATEWAY_IMAGE.to_owned();
        return true;
    }
    false
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ClusterProxyConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub https: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_proxy: Option<String>,
}

impl ClusterProxyConfig {
    pub fn is_empty(&self) -> bool {
        self.http.is_none() && self.https.is_none() && self.all.is_none() && self.no_proxy.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterSettings {
    pub schema_version: u32,
    pub cluster_id: String,
    pub controller_id: String,
    pub controller_port: u16,
    #[serde(default, skip_serializing_if = "ClusterProxyConfig::is_empty")]
    pub proxy: ClusterProxyConfig,
    #[serde(default)]
    pub agent: ClusterAgentConfig,
    pub gateway: ClusterGatewayConfig,
    pub deployment: DeploymentPolicy,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterAgentConfig {
    #[serde(default)]
    pub image_prune: AgentImagePruneConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentImagePruneConfig {
    #[serde(default = "default_agent_image_prune_enabled")]
    pub enabled: bool,
    #[serde(default = "default_agent_image_prune_interval_seconds")]
    pub interval_seconds: u64,
}

impl Default for AgentImagePruneConfig {
    fn default() -> Self {
        Self {
            enabled: default_agent_image_prune_enabled(),
            interval_seconds: default_agent_image_prune_interval_seconds(),
        }
    }
}

const fn default_agent_image_prune_enabled() -> bool {
    true
}

const fn default_agent_image_prune_interval_seconds() -> u64 {
    DEFAULT_AGENT_IMAGE_PRUNE_INTERVAL_SECONDS
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
pub struct ClusterConfigUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_image_prune_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_image_prune_interval_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_http: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_https: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_all: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_no_proxy: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_listen: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_metrics_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_metrics_per_host: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_cache_max_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_cache_low_water_percent: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_cache_admission_window_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_cache_admission_cache_after_requests: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_cache_sqlite_touch_window_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_cache_sqlite_cache_size_kib: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_cache_sqlite_mmap_size_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_cache_sqlite_read_connections: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_cache_sqlite_busy_timeout_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_cache_sqlite_cleanup_interval_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_cache_sqlite_journal_size_limit_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_logging_runtime_level: Option<GatewayLogLevel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_logging_access_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_logging_access_format: Option<GatewayAccessLogFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_logging_access_sampling_enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_logging_access_sampling_first: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_logging_access_sampling_thereafter: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_shutdown_grace_period_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_http_read_header_timeout_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_http_read_body_timeout_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_http_write_timeout_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_http_idle_timeout_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_http_max_header_bytes: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_http_http3_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub unset: BTreeSet<ClusterConfigField>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum ClusterConfigField {
    #[serde(rename = "agent.image-prune.enabled")]
    AgentImagePruneEnabled,
    #[serde(rename = "agent.image-prune.interval-seconds")]
    AgentImagePruneIntervalSeconds,
    #[serde(rename = "proxy.http")]
    ProxyHttp,
    #[serde(rename = "proxy.https")]
    ProxyHttps,
    #[serde(rename = "proxy.all")]
    ProxyAll,
    #[serde(rename = "proxy.no-proxy")]
    ProxyNoProxy,
    #[serde(rename = "gateway.image")]
    GatewayImage,
    #[serde(rename = "gateway.listen")]
    GatewayListen,
    #[serde(rename = "gateway.metrics.enabled")]
    GatewayMetricsEnabled,
    #[serde(rename = "gateway.metrics.per-host")]
    GatewayMetricsPerHost,
    #[serde(rename = "gateway.cache.max-size-bytes")]
    GatewayCacheMaxSizeBytes,
    #[serde(rename = "gateway.cache.low-water-percent")]
    GatewayCacheLowWaterPercent,
    #[serde(rename = "gateway.cache.admission.window-seconds")]
    GatewayCacheAdmissionWindowSeconds,
    #[serde(rename = "gateway.cache.admission.cache-after-requests")]
    GatewayCacheAdmissionCacheAfterRequests,
    #[serde(rename = "gateway.cache.sqlite.touch-window-seconds")]
    GatewayCacheSqliteTouchWindowSeconds,
    #[serde(rename = "gateway.cache.sqlite.cache-size-kib")]
    GatewayCacheSqliteCacheSizeKib,
    #[serde(rename = "gateway.cache.sqlite.mmap-size-bytes")]
    GatewayCacheSqliteMmapSizeBytes,
    #[serde(rename = "gateway.cache.sqlite.read-connections")]
    GatewayCacheSqliteReadConnections,
    #[serde(rename = "gateway.cache.sqlite.busy-timeout-seconds")]
    GatewayCacheSqliteBusyTimeoutSeconds,
    #[serde(rename = "gateway.cache.sqlite.cleanup-interval-seconds")]
    GatewayCacheSqliteCleanupIntervalSeconds,
    #[serde(rename = "gateway.cache.sqlite.journal-size-limit-bytes")]
    GatewayCacheSqliteJournalSizeLimitBytes,
    #[serde(rename = "gateway.logging.runtime.level")]
    GatewayLoggingRuntimeLevel,
    #[serde(rename = "gateway.logging.access.enabled")]
    GatewayLoggingAccessEnabled,
    #[serde(rename = "gateway.logging.access.format")]
    GatewayLoggingAccessFormat,
    #[serde(rename = "gateway.logging.access.sampling.enabled")]
    GatewayLoggingAccessSamplingEnabled,
    #[serde(rename = "gateway.logging.access.sampling.first")]
    GatewayLoggingAccessSamplingFirst,
    #[serde(rename = "gateway.logging.access.sampling.thereafter")]
    GatewayLoggingAccessSamplingThereafter,
    #[serde(rename = "gateway.shutdown.grace-period-seconds")]
    GatewayShutdownGracePeriodSeconds,
    #[serde(rename = "gateway.http.timeouts.read-header-seconds")]
    GatewayHttpReadHeaderTimeoutSeconds,
    #[serde(rename = "gateway.http.timeouts.read-body-seconds")]
    GatewayHttpReadBodyTimeoutSeconds,
    #[serde(rename = "gateway.http.timeouts.write-seconds")]
    GatewayHttpWriteTimeoutSeconds,
    #[serde(rename = "gateway.http.timeouts.idle-seconds")]
    GatewayHttpIdleTimeoutSeconds,
    #[serde(rename = "gateway.http.max-header-bytes")]
    GatewayHttpMaxHeaderBytes,
    #[serde(rename = "gateway.http.http3-enabled")]
    GatewayHttpHttp3Enabled,
    #[serde(rename = "deployment.progress-deadline-seconds")]
    DeploymentProgressDeadlineSeconds,
    #[serde(rename = "deployment.image-pull.idle-timeout-seconds")]
    DeploymentImagePullIdleTimeoutSeconds,
    #[serde(rename = "deployment.image-pull.max-attempts")]
    DeploymentImagePullMaxAttempts,
    #[serde(rename = "deployment.image-pull.initial-backoff-seconds")]
    DeploymentImagePullInitialBackoffSeconds,
    #[serde(rename = "deployment.image-pull.max-backoff-seconds")]
    DeploymentImagePullMaxBackoffSeconds,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swarmlite_version: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
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
            image: None,
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
pub struct GatewayClusterStatusResponse {
    pub cluster_id: String,
    pub desired_generation: u64,
    pub config: GatewayPublicConfig,
    pub nodes: Vec<GatewayNodeStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicConfig {
    pub image: String,
    pub listen: Vec<String>,
    pub metrics: GatewayPublicMetricsConfig,
    pub cache: GatewayPublicCacheConfig,
    pub logging: GatewayPublicLoggingConfig,
    pub shutdown: GatewayPublicShutdownConfig,
    pub http: GatewayPublicHttpConfig,
}

impl From<&ClusterGatewayConfig> for GatewayPublicConfig {
    fn from(config: &ClusterGatewayConfig) -> Self {
        Self {
            image: config.image.clone(),
            listen: config.listen.clone(),
            metrics: GatewayPublicMetricsConfig {
                enabled: config.metrics.enabled,
                per_host: config.metrics.per_host,
            },
            cache: GatewayPublicCacheConfig {
                max_size_bytes: config.cache.max_size_bytes,
                low_water_percent: config.cache.low_water_percent,
                admission: GatewayPublicCacheAdmissionConfig {
                    window_seconds: config.cache.admission.window_seconds,
                    cache_after_requests: config.cache.admission.cache_after_requests,
                },
                sqlite: GatewayPublicCacheSqliteConfig {
                    touch_window_seconds: config.cache.sqlite.touch_window_seconds,
                    cache_size_kib: config.cache.sqlite.cache_size_kib,
                    mmap_size_bytes: config.cache.sqlite.mmap_size_bytes,
                    read_connections: config.cache.sqlite.read_connections,
                    busy_timeout_seconds: config.cache.sqlite.busy_timeout_seconds,
                    cleanup_interval_seconds: config.cache.sqlite.cleanup_interval_seconds,
                    journal_size_limit_bytes: config.cache.sqlite.journal_size_limit_bytes,
                },
            },
            logging: GatewayPublicLoggingConfig {
                runtime: GatewayPublicRuntimeLogConfig {
                    level: config.logging.runtime.level,
                },
                access: GatewayPublicAccessLogConfig {
                    enabled: config.logging.access.enabled,
                    format: config.logging.access.format,
                    sampling: GatewayPublicAccessLogSamplingConfig {
                        enabled: config.logging.access.sampling.enabled,
                        first: config.logging.access.sampling.first,
                        thereafter: config.logging.access.sampling.thereafter,
                    },
                },
            },
            shutdown: GatewayPublicShutdownConfig {
                grace_period_seconds: config.shutdown.grace_period_seconds,
            },
            http: GatewayPublicHttpConfig {
                timeouts: GatewayPublicHttpTimeoutsConfig {
                    read_header_seconds: config.http.timeouts.read_header_seconds,
                    read_body_seconds: config.http.timeouts.read_body_seconds,
                    write_seconds: config.http.timeouts.write_seconds,
                    idle_seconds: config.http.timeouts.idle_seconds,
                },
                max_header_bytes: config.http.max_header_bytes,
                http3_enabled: config.http.http3_enabled,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicMetricsConfig {
    pub enabled: Option<bool>,
    pub per_host: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicCacheConfig {
    pub max_size_bytes: Option<u64>,
    pub low_water_percent: Option<u8>,
    pub admission: GatewayPublicCacheAdmissionConfig,
    pub sqlite: GatewayPublicCacheSqliteConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicCacheAdmissionConfig {
    pub window_seconds: Option<u64>,
    pub cache_after_requests: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicCacheSqliteConfig {
    pub touch_window_seconds: Option<u64>,
    pub cache_size_kib: Option<u64>,
    pub mmap_size_bytes: Option<u64>,
    pub read_connections: Option<u8>,
    pub busy_timeout_seconds: Option<u64>,
    pub cleanup_interval_seconds: Option<u64>,
    pub journal_size_limit_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicLoggingConfig {
    pub runtime: GatewayPublicRuntimeLogConfig,
    pub access: GatewayPublicAccessLogConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicRuntimeLogConfig {
    pub level: Option<GatewayLogLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicAccessLogConfig {
    pub enabled: Option<bool>,
    pub format: Option<GatewayAccessLogFormat>,
    pub sampling: GatewayPublicAccessLogSamplingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicAccessLogSamplingConfig {
    pub enabled: Option<bool>,
    pub first: Option<u32>,
    pub thereafter: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicShutdownConfig {
    pub grace_period_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicHttpConfig {
    pub timeouts: GatewayPublicHttpTimeoutsConfig,
    pub max_header_bytes: Option<u32>,
    pub http3_enabled: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayPublicHttpTimeoutsConfig {
    pub read_header_seconds: Option<u64>,
    pub read_body_seconds: Option<u64>,
    pub write_seconds: Option<u64>,
    pub idle_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayNodeStatus {
    pub node_id: String,
    pub address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swarmlite_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    pub enabled: bool,
    pub status: GatewayNodeStatusKind,
    pub desired_generation: Option<u64>,
    pub applied_generation: Option<u64>,
    pub retryable: Option<bool>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatewayNodeStatusKind {
    Disabled,
    Offline,
    Pending,
    Updating,
    Ready,
    Error,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentListResponse {
    pub stacks: Vec<StackDeploymentListResponse>,
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
mod tests {
    use super::*;

    #[test]
    fn default_gateway_image_is_pinned_to_the_package_version() {
        assert_eq!(
            DEFAULT_GATEWAY_IMAGE,
            format!(
                "ghcr.io/gfreezy/swarmlite-caddy:v{}",
                env!("CARGO_PKG_VERSION")
            )
        );
    }

    #[test]
    fn refreshes_only_managed_and_legacy_gateway_images() {
        let mut legacy = ClusterGatewayConfig {
            image: LEGACY_DEFAULT_GATEWAY_IMAGE.into(),
            managed_image: false,
            ..Default::default()
        };
        assert!(refresh_managed_gateway_image(&mut legacy));
        assert_eq!(legacy.image, DEFAULT_GATEWAY_IMAGE);
        assert!(legacy.managed_image);

        let mut managed = ClusterGatewayConfig {
            image: "ghcr.io/gfreezy/swarmlite-caddy:v0.0.1".into(),
            managed_image: true,
            ..Default::default()
        };
        assert!(refresh_managed_gateway_image(&mut managed));
        assert_eq!(managed.image, DEFAULT_GATEWAY_IMAGE);

        let mut custom = ClusterGatewayConfig {
            image: "registry.example.com/caddy:latest".into(),
            managed_image: false,
            ..Default::default()
        };
        assert!(!refresh_managed_gateway_image(&mut custom));
        assert_eq!(custom.image, "registry.example.com/caddy:latest");
    }

    #[test]
    fn cluster_settings_without_optional_sections_remain_compatible() {
        let expected = ClusterSettings {
            schema_version: CLUSTER_SCHEMA_VERSION,
            cluster_id: "cluster-a".into(),
            controller_id: "controller-a".into(),
            controller_port: 17_080,
            proxy: ClusterProxyConfig::default(),
            agent: ClusterAgentConfig::default(),
            gateway: ClusterGatewayConfig::default(),
            deployment: DeploymentPolicy::default(),
        };
        let mut value = serde_json::to_value(&expected).unwrap();
        value.as_object_mut().unwrap().remove("proxy");
        value.as_object_mut().unwrap().remove("agent");
        value["gateway"]["cache"] = serde_json::json!({
            "hit_sample_ratio": 32,
            "access_update_interval_seconds": 300
        });
        assert_eq!(
            serde_json::from_value::<ClusterSettings>(value).unwrap(),
            expected
        );
    }

    #[test]
    fn old_gateway_config_without_management_flag_remains_custom() {
        let mut decoded: ClusterGatewayConfig = serde_json::from_str(
            r#"{"listen":[":80",":443"],"image":"registry.example.com/caddy:v1"}"#,
        )
        .unwrap();
        assert!(!decoded.managed_image);
        assert_eq!(decoded.metrics, GatewayMetricsConfig::default());
        assert_eq!(decoded.cache, GatewayCacheConfig::default());
        assert_eq!(decoded.logging, GatewayLoggingConfig::default());
        assert_eq!(decoded.shutdown, GatewayShutdownConfig::default());
        assert_eq!(decoded.http, GatewayHttpConfig::default());
        assert!(!refresh_managed_gateway_image(&mut decoded));
    }

    #[test]
    fn gateway_config_serialization_distinguishes_unset_zero_and_false() {
        let unset = serde_json::to_value(ClusterGatewayConfig::default()).unwrap();
        assert!(unset.get("metrics").is_none());
        assert!(unset.get("cache").is_none());
        assert!(unset.get("http").is_none());

        let mut configured = ClusterGatewayConfig::default();
        configured.metrics.enabled = Some(false);
        configured.cache.max_size_bytes = Some(2_147_483_648);
        configured.http.timeouts.read_header_seconds = Some(0);
        let configured = serde_json::to_value(configured).unwrap();
        assert_eq!(configured["metrics"]["enabled"], false);
        assert_eq!(configured["cache"]["max_size_bytes"], 2_147_483_648_u64);
        assert_eq!(configured["http"]["timeouts"]["read_header_seconds"], 0);
    }

    #[test]
    fn config_update_serializes_unset_as_dotted_paths() {
        let update = ClusterConfigUpdate {
            unset: BTreeSet::from([
                ClusterConfigField::GatewayMetricsEnabled,
                ClusterConfigField::GatewayHttpReadHeaderTimeoutSeconds,
            ]),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(update).unwrap(),
            serde_json::json!({
                "unset": [
                    "gateway.metrics.enabled",
                    "gateway.http.timeouts.read-header-seconds"
                ]
            })
        );
    }

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
