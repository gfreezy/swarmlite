use super::*;

impl Controller {
    pub(super) async fn get_cluster_config(
        &self,
    ) -> Result<ClusterConfigResponse, ControllerError> {
        let inner = self.inner.lock().await;
        Ok(ClusterConfigResponse {
            generation: inner.generation,
            config: inner.cluster.clone(),
        })
    }

    pub(super) async fn update_cluster_config(
        &self,
        update: ClusterConfigUpdate,
    ) -> Result<ClusterConfigResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        if update == ClusterConfigUpdate::default() {
            return Err(ControllerError::Invalid(
                "cluster configuration update must contain a key".to_owned(),
            ));
        }
        let conflicts_with_unset = [
            (
                update.agent_image_prune_enabled.is_some(),
                ClusterConfigField::AgentImagePruneEnabled,
            ),
            (
                update.agent_image_prune_interval_seconds.is_some(),
                ClusterConfigField::AgentImagePruneIntervalSeconds,
            ),
            (update.proxy_http.is_some(), ClusterConfigField::ProxyHttp),
            (update.proxy_https.is_some(), ClusterConfigField::ProxyHttps),
            (update.proxy_all.is_some(), ClusterConfigField::ProxyAll),
            (
                update.proxy_no_proxy.is_some(),
                ClusterConfigField::ProxyNoProxy,
            ),
            (
                update.gateway_image.is_some(),
                ClusterConfigField::GatewayImage,
            ),
            (
                update.gateway_listen.is_some(),
                ClusterConfigField::GatewayListen,
            ),
            (
                update.gateway_metrics_enabled.is_some(),
                ClusterConfigField::GatewayMetricsEnabled,
            ),
            (
                update.gateway_metrics_per_host.is_some(),
                ClusterConfigField::GatewayMetricsPerHost,
            ),
            (
                update.gateway_cache_max_size_bytes.is_some(),
                ClusterConfigField::GatewayCacheMaxSizeBytes,
            ),
            (
                update.gateway_cache_low_water_percent.is_some(),
                ClusterConfigField::GatewayCacheLowWaterPercent,
            ),
            (
                update.gateway_cache_admission_window_seconds.is_some(),
                ClusterConfigField::GatewayCacheAdmissionWindowSeconds,
            ),
            (
                update
                    .gateway_cache_admission_cache_after_requests
                    .is_some(),
                ClusterConfigField::GatewayCacheAdmissionCacheAfterRequests,
            ),
            (
                update.gateway_cache_sqlite_touch_window_seconds.is_some(),
                ClusterConfigField::GatewayCacheSqliteTouchWindowSeconds,
            ),
            (
                update.gateway_cache_sqlite_cache_size_kib.is_some(),
                ClusterConfigField::GatewayCacheSqliteCacheSizeKib,
            ),
            (
                update.gateway_cache_sqlite_mmap_size_bytes.is_some(),
                ClusterConfigField::GatewayCacheSqliteMmapSizeBytes,
            ),
            (
                update.gateway_cache_sqlite_read_connections.is_some(),
                ClusterConfigField::GatewayCacheSqliteReadConnections,
            ),
            (
                update.gateway_cache_sqlite_busy_timeout_seconds.is_some(),
                ClusterConfigField::GatewayCacheSqliteBusyTimeoutSeconds,
            ),
            (
                update
                    .gateway_cache_sqlite_cleanup_interval_seconds
                    .is_some(),
                ClusterConfigField::GatewayCacheSqliteCleanupIntervalSeconds,
            ),
            (
                update
                    .gateway_cache_sqlite_journal_size_limit_bytes
                    .is_some(),
                ClusterConfigField::GatewayCacheSqliteJournalSizeLimitBytes,
            ),
            (
                update.gateway_logging_runtime_level.is_some(),
                ClusterConfigField::GatewayLoggingRuntimeLevel,
            ),
            (
                update.gateway_logging_access_enabled.is_some(),
                ClusterConfigField::GatewayLoggingAccessEnabled,
            ),
            (
                update.gateway_logging_access_format.is_some(),
                ClusterConfigField::GatewayLoggingAccessFormat,
            ),
            (
                update.gateway_logging_access_sampling_enabled.is_some(),
                ClusterConfigField::GatewayLoggingAccessSamplingEnabled,
            ),
            (
                update.gateway_logging_access_sampling_first.is_some(),
                ClusterConfigField::GatewayLoggingAccessSamplingFirst,
            ),
            (
                update.gateway_logging_access_sampling_thereafter.is_some(),
                ClusterConfigField::GatewayLoggingAccessSamplingThereafter,
            ),
            (
                update.gateway_shutdown_grace_period_seconds.is_some(),
                ClusterConfigField::GatewayShutdownGracePeriodSeconds,
            ),
            (
                update.gateway_http_read_header_timeout_seconds.is_some(),
                ClusterConfigField::GatewayHttpReadHeaderTimeoutSeconds,
            ),
            (
                update.gateway_http_read_body_timeout_seconds.is_some(),
                ClusterConfigField::GatewayHttpReadBodyTimeoutSeconds,
            ),
            (
                update.gateway_http_write_timeout_seconds.is_some(),
                ClusterConfigField::GatewayHttpWriteTimeoutSeconds,
            ),
            (
                update.gateway_http_idle_timeout_seconds.is_some(),
                ClusterConfigField::GatewayHttpIdleTimeoutSeconds,
            ),
            (
                update.gateway_http_max_header_bytes.is_some(),
                ClusterConfigField::GatewayHttpMaxHeaderBytes,
            ),
            (
                update.gateway_http_http3_enabled.is_some(),
                ClusterConfigField::GatewayHttpHttp3Enabled,
            ),
            (
                update.deployment_progress_deadline_seconds.is_some(),
                ClusterConfigField::DeploymentProgressDeadlineSeconds,
            ),
            (
                update.image_pull_idle_timeout_seconds.is_some(),
                ClusterConfigField::DeploymentImagePullIdleTimeoutSeconds,
            ),
            (
                update.image_pull_max_attempts.is_some(),
                ClusterConfigField::DeploymentImagePullMaxAttempts,
            ),
            (
                update.image_pull_initial_backoff_seconds.is_some(),
                ClusterConfigField::DeploymentImagePullInitialBackoffSeconds,
            ),
            (
                update.image_pull_max_backoff_seconds.is_some(),
                ClusterConfigField::DeploymentImagePullMaxBackoffSeconds,
            ),
        ]
        .into_iter()
        .any(|(is_set, field)| is_set && update.unset.contains(&field));
        if conflicts_with_unset {
            return Err(ControllerError::Invalid(
                "cluster configuration update cannot set and unset the same key".to_owned(),
            ));
        }
        if update
            .gateway_image
            .as_deref()
            .is_some_and(|image| !valid_gateway_image(image))
        {
            return Err(ControllerError::Invalid(
                "gateway.image must be a non-empty OCI image reference without whitespace"
                    .to_owned(),
            ));
        }

        let mut cluster = inner.cluster.clone();
        let previous_cluster = inner.cluster.clone();
        let agent_defaults = crate::model::ClusterAgentConfig::default();
        let gateway_defaults = crate::model::ClusterGatewayConfig::default();
        let deployment_defaults = crate::model::DeploymentPolicy::default();

        for field in &update.unset {
            match field {
                ClusterConfigField::AgentImagePruneEnabled => {
                    cluster.agent.image_prune.enabled = agent_defaults.image_prune.enabled
                }
                ClusterConfigField::AgentImagePruneIntervalSeconds => {
                    cluster.agent.image_prune.interval_seconds =
                        agent_defaults.image_prune.interval_seconds
                }
                ClusterConfigField::ProxyHttp => cluster.proxy.http = None,
                ClusterConfigField::ProxyHttps => cluster.proxy.https = None,
                ClusterConfigField::ProxyAll => cluster.proxy.all = None,
                ClusterConfigField::ProxyNoProxy => cluster.proxy.no_proxy = None,
                ClusterConfigField::GatewayImage => {
                    cluster.gateway.image.clone_from(&gateway_defaults.image);
                    cluster.gateway.managed_image = true;
                }
                ClusterConfigField::GatewayListen => {
                    cluster.gateway.listen.clone_from(&gateway_defaults.listen)
                }
                ClusterConfigField::GatewayMetricsEnabled => cluster.gateway.metrics.enabled = None,
                ClusterConfigField::GatewayMetricsPerHost => {
                    cluster.gateway.metrics.per_host = None
                }
                ClusterConfigField::GatewayCacheMaxSizeBytes => {
                    cluster.gateway.cache.max_size_bytes = None
                }
                ClusterConfigField::GatewayCacheLowWaterPercent => {
                    cluster.gateway.cache.low_water_percent = None
                }
                ClusterConfigField::GatewayCacheAdmissionWindowSeconds => {
                    cluster.gateway.cache.admission.window_seconds = None
                }
                ClusterConfigField::GatewayCacheAdmissionCacheAfterRequests => {
                    cluster.gateway.cache.admission.cache_after_requests = None
                }
                ClusterConfigField::GatewayCacheSqliteTouchWindowSeconds => {
                    cluster.gateway.cache.sqlite.touch_window_seconds = None
                }
                ClusterConfigField::GatewayCacheSqliteCacheSizeKib => {
                    cluster.gateway.cache.sqlite.cache_size_kib = None
                }
                ClusterConfigField::GatewayCacheSqliteMmapSizeBytes => {
                    cluster.gateway.cache.sqlite.mmap_size_bytes = None
                }
                ClusterConfigField::GatewayCacheSqliteReadConnections => {
                    cluster.gateway.cache.sqlite.read_connections = None
                }
                ClusterConfigField::GatewayCacheSqliteBusyTimeoutSeconds => {
                    cluster.gateway.cache.sqlite.busy_timeout_seconds = None
                }
                ClusterConfigField::GatewayCacheSqliteCleanupIntervalSeconds => {
                    cluster.gateway.cache.sqlite.cleanup_interval_seconds = None
                }
                ClusterConfigField::GatewayCacheSqliteJournalSizeLimitBytes => {
                    cluster.gateway.cache.sqlite.journal_size_limit_bytes = None
                }
                ClusterConfigField::GatewayLoggingRuntimeLevel => {
                    cluster.gateway.logging.runtime.level = None
                }
                ClusterConfigField::GatewayLoggingAccessEnabled => {
                    cluster.gateway.logging.access.enabled = None
                }
                ClusterConfigField::GatewayLoggingAccessFormat => {
                    cluster.gateway.logging.access.format = None
                }
                ClusterConfigField::GatewayLoggingAccessSamplingEnabled => {
                    cluster.gateway.logging.access.sampling.enabled = None
                }
                ClusterConfigField::GatewayLoggingAccessSamplingFirst => {
                    cluster.gateway.logging.access.sampling.first = None
                }
                ClusterConfigField::GatewayLoggingAccessSamplingThereafter => {
                    cluster.gateway.logging.access.sampling.thereafter = None
                }
                ClusterConfigField::GatewayShutdownGracePeriodSeconds => {
                    cluster.gateway.shutdown.grace_period_seconds = None
                }
                ClusterConfigField::GatewayHttpReadHeaderTimeoutSeconds => {
                    cluster.gateway.http.timeouts.read_header_seconds = None
                }
                ClusterConfigField::GatewayHttpReadBodyTimeoutSeconds => {
                    cluster.gateway.http.timeouts.read_body_seconds = None
                }
                ClusterConfigField::GatewayHttpWriteTimeoutSeconds => {
                    cluster.gateway.http.timeouts.write_seconds = None
                }
                ClusterConfigField::GatewayHttpIdleTimeoutSeconds => {
                    cluster.gateway.http.timeouts.idle_seconds = None
                }
                ClusterConfigField::GatewayHttpMaxHeaderBytes => {
                    cluster.gateway.http.max_header_bytes = None
                }
                ClusterConfigField::GatewayHttpHttp3Enabled => {
                    cluster.gateway.http.http3_enabled = None
                }
                ClusterConfigField::DeploymentProgressDeadlineSeconds => {
                    cluster.deployment.progress_deadline_seconds =
                        deployment_defaults.progress_deadline_seconds
                }
                ClusterConfigField::DeploymentImagePullIdleTimeoutSeconds => {
                    cluster.deployment.image_pull_idle_timeout_seconds =
                        deployment_defaults.image_pull_idle_timeout_seconds
                }
                ClusterConfigField::DeploymentImagePullMaxAttempts => {
                    cluster.deployment.image_pull_max_attempts =
                        deployment_defaults.image_pull_max_attempts
                }
                ClusterConfigField::DeploymentImagePullInitialBackoffSeconds => {
                    cluster.deployment.image_pull_initial_backoff_seconds =
                        deployment_defaults.image_pull_initial_backoff_seconds
                }
                ClusterConfigField::DeploymentImagePullMaxBackoffSeconds => {
                    cluster.deployment.image_pull_max_backoff_seconds =
                        deployment_defaults.image_pull_max_backoff_seconds
                }
            }
        }

        if let Some(value) = update.agent_image_prune_enabled {
            cluster.agent.image_prune.enabled = value;
        }
        if let Some(value) = update.agent_image_prune_interval_seconds {
            cluster.agent.image_prune.interval_seconds = value;
        }
        if let Some(value) = update.proxy_http {
            cluster.proxy.http = Some(value);
        }
        if let Some(value) = update.proxy_https {
            cluster.proxy.https = Some(value);
        }
        if let Some(value) = update.proxy_all {
            cluster.proxy.all = Some(value);
        }
        if let Some(value) = update.proxy_no_proxy {
            cluster.proxy.no_proxy = Some(value);
        }
        if let Some(image) = update.gateway_image {
            cluster.gateway.image = image;
            cluster.gateway.managed_image = false;
        }
        if let Some(value) = update.gateway_listen {
            cluster.gateway.listen = value;
        }
        if let Some(value) = update.gateway_metrics_enabled {
            cluster.gateway.metrics.enabled = Some(value);
        }
        if let Some(value) = update.gateway_metrics_per_host {
            cluster.gateway.metrics.per_host = Some(value);
        }
        if let Some(value) = update.gateway_cache_max_size_bytes {
            cluster.gateway.cache.max_size_bytes = Some(value);
        }
        if let Some(value) = update.gateway_cache_low_water_percent {
            cluster.gateway.cache.low_water_percent = Some(value);
        }
        if let Some(value) = update.gateway_cache_admission_window_seconds {
            cluster.gateway.cache.admission.window_seconds = Some(value);
        }
        if let Some(value) = update.gateway_cache_admission_cache_after_requests {
            cluster.gateway.cache.admission.cache_after_requests = Some(value);
        }
        if let Some(value) = update.gateway_cache_sqlite_touch_window_seconds {
            cluster.gateway.cache.sqlite.touch_window_seconds = Some(value);
        }
        if let Some(value) = update.gateway_cache_sqlite_cache_size_kib {
            cluster.gateway.cache.sqlite.cache_size_kib = Some(value);
        }
        if let Some(value) = update.gateway_cache_sqlite_mmap_size_bytes {
            cluster.gateway.cache.sqlite.mmap_size_bytes = Some(value);
        }
        if let Some(value) = update.gateway_cache_sqlite_read_connections {
            cluster.gateway.cache.sqlite.read_connections = Some(value);
        }
        if let Some(value) = update.gateway_cache_sqlite_busy_timeout_seconds {
            cluster.gateway.cache.sqlite.busy_timeout_seconds = Some(value);
        }
        if let Some(value) = update.gateway_cache_sqlite_cleanup_interval_seconds {
            cluster.gateway.cache.sqlite.cleanup_interval_seconds = Some(value);
        }
        if let Some(value) = update.gateway_cache_sqlite_journal_size_limit_bytes {
            cluster.gateway.cache.sqlite.journal_size_limit_bytes = Some(value);
        }
        if let Some(value) = update.gateway_logging_runtime_level {
            cluster.gateway.logging.runtime.level = Some(value);
        }
        if let Some(value) = update.gateway_logging_access_enabled {
            cluster.gateway.logging.access.enabled = Some(value);
        }
        if let Some(value) = update.gateway_logging_access_format {
            cluster.gateway.logging.access.format = Some(value);
        }
        if let Some(value) = update.gateway_logging_access_sampling_enabled {
            cluster.gateway.logging.access.sampling.enabled = Some(value);
        }
        if let Some(value) = update.gateway_logging_access_sampling_first {
            cluster.gateway.logging.access.sampling.first = Some(value);
        }
        if let Some(value) = update.gateway_logging_access_sampling_thereafter {
            cluster.gateway.logging.access.sampling.thereafter = Some(value);
        }
        if let Some(value) = update.gateway_shutdown_grace_period_seconds {
            cluster.gateway.shutdown.grace_period_seconds = Some(value);
        }
        if let Some(value) = update.gateway_http_read_header_timeout_seconds {
            cluster.gateway.http.timeouts.read_header_seconds = Some(value);
        }
        if let Some(value) = update.gateway_http_read_body_timeout_seconds {
            cluster.gateway.http.timeouts.read_body_seconds = Some(value);
        }
        if let Some(value) = update.gateway_http_write_timeout_seconds {
            cluster.gateway.http.timeouts.write_seconds = Some(value);
        }
        if let Some(value) = update.gateway_http_idle_timeout_seconds {
            cluster.gateway.http.timeouts.idle_seconds = Some(value);
        }
        if let Some(value) = update.gateway_http_max_header_bytes {
            cluster.gateway.http.max_header_bytes = Some(value);
        }
        if let Some(value) = update.gateway_http_http3_enabled {
            cluster.gateway.http.http3_enabled = Some(value);
        }
        if let Some(value) = update.deployment_progress_deadline_seconds {
            cluster.deployment.progress_deadline_seconds = value;
        }
        if let Some(value) = update.image_pull_idle_timeout_seconds {
            cluster.deployment.image_pull_idle_timeout_seconds = value;
        }
        if let Some(value) = update.image_pull_max_attempts {
            cluster.deployment.image_pull_max_attempts = value;
        }
        if let Some(value) = update.image_pull_initial_backoff_seconds {
            cluster.deployment.image_pull_initial_backoff_seconds = value;
        }
        if let Some(value) = update.image_pull_max_backoff_seconds {
            cluster.deployment.image_pull_max_backoff_seconds = value;
        }
        if cluster.gateway.listen.is_empty()
            || cluster
                .gateway
                .listen
                .iter()
                .any(|listen| listen.is_empty() || listen.trim() != listen)
        {
            return Err(ControllerError::Invalid(
                "gateway.listen must contain at least one non-empty address without surrounding whitespace"
                    .into(),
			));
        }
        if cluster
            .gateway
            .cache
            .max_size_bytes
            .is_some_and(|value| value == 0 || value > crate::model::MAX_GATEWAY_CACHE_SIGNED_SIZE)
        {
            return Err(ControllerError::Invalid(format!(
                "gateway.cache.max-size-bytes must be between 1 and {}",
                crate::model::MAX_GATEWAY_CACHE_SIGNED_SIZE
            )));
        }
        if cluster
            .gateway
            .cache
            .low_water_percent
            .is_some_and(|value| !(1..100).contains(&value))
        {
            return Err(ControllerError::Invalid(
                "gateway.cache.low-water-percent must be between 1 and 99".into(),
            ));
        }
        if cluster
            .gateway
            .cache
            .admission
            .cache_after_requests
            .is_some_and(|value| {
                value == 0 || value > crate::model::MAX_GATEWAY_CACHE_AFTER_REQUESTS
            })
        {
            return Err(ControllerError::Invalid(format!(
                "gateway.cache.admission.cache-after-requests must be between 1 and {}",
                crate::model::MAX_GATEWAY_CACHE_AFTER_REQUESTS
            )));
        }
        if cluster
            .gateway
            .cache
            .sqlite
            .cache_size_kib
            .is_some_and(|value| value > crate::model::MAX_GATEWAY_CACHE_SIGNED_SIZE)
        {
            return Err(ControllerError::Invalid(format!(
                "gateway.cache.sqlite.cache-size-kib cannot exceed {}",
                crate::model::MAX_GATEWAY_CACHE_SIGNED_SIZE
            )));
        }
        if cluster
            .gateway
            .cache
            .sqlite
            .mmap_size_bytes
            .is_some_and(|value| value > crate::model::MAX_GATEWAY_CACHE_SIGNED_SIZE)
        {
            return Err(ControllerError::Invalid(format!(
                "gateway.cache.sqlite.mmap-size-bytes cannot exceed {}",
                crate::model::MAX_GATEWAY_CACHE_SIGNED_SIZE
            )));
        }
        if cluster
            .gateway
            .cache
            .sqlite
            .read_connections
            .is_some_and(|value| {
                value == 0 || value > crate::model::MAX_GATEWAY_CACHE_SQLITE_READ_CONNECTIONS
            })
        {
            return Err(ControllerError::Invalid(format!(
                "gateway.cache.sqlite.read-connections must be between 1 and {}",
                crate::model::MAX_GATEWAY_CACHE_SQLITE_READ_CONNECTIONS
            )));
        }
        if cluster.gateway.cache.sqlite.busy_timeout_seconds == Some(0) {
            return Err(ControllerError::Invalid(
                "gateway.cache.sqlite.busy-timeout-seconds must be greater than zero".into(),
            ));
        }
        if cluster.gateway.cache.sqlite.cleanup_interval_seconds == Some(0) {
            return Err(ControllerError::Invalid(
                "gateway.cache.sqlite.cleanup-interval-seconds must be greater than zero".into(),
            ));
        }
        if cluster
            .gateway
            .cache
            .sqlite
            .journal_size_limit_bytes
            .is_some_and(|value| value == 0 || value > crate::model::MAX_GATEWAY_CACHE_SIGNED_SIZE)
        {
            return Err(ControllerError::Invalid(format!(
                "gateway.cache.sqlite.journal-size-limit-bytes must be between 1 and {}",
                crate::model::MAX_GATEWAY_CACHE_SIGNED_SIZE
            )));
        }
        let caddy_durations = [
            cluster.gateway.shutdown.grace_period_seconds,
            cluster.gateway.cache.admission.window_seconds,
            cluster.gateway.cache.sqlite.touch_window_seconds,
            cluster.gateway.cache.sqlite.busy_timeout_seconds,
            cluster.gateway.cache.sqlite.cleanup_interval_seconds,
            cluster.gateway.http.timeouts.read_header_seconds,
            cluster.gateway.http.timeouts.read_body_seconds,
            cluster.gateway.http.timeouts.write_seconds,
            cluster.gateway.http.timeouts.idle_seconds,
        ];
        if caddy_durations
            .into_iter()
            .flatten()
            .any(|seconds| seconds > MAX_CADDY_DURATION_SECONDS)
        {
            return Err(ControllerError::Invalid(format!(
                "Gateway Caddy durations cannot exceed {MAX_CADDY_DURATION_SECONDS} seconds"
            )));
        }
        if cluster.gateway.cache.admission.window_seconds == Some(0) {
            return Err(ControllerError::Invalid(
                "gateway.cache.admission.window-seconds must be greater than zero".into(),
            ));
        }
        if cluster.gateway.cache.sqlite.touch_window_seconds == Some(0) {
            return Err(ControllerError::Invalid(
                "gateway.cache.sqlite.touch-window-seconds must be greater than zero".into(),
            ));
        }
        if cluster.agent.image_prune.interval_seconds == 0 {
            return Err(ControllerError::Invalid(
                "agent.image-prune.interval-seconds must be greater than zero".into(),
            ));
        }
        if cluster.deployment.progress_deadline_seconds == 0 {
            return Err(ControllerError::Invalid(
                "deployment.progress-deadline-seconds must be greater than zero".into(),
            ));
        }
        if cluster.deployment.image_pull_idle_timeout_seconds == 0 {
            return Err(ControllerError::Invalid(
                "deployment.image-pull.idle-timeout-seconds must be greater than zero".into(),
            ));
        }
        if cluster.deployment.image_pull_max_attempts == 0 {
            return Err(ControllerError::Invalid(
                "deployment.image-pull.max-attempts must be greater than zero".into(),
            ));
        }
        if cluster.deployment.image_pull_initial_backoff_seconds
            > cluster.deployment.image_pull_max_backoff_seconds
        {
            return Err(ControllerError::Invalid(
                "deployment.image-pull.initial-backoff-seconds cannot exceed deployment.image-pull.max-backoff-seconds".into(),
            ));
        }
        let proxy = image_proxy_config(&cluster.proxy).map_err(|error| {
            ControllerError::Invalid(format!("invalid proxy configuration: {error:#}"))
        })?;
        let proxy_changed = cluster.proxy != previous_cluster.proxy;
        let changed = cluster != previous_cluster;

        if changed {
            inner.cluster = cluster.clone();
            if let Err(error) = self.commit_locked(&mut inner).await {
                inner.cluster = previous_cluster;
                return Err(error.into());
            }
            if proxy_changed {
                self.image_registry.set_proxy(proxy);
            }
            info!("updated cluster configuration");
        }

        Ok(ClusterConfigResponse {
            generation: inner.generation,
            config: cluster,
        })
    }
}
