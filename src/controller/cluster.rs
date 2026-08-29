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
        let ClusterConfigUpdate {
            gateway_image,
            deployment_progress_deadline_seconds,
            image_pull_idle_timeout_seconds,
            image_pull_max_attempts,
            image_pull_initial_backoff_seconds,
            image_pull_max_backoff_seconds,
        } = update;
        if gateway_image.is_none()
            && deployment_progress_deadline_seconds.is_none()
            && image_pull_idle_timeout_seconds.is_none()
            && image_pull_max_attempts.is_none()
            && image_pull_initial_backoff_seconds.is_none()
            && image_pull_max_backoff_seconds.is_none()
        {
            return Err(ControllerError::Invalid(
                "cluster configuration update must contain a key".to_owned(),
            ));
        }
        if gateway_image
            .as_deref()
            .is_some_and(|image| !valid_gateway_image(image))
        {
            return Err(ControllerError::Invalid(
                "gateway-image must be a non-empty OCI image reference without whitespace"
                    .to_owned(),
            ));
        }

        let mut cluster = inner.cluster.clone();
        let previous_cluster = inner.cluster.clone();
        let mut changed = false;

        if let Some(image) = gateway_image
            && (cluster.gateway.image != image || cluster.gateway.managed_image)
        {
            cluster.gateway.image = image;
            cluster.gateway.managed_image = false;
            changed = true;
        }
        if let Some(value) = deployment_progress_deadline_seconds {
            cluster.deployment.progress_deadline_seconds = value;
        }
        if let Some(value) = image_pull_idle_timeout_seconds {
            cluster.deployment.image_pull_idle_timeout_seconds = value;
        }
        if let Some(value) = image_pull_max_attempts {
            cluster.deployment.image_pull_max_attempts = value;
        }
        if let Some(value) = image_pull_initial_backoff_seconds {
            cluster.deployment.image_pull_initial_backoff_seconds = value;
        }
        if let Some(value) = image_pull_max_backoff_seconds {
            cluster.deployment.image_pull_max_backoff_seconds = value;
        }
        if cluster.deployment.progress_deadline_seconds == 0 {
            return Err(ControllerError::Invalid(
                "deployment-progress-deadline-seconds must be greater than zero".into(),
            ));
        }
        if cluster.deployment.image_pull_idle_timeout_seconds == 0 {
            return Err(ControllerError::Invalid(
                "image-pull-idle-timeout-seconds must be greater than zero".into(),
            ));
        }
        if cluster.deployment.image_pull_max_attempts == 0 {
            return Err(ControllerError::Invalid(
                "image-pull-max-attempts must be greater than zero".into(),
            ));
        }
        if cluster.deployment.image_pull_initial_backoff_seconds
            > cluster.deployment.image_pull_max_backoff_seconds
        {
            return Err(ControllerError::Invalid(
                "image-pull-initial-backoff-seconds cannot exceed image-pull-max-backoff-seconds"
                    .into(),
            ));
        }
        changed |= cluster != previous_cluster;

        if changed {
            inner.cluster = cluster.clone();
            if let Err(error) = self.commit_locked(&mut inner).await {
                inner.cluster = previous_cluster;
                return Err(error.into());
            }
            info!("updated cluster configuration");
        }

        Ok(ClusterConfigResponse {
            generation: inner.generation,
            config: cluster,
        })
    }
}
