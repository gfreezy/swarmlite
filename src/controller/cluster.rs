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
        let ClusterConfigUpdate { gateway_image } = update;
        if gateway_image.is_none() {
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
            && cluster.gateway.image != image
        {
            cluster.gateway.image = image;
            changed = true;
        }

        if changed {
            inner.cluster = cluster.clone();
            if let Err(error) = self.commit_locked(&mut inner).await {
                inner.cluster = previous_cluster;
                return Err(error.into());
            }
            info!(gateway_image = %cluster.gateway.image, "updated cluster configuration");
        }

        Ok(ClusterConfigResponse {
            generation: inner.generation,
            config: cluster,
        })
    }
}
