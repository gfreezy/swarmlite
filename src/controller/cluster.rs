use super::*;

impl Controller {
    pub(super) async fn get_cluster_config(
        &self,
    ) -> Result<ClusterConfigResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/config"));
        }
        Ok(ClusterConfigResponse {
            generation: inner.generation,
            config: self.cluster_settings(&inner)?,
        })
    }

    pub(super) async fn update_cluster_config(
        &self,
        update: ClusterConfigUpdate,
    ) -> Result<ClusterConfigResponse, ControllerError> {
        let mut inner = self.inner.lock().await;
        self.expire_local_lease(&mut inner);
        if !inner.is_leader {
            return Err(self.leader_redirect("/v1/config"));
        }

        let ClusterConfigUpdate {
            mode,
            gateway_image,
        } = update;
        if mode.is_none() && gateway_image.is_none() {
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

        let mut cluster = self.cluster_settings(&inner)?;
        let previous_cluster = inner.cluster.clone();
        let previous_state = inner.state.clone();
        let mut changed = false;

        if let Some(mode) = mode {
            if cluster.mode == ClusterMode::Ha && mode == ClusterMode::Standalone {
                return Err(ControllerError::Conflict(
                    "switching an HA cluster back to standalone is not supported".to_owned(),
                ));
            }
            if cluster.mode != mode {
                cluster.mode = mode;
                if mode == ClusterMode::Ha {
                    fill_automatic_ha_controllers(&mut inner.state);
                }
                changed = true;
            }
        }

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
                inner.state = previous_state;
                return Err(error.into());
            }
            info!(mode = ?cluster.mode, gateway_image = %cluster.gateway.image, "updated cluster configuration");
        }

        Ok(ClusterConfigResponse {
            generation: inner.generation,
            config: cluster,
        })
    }
}
