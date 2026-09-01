use super::*;

impl Controller {
    pub(super) async fn image_registry_auth(
        &self,
        registry: &str,
    ) -> swarmlite_registry::RegistryAuth {
        let registry = match registry {
            "index.docker.io" | "registry-1.docker.io" => "docker.io",
            registry => registry,
        };
        self.inner
            .lock()
            .await
            .state
            .registry_credentials
            .get(registry)
            .map(|credential| {
                swarmlite_registry::RegistryAuth::Basic(
                    credential.username.clone(),
                    credential.password.clone(),
                )
            })
            .unwrap_or(swarmlite_registry::RegistryAuth::Anonymous)
    }

    pub(super) async fn set_registry_credential(
        &self,
        request: RegistryLoginRequest,
    ) -> Result<RegistryLoginResponse, ControllerError> {
        let (registry, credential) = crate::registry::validate_login(request)
            .map_err(|error| ControllerError::Invalid(error.to_string()))?;
        let response = RegistryLoginResponse {
            registry: registry.clone(),
            username: credential.username.clone(),
        };

        let mut inner = self.inner.lock().await;
        if inner.state.registry_credentials.get(&registry) == Some(&credential) {
            return Ok(response);
        }
        let previous = inner.state.clone();
        inner
            .state
            .registry_credentials
            .insert(registry, credential);
        if let Err(error) = self.commit_locked(&mut inner).await {
            inner.state = previous;
            return Err(error.into());
        }
        Ok(response)
    }
}
