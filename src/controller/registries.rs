use super::*;

impl Controller {
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
