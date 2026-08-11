use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use bollard::auth::DockerCredentials;
use sha2::{Digest, Sha256};
use url::Host;

use crate::{
    local_state::LocalState,
    model::{RegistryCredential, RegistryLoginRequest},
};

const REGISTRY_CREDENTIALS_KEY: &str = "registry_credentials";
const DOCKER_HUB_REGISTRY: &str = "docker.io";
const MAX_USERNAME_BYTES: usize = 512;
pub const MAX_PASSWORD_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub(crate) struct RegistryCredentialStore {
    local_state: LocalState,
}

impl RegistryCredentialStore {
    pub(crate) fn new(local_state: LocalState) -> Self {
        Self { local_state }
    }

    pub(crate) fn credentials_for_image(&self, image: &str) -> Result<Option<DockerCredentials>> {
        let registry = registry_from_image(image)?;
        let credentials = self.snapshot()?;
        Ok(credentials
            .get(&registry)
            .map(|credential| DockerCredentials {
                username: Some(credential.username.clone()),
                password: Some(credential.password.clone()),
                serveraddress: Some(docker_server_address(&registry)),
                ..Default::default()
            }))
    }

    pub(crate) fn replace(&self, credentials: &BTreeMap<String, RegistryCredential>) -> Result<()> {
        if self.snapshot()? != *credentials {
            self.local_state
                .put(REGISTRY_CREDENTIALS_KEY, credentials)?;
        }
        Ok(())
    }

    pub(crate) fn snapshot(&self) -> Result<BTreeMap<String, RegistryCredential>> {
        Ok(self
            .local_state
            .get(REGISTRY_CREDENTIALS_KEY)?
            .unwrap_or_default())
    }
}

pub(crate) fn validate_login(
    request: RegistryLoginRequest,
) -> Result<(String, RegistryCredential)> {
    let registry = normalize_registry(&request.registry)?;
    validate_username(&request.username)?;
    validate_password(&request.password)?;
    Ok((
        registry,
        RegistryCredential {
            username: request.username,
            password: request.password,
        },
    ))
}

pub(crate) fn credentials_hash(credentials: &BTreeMap<String, RegistryCredential>) -> String {
    let encoded =
        serde_json::to_vec(credentials).expect("registry credential serialization cannot fail");
    let digest = Sha256::digest(encoded);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn validate_username(username: &str) -> Result<()> {
    if username.is_empty()
        || username.len() > MAX_USERNAME_BYTES
        || username.trim() != username
        || username.chars().any(char::is_control)
    {
        bail!(
            "registry username must contain 1 to {MAX_USERNAME_BYTES} bytes without control characters or surrounding whitespace"
        );
    }
    Ok(())
}

fn validate_password(password: &str) -> Result<()> {
    if password.is_empty() {
        bail!("registry password read from stdin must not be empty");
    }
    if password.len() > MAX_PASSWORD_BYTES {
        bail!("registry password must contain at most {MAX_PASSWORD_BYTES} bytes");
    }
    if password.contains('\0') {
        bail!("registry password must not contain a NUL character");
    }
    Ok(())
}

pub(crate) fn normalize_registry(registry: &str) -> Result<String> {
    if registry.is_empty()
        || registry.trim() != registry
        || registry.chars().any(char::is_whitespace)
        || registry.contains("//")
        || registry.contains('/')
    {
        bail!("registry must be a hostname with an optional port, such as ghcr.io");
    }

    let url = url::Url::parse(&format!("https://{registry}"))
        .with_context(|| format!("invalid registry hostname {registry:?}"))?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        bail!("registry must be a hostname with an optional port, such as ghcr.io");
    }
    let host = match url.host().context("registry hostname is missing")? {
        Host::Domain(domain) => domain.to_ascii_lowercase(),
        Host::Ipv4(address) => address.to_string(),
        Host::Ipv6(address) => format!("[{address}]"),
    };
    let registry = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    };
    Ok(match registry.as_str() {
        "index.docker.io" | "registry-1.docker.io" => DOCKER_HUB_REGISTRY.to_owned(),
        _ => registry,
    })
}

fn registry_from_image(image: &str) -> Result<String> {
    let Some((first, _)) = image.split_once('/') else {
        return Ok(DOCKER_HUB_REGISTRY.to_owned());
    };
    if first.contains('.') || first.contains(':') || first.eq_ignore_ascii_case("localhost") {
        return normalize_registry(first)
            .with_context(|| format!("invalid registry in image reference {image:?}"));
    }
    Ok(DOCKER_HUB_REGISTRY.to_owned())
}

fn docker_server_address(registry: &str) -> String {
    if registry == DOCKER_HUB_REGISTRY {
        "https://index.docker.io/v1/".to_owned()
    } else {
        registry.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_registry_hosts_and_resolves_image_registries() {
        assert_eq!(normalize_registry("GHCR.IO").unwrap(), "ghcr.io");
        assert_eq!(
            normalize_registry("registry.example.com:5000").unwrap(),
            "registry.example.com:5000"
        );
        assert_eq!(normalize_registry("index.docker.io").unwrap(), "docker.io");
        assert!(normalize_registry("https://ghcr.io").is_err());
        assert!(normalize_registry("ghcr.io/example").is_err());

        assert_eq!(registry_from_image("ubuntu:latest").unwrap(), "docker.io");
        assert_eq!(
            registry_from_image("library/ubuntu:latest").unwrap(),
            "docker.io"
        );
        assert_eq!(
            registry_from_image("ghcr.io/example/app:v1").unwrap(),
            "ghcr.io"
        );
        assert_eq!(
            registry_from_image("localhost:5000/example/app:v1").unwrap(),
            "localhost:5000"
        );
    }

    #[test]
    fn stores_credentials_per_registry_and_observes_updates() {
        let directory = tempfile::tempdir().unwrap();
        let local_state = LocalState::open(directory.path()).unwrap();
        let store = RegistryCredentialStore::new(local_state);

        let credentials = BTreeMap::from([(
            "ghcr.io".to_owned(),
            RegistryCredential {
                username: "octocat".to_owned(),
                password: "token-one".to_owned(),
            },
        )]);
        store.replace(&credentials).unwrap();
        let credential = store
            .credentials_for_image("ghcr.io/example/private:v1")
            .unwrap()
            .unwrap();
        assert_eq!(credential.username.as_deref(), Some("octocat"));
        assert_eq!(credential.password.as_deref(), Some("token-one"));
        assert_eq!(credential.serveraddress.as_deref(), Some("ghcr.io"));

        let credentials = BTreeMap::from([(
            "ghcr.io".to_owned(),
            RegistryCredential {
                username: "octocat".to_owned(),
                password: "token-two".to_owned(),
            },
        )]);
        store.replace(&credentials).unwrap();
        let credential = store
            .credentials_for_image("ghcr.io/example/private:v2")
            .unwrap()
            .unwrap();
        assert_eq!(credential.password.as_deref(), Some("token-two"));
        assert!(
            store
                .credentials_for_image("docker.io/library/alpine:latest")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_invalid_credentials() {
        let request = |username: &str, password: &str| RegistryLoginRequest {
            registry: "ghcr.io".to_owned(),
            username: username.to_owned(),
            password: password.to_owned(),
        };
        assert!(validate_login(request("", "token")).is_err());
        assert!(validate_login(request("octocat", "")).is_err());
        assert!(validate_login(request("octocat", "bad\0token")).is_err());
    }

    #[test]
    fn hashes_credentials_without_exposing_them() {
        let first = BTreeMap::from([(
            "ghcr.io".to_owned(),
            RegistryCredential {
                username: "octocat".to_owned(),
                password: "token-one".to_owned(),
            },
        )]);
        let mut second = first.clone();
        second.get_mut("ghcr.io").unwrap().password = "token-two".to_owned();
        assert_eq!(credentials_hash(&first).len(), 64);
        assert_ne!(credentials_hash(&first), credentials_hash(&second));
    }
}
