use std::{fmt, str::FromStr};

use anyhow::{Context, Result, bail};

const DOCKER_HUB: &str = "docker.io";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageReference {
    registry: String,
    repository: String,
    reference: String,
    digest: bool,
}

impl ImageReference {
    pub fn parse(value: &str) -> Result<Self> {
        let value = value.trim();
        if value.is_empty() || value.chars().any(char::is_whitespace) {
            bail!("invalid empty or whitespace-containing image reference");
        }
        let (name, reference, digest) = if let Some((name, digest)) = value.rsplit_once('@') {
            if !digest.contains(':') {
                bail!("image digest must include an algorithm");
            }
            (name, digest.to_owned(), true)
        } else {
            let last_slash = value.rfind('/');
            let last_colon = value.rfind(':');
            if let Some(colon) =
                last_colon.filter(|colon| last_slash.is_none_or(|slash| *colon > slash))
            {
                (&value[..colon], value[colon + 1..].to_owned(), false)
            } else {
                (value, "latest".to_owned(), false)
            }
        };
        if name.is_empty() || reference.is_empty() {
            bail!("invalid image reference {value:?}");
        }
        let mut components = name.split('/');
        let first = components.next().expect("name is non-empty");
        let explicit_registry = first == "localhost" || first.contains('.') || first.contains(':');
        let (registry, mut repository) = if explicit_registry {
            let rest = components.collect::<Vec<_>>().join("/");
            if rest.is_empty() {
                bail!("image reference with registry must include a repository");
            }
            (normalize_registry(first), rest)
        } else {
            (DOCKER_HUB.to_owned(), name.to_owned())
        };
        if registry == DOCKER_HUB && !repository.contains('/') {
            repository = format!("library/{repository}");
        }
        validate_repository(&repository)?;
        Ok(Self {
            registry,
            repository,
            reference,
            digest,
        })
    }

    pub fn registry(&self) -> &str {
        &self.registry
    }
    pub fn repository(&self) -> &str {
        &self.repository
    }
    pub fn reference(&self) -> &str {
        &self.reference
    }
    pub fn is_digest(&self) -> bool {
        self.digest
    }

    pub fn relay_reference(&self, relay: &str) -> String {
        let separator = if self.digest { '@' } else { ':' };
        format!(
            "{relay}/f/{}/{}{}{}",
            self.registry, self.repository, separator, self.reference
        )
    }

    pub fn relay_manifest_path(&self) -> String {
        format!(
            "/v2/f/{}/{}/manifests/{}",
            self.registry, self.repository, self.reference
        )
    }

    pub fn tag_parts(&self) -> Option<(String, String)> {
        (!self.digest).then(|| {
            let repository = if self.registry == DOCKER_HUB {
                self.repository.clone()
            } else {
                format!("{}/{}", self.registry, self.repository)
            };
            (repository, self.reference.clone())
        })
    }
}

impl fmt::Display for ImageReference {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let separator = if self.digest { '@' } else { ':' };
        if self.registry == DOCKER_HUB {
            write!(
                formatter,
                "{}{}{}",
                self.repository, separator, self.reference
            )
        } else {
            write!(
                formatter,
                "{}/{}{}{}",
                self.registry, self.repository, separator, self.reference
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryResource {
    Manifest(String),
    Blob(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryRequest {
    pub registry: String,
    pub repository: String,
    pub resource: RegistryResource,
}

impl RegistryRequest {
    pub fn parse(path: &str) -> Result<Self> {
        let path = path.trim_start_matches('/');
        let forwarded = path
            .strip_prefix("f/")
            .context("registry path must start with f/")?;
        let (prefix, resource) =
            if let Some((prefix, reference)) = forwarded.rsplit_once("/manifests/") {
                (prefix, RegistryResource::Manifest(reference.to_owned()))
            } else if let Some((prefix, digest)) = forwarded.rsplit_once("/blobs/") {
                (prefix, RegistryResource::Blob(digest.to_owned()))
            } else {
                bail!("unsupported registry path");
            };
        let (registry, repository) = prefix
            .split_once('/')
            .context("registry path must include a repository")?;
        if registry.is_empty() || repository.is_empty() || registry.contains("..") {
            bail!("invalid registry path");
        }
        validate_repository(repository)?;
        Ok(Self {
            registry: normalize_registry(registry),
            repository: repository.to_owned(),
            resource,
        })
    }

    pub fn oci_reference(&self) -> Result<oci_client::Reference> {
        let reference = match &self.resource {
            RegistryResource::Manifest(reference) if reference.contains(':') => {
                format!("{}/{}@{reference}", self.registry, self.repository)
            }
            RegistryResource::Manifest(reference) => {
                format!("{}/{}:{reference}", self.registry, self.repository)
            }
            RegistryResource::Blob(digest) => {
                format!("{}/{}@{digest}", self.registry, self.repository)
            }
        };
        oci_client::Reference::from_str(&reference)
            .with_context(|| format!("invalid OCI reference {reference:?}"))
    }
}

fn normalize_registry(registry: &str) -> String {
    match registry.to_ascii_lowercase().as_str() {
        "index.docker.io" | "registry-1.docker.io" => DOCKER_HUB.to_owned(),
        other => other.to_owned(),
    }
}

fn validate_repository(repository: &str) -> Result<()> {
    if repository.starts_with('/')
        || repository.ends_with('/')
        || repository
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || repository
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
    {
        bail!("invalid image repository {repository:?}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_docker_hub_and_rewrites_for_relay() {
        let image = ImageReference::parse("nginx").unwrap();
        assert_eq!(image.registry(), "docker.io");
        assert_eq!(image.repository(), "library/nginx");
        assert_eq!(image.reference(), "latest");
        assert_eq!(
            image.relay_reference("127.0.0.1:1234"),
            "127.0.0.1:1234/f/docker.io/library/nginx:latest"
        );
        assert_eq!(
            image.tag_parts(),
            Some(("library/nginx".to_owned(), "latest".to_owned()))
        );
    }

    #[test]
    fn preserves_explicit_registry_and_digest() {
        let image = ImageReference::parse("ghcr.io/acme/api@sha256:deadbeef").unwrap();
        assert!(image.is_digest());
        assert_eq!(
            image.relay_reference("localhost:9000"),
            "localhost:9000/f/ghcr.io/acme/api@sha256:deadbeef"
        );
        assert_eq!(image.tag_parts(), None);
    }

    #[test]
    fn parses_registry_distribution_paths() {
        assert_eq!(
            RegistryRequest::parse("f/ghcr.io/acme/api/manifests/1.2").unwrap(),
            RegistryRequest {
                registry: "ghcr.io".to_owned(),
                repository: "acme/api".to_owned(),
                resource: RegistryResource::Manifest("1.2".to_owned()),
            }
        );
        assert!(RegistryRequest::parse("acme/api/manifests/latest").is_err());
    }
}
