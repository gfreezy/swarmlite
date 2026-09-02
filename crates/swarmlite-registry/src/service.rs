use std::{
    io::SeekFrom,
    path::PathBuf,
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result};
use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use futures_util::StreamExt;
use oci_client::{
    Client,
    client::ClientConfig,
    manifest::{
        IMAGE_MANIFEST_LIST_MEDIA_TYPE, IMAGE_MANIFEST_MEDIA_TYPE, OCI_IMAGE_INDEX_MEDIA_TYPE,
        OCI_IMAGE_MEDIA_TYPE, OciDescriptor,
    },
    secrets::RegistryAuth,
};
use serde_json::json;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tracing::warn;

use crate::{
    OutboundProxyConfig, RegistryCacheConfig, RegistryCacheStats, RegistryRequest,
    RegistryResource,
    cache::{CacheWriter, CachedObject, RegistryCache},
};

const ACCEPTED_MANIFEST_TYPES: &[&str] = &[
    OCI_IMAGE_MEDIA_TYPE,
    OCI_IMAGE_INDEX_MEDIA_TYPE,
    IMAGE_MANIFEST_MEDIA_TYPE,
    IMAGE_MANIFEST_LIST_MEDIA_TYPE,
];

#[derive(Debug, Clone)]
pub struct RegistryServiceConfig {
    pub cache: RegistryCacheConfig,
    pub proxy: OutboundProxyConfig,
}

impl RegistryServiceConfig {
    pub fn new(cache_root: PathBuf, proxy: OutboundProxyConfig) -> Result<Self> {
        Ok(Self {
            cache: RegistryCacheConfig::new(cache_root),
            proxy,
        })
    }
}

#[derive(Clone)]
pub struct RegistryService {
    cache: RegistryCache,
    proxy: Arc<RwLock<OutboundProxyConfig>>,
}

impl RegistryService {
    pub async fn new(config: RegistryServiceConfig) -> Result<Self> {
        Ok(Self {
            cache: RegistryCache::open(config.cache).await?,
            proxy: Arc::new(RwLock::new(config.proxy)),
        })
    }

    pub fn set_proxy(&self, proxy: OutboundProxyConfig) {
        *self.proxy.write().expect("image proxy lock poisoned") = proxy;
    }

    pub fn ping(&self) -> Response {
        let mut response = StatusCode::OK.into_response();
        response.headers_mut().insert(
            "docker-distribution-api-version",
            HeaderValue::from_static("registry/2.0"),
        );
        response.headers_mut().insert(
            "x-swarmlite-image-proxy",
            HeaderValue::from_static(
                if self
                    .proxy
                    .read()
                    .expect("image proxy lock poisoned")
                    .enabled()
                {
                    "enabled"
                } else {
                    "disabled"
                },
            ),
        );
        response
    }

    pub async fn handle(
        &self,
        method: Method,
        path: &str,
        request_headers: &HeaderMap,
        auth: RegistryAuth,
    ) -> Response {
        if method != Method::GET && method != Method::HEAD {
            return registry_error(
                StatusCode::METHOD_NOT_ALLOWED,
                "UNSUPPORTED",
                "only GET and HEAD are supported",
            );
        }
        let request = match RegistryRequest::parse(path) {
            Ok(request) => request,
            Err(error) => {
                return registry_error(StatusCode::BAD_REQUEST, "NAME_INVALID", &error.to_string());
            }
        };
        let result = match &request.resource {
            RegistryResource::Manifest(reference) => {
                self.manifest(
                    &request,
                    reference,
                    &auth,
                    method == Method::HEAD,
                    request_headers
                        .get("x-swarmlite-proxy-probe")
                        .is_some_and(|value| value == "1"),
                )
                .await
            }
            RegistryResource::Blob(digest) => {
                self.blob(
                    &request,
                    digest,
                    &auth,
                    method == Method::HEAD,
                    request_headers,
                )
                .await
            }
        };
        match result {
            Ok(mut response) => {
                response.headers_mut().insert(
                    "docker-distribution-api-version",
                    HeaderValue::from_static("registry/2.0"),
                );
                response
            }
            Err(error) => {
                let message = format!("{error:#}");
                let lowercase = message.to_ascii_lowercase();
                let status = if lowercase.contains("unauthorized") || lowercase.contains("denied") {
                    StatusCode::UNAUTHORIZED
                } else if lowercase.contains("not found") || lowercase.contains("manifest unknown")
                {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::BAD_GATEWAY
                };
                registry_error(status, "UNKNOWN", &message)
            }
        }
    }

    pub async fn cache_stats(&self) -> Result<RegistryCacheStats> {
        self.cache.stats().await
    }

    fn client(&self) -> Result<Client> {
        let proxy = self
            .proxy
            .read()
            .expect("image proxy lock poisoned")
            .clone();
        Client::try_from(ClientConfig {
            http_proxy: proxy.http_proxy_url().map(ToOwned::to_owned),
            https_proxy: proxy.https_proxy_url().map(ToOwned::to_owned),
            no_proxy: proxy.no_proxy().map(ToOwned::to_owned),
            ..Default::default()
        })
        .context("invalid Controller image proxy configuration")
    }

    async fn manifest(
        &self,
        request: &RegistryRequest,
        reference: &str,
        auth: &RegistryAuth,
        head: bool,
        force_upstream: bool,
    ) -> Result<Response> {
        if !force_upstream
            && reference.starts_with("sha256:")
            && let Some(object) = self.cached(reference).await
        {
            match self
                .cached_response(object, reference, manifest_media_type_from_file, head, None)
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) => {
                    warn!(%error, digest = %reference, "failed to serve cached manifest; fetching upstream");
                }
            }
        }

        let image = request.oci_reference()?;
        let client = self.client()?;
        let (bytes, digest) = client
            .pull_manifest_raw(&image, auth, ACCEPTED_MANIFEST_TYPES)
            .await
            .with_context(|| format!("failed to pull manifest for {image}"))?;
        let media_type = manifest_media_type(&bytes);
        if let Err(error) = self.cache.store_bytes(&digest, &bytes).await {
            warn!(%error, %digest, "failed to cache registry manifest; continuing without cache");
        }
        bytes_response(bytes, &digest, &media_type, head)
    }

    async fn blob(
        &self,
        request: &RegistryRequest,
        digest: &str,
        auth: &RegistryAuth,
        head: bool,
        headers: &HeaderMap,
    ) -> Result<Response> {
        if let Some(object) = self.cached(digest).await {
            match self
                .cached_response(
                    object,
                    digest,
                    |_| "application/octet-stream".to_owned(),
                    head,
                    headers.get(header::RANGE),
                )
                .await
            {
                Ok(response) => return Ok(response),
                Err(error) => {
                    warn!(%error, %digest, "failed to serve cached blob; fetching upstream");
                }
            }
        }

        let image = request.oci_reference()?;
        let client = self.client()?;
        client.store_auth_if_needed(&request.registry, auth).await;
        let descriptor = OciDescriptor {
            digest: digest.to_owned(),
            ..Default::default()
        };
        let upstream = client
            .pull_blob_stream(&image, &descriptor)
            .await
            .with_context(|| format!("failed to pull blob {digest}"))?;
        let size = upstream.content_length;
        if head {
            return empty_blob_response(digest, size);
        }
        let writer = match self.cache.begin_write(digest, size).await {
            Ok(writer) => Some(writer),
            Err(error) => {
                warn!(%error, %digest, "failed to open registry cache writer; continuing as passthrough");
                None
            }
        };
        caching_stream_response(upstream.stream, digest, size, writer)
    }

    async fn cached_response(
        &self,
        object: CachedObject,
        digest: &str,
        media_type: impl FnOnce(&[u8]) -> String,
        head: bool,
        range: Option<&HeaderValue>,
    ) -> Result<Response> {
        let (start, end, status) = parse_range(range, object.size)?;
        let length = end.saturating_sub(start).saturating_add(1);
        let content_type = if head || object.size > 4 * 1024 * 1024 {
            media_type(&[])
        } else {
            let bytes = tokio::fs::read(&object.path).await?;
            media_type(&bytes)
        };
        let mut builder = Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, content_type)
            .header(header::CONTENT_LENGTH, length)
            .header("docker-content-digest", digest)
            .header(header::ACCEPT_RANGES, "bytes");
        if status == StatusCode::PARTIAL_CONTENT {
            builder = builder.header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{}", object.size),
            );
        }
        if head {
            return Ok(builder.body(Body::empty())?);
        }
        let mut file = File::open(&object.path).await?;
        file.seek(SeekFrom::Start(start)).await?;
        let lease = self.cache.lease(&object);
        let stream = async_stream::stream! {
            let _lease = lease;
            let mut remaining = length;
            let mut buffer = vec![0_u8; 64 * 1024];
            while remaining > 0 {
                let wanted = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
                let read = match file.read(&mut buffer[..wanted]).await {
                    Ok(read) => read,
                    Err(error) => {
                        yield Err::<Bytes, std::io::Error>(error);
                        break;
                    }
                };
                if read == 0 { break }
                remaining -= read as u64;
                yield Ok::<Bytes, std::io::Error>(Bytes::copy_from_slice(&buffer[..read]));
            }
        };
        Ok(builder.body(Body::from_stream(stream))?)
    }

    async fn cached(&self, digest: &str) -> Option<CachedObject> {
        match self.cache.get(digest).await {
            Ok(object) => object,
            Err(error) => {
                warn!(%error, %digest, "failed to read registry cache; fetching upstream");
                None
            }
        }
    }
}

fn manifest_media_type(bytes: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| value.get("mediaType")?.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| OCI_IMAGE_MEDIA_TYPE.to_owned())
}

fn manifest_media_type_from_file(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        OCI_IMAGE_MEDIA_TYPE.to_owned()
    } else {
        manifest_media_type(bytes)
    }
}

fn bytes_response(bytes: Bytes, digest: &str, media_type: &str, head: bool) -> Result<Response> {
    let body = if head {
        Body::empty()
    } else {
        Body::from(bytes.clone())
    };
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, media_type)
        .header(header::CONTENT_LENGTH, bytes.len())
        .header("docker-content-digest", digest)
        .body(body)?)
}

fn empty_blob_response(digest: &str, size: Option<u64>) -> Result<Response> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("docker-content-digest", digest)
        .header(header::ACCEPT_RANGES, "bytes");
    if let Some(size) = size {
        builder = builder.header(header::CONTENT_LENGTH, size)
    }
    Ok(builder.body(Body::empty())?)
}

fn stream_response(
    stream: impl futures_util::Stream<Item = std::result::Result<Bytes, std::io::Error>>
    + Send
    + 'static,
    digest: &str,
    size: Option<u64>,
) -> Result<Response> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header("docker-content-digest", digest)
        .header(header::ACCEPT_RANGES, "bytes");
    if let Some(size) = size {
        builder = builder.header(header::CONTENT_LENGTH, size)
    }
    Ok(builder.body(Body::from_stream(stream))?)
}

fn caching_stream_response(
    mut stream: futures_util::stream::BoxStream<
        'static,
        std::result::Result<Bytes, std::io::Error>,
    >,
    digest: &str,
    size: Option<u64>,
    mut writer: Option<CacheWriter>,
) -> Result<Response> {
    let response_digest = digest.to_owned();
    let stream_digest = response_digest.clone();
    let body = async_stream::stream! {
        while let Some(item) = stream.next().await {
            match item {
                Ok(bytes) => {
                    if let Some(cache_writer) = writer.as_mut()
                        && let Err(error) = cache_writer.write(&bytes).await
                    {
                        warn!(%error, digest = %stream_digest, "registry cache write failed; continuing as passthrough");
                        if let Some(cache_writer) = writer.take() {
                            cache_writer.abort().await;
                        }
                    }
                    if writer.as_ref().is_some_and(CacheWriter::has_expected_size)
                        && let Some(cache_writer) = writer.take()
                        && let Err(error) = cache_writer.commit().await
                    {
                        warn!(%error, digest = %stream_digest, "failed to commit registry cache object");
                    }
                    yield Ok::<Bytes, std::io::Error>(bytes);
                }
                Err(error) => {
                    if let Some(cache_writer) = writer.take() {
                        cache_writer.abort().await;
                    }
                    yield Err::<Bytes, std::io::Error>(error);
                    return;
                }
            }
        }
        if let Some(cache_writer) = writer.take()
            && let Err(error) = cache_writer.commit().await
        {
            warn!(%error, digest = %stream_digest, "failed to commit registry cache object");
        }
    };
    stream_response(body, &response_digest, size)
}

fn parse_range(range: Option<&HeaderValue>, size: u64) -> Result<(u64, u64, StatusCode)> {
    let Some(range) = range else {
        return Ok((0, size.saturating_sub(1), StatusCode::OK));
    };
    let value = range
        .to_str()?
        .strip_prefix("bytes=")
        .context("unsupported Range header")?;
    if value.contains(',') {
        anyhow::bail!("multiple ranges are unsupported")
    }
    let (start, end) = value.split_once('-').context("invalid Range header")?;
    let start = start.parse::<u64>().context("invalid range start")?;
    let end = if end.is_empty() {
        size.saturating_sub(1)
    } else {
        end.parse::<u64>().context("invalid range end")?
    };
    if size == 0 || start > end || end >= size {
        anyhow::bail!("range is outside the cached object")
    }
    Ok((start, end, StatusCode::PARTIAL_CONTENT))
}

fn registry_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_vec(&json!({"errors": [{"code": code, "message": message}]}))
            .unwrap_or_default(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_byte_ranges() {
        assert_eq!(
            parse_range(Some(&HeaderValue::from_static("bytes=2-4")), 10).unwrap(),
            (2, 4, StatusCode::PARTIAL_CONTENT)
        );
        assert!(parse_range(Some(&HeaderValue::from_static("bytes=8-12")), 10).is_err());
    }

    #[tokio::test]
    async fn ping_only_advertises_an_explicit_proxy() {
        let directory = tempfile::tempdir().unwrap();
        let config = RegistryServiceConfig {
            cache: RegistryCacheConfig::new(directory.path().to_owned()),
            proxy: OutboundProxyConfig::default(),
        };
        let service = RegistryService::new(config).await.unwrap();
        assert_eq!(
            service.ping().headers()["x-swarmlite-image-proxy"],
            "disabled"
        );

        let directory = tempfile::tempdir().unwrap();
        let config = RegistryServiceConfig {
            cache: RegistryCacheConfig::new(directory.path().to_owned()),
            proxy: OutboundProxyConfig::new(
                None,
                Some("http://127.0.0.1:3128".to_owned()),
                None,
                None,
            )
            .unwrap(),
        };
        let service = RegistryService::new(config).await.unwrap();
        assert_eq!(
            service.ping().headers()["x-swarmlite-image-proxy"],
            "enabled"
        );
    }

    #[tokio::test]
    async fn commits_known_length_cache_before_yielding_the_last_chunk() {
        let directory = tempfile::tempdir().unwrap();
        let cache = RegistryCache::open(RegistryCacheConfig::new(directory.path().to_owned()))
            .await
            .unwrap();
        let digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let writer = cache.begin_write(digest, Some(3)).await.unwrap();
        let upstream = futures_util::stream::iter([Ok(Bytes::from_static(b"abc"))]).boxed();
        let response = caching_stream_response(upstream, digest, Some(3), Some(writer)).unwrap();
        let mut body = response.into_body().into_data_stream();

        assert_eq!(
            body.next().await.unwrap().unwrap(),
            Bytes::from_static(b"abc")
        );
        drop(body);

        let cached = cache.get(digest).await.unwrap().unwrap();
        assert_eq!(tokio::fs::read(cached.path).await.unwrap(), b"abc");
    }
}
