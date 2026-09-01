use std::{
    fs,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use axum::{
    Router,
    extract::{OriginalUri, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::any,
};
use bollard::{
    API_DEFAULT_VERSION, Docker,
    query_parameters::{RemoveImageOptionsBuilder, TagImageOptionsBuilder},
};
use serde_json::Value;
use swarmlite_platform::{
    config::{ResolvedRuntimeConfig, RuntimeKind},
    local_state::LocalState,
    model::DeploymentPolicy,
    registry::RegistryCredentialStore,
    runtime::{ContainerRuntime, DockerCompatibleRuntime, RuntimeImageProgress},
};
use swarmlite_registry::{
    ImageReference, OutboundProxyConfig, RegistryAuth, RegistryCacheConfig, RegistryService,
    RegistryServiceConfig, spawn_relay,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

const IMAGE: &str = "registry.k8s.io/pause:3.10";
const CONTROLLER_TOKEN: &str = "image-proxy-e2e-token";
const DOCKER_SOCKET: &str = "/var/run/docker.sock";

#[derive(Clone)]
struct ControllerState {
    registry: Arc<RegistryService>,
}

struct TestServer {
    address: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct TestProxy {
    address: SocketAddr,
    scheme: &'static str,
    connections: Arc<AtomicUsize>,
    task: Option<JoinHandle<()>>,
}

impl TestProxy {
    fn url(&self) -> String {
        format!("{}://{}", self.scheme, self.address)
    }

    fn connection_count(&self) -> usize {
        self.connections.load(Ordering::Relaxed)
    }

    async fn shutdown(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for TestProxy {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[tokio::test]
#[ignore = "requires a Linux Docker daemon and outbound access to registry.k8s.io"]
async fn docker_pull_uses_controller_proxy_cache_and_falls_back_to_direct_pull() -> Result<()> {
    let docker = Docker::connect_with_socket(DOCKER_SOCKET, 120, API_DEFAULT_VERSION)
        .context("connect to Docker for image proxy E2E")?;
    docker
        .ping()
        .await
        .context("image proxy E2E requires a running Docker daemon")?;

    let original_image_id = docker
        .inspect_image(IMAGE)
        .await
        .ok()
        .and_then(|image| image.id);
    let result = async {
        run_e2e(&docker).await?;
        socks5_proxy_reaches_the_controller_registry().await
    }
    .await;
    let cleanup = restore_original_image(&docker, original_image_id).await;
    result.and(cleanup)
}

async fn restore_original_image(docker: &Docker, original_image_id: Option<String>) -> Result<()> {
    if let Some(image_id) = original_image_id {
        let (repository, tag) = ImageReference::parse(IMAGE)?
            .tag_parts()
            .context("E2E image must have a tag")?;
        let options = TagImageOptionsBuilder::default()
            .repo(&repository)
            .tag(&tag)
            .build();
        docker
            .tag_image(&image_id, Some(options))
            .await
            .context("restore pre-existing E2E image tag")?;
    } else {
        let remove = RemoveImageOptionsBuilder::default()
            .force(true)
            .noprune(false)
            .build();
        let _ = docker.remove_image(IMAGE, Some(remove), None).await;
    }
    Ok(())
}

async fn run_e2e(docker: &Docker) -> Result<()> {
    let directory = tempfile::tempdir().context("create image proxy E2E directory")?;
    let cache_root = directory.path().join("registry-cache");
    let mut proxy = spawn_connect_proxy().await?;
    let registry_config = RegistryServiceConfig {
        cache: RegistryCacheConfig::new(cache_root.clone()),
        proxy: OutboundProxyConfig::new(Some(proxy.url()), None, None, None)?,
    };
    let registry = Arc::new(RegistryService::new(registry_config).await?);
    let controller = spawn_controller(registry.clone()).await?;
    let relay = spawn_relay(format!("http://{}", controller.address), CONTROLLER_TOKEN).await?;

    let relay_http = reqwest::Client::builder().no_proxy().build()?;
    let ping = relay_http
        .head(format!("http://{}/v2/", relay.authority()))
        .send()
        .await?;
    if ping.status() != StatusCode::OK
        || ping
            .headers()
            .get("x-swarmlite-image-proxy")
            .and_then(|value| value.to_str().ok())
            != Some("enabled")
    {
        bail!("Agent relay did not expose an enabled Controller image proxy");
    }
    let reference = ImageReference::parse(IMAGE)?;
    let probe_url = format!(
        "http://{}{}",
        relay.authority(),
        reference.relay_manifest_path()
    );
    let probe = relay_http
        .head(&probe_url)
        .header("x-swarmlite-proxy-probe", "1")
        .send()
        .await?;
    if probe.status() != StatusCode::OK {
        let status = probe.status();
        let detail = relay_http
            .get(&probe_url)
            .header("x-swarmlite-proxy-probe", "1")
            .send()
            .await?
            .text()
            .await?;
        bail!(
            "Controller image proxy probe returned {status} after {} CONNECT request(s): {detail}",
            proxy.connection_count()
        );
    }
    if proxy.connection_count() == 0 {
        bail!("Controller probe did not connect to the configured HTTP CONNECT proxy");
    }

    let local_state = LocalState::open(&directory.path().join("state"))?;
    let runtime = DockerCompatibleRuntime::connect_with_image_relay(
        &ResolvedRuntimeConfig {
            kind: RuntimeKind::Docker,
            socket: DOCKER_SOCKET.to_owned(),
        },
        RegistryCredentialStore::new(local_state),
        directory.path().join("configs"),
        DeploymentPolicy {
            image_pull_idle_timeout_seconds: 60,
            image_pull_max_attempts: 1,
            ..DeploymentPolicy::default()
        },
        relay.authority().to_owned(),
    )?;

    let image_id = runtime
        .resolve_image(IMAGE, &RuntimeImageProgress::default())
        .await
        .context("pull image through Controller registry proxy")?;
    let inspect = docker.inspect_image(IMAGE).await?;
    if inspect.id.as_deref() != Some(image_id.as_str()) {
        bail!(
            "restored image tag resolved to {:?}, expected {image_id}",
            inspect.id
        );
    }

    let temporary = ImageReference::parse(IMAGE)?.relay_reference(relay.authority());
    if docker.inspect_image(&temporary).await.is_ok() {
        bail!("temporary relay image tag was not removed: {temporary}");
    }
    let stats = registry.cache_stats().await?;
    if stats.objects == 0 || stats.bytes == 0 {
        bail!("Controller registry cache remained empty after proxied Docker pull");
    }

    let layer_digest = find_cached_layer_digest(&cache_root)?;
    let layer_url = format!(
        "http://{}/v2/f/registry.k8s.io/pause/blobs/{layer_digest}",
        relay.authority()
    );
    let upstream_layer = relay_http
        .get(&layer_url)
        .send()
        .await
        .context("prime Controller layer cache through Agent relay")?;
    if upstream_layer.status() != StatusCode::OK {
        bail!(
            "layer cache-prime request failed: {}",
            upstream_layer.status()
        );
    }
    let upstream_layer = upstream_layer.bytes().await?;
    proxy.shutdown().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let cached_response = relay_http
        .get(&layer_url)
        .send()
        .await
        .context("request cached layer through Agent relay")?;
    if cached_response.status() != StatusCode::OK {
        bail!(
            "cached layer request failed after upstream proxy shutdown: {}",
            cached_response.status()
        );
    }
    if cached_response.bytes().await? != upstream_layer {
        bail!("cached layer response differed from the upstream layer");
    }

    let fallback_id = runtime
        .resolve_image(IMAGE, &RuntimeImageProgress::default())
        .await
        .context("fall back to Docker's direct pull after proxy became unavailable")?;
    if fallback_id != image_id {
        bail!("direct fallback resolved {fallback_id}, expected {image_id}");
    }

    Ok(())
}

async fn socks5_proxy_reaches_the_controller_registry() -> Result<()> {
    let directory = tempfile::tempdir().context("create SOCKS5 E2E directory")?;
    let proxy = spawn_socks5_proxy().await?;
    let registry = Arc::new(
        RegistryService::new(RegistryServiceConfig {
            cache: RegistryCacheConfig::new(directory.path().join("registry-cache")),
            proxy: OutboundProxyConfig::new(None, None, Some(proxy.url()), None)?,
        })
        .await?,
    );
    let controller = spawn_controller(registry).await?;
    let relay = spawn_relay(format!("http://{}", controller.address), CONTROLLER_TOKEN).await?;
    let reference = ImageReference::parse(IMAGE)?;
    let response = reqwest::Client::builder()
        .no_proxy()
        .build()?
        .head(format!(
            "http://{}{}",
            relay.authority(),
            reference.relay_manifest_path()
        ))
        .header("x-swarmlite-proxy-probe", "1")
        .send()
        .await
        .context("probe Registry through SOCKS5 proxy")?;
    if response.status() != StatusCode::OK {
        bail!("SOCKS5 Registry probe returned {}", response.status());
    }
    if proxy.connection_count() == 0 {
        bail!("Controller did not connect to the configured SOCKS5 proxy");
    }
    Ok(())
}

async fn spawn_controller(registry: Arc<RegistryService>) -> Result<TestServer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let app = Router::new()
        .route("/v2/", any(controller_registry))
        .route("/v2/{*path}", any(controller_registry))
        .with_state(ControllerState { registry });
    let task = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            eprintln!("image proxy E2E Controller stopped: {error}");
        }
    });
    Ok(TestServer { address, task })
}

async fn controller_registry(
    State(state): State<ControllerState>,
    OriginalUri(uri): OriginalUri,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let authorized = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some(&format!("Bearer {CONTROLLER_TOKEN}"));
    if !authorized {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if uri.path() == "/v2/" {
        return state.registry.ping();
    }
    let path = uri.path().strip_prefix("/v2/").unwrap_or(uri.path());
    state
        .registry
        .handle(method, path, &headers, RegistryAuth::Anonymous)
        .await
}

async fn spawn_connect_proxy() -> Result<TestProxy> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let connections = Arc::new(AtomicUsize::new(0));
    let count = connections.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            count.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                if let Err(error) = forward_connect(socket).await {
                    eprintln!("image proxy E2E CONNECT failed: {error:#}");
                }
            });
        }
    });
    Ok(TestProxy {
        address,
        scheme: "http",
        connections,
        task: Some(task),
    })
}

async fn spawn_socks5_proxy() -> Result<TestProxy> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let connections = Arc::new(AtomicUsize::new(0));
    let count = connections.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            count.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(async move {
                if let Err(error) = forward_socks5(socket).await {
                    eprintln!("image proxy E2E SOCKS5 connection failed: {error:#}");
                }
            });
        }
    });
    Ok(TestProxy {
        address,
        scheme: "socks5h",
        connections,
        task: Some(task),
    })
}

async fn forward_connect(mut downstream: TcpStream) -> Result<()> {
    let mut request = Vec::with_capacity(1024);
    let header_end = loop {
        if request.len() >= 16 * 1024 {
            bail!("proxy request headers exceeded 16 KiB");
        }
        let mut buffer = [0_u8; 1024];
        let read = downstream.read(&mut buffer).await?;
        if read == 0 {
            bail!("proxy client closed before sending request headers");
        }
        request.extend_from_slice(&buffer[..read]);
        if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    if header_end != request.len() {
        bail!("proxy client sent a request body before the CONNECT response");
    }
    let request = std::str::from_utf8(&request).context("proxy request was not UTF-8")?;
    let first_line = request.lines().next().context("proxy request was empty")?;
    let mut parts = first_line.split_whitespace();
    if parts.next() != Some("CONNECT") {
        downstream
            .write_all(b"HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
            .await?;
        bail!("proxy received a non-CONNECT request");
    }
    let authority = parts.next().context("CONNECT authority was missing")?;
    let mut upstream = match TcpStream::connect(authority).await {
        Ok(upstream) => upstream,
        Err(error) => {
            downstream
                .write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
                .await?;
            return Err(error).with_context(|| format!("connect to {authority}"));
        }
    };
    downstream
        .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        .await?;
    tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await?;
    Ok(())
}

async fn forward_socks5(mut downstream: TcpStream) -> Result<()> {
    let mut greeting = [0_u8; 2];
    downstream.read_exact(&mut greeting).await?;
    if greeting[0] != 5 {
        bail!("unsupported SOCKS version {}", greeting[0]);
    }
    let mut methods = vec![0_u8; usize::from(greeting[1])];
    downstream.read_exact(&mut methods).await?;
    if !methods.contains(&0) {
        downstream.write_all(&[5, 0xff]).await?;
        bail!("SOCKS5 client did not offer unauthenticated mode");
    }
    downstream.write_all(&[5, 0]).await?;

    let mut request = [0_u8; 4];
    downstream.read_exact(&mut request).await?;
    if request[..3] != [5, 1, 0] {
        bail!("SOCKS5 client sent an unsupported command");
    }
    let host = match request[3] {
        1 => {
            let mut address = [0_u8; 4];
            downstream.read_exact(&mut address).await?;
            Ipv4Addr::from(address).to_string()
        }
        3 => {
            let length = downstream.read_u8().await?;
            let mut domain = vec![0_u8; usize::from(length)];
            downstream.read_exact(&mut domain).await?;
            String::from_utf8(domain).context("SOCKS5 domain was not UTF-8")?
        }
        4 => {
            let mut address = [0_u8; 16];
            downstream.read_exact(&mut address).await?;
            Ipv6Addr::from(address).to_string()
        }
        address_type => bail!("unsupported SOCKS5 address type {address_type}"),
    };
    let port = downstream.read_u16().await?;
    let mut upstream = match TcpStream::connect((host.as_str(), port)).await {
        Ok(upstream) => upstream,
        Err(error) => {
            downstream
                .write_all(&[5, 5, 0, 1, 0, 0, 0, 0, 0, 0])
                .await?;
            return Err(error).with_context(|| format!("connect to {host}:{port}"));
        }
    };
    downstream
        .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
        .await?;
    tokio::io::copy_bidirectional(&mut downstream, &mut upstream).await?;
    Ok(())
}

fn find_cached_layer_digest(cache_root: &Path) -> Result<String> {
    let objects = cache_root.join("objects/sha256");
    for prefix in read_directories(&objects)? {
        for path in read_files(&prefix)? {
            let bytes = fs::read(&path)?;
            let Ok(manifest) = serde_json::from_slice::<Value>(&bytes) else {
                continue;
            };
            let Some(digest) = manifest
                .get("layers")
                .and_then(Value::as_array)
                .and_then(|layers| layers.first())
                .and_then(|layer| layer.get("digest"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            digest
                .strip_prefix("sha256:")
                .context("cached layer did not use sha256")?;
            return Ok(digest.to_owned());
        }
    }
    bail!("no cached platform manifest with a layer was found")
}

fn read_directories(root: &Path) -> Result<Vec<PathBuf>> {
    Ok(fs::read_dir(root)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_dir())
        .collect())
}

fn read_files(root: &Path) -> Result<Vec<PathBuf>> {
    Ok(fs::read_dir(root)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .collect())
}
