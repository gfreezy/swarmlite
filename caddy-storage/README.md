# Independent Caddy gateway storage

When a node has its Gateway enabled, `swarmlite serve` creates an independent Caddy container with
its own restart policy and persistent `/data`, `/config`, and `/cache` volumes. The default gateway
image includes this directory's `caddy.storage.swarmlite`,
`http.handlers.swarmlite_gateway_probe`, and native `http.handlers.cache` modules, Caddy's
`http.handlers.encode`, and the standard `zstd` and `gzip` encoders. The response cache persists
directly to SQLite; cache-handler, Souin, and their storage-provider abstraction are not linked.
No separately maintained Compose stack is required.

The module consumes Swarmlite's generic KV and lock APIs; those APIs contain no Caddy-specific
behavior. The authoritative certificate storage remains local CertMagic `FileStorage`, while
remote keys use the fixed `caddy/` namespace as a best-effort cache and distributed lock service.

## Publish the gateway image

Gateway nodes do not build Caddy locally. Each Swarmlite release references the immutable
`ghcr.io/gfreezy/swarmlite-caddy:v<VERSION>` tag with the same version. A path-filtered CI workflow
builds the image for Linux AMD64 and ARM64 when `caddy-storage/**` or its image workflow changes.
Pull requests build without publishing; branch pushes publish a combined multi-platform image to
GHCR with `sha-<commit>`, and the default branch also updates `latest`.

Release tags compare the current and previous release's complete `caddy-storage/` Git trees. A
changed tree is rebuilt and published under the release version. An unchanged tree is not rebuilt;
the new version and commit tags are attached to the previous release's exact manifest digest. The
release fails if an existing immutable version tag points somewhere else. From the repository
root, build and publish the current package version manually with:

```bash
VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)"
docker build -t "ghcr.io/gfreezy/swarmlite-caddy:v${VERSION}" ./caddy-storage
docker push "ghcr.io/gfreezy/swarmlite-caddy:v${VERSION}"
```

Caddy stays pinned to a tested version in both `go.mod` and the runtime base image. Dependabot
checks weekly for a newer stable Caddy release and groups the Go module, CertMagic, and Docker image
updates into one pull request. The pull request must pass the Go tests and the multi-platform image
build before it is merged and published; production builds never consume an untested floating
upstream `latest` tag directly.

Select another published image during cluster initialization or update it later:

```bash
swarmlite init --gateway-image registry.example.com/swarmlite-caddy:1.0.0
swarmlite config set gateway-image registry.example.com/swarmlite-caddy:1.1.0
```

The image reference is stored in the controller's SQLite database. The selected image must provide
`caddy`, `caddy.storage.swarmlite`, `http.handlers.swarmlite_gateway_probe`,
`http.handlers.cache`, `http.handlers.encode`, `http.encoders.zstd`, and `http.encoders.gzip`.
Gateway nodes pull it before replacing an existing container, and keep the existing `/data`,
`/config`, and `/cache` volumes.

## Automatic configuration

Swarmlite generates the storage block and a highest-priority HTTP owner-probe route automatically,
injects the cluster token through `SWARMLITE_TOKEN` and the node ID through
`SWARMLITE_GATEWAY_ID`, and sends the cluster's fixed controller URL to the node through heartbeat
configuration. The node atomically loads the complete configuration through Caddy's loopback-only
admin API. The token is not written into Caddy's JSON configuration or container labels.

Each Stack rule with a `cache` object receives Swarmlite's native `http.handlers.cache`; uncached
routes are untouched and no global cache application is generated. The handler stores complete
status, headers, body, freshness timestamps, and `Vary` dimensions in
`/cache/native-v1/cache.db`. SQLite runs in WAL mode with one serialized writer and four query-only
reader connections, mmap is disabled, and expired rows are cleaned every five minutes. WAL
checkpoints default to 8,192 pages (about 32 MiB) to avoid checkpointing after every large cached
response. Cached
response payload is logically capped at 1 GiB per Gateway by default and can be changed through
the cluster-level `gateway.cache.max-size-bytes` setting. Sampled hits are deduplicated through a
64 KiB Bloom filter and written in batches to a small access-metadata table, avoiding updates to rows
that contain response bodies. Expired rows are removed first; approximate LRU eviction then returns
usage to a 90% low-water mark. The logical limit counts the compact key, serialized headers, and
body; reusable SQLite pages and WAL bytes may make the physical files larger. SQLite secure-delete
is explicitly disabled for this disposable data. On a schema change, the old database and its
WAL/SHM sidecars are renamed out of the active path, a fresh database is opened immediately, and
the stale files are removed in the background instead of dropping multi-gigabyte tables while
Caddy provisions the new configuration.

The declared route TTL controls freshness. Request `no-store`, conditional, range, CONNECT, and
protocol-upgrade requests bypass caching, as do responses carrying `Set-Cookie` or
`Content-Range`. Authorization does not affect cache eligibility or key identity, and response
`Cache-Control` directives are ignored. Cache keys contain scheme, host, method, path, query,
request-body hash and content type, configured `key.headers`, and response `Vary` fields; Souin's
`key.disable_query` omits the query component. An `allowed_http_verbs` allowlist is optional; when
it is absent, only `GET` responses are stored and `HEAD` may reuse a matching `GET` response.
Request bodies above
`max_request_body_bytes` and responses above `max_cacheable_body_bytes` bypass storage. Concurrent
misses for the same key are coalesced, and SQLite errors fail open to the upstream. Expired entries
are never served; stale-while-revalidate, stale-if-error, conditional revalidation, and purge APIs
remain outside this first native-cache phase.

Generated proxy routes put `http.handlers.encode` before cache and reverse-proxy handlers. Caddy
therefore negotiates Zstandard or gzip after the downstream response is produced, uses its
512-byte minimum response length, preserves an existing upstream `Content-Encoding`, and emits
`Vary: Accept-Encoding` for responses it compresses.

For a manually configured Caddy instance, export a valid cluster token and stable Gateway ID. The
example includes the owner-probe handler route also used by [`bootstrap.json`](bootstrap.json):

```bash
export SWARMLITE_TOKEN='<cluster-token>'
export SWARMLITE_GATEWAY_ID='<gateway-node-id>'
go -C caddy-storage build -o /tmp/swarmlite-caddy ./cmd/caddy
/tmp/swarmlite-caddy run --resume --config examples/caddy-with-swarmlite-kv.json
```

The storage JSON is:

```json
{
  "storage": {
    "module": "swarmlite",
    "controller": "http://10.0.0.21:17080",
    "token_env": "SWARMLITE_TOKEN",
    "gateway_id_env": "SWARMLITE_GATEWAY_ID",
    "timeout": "500ms",
    "probe_timeout": "2s",
    "owner_cache_ttl": "1m",
    "lock_lease": "30s"
  }
}
```

`root` is optional. When omitted, the adapter uses Caddy's normal application data directory.
Keep that directory on persistent local storage. The equivalent Caddyfile global option is:

```caddyfile
{
    storage swarmlite {
        controller http://10.0.0.21:17080
        token_env SWARMLITE_TOKEN
        gateway_id_env SWARMLITE_GATEWAY_ID
        timeout 500ms
        probe_timeout 2s
        owner_cache_ttl 1m
        lock_lease 30s
    }
}
```

## Failure behavior

- `Store` and `Delete` complete locally first; publishing to KV is best effort.
- `Load` reads locally first. A KV hit on a local miss is copied into local storage.
- Certificate issuance first probes the target hostname over HTTP and verifies the reached
  Gateway's signed node ID. Only that Gateway may request the shared hostname lock.
- A valid owner result is cached for `owner_cache_ttl`; a probe failure without a cached result
  defers new issuance. It does not affect already loaded certificates or HTTPS traffic.
- A live distributed lock prevents duplicate work across machines and keeps the lock name
  `caddy/locks/issue_cert_<hostname>`.
- An unavailable Controller falls back to Caddy's normal local lock only after this Gateway has
  been established as the hostname owner, directly or from its recent cache.
- A reported busy distributed lock does not fall back, because another machine still owns it.
- Wildcard certificates skip the HTTP owner check and retain the distributed-lock behavior.
- KV values are plaintext base64. There is no application-level encryption.

Consequently, Swarmlite failure never makes existing local Caddy storage unavailable. If the
entire Swarmlite cluster is rebuilt, each Caddy instance keeps using its local certificates in
Caddy's standard data layout. During a Controller outage, a hostname routed to exactly one Gateway
can still be issued there; an actively load-balanced hostname needs the Controller for cross-node
exclusion.
