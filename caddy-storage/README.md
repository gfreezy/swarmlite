# Independent Caddy gateway storage

When a node has its Gateway enabled, `swarmlite serve` creates an independent Caddy container with
its own restart policy and persistent `/data`, `/config`, and `/cache` volumes. The default gateway
image includes this directory's `caddy.storage.swarmlite` module, Caddy's `http.handlers.cache`,
and the `storages.cache.badger` provider. No separately maintained Compose stack is required.

The module consumes Swarmlite's generic KV and lock APIs; those APIs contain no Caddy-specific
behavior. The authoritative certificate storage remains local CertMagic `FileStorage`, while
remote keys use the fixed `caddy/` namespace as a best-effort cache and distributed lock service.

## Publish the gateway image

Gateway nodes do not build Caddy locally. By default they pull
`ghcr.io/gfreezy/swarmlite-caddy:latest`. A path-filtered CI workflow builds the image for Linux
AMD64 and ARM64 only when `caddy-storage/**` or its image workflow changes. Pull requests build
without publishing; pushes publish a combined multi-platform image to GHCR. Every published image
gets a `sha-<commit>` tag, and the default branch also updates `latest`. Build and publish it
manually with:

```bash
docker build -t ghcr.io/gfreezy/swarmlite-caddy:latest ./caddy-storage
docker push ghcr.io/gfreezy/swarmlite-caddy:latest
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
`caddy`, `caddy.storage.swarmlite`, `http.handlers.cache`, and `storages.cache.badger`. Gateway
nodes pull it before replacing an existing container, and keep the existing `/data`, `/config`,
and `/cache` volumes.

## Automatic configuration

Swarmlite generates the storage block automatically, injects the cluster token through
`SWARMLITE_TOKEN`, and sends the cluster's fixed controller URL to the node through heartbeat
configuration. The node atomically loads the complete configuration through Caddy's loopback-only
admin API. The token is not written into Caddy's JSON configuration or container labels.

The generated global cache application uses `mode: bypass` and a Badger database whose `Dir` and
`ValueDir` are `/cache/badger`. A route only receives `http.handlers.cache` when its Stack rule has
a `cache` object. Consequently uncached routes are untouched, while cached routes use their
declared TTL without consulting upstream `Cache-Control` headers.

For a manually configured Caddy instance, export a valid cluster token and run the example:

```bash
export SWARMLITE_TOKEN='<cluster-token>'
go build -o target/caddy ./caddy-storage/cmd/caddy
target/caddy run --resume --config examples/caddy-with-swarmlite-kv.json
```

The storage JSON is:

```json
{
  "storage": {
    "module": "swarmlite",
    "controller": "http://10.0.0.21:8080",
    "token_env": "SWARMLITE_TOKEN",
    "timeout": "500ms",
    "lock_lease": "30s"
  }
}
```

`root` is optional. When omitted, the adapter uses Caddy's normal application data directory.
Keep that directory on persistent local storage. The equivalent Caddyfile global option is:

```caddyfile
{
    storage swarmlite {
        controller http://10.0.0.21:8080
        token_env SWARMLITE_TOKEN
        timeout 500ms
        lock_lease 30s
    }
}
```

## Failure behavior

- `Store` and `Delete` complete locally first; publishing to KV is best effort.
- `Load` reads locally first. A KV hit on a local miss is copied into local storage.
- A live distributed lock prevents duplicate work across machines.
- An unavailable controller falls back to Caddy's normal local lock.
- A reported busy distributed lock does not fall back, because another machine still owns it.
- KV values are plaintext base64. There is no application-level encryption.

Consequently, Swarmlite failure never makes existing local Caddy storage unavailable. It can only
reduce cross-machine reuse and allow duplicate certificate requests. If the entire Swarmlite
cluster is rebuilt, each Caddy instance keeps using its local certificates. A standard Caddy
binary can use the same local data directory without migrating certificate data.
