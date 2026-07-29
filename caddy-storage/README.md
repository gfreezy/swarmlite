# Independent Caddy gateway storage

When a node has the `gateway` role, `swarmlite serve` creates an independent Caddy container with
its own restart policy and persistent `/data` and `/config` volumes. The default gateway image
includes and enables this directory's `caddy.storage.swarmlite` module. No separately maintained
Compose stack is required.

The module consumes Swarmlite's generic KV and lock APIs; those APIs contain no Caddy-specific
behavior. The authoritative certificate storage remains local CertMagic `FileStorage`, while
remote keys use the fixed `caddy/` namespace as a best-effort cache and distributed lock service.

## Publish the gateway image

Gateway nodes do not build Caddy locally. By default they pull
`ghcr.io/swarmlite/swarmlite-caddy:latest`. CI can build and publish that image from this
directory:

```bash
docker build -t ghcr.io/swarmlite/swarmlite-caddy:latest ./caddy-storage
docker push ghcr.io/swarmlite/swarmlite-caddy:latest
```

Select another published image during cluster initialization or update it later:

```bash
swarmlite init --gateway-image registry.example.com/swarmlite-caddy:1.0.0
swarmlite config set gateway-image registry.example.com/swarmlite-caddy:1.1.0
```

The image reference is Raft-replicated cluster configuration. The selected image must provide
both `caddy` and the `caddy.storage.swarmlite` module. Gateway nodes pull it before replacing an
existing container, and keep the existing `/data` and `/config` volumes.

## Automatic configuration

Swarmlite generates the storage block automatically, injects the cluster token through
`SWARMLITE_TOKEN`, and refreshes the active controller URLs through Caddy's admin API. The token
is not written into Caddy's JSON configuration or container labels. Generated storage updates also
carry `controller_set_generation`; Swarmlite records a successful Caddy Admin API update as that
Gateway's acknowledgement before allowing a Controller voter to be removed.

For manual testing, build the included binary and run the example:

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
    "controllers": ["http://10.0.0.21:8080"],
    "token_env": "SWARMLITE_TOKEN",
    "timeout": "500ms",
    "lock_lease": "30s"
  }
}
```

`root` is optional. When omitted, the adapter uses Caddy's normal application data directory.
Keep that directory on persistent local storage. One controller URL is enough; listing several
only keeps the optional cache available when that first controller is down. The equivalent
Caddyfile global option is:

```caddyfile
{
    storage swarmlite {
        controller http://10.0.0.21:8080
        controller http://10.0.0.22:8080
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
- An unavailable controller or missing Raft quorum falls back to Caddy's normal local lock.
- A reported busy distributed lock does not fall back, because another machine still owns it.
- KV values are plaintext base64. There is no application-level encryption.

Consequently, Swarmlite failure never makes existing local Caddy storage unavailable. It can only
reduce cross-machine reuse and allow duplicate certificate requests. If the entire Swarmlite
cluster is rebuilt, each Caddy instance keeps using its local certificates. A standard Caddy
binary can use the same local data directory without migrating certificate data.
