# Caddy storage adapter

This optional Caddy module consumes Swarmlite's generic KV and lock APIs. It does not make
Swarmlite the source of truth for certificates. The authoritative storage is always a local
CertMagic `FileStorage`, using Caddy's normal data directory by default. Remote keys are kept
under the fixed `caddy/` namespace so they do not collide with other KV consumers.

## Build

Build the included Caddy binary with the standard modules plus this adapter:

```bash
cd caddy-storage
go build -o ../target/caddy ./cmd/caddy
```

Or build its container image:

```bash
docker build -t swarmlite-caddy ./caddy-storage
```

## Configure

Export the same token printed by `swarmlite join-token`, then run the custom binary:

```bash
export SWARMLITE_TOKEN='<cluster-token>'
target/caddy run --config examples/caddy-with-swarmlite-kv.json
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

Consequently, Swarmlite failure never makes an existing local Caddy storage unavailable. It can
only reduce cross-machine reuse and allow duplicate certificate requests. If the entire
Swarmlite cluster is rebuilt, each Caddy instance keeps using its local certificates. A standard
Caddy binary can use the same local data directory without migrating certificate data.
