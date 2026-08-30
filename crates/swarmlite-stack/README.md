# swarmlite-stack

`swarmlite-stack` owns Swarmlite's Stack configuration domain:

- Docker Compose/Swarm-compatible `services` parsing and normalization;
- Compose-compatible file-backed `configs` declarations and Service mounts;
- the internal `ServiceSpec`, port, and healthcheck models;
- the `x-swarmlite` routing data model;
- the optional `x-swarmlite.name` default used by the deploy CLI;
- private registry credentials under `x-swarmlite.registries`;
- normalization and validation;
- internal service port discovery from route references;
- deterministic rule ordering;
- Caddy JSON generation.

The crate has no dependency on controllers, scheduling, SQLite, or a container runtime. It
parses a complete Stack file in one pass, validates `backend.service` against that file's `services`
map, and exposes a renderer callback which resolves an internal `(stack, service, port)` into
healthy dial addresses. The main `swarmlite` crate implements that runtime adapter from replicated
cluster state.

The editor schema is at [schema/stack.schema.json](schema/stack.schema.json). Route-level `cache`
completion is kept separately in
[schema/cache-handler.schema.json](schema/cache-handler.schema.json) and referenced by the main
schema. The parser stores `cache` as a raw JSON object rather than defining every cache-handler
field in Rust; generation places those fields in the native `Configuration.DefaultCache` envelope
before rewrite and reverse-proxy handlers. Every proxy route starts with Caddy's standard `encode`
handler using Zstandard, gzip, and the default 512-byte threshold. Placing `encode` outside the
cache handler keeps cached representations independent of a client's `Accept-Encoding`. During
deployment, the main process passes the normalized services from the same Stack file to
`validate_and_normalize`;
no external validation service or network request is involved. This in-process check is
authoritative because the standard JSON Schema implementation used by VS Code cannot compare a
route backend with arbitrary keys and ports in the sibling `services` map.

Set `x-swarmlite.name` to let `swarmlite deploy` use a Stack name from the document. An explicit
command-line Stack name takes precedence.

`x-swarmlite.trusted_proxies` supplies the default IP/CIDR list for every route's generated Caddy
`reverse_proxy` handler. `http_routes[].trusted_proxies` overrides that default; an explicit empty
list disables it for that route. The Caddy-compatible `private_ranges` shortcut is expanded during
normalization.

`x-swarmlite.registries.<host>` accepts a required `username` and `password`. The parser keeps
credentials separate from normalized Service specifications and redacts passwords from debug
output. The main crate performs authoritative registry-host and credential validation before
merging them into cluster state.

## Supported service fields

The service parser intentionally implements a focused Compose/Swarm subset. Unknown fields are
rejected instead of being silently ignored. The complete supported surface is:

| Field | Supported forms |
| --- | --- |
| `image` | Non-empty image reference; required |
| `pull_policy` | `always`, `missing` (default), or `never` |
| `command`, `entrypoint` | String with shell-style word splitting, or string array |
| `environment` | Scalar map or string array |
| `labels` | Scalar map or string array; attached to task containers |
| `expose` | Internal target number or `target[/tcp|udp]`; port ranges are not supported |
| `ports` | Target number, `target[/tcp|udp]`, or long syntax with `target` and optional `protocol`; the container runtime assigns the host port |
| `volumes` | Node-local named volumes, bind mounts, or anonymous volumes; short syntax or long syntax with `target`, optional `source` and `read_only` |
| `configs` | Top-level config name, or long syntax with `source`, optional absolute `target`, numeric `uid`/`gid`, and octal `mode` |
| `healthcheck` | `test`, `disable`, `interval`, `timeout`, `retries`, `start_period`, and `start_interval` |
| `stop_grace_period` | Human-readable duration; defaults to `10s` |
| `deploy.mode` | Only `replicated` |
| `deploy.replicas` | Non-negative integer; defaults to `1` |
| `deploy.labels` | Scalar map or string array; stored as service metadata |
| `deploy.placement.constraints` | Hard `node.id` or `node.labels.*` comparisons using `==` or `!=` |
| `deploy.placement.max_replicas_per_node` | Non-negative steady-state per-node limit; unset or `0` means unlimited; `start-first` replacements may temporarily exceed it |
| `deploy.update_config` | `parallelism` and `order: start-first|stop-first` |

An internal route may omit `backend.port` when its Service declares exactly one distinct TCP
target across `expose` and `ports`. A Service with zero or multiple TCP targets requires an
explicit declared target. External `backend.host` routes always require `port`.

Fixed `published` values are rejected. The container runtime assigns an ephemeral host port so
replicated and `start-first` Services can run old and replacement tasks on the same node without a
port collision.

Top-level `configs.<name>.file` paths are resolved by the CLI relative to the Stack file. Config
contents are uploaded separately from the Compose YAML and resolved to immutable SHA-256 digests
before the normalized Service specifications are persisted. External configs are not supported.

See [../../examples/services-all.yaml](../../examples/services-all.yaml) for every accepted shape in
one Stack file. Fields such as `build`, `depends_on`, `networks`, `restart`, `resources`, and
`secrets` are not currently implemented.
