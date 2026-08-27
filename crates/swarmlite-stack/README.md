# swarmlite-stack

`swarmlite-stack` owns Swarmlite's Stack configuration domain:

- Docker Compose/Swarm-compatible `services` parsing and normalization;
- the internal `ServiceSpec`, port, and healthcheck models;
- the `x-swarmlite` routing data model;
- the optional `x-swarmlite.name` default used by the deploy CLI;
- normalization and validation;
- internal service port discovery from route references;
- deterministic rule ordering;
- Caddy JSON generation.

The crate has no dependency on controllers, scheduling, SQLite, or a container runtime. It
parses a complete Stack file in one pass, validates `backend.service` against that file's `services`
map, and exposes a renderer callback which resolves an internal `(stack, service, port)` into
healthy dial addresses. The main `swarmlite` crate implements that runtime adapter from replicated
cluster state.

The editor schema is at [schema/stack.schema.json](schema/stack.schema.json). During deployment, the
main process passes the normalized services from the same Stack file to `validate_and_normalize`;
no external validation service or network request is involved. This in-process check is
authoritative because the standard JSON Schema implementation used by VS Code cannot compare a
route backend with arbitrary keys and ports in the sibling `services` map.

Set `x-swarmlite.name` to let `swarmlite deploy` use a Stack name from the document. An explicit
command-line Stack name takes precedence.

## Supported service fields

The service parser intentionally implements a focused Compose/Swarm subset. Unknown fields are
rejected instead of being silently ignored. The complete supported surface is:

| Field | Supported forms |
| --- | --- |
| `image` | Non-empty image reference; required |
| `pull_policy` | `always`, `missing` (default), `if_not_present` (alias for `missing`), or `never` |
| `command`, `entrypoint` | String with shell-style word splitting, or string array |
| `environment` | Scalar map or string array |
| `labels` | Scalar map or string array; attached to task containers |
| `expose` | Internal target number or `target[/tcp|udp]`; port ranges are not supported |
| `ports` | Target number, `target[/tcp|udp]`, `published:target[/tcp|udp]`, or long syntax with `target`, optional `published` and `protocol` |
| `volumes` | Docker short bind syntax, or long syntax with `target`, optional `source` and `read_only` |
| `healthcheck` | `test`, `disable`, `interval`, `timeout`, `retries`, `start_period`, and `start_interval` |
| `stop_grace_period` | Human-readable duration; defaults to `10s` |
| `deploy.mode` | Only `replicated` |
| `deploy.replicas` | Non-negative integer; defaults to `1` |
| `deploy.labels` | Scalar map or string array; stored as service metadata |
| `deploy.placement.constraints` | Hard `node.id`, `node.hostname`, or `node.labels.*` comparisons using `==` or `!=` |
| `deploy.update_config` | `parallelism` and `order: start-first|stop-first` |

An internal route may omit `backend.port` when its Service declares exactly one distinct TCP
target across `expose` and `ports`. A Service with zero or multiple TCP targets requires an
explicit declared target. External `backend.host` routes always require `port`.

Published ports are node-local. A fixed `published` value allows only one task per protocol on a
node, so replicated routed Services should normally use `expose` and dynamically allocated task
ports. Fixed published ports may also require `deploy.update_config.order: stop-first` when no
spare node is available for a replacement.

See [../../examples/services-all.yaml](../../examples/services-all.yaml) for every accepted shape in
one Stack file. Fields such as `build`, `depends_on`, `networks`, `restart`, `resources`, `configs`,
and `secrets` are not currently implemented. Compatibility-only `ports.mode` and `volumes.type`
are also rejected because Swarmlite does not preserve or interpret them.
