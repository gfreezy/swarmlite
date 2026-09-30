# Reference

[← README](../README.md)

- [Command reference](#command-reference)
- [Schemas and examples](#schemas-and-examples)
- [Default ports and paths](#default-ports-and-paths)
- [Generic KV API](#generic-kv-api)
- [Current limitations](#current-limitations)

## Command reference

Run `swarmlite COMMAND --help` for complete arguments.

```text
ui                   open a local web console with inspection and CLI operations
init                 initialize a single-controller cluster
join                 configure another node from cluster settings
join-token           print the generated join command
connection-info      print the stored Controller address and cluster token
upgrade              install the latest or a selected GitHub Release
serve                run this node's fixed components
config get|set|unset|explain read, update, clear, or describe cluster-wide settings
gateway status|enable|disable
                     inspect all Gateways or update one node's Gateway switch
node stats [NODE] [--watch] [--json] [--history 5m|15m|1h|24h|7d|30d|365d]
node label get|set|remove
                     read or update one node's placement labels
registry login       store private registry credentials
service scale|restart
                     scale or roll long-running Services
job run|history|cancel
                     manage one-shot and scheduled jobs
deploy               deploy or update a Stack
deployment status [STACK]
deployment history [STACK]
deployment attach|retry|rollback STACK
                     inspect, follow, or recover Stack deployments
ls [STACK] [--json]  list Service and Job definitions
ps [TARGET]          list tasks, optionally for one Stack, Service, or Job
inspect TARGET       inspect a Service or Job definition and its tasks
logs SERVICE|JOB|TASK_NAME|TASK_ID
                     stream container logs
rm STACK             remove Stacks
status [--json]      inspect cluster state
```

There are no separate public `controller`, `agent`, or `gateway` runtime commands.

Human-readable output uses color automatically when its destination is a terminal: cyan identifies
resources, configured values, and active work; green marks healthy or successful states; yellow
marks pending or degraded states; red marks failures; magenta marks numeric values; and dim text
marks inactive or unset values. Use the global `--color auto|always|never` option or
`SWARMLITE_COLOR` to override
automatic detection; `NO_COLOR` disables color while the mode is `auto`. Explicit machine-readable
output (`--json`, `ps --quiet`, and `logs --raw`) never adds styling.

## Schemas and examples

| Resource | Purpose |
| --- | --- |
| [`stack.schema.json`](https://raw.githubusercontent.com/gfreezy/swarmlite/main/crates/swarmlite-stack/schema/stack.schema.json) | Complete Stack schema and editor completion |
| [`cache-handler.schema.json`](https://raw.githubusercontent.com/gfreezy/swarmlite/main/crates/swarmlite-stack/schema/cache-handler.schema.json) | Native Gateway cache settings |
| [`examples/services-all.yaml`](../examples/services-all.yaml) | Service fields |
| [`examples/routing-all.yaml`](../examples/routing-all.yaml) | HTTP and HTTPS routing |
| [`examples/configs.yaml`](../examples/configs.yaml) | File-backed configs |
| [`examples/jobs.yaml`](../examples/jobs.yaml) | Scheduled and manual jobs |
| [Web UI guide](../ui/README.md) | Navigation, operations, monitoring and frontend development |
| [`docs/kv-api.md`](../docs/kv-api.md) | Generic Controller KV API |
| [`caddy-storage/README.md`](../caddy-storage/README.md) | Custom Gateway image and Caddy modules |

## Default ports and paths

| Value | Default | Purpose |
| --- | --- | --- |
| Controller API | TCP `17080` | Agent and management API |
| Gateway HTTP | TCP `80` | HTTP serving and redirect |
| Gateway HTTPS | TCP `443` | HTTPS serving |
| Caddy admin API | `127.0.0.1:2019` | Local atomic configuration |
| Staged Caddy admin API | `127.0.0.1:2020` | Temporary endpoint during Gateway replacement |
| Certificate sync admin API | `127.0.0.1:2021` | Temporary certificate snapshot helper endpoint |
| CLI and node process | `/usr/local/bin/swarmlite` | System installation binary |
| Node data | `/var/lib/swarmlite` | Identity, SQLite state, and Agent config cache |
| Installed runtime settings | `/etc/swarmlite/runtime.env` | Data directory, runtime, and socket |
| systemd unit | `/etc/systemd/system/swarmlite.service` | Node service |

Foreground user mode stores data under `$XDG_STATE_HOME/swarmlite` or
`$HOME/.local/state/swarmlite`.

## Generic KV API

The authenticated Controller KV API stores opaque base64 values with last-write-wins ordering from
the single Controller's SQLite transaction sequence. It has no built-in Caddy, certificate, or TLS
semantics.

Available endpoints are:

- `GET`, `PUT`, and `DELETE /v1/kv`
- `GET /v1/kv/keys`
- `GET /v1/kv/stat`
- `POST /v1/kv/locks/{acquire,renew,release}`

Optional-cache consumers should continue locally while it is unavailable. Request and response
formats are in [`docs/kv-api.md`](../docs/kv-api.md).

## Current limitations

- Linux Docker and Podman nodes are the intended production targets.
- Only replicated Services are supported; `deploy.mode: global` is rejected.
- Compose `build`, external `configs`, `secrets`, resource reservations, and autoscaling are not
  supported.
- Per-container resource statistics and interactive `exec` are not implemented.
  Host monitoring is available through `node stats` and the Web UI.
- Gateway routing supports the documented host, path, rewrite, backend, and cache model rather than
  arbitrary Caddy handlers.
