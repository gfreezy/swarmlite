# Run the cluster

[← README](../README.md)

- [Install, upgrade, and uninstall](#install-upgrade-and-uninstall)
- [Initialize the Controller and join nodes](#initialize-the-controller-and-join-nodes)
- [Manage Gateways](#manage-gateways)
- [Node monitoring](#node-monitoring)
- [Configure labels and deployment policy](#configure-labels-and-deployment-policy)
- [Check and maintain the cluster](#check-and-maintain-the-cluster)
- [Rebuild a lost Controller](#rebuild-a-lost-controller)
- [Secure the trusted network](#secure-the-trusted-network)

This section is for the person responsible for machines, networking, membership, and recovery.
Swarmlite has no separate day-two operations subsystem: inspect the cluster, fix the external
dependency, and let reconciliation continue.

## Install, upgrade, and uninstall

On a Linux systemd server:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh | sudo sh
```

The installer detects Docker or Podman, installs Docker when neither is present, verifies the
release checksum, installs the CLI and systemd unit, and enables the service without starting an
uninitialized node. Select rootful Podman explicitly with:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh \
  | sudo sh -s -- --runtime podman
```

Upgrade an installed node with:

```bash
sudo swarmlite upgrade
```

Pass `--version` with an existing release tag to pin a release. Upgrade Controllers, Agents and
management clients together. A schema-11 Controller database must first be upgraded through
v0.1.41 before using a schema-12 release; see [storage compatibility](applications.md#scheduled-jobs).

The macOS ARM64 installer installs the CLI only and should run without `sudo`. It currently
requires an accessible Docker-compatible socket (for example, a running OrbStack or Docker Desktop):

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh | sh
```

Uninstall Swarmlite while preserving node data and managed containers:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh \
  | sudo sh -s -- --uninstall
```

Add `--purge` only when you also intend to delete `/var/lib/swarmlite`. Neither mode removes
Docker, Podman, or managed workload containers.

## Initialize the Controller and join nodes

Initialize exactly one Controller:

```bash
sudo swarmlite init
sudo systemctl start swarmlite
```

The Controller listens on TCP `17080` by default. Use `--controller-port` to change it,
`--advertise-address` when automatic address detection is not reachable by other nodes, and
`--no-gateway` when ingress will run elsewhere:

```bash
sudo swarmlite init --advertise-address 10.0.0.21 --no-gateway
```

On the Controller, print the generated join command:

```bash
sudo swarmlite join-token
```

Install Swarmlite on the new node, run the printed command there, and start the service:

```bash
sudo swarmlite join http://10.0.0.21:17080 --token '<generated-token>'
sudo systemctl start swarmlite
```

Add `--gateway` when the new node should also accept ingress. A joined node runs an Agent; it does
not become another Controller.

## Manage Gateways

Read or change the Gateway switch from the Controller:

```bash
sudo swarmlite gateway status
sudo swarmlite gateway status --json
sudo swarmlite gateway enable node-a
sudo swarmlite gateway disable node-a
```

`gateway status` prints the shared Gateway configuration once, followed by every node's enabled,
address, rollout generation, retryability, and error state. Its JSON form preserves unset optional
configuration fields as `null`.

At least one Gateway must be enabled before deploying a Stack with HTTP routes.

> [!WARNING]
> Disabling a Gateway first commits and verifies its exact certificate manifest in the Controller;
> if that barrier fails, the Gateway is preserved. A successful disable then deletes the Caddy
> container and local volumes, including its autosave, recovery snapshot, and response cache. A
> later enable restores certificates from the Controller and regenerates the remaining state.

Gateway startup and configuration are best-effort. A Gateway error does not stop the Agent or
Controller, and the previously accepted Caddy configuration remains active when a new
configuration is rejected. Inspect errors with `swarmlite status` or
`swarmlite status --json`.

The default Gateway image is
`ghcr.io/gfreezy/swarmlite-caddy:v<VERSION>` matching Swarmlite. Managed clusters advance it during
upgrade. Pinning `gateway.image` makes it user-managed:

```bash
sudo swarmlite config set gateway.image registry.example.com/swarmlite-caddy:1.1.0
sudo swarmlite config get
```

Gateway configuration changes are normally loaded in place without restarting Caddy. When the
requested image resolves to the same local image digest and the container runtime settings have
not changed, an image-reference change is also handled in place.

An actual image-digest or runtime change uses a single-node blue/green replacement. Swarmlite
starts an empty candidate on host networking with only its loopback admin endpoint, restores its
certificate snapshot from the Controller, writes the Controller-generated recovery snapshot, and
loads the public `80`/`443` listeners only after preparation succeeds. Caddy's listener reuse lets
the prepared candidate overlap the active Gateway; Swarmlite then gracefully drains and removes
the old container. The online Gateway always exposes its loopback admin API on `127.0.0.1:2019`;
the candidate uses `127.0.0.1:2020` only while it overlaps the old process, then moves its admin
listener to `2019` without restarting. A failed preparation leaves the old Gateway serving.

Each candidate receives fresh `/data`, `/config`, and `/cache` volumes. Certificate files are
verified against an exact Controller manifest; Caddy autosave and Swarmlite recovery data are
regenerated from the Controller; the response cache is disposable and starts cold. Consequently,
Gateway container compatibility is not controlled by a Gateway or autosave schema label. The
native cache keeps its own internal SQLite migration behavior, and the Controller recovery
snapshot keeps its existing recovery-format validation.

| Data | Replacement source and compatibility rule |
| --- | --- |
| TLS certificates and account state | Opaque files from the exact Controller manifest; size and SHA-256 must match, with no new format/version conversion |
| Active Caddy config and autosave | The Controller sends the complete current config; the candidate writes a fresh autosave after `/load` |
| Swarmlite recovery snapshot | The Controller sends the current snapshot; its existing cluster/generation validation still applies |
| Native response cache | Not transferred; Green starts with a fresh cache database, whose SQLite schema remains internal to the cache module |
| Caddy instance ID, storage-clean timestamps, and lock files | Not transferred; they are instance-local and regenerated |

Gateway replacement requires the current blue/green container layout. Custom images and
listeners are described in [`caddy-storage/README.md`](../caddy-storage/README.md).

Mutable cluster settings use dotted scopes. Optional settings are omitted until explicitly set; an
explicit `0` or `false` remains distinct from an unset value. Clear a value with
`swarmlite config unset KEY`; optional Proxy and Caddy settings become unset, while clearing
`gateway.image`, `gateway.listen`, or a deployment setting restores the Swarmlite default.
Unknown fields found while loading persisted cluster settings are ignored and disappear on the
next save; CLI keys and supported values remain validated by both the CLI and Controller.

Configuration discovery combines Docker-style command help with scoped, `kubectl explain`-style
details:

```bash
# Complete mutable configuration. Unset optional values are null in this JSON output.
swarmlite config get

# One current value. Unset values print, for example, "unset (Caddy default)".
swarmlite config get gateway.metrics.enabled

# All keys, a dotted scope, or one key with its current/default/apply details.
swarmlite config explain
swarmlite config explain proxy
swarmlite config explain gateway.logging
swarmlite config explain gateway.cache
swarmlite config explain gateway.cache.admission
swarmlite config explain gateway.cache.sqlite
swarmlite config explain gateway.http.timeouts
swarmlite config explain gateway.logging.access.format

# The set help continues to enumerate every accepted key.
swarmlite config set --help
```

Only the dotted key names below are accepted. Scope segments use `.`, while compound words within
one segment use `-`. Invalid values report the applicable enum candidates or numeric constraints.

| Key | Value | Effect |
| --- | --- | --- |
| `agent.image-prune.enabled` | `true`/`false` | Periodically remove all images unused by any container on every node; default `true` |
| `agent.image-prune.interval-seconds` | positive integer | Delay between unused-image prune operations; default 604800 seconds (7 days) |
| `proxy.http` | absolute proxy URL | Controller proxy for HTTP destinations; supports HTTP, HTTPS, SOCKS5, and SOCKS5H URLs |
| `proxy.https` | absolute proxy URL | Controller proxy for HTTPS destinations; supports HTTP, HTTPS, SOCKS5, and SOCKS5H URLs |
| `proxy.all` | absolute proxy URL | Fallback Controller proxy for protocols without a specific proxy |
| `proxy.no-proxy` | comma-separated host/address list | Destinations that bypass configured proxies |
| `gateway.image` | OCI image reference | Gateway image; replaces the container only when the resolved image digest changes |
| `gateway.listen` | comma-separated addresses | Published Gateway listeners; loaded through Caddy's Admin API |
| `gateway.metrics.enabled` | `true`/`false` | HTTP request metrics on the online Gateway's fixed local admin endpoint (`127.0.0.1:2019`) |
| `gateway.metrics.per-host` | `true`/`false` | Host-labelled metrics; high-cardinality hosts can consume more memory |
| `gateway.cache.max-size-bytes` | positive integer | Logical response-cache capacity per Gateway; default 1 GiB |
| `gateway.cache.low-water-percent` | `1`–`99` | Target usage after LRU eviction; default 90% |
| `gateway.cache.admission.window-seconds` | positive integer | Window for tracking uncached request frequency; default 300 seconds |
| `gateway.cache.admission.cache-after-requests` | `1`–`8` | Cache on this request number within the admission window; default 3 |
| `gateway.cache.sqlite.touch-window-seconds` | positive integer | Persist at most one LRU access update per cache entry in this window; default 300 seconds |
| `gateway.cache.sqlite.cache-size-kib` | non-negative integer | SQLite page cache per connection; `0` uses SQLite default |
| `gateway.cache.sqlite.mmap-size-bytes` | non-negative integer | SQLite mmap limit per read connection; default 256 MiB, `0` disables mmap |
| `gateway.cache.sqlite.read-connections` | `1`–`16` | Query-only SQLite reader pool; default 4 |
| `gateway.cache.sqlite.busy-timeout-seconds` | positive integer | SQLite operation/lock timeout; default 5 seconds |
| `gateway.cache.sqlite.cleanup-interval-seconds` | positive integer | Expiry cleanup and capacity-check interval; default 300 seconds |
| `gateway.cache.sqlite.journal-size-limit-bytes` | positive integer | WAL retention limit after checkpoints; default 64 MiB |
| `gateway.logging.runtime.level` | `debug`, `info`, `warn`, `error` | Caddy runtime log level; output is fixed to stderr |
| `gateway.logging.access.enabled` | `true`/`false` | HTTP access logs; output is fixed to stdout |
| `gateway.logging.access.format` | `json`, `console` | Access log encoder |
| `gateway.logging.access.sampling.enabled` | `true`/`false` | Access log sampling with a fixed one-second window |
| `gateway.logging.access.sampling.first` | non-negative integer | Entries retained first in each sampling window |
| `gateway.logging.access.sampling.thereafter` | non-negative integer | Retain one entry per this many after the initial entries |
| `gateway.shutdown.grace-period-seconds` | non-negative integer | Caddy connection drain period; `0` means unlimited |
| `gateway.http.timeouts.read-header-seconds` | non-negative integer | Request-header read timeout |
| `gateway.http.timeouts.read-body-seconds` | non-negative integer | Request-body read timeout |
| `gateway.http.timeouts.write-seconds` | non-negative integer | Response write timeout |
| `gateway.http.timeouts.idle-seconds` | non-negative integer | Keep-Alive idle timeout |
| `gateway.http.max-header-bytes` | non-negative integer | Maximum request-header bytes |
| `gateway.http.http3-enabled` | `true`/`false` | HTTP/3 on the Gateway UDP 443 listener |

For example:

```bash
swarmlite config set agent.image-prune.enabled false
swarmlite config set agent.image-prune.interval-seconds 86400
swarmlite config set gateway.metrics.enabled true
swarmlite config set gateway.cache.max-size-bytes 2147483648
swarmlite config set gateway.cache.low-water-percent 85
swarmlite config set gateway.cache.admission.window-seconds 300
swarmlite config set gateway.cache.admission.cache-after-requests 3
swarmlite config set gateway.cache.sqlite.touch-window-seconds 300
swarmlite config set gateway.cache.sqlite.mmap-size-bytes 268435456
swarmlite config set gateway.logging.access.enabled true
swarmlite config set gateway.logging.access.format json
swarmlite config set gateway.http.timeouts.read-header-seconds 10
swarmlite config unset gateway.http.timeouts.read-header-seconds
```

Image pruning uses the node's Docker-compatible native prune API with `dangling=false`, equivalent
to `docker image prune -a -f`. It affects every image unused by both running and stopped containers
on that node, including images pulled outside Swarmlite. The first cleanup waits for one complete
interval after the Agent starts or after either image-prune setting changes.

## Node monitoring

Node monitoring is available without a Stack YAML file:

```bash
swarmlite node stats
swarmlite node stats node-a --watch
swarmlite node stats node-a --history 24h
swarmlite node stats node-a --history 365d --json
```

Linux Agents sample host CPU, I/O wait, load, memory/Swap, local filesystem capacity/inodes,
block-device I/O and network counters every five seconds. CPU is normalized across all cores;
memory usage uses `MemAvailable`; filesystem percentages use `used / (used + available)`.
Disk I/O totals exclude partitions and stacked devices; network totals exclude loopback and
virtual interfaces. Per-device/interface details remain visible. The first rate sample is unavailable,
not zero. These are host metrics, not container resource limits. CLI output uses green below 75%,
yellow from 75%, and red from 90%; these are presentation thresholds, not alert rules. Stale readings
are explicitly marked. `NO_COLOR`, `--color never`, JSON and redirected output omit ANSI colors;
`--watch --json` emits NDJSON. An upgraded Linux Agent is required for live readings.

The Controller retains only the latest full sample per node, a 64-point mailbox and a 256-point
compact write batch in memory. An independent worker writes `metrics.sqlite` in the Controller
state directory every 30 seconds (or when the batch fills), with WAL and a 512 KiB connection cache.
History queries do not hold the cluster-state lock. A crash can lose the unflushed batch; storage
failures are reported while live sampling continues. Historical points contain aggregate host values,
not repeated device inventories. SQLite pages freed by retention are reused.

| Stored resolution | Retention | Selected query ranges |
| --- | --- | --- |
| Original samples (~5 seconds) | 15 minutes | 5m, 15m |
| 1 minute | 24 hours | 1h, 24h |
| 1 hour | 30 days | 7d, 30d |
| 1 day | 365 days | 365d |

UTC buckets are updated incrementally from source samples in the same transaction as raw inserts.
Per-metric sums, valid-sample counts and maxima preserve correct sample-weighted averages across
batches and restarts; missing readings are not treated as zero. Duplicate source timestamps are
ignored. Expired buckets are cleaned every flush, even without active nodes. CLI history and the
Web Nodes page select resolution automatically. The Web time picker also supports custom start/end
intervals; the oldest requested timestamp determines available resolution. Bucket averages may
include values outside exact custom boundaries. History survives Controller restart; live freshness
is rebuilt from new samples. There is no long-term store beyond 365 days.

## Configure labels and deployment policy

Set node labels while initializing or joining:

```bash
sudo swarmlite init --label region=cn-east --label disk=nvme
sudo swarmlite join http://10.0.0.21:17080 \
  --token '<generated-token>' \
  --label region=cn-east
```

Change labels through the Controller after the node joins:

```bash
sudo swarmlite node label get node-a
sudo swarmlite node label set node-a region cn-north
sudo swarmlite node label remove node-a disk
```

A label change drains tasks that no longer satisfy their constraints and schedules replacements
on eligible live nodes.

Cluster-wide deployment and pull settings are also changed through the Controller:

```bash
swarmlite config set deployment.progress-deadline-seconds 600
swarmlite config set deployment.image-pull.idle-timeout-seconds 90
swarmlite config set deployment.image-pull.max-attempts 5
swarmlite config set deployment.image-pull.initial-backoff-seconds 2
swarmlite config set deployment.image-pull.max-backoff-seconds 60
```

Defaults are a 300-second progress deadline, a 60-second pull idle deadline, five pull attempts,
and exponential backoff from 2 to 60 seconds.

## Check and maintain the cluster

Start with:

```bash
sudo swarmlite status
sudo swarmlite status --json
sudo systemctl status swarmlite
sudo journalctl -u swarmlite -f
```

The human-readable `status` output includes cluster Issues. The JSON output exposes structured
details such as `gateway.endpoint_errors`, `recovery.awaiting_adoption`, and
`recovery.conflicting_slots`.

It is safe to restart `swarmlite serve` independently on the Controller or any Agent:

```bash
sudo systemctl restart swarmlite
```

Running containers, runtime-owned host-port mappings, and Caddy's accepted configuration continue
serving. Management pauses only where its control dependency is unavailable:

| Interruption | Serving data plane | Temporarily unavailable |
| --- | --- | --- |
| Controller restart | Existing workloads and routes continue | Deployments, scheduling, and cluster-wide coordination |
| Agent restart | Containers and host ports on that node continue | Reconciliation and logs for that node |
| Controller-Agent partition | Both sides retain their last applied state | Fresh coordination between the two sides |

After reconnecting, Agents inspect runtime labels, adopt matching containers, and reconcile actual
differences. A long partition can leave an old container serving while the Controller schedules a
replacement elsewhere; applications that require strict single-instance behavior must provide
their own coordination.

## Rebuild a lost Controller

The Controller database is not the primary recovery mechanism. The data plane already preserves
the important serving state:

- Docker or Podman retains workload containers and host-port mappings;
- Caddy retains the last accepted routes and working upstreams in its persistent `/config` volume;
- managed container labels retain the identities and specifications needed for adoption.

Keep the declarative Stack files outside the cluster. A Controller database backup is useful only
when control-plane-only records such as deployment history must also survive.

To rebuild, stop `swarmlite serve` on every node. Choose a machine that still has a managed Gateway
container and its persistent `/config` volume, then run:

```bash
sudo systemctl stop swarmlite
sudo swarmlite init --recover
sudo systemctl start swarmlite
```

Recovery reads the highest valid structured route snapshot, archives replaced local state under
`recovery-backup/`, and imports the route directory before reconciliation starts. Equal-generation
snapshots with different contents are a hard conflict. If no valid snapshot exists, recovery
refuses to start a Controller that could publish an empty Gateway configuration.

The recovered routes and old upstreams remain active while nodes rejoin. Recovery rotates the join
token but does not delete workload containers. Print the new join command:

```bash
sudo swarmlite join-token
```

Run that command on every other node and start its `swarmlite` service. Then redeploy the original
files under the same Stack names:

```bash
sudo swarmlite deploy --compose-file stack.yaml demo
```

Matching containers are adopted; redeploying replaces each recovered route fragment with the
complete desired Stack definition.

## Secure the trusted network

Every Agent needs access to its Docker or Podman socket; treat that access as root-equivalent.
Every advertised node address and allocated task port must be reachable from all Gateways.
Swarmlite does not configure firewalls, traverse NAT, create an overlay network, or provide
cross-node DNS.

The Controller-Agent bearer token authenticates requests but plain HTTP does not provide
confidentiality or transport integrity. Do not expose TCP `17080` to the public Internet. Put the
cluster on a trusted private network, restrict it with host or network firewalls, and use WireGuard
or another VPN when the underlying network is not trusted. TLS termination in front of the
Controller is also supported operationally; management clients can use SSH mode.

Override runtime detection only when needed:

```bash
swarmlite serve --runtime podman --runtime-socket /run/podman/podman.sock
```
