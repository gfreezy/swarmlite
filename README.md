# Swarmlite

Swarmlite is a small Rust container orchestrator for machines in one LAN or region. It provides a
single control plane, multi-node scheduling, rolling deployments, service logs, placement rules,
and optional HTTPS routing without the operational weight of Kubernetes.

> [!IMPORTANT]
> Swarmlite is an MVP, not a drop-in replacement for Docker Swarm or Kubernetes.

## Design goals and tradeoffs

The central rule is that the control plane may pause while the serving data plane continues.

| Goal | Architectural decision | Accepted tradeoff |
| --- | --- | --- |
| Keep services online while control processes restart | Docker or Podman owns containers and port mappings; Caddy persists its accepted configuration | Deployments, logs, and reconciliation pause when their control dependency is unavailable |
| Keep the control plane small and replaceable | One fixed Controller uses SQLite while running; recovery rebuilds it from the surviving data plane | No election or automatic failover; control-plane-only history is not reconstructed |
| Keep networking direct and observable | Agents use authenticated HTTP/JSON; Gateways route to dynamic host ports | The trusted network must provide reachability, firewalling, and transport protection when needed |
| Preserve availability during partitions | Disconnects freeze the last applied state; deletion requires an explicit desired-state command | A long partition can temporarily leave duplicate or stale containers, so strict singleton workloads need application-level coordination |

## Contents

- [Quick start](#quick-start) — deploy two services behind one HTTPS domain.
- [Deploy applications](#deploy-applications) — define, update, route, and inspect Stacks.
- [Run the cluster](#run-the-cluster) — install nodes, operate Gateways, and recover the Controller.
- [How Swarmlite works](#how-swarmlite-works) — understand the architecture and its deliberate tradeoffs.
- [Reference](#reference) — find commands, schemas, paths, ports, and current limitations.
- [Development](#development) — build and test Swarmlite.

## Quick start

This path creates a single-node Linux cluster, deploys two services, and publishes them under one
domain with automatic HTTPS.

### 1. Prepare the server and domain

You need:

- a Linux server with systemd;
- a domain such as `app.example.com` whose DNS A or AAAA record points to the server;
- inbound TCP ports `80` and `443` open to the Internet so Caddy can serve traffic and obtain a
  certificate.

Replace `app.example.com` in the commands and Stack file below with your real domain.

### 2. Install and initialize Swarmlite

The installer reuses Docker or Podman when available and installs Docker when neither is present:

```bash
curl -fsSL https://github.com/gfreezy/swarmlite/releases/latest/download/install.sh | sudo sh
sudo swarmlite init
sudo systemctl start swarmlite
sudo swarmlite status
```

The initialized node is the cluster's fixed Controller. It also runs an Agent, and its Gateway is
enabled by default.

### 3. Deploy two services with automatic HTTPS

Create `swarmlite.yaml`:

```yaml
services:
  web:
    image: nginx:alpine
    expose:
      - "80"
    deploy:
      replicas: 1

  api:
    image: traefik/whoami:latest
    expose:
      - "80"
    deploy:
      replicas: 1

x-swarmlite:
  name: demo
  tls: serve
  http: redirect
  http_routes:
    - hostnames: [app.example.com]
      rules:
        - matches:
            - path: /api
          rewrite:
            strip_prefix: true
          backend:
            service: api
        - backend:
            service: web
```

Deploy the Stack:

```bash
sudo swarmlite deploy
```

The Gateway obtains and renews the certificate automatically. Requests under `/api` go to
`demo.api`; all other paths go to `demo.web`. Docker or Podman chooses a free host port for every
task, and Swarmlite updates the Gateway automatically.

Verify HTTPS and the HTTP-to-HTTPS redirect:

```bash
curl https://app.example.com/
curl https://app.example.com/api/
curl -I http://app.example.com/
```

`deploy` waits for the desired state to become healthy. `--detach` returns after the Controller
accepts the deployment. Closing the CLI only detaches the observer; it does not cancel the desired
state.

### 4. Use the everyday commands

Services use the qualified name `STACK.SERVICE`:

```bash
sudo swarmlite ls
sudo swarmlite ps demo
sudo swarmlite inspect demo.web
sudo swarmlite logs --tail 200 demo.web
sudo swarmlite logs --follow demo.api
sudo swarmlite scale demo.web=2
sudo swarmlite restart demo.api
sudo swarmlite deployment status demo
sudo swarmlite deployment attach demo
```

### 5. Deploy from another machine over SSH

Install the Swarmlite CLI on your workstation, keep `swarmlite.yaml` there, and point management
commands at the server:

```bash
swarmlite deploy --controller ssh://root@server.example.com
swarmlite ps --controller ssh://root@server.example.com demo
swarmlite logs --controller ssh://root@server.example.com --follow demo.web
```

Swarmlite opens a temporary SSH tunnel and reads the protected Controller connection settings on
the server. Application traffic and node-to-Controller traffic do not use this tunnel.

### 6. Clean up or expand

Remove the example Stack:

```bash
sudo swarmlite rm demo
```

To add nodes or configure more Gateways, continue with [Run the cluster](#run-the-cluster). For
production Stack options, see [Deploy applications](#deploy-applications).

## Deploy applications

This section is for application owners. Cluster installation, node membership, and recovery are in
[Run the cluster](#run-the-cluster).

### Stack file basics

Swarmlite reads `swarmlite.yaml` from the current directory by default. It supports a focused
Docker Compose/Swarm service model and keeps Swarmlite-specific settings under `x-swarmlite`:

```yaml
services:
  api:
    image: example/api:1.0
    expose:
      - "8080"
    deploy:
      replicas: 2

x-swarmlite:
  name: production
```

Use `--compose-file` or `-c` for another file. The name in `x-swarmlite.name` is the default Stack
name; a positional name overrides it:

```bash
swarmlite deploy
swarmlite deploy --compose-file stack.yaml
swarmlite deploy temporary-preview
swarmlite deploy --dry-run
```

`--dry-run` performs the same parsing and Controller preflight checks as a deployment without
changing cluster state. Editor completion is available through
[`stack.schema.json`](https://raw.githubusercontent.com/gfreezy/swarmlite/main/crates/swarmlite-stack/schema/stack.schema.json):

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/gfreezy/swarmlite/main/crates/swarmlite-stack/schema/stack.schema.json
```

See [`examples/services-all.yaml`](examples/services-all.yaml) for Service fields and
[`examples/routing-all.yaml`](examples/routing-all.yaml) for routing fields.

### Task environment templates

Environment values support the same Go-template context names as Docker Swarm. The Agent expands
them after the task is assigned to a node and immediately before creating the container. The
environment variable name is literal and chosen by the Stack author; only the value after `=` is
expanded:

```yaml
services:
  api:
    image: example/api:1.0
    environment:
      SERVICE_NAME: "{{.Service.Name}}"
      TASK_INSTANCE: '{{join "-" .Service.Name .Task.Slot}}'
      TASK_ID: "{{.Task.ID}}"
      NODE_HOSTNAME: "{{.Node.Hostname}}"
      SERVICE_OWNER: '{{index .Service.Labels "com.example.owner"}}'
    deploy:
      labels:
        com.example.owner: platform
```

The available context matches SwarmKit:

| Template | Swarmlite value |
| --- | --- |
| `.Service.ID` | Stack-qualified Service ID, such as `production.api` |
| `.Service.Name` | Service name from the Stack file, such as `api` |
| `.Service.Labels` | Service metadata from `deploy.labels` |
| `.Node.ID` | Swarmlite node ID |
| `.Node.Hostname` | Hostname detected by the Agent |
| `.Node.Platform.Architecture` | Agent host architecture |
| `.Node.Platform.OS` | Agent host operating system |
| `.Task.ID` | Unique task ID |
| `.Task.Name` | `<service>.<slot>.<task-id>` |
| `.Task.Slot` | Stable one-based replica slot |

SwarmKit's `join` function and Go-template features such as `index`, `if`, comparisons, and
`printf` are available. A bare environment entry without `=` is left unchanged. Template syntax and field
names are case-sensitive; invalid templates are rejected while parsing the Stack.

### Deployment lifecycle

`deploy`, `scale`, `restart`, and `rm` submit desired state to the Controller and wait for
convergence by default. They support `--detach`. Deployments are durable and can be observed from a
new CLI process:

```bash
swarmlite deployment status
swarmlite deployment status production
swarmlite deployment status production --generation 42
swarmlite deployment attach production
swarmlite deployment history production
swarmlite deployment retry production
swarmlite deployment rollback production
swarmlite deployment rollback production --to-generation 40
swarmlite deploy --replace
```

Only one generation of a Stack may be active. A normal deploy returns `409 Conflict` while that
Stack is `reconciling`, `stalled`, or `blocked`. Attach to it, repair the dependency and retry it,
or deliberately supersede it with `--replace`. Different Stacks reconcile independently.

A deployment becomes:

- `healthy` when every desired replica is applied and healthy, obsolete tasks are gone, and all
  enabled Gateways have accepted the routes;
- `stalled` after the progress deadline passes without observable progress, and returns to
  `reconciling` automatically if progress resumes;
- `blocked` when operator action is required, such as fixing registry authentication or a port
  conflict;
- `failed` after a non-recoverable attempt error;
- `superseded` when another generation intentionally replaces it.

The default progress deadline is 300 seconds of inactivity, not a maximum total deployment time.
`restart` always increments the Service revision and performs its configured rolling replacement.
Old healthy tasks remain routable until their replacements are healthy and Gateways accept the new
upstreams.

### Inspect services, tasks, and logs

`deploy`, `ls`, and `rm` target Stacks. `inspect`, `scale`, and `restart` target a Service in
`STACK.SERVICE` form. `ps` accepts a Stack or Service. `logs` accepts a Service, task name, or task
ID:

```bash
swarmlite ls [STACK]
swarmlite ps [STACK|SERVICE]
swarmlite inspect STACK.SERVICE
swarmlite logs --tail 200 STACK.SERVICE
swarmlite logs --follow STACK.SERVICE
swarmlite logs --follow STACK.SERVICE.SLOT
swarmlite scale STACK.SERVICE=3
swarmlite restart STACK.SERVICE
swarmlite rm STACK
```

During a rolling update, an old and new task may temporarily share a slot name. Use the task ID
from `swarmlite ps` to select one exactly. Log sessions select at most 64 tasks and cap `--tail` at
10,000 lines.

### Images and private registries

Control image checks per Service with `pull_policy`:

```yaml
services:
  api:
    image: ghcr.io/example/api:latest
    pull_policy: always
```

Supported values are `always`, `missing` (the default), and `never`. `always`, and `missing` for
an omitted or `latest` tag, compare the pulled image ID with running tasks. An unchanged ID does
not restart them; a changed ID uses the normal rolling-update path.

Store private registry credentials once on the Controller. The password or token is read from
standard input and synchronized to Agents:

```bash
printf '%s' "$GHCR_TOKEN" | sudo swarmlite registry login ghcr.io \
  --username github-user --password-stdin
```

Credentials can also be declared under `x-swarmlite.registries`, but then remain plain text in the
Stack file. Keep that file private and never commit real credentials. Registry credentials are
stored in protected Controller and Agent state; they are omitted from status and Service
specifications.

### Image proxy

When an image proxy is configured and can reach the target Registry, image pulls are relayed
through the Controller without changing Docker or Podman service settings. Before each pull, the
Agent probes the target manifest through its ephemeral loopback-only Registry relay. A successful
probe enables reference rewriting for that pull; no proxy configuration, an unreachable proxy, or
an unreachable target Registry preserves the runtime's normal direct pull path. Gateway image
upgrades use this same decision path. After a proxied pull, Swarmlite restores the original tag and
removes the temporary relay tag, so normal tagged images remain visible under their original names
in `docker image ls` and container inspection. Digest-pinned images are created by image ID and may
retain their relay repository digest until the runtime prunes the image.

The Controller serves this pull-only Registry on its existing port under `/v2/*`. It handles
upstream authentication and keeps a content-addressed cache shared by all nodes. Cached objects
expire 30 minutes after their last access; a scan runs every 5 minutes, and abandoned partial
downloads expire after one hour. There is no size or LRU policy. A cache write failure falls back
to an uncached upstream pull. Node image storage is owned by Docker or Podman; use the runtime's
normal `image prune` policy when node disk reclamation is required.

Only the Controller needs persistent outbound proxy settings. Configure protocol-specific proxies
independently, or set `proxy.all` as their fallback:

```bash
sudo swarmlite config set proxy.http http://proxy.example.com:3128
sudo swarmlite config set proxy.https http://proxy.example.com:3128
sudo swarmlite config set proxy.no-proxy registry.internal.example.com
```

HTTP, HTTPS, SOCKS5, and SOCKS5-with-proxy-DNS URLs are accepted. To use one SOCKS proxy for every
destination, set `proxy.all`; `socks5h` is recommended so DNS resolution also happens through the
proxy:

```bash
sudo swarmlite config set proxy.all socks5h://proxy.example.com:1080
```

Changes apply to the Controller image proxy without restarting it. `proxy.http` and `proxy.https`
override `proxy.all` for their respective destination protocols; `proxy.no-proxy` alone does not
enable proxying. Clear a value with `swarmlite config unset KEY`.

`swarmlite upgrade` first reads the conventional `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY`, and
`NO_PROXY` process environment, accepting both uppercase and lowercase names and preferring the
lowercase value when both cases are set. If no process proxy is configured and the Controller is
available, the command reads the cluster proxy configuration. It exports the selected values in
both cases to the installer, so the initial download and the installer's `curl` downloads follow
the same route. If neither source supplies a proxy, or a selected proxy is unavailable, the
download retries directly. A manual
`docker pull ghcr.io/example/api:latest` still contacts the original Registry directly; a later
`docker run` can reuse the original tag restored by Swarmlite.

### Config files, volumes, and placement

Use Compose `configs` to distribute a file beside the Stack file to every node that runs a Service:

```yaml
services:
  app:
    image: example/app:1.0
    configs:
      - source: app-config
        target: /etc/app/config.yaml
        uid: "103"
        gid: "104"
        mode: 0444

configs:
  app-config:
    file: ./config.yaml
```

The CLI resolves the file relative to the Stack file, uploads it by SHA-256 digest, and Agents
verify and cache it before creating containers. Changing the bytes rolls affected Services;
redeploying identical bytes does not. Each config is limited to 1 MiB and one deployment may
upload at most 8 MiB. External configs are not supported. See
[`examples/configs.yaml`](examples/configs.yaml).

Named volumes and bind mounts are node-local. Use node labels and placement constraints when data
or hardware ties a Service to specific machines:

```yaml
services:
  api:
    image: example/api:1.0
    deploy:
      replicas: 2
      placement:
        constraints:
          - node.labels.region == cn-north
          - node.labels.disk == nvme
        max_replicas_per_node: 1
```

Constraints are hard requirements. Swarmlite leaves a Service under-replicated when no eligible
node exists instead of ignoring them. `max_replicas_per_node: 0`, or omitting the field, means no
limit.

### HTTP and HTTPS routes

Gateway nodes listen on `:80` and `:443` by default. DNS must point each hostname at a Gateway.
`tls: serve` and `http: redirect` are the defaults, so Caddy obtains certificates and redirects
HTTP to HTTPS automatically.

Routes point to a Service in the same Stack:

```yaml
services:
  api:
    image: example/api:1.0
    expose:
      - "8080"

x-swarmlite:
  http_routes:
    - hostnames: [api.example.com]
      rules:
        - matches:
            - path: /v1
          rewrite:
            strip_prefix: true
          backend:
            service: api
```

When a Service declares exactly one TCP target across `expose` and `ports`, `backend.port` is
inferred. Multiple targets require an explicit declared target. Docker or Podman allocates an
ephemeral host port for every routed task; Gateways connect to
`node-advertise-address:allocated-host-port`.

Every proxied route enables Caddy response compression automatically; existing Stacks need no
configuration change. The Gateway prefers Zstandard when the client advertises `zstd`, falls back
to `gzip`, and leaves responses shorter than Caddy's default 512-byte minimum uncompressed. The
`encode` handler wraps cache and proxy handlers, so a cached rule stores the upstream representation
before client-specific content encoding is applied. Caddy also leaves an upstream response with an
existing `Content-Encoding` untouched and adds `Vary: Accept-Encoding` when it compresses a response.

For replicated routed Services, prefer `expose`. Fixed `ports.published` values are rejected so a
`start-first` replacement can coexist with the old task on one node.

Route features include:

- exact, prefix (default), and RE2-compatible regex path matches;
- `strip_prefix`, `replace_prefix`, or `replace_path` rewrites;
- internal Service backends and external `backend.host` targets;
- HTTP, HTTPS, and h2c upstream protocols;
- canonical-hostname redirects;
- per-route trusted proxy lists;
- optional node-local Caddy response caching.

Caching is an explicit route-level choice. The native Gateway handler stores responses directly in
SQLite and uses Souin's established `allowed_http_verbs` and `key` configuration names. The cache
key includes the method, query string, request-body hash, configured headers, and origin `Vary`
fields unless disabled by the supported key settings. For example:

```yaml
cache:
  ttl: 5m
  allowed_http_verbs: [GET, POST]
  max_cacheable_body_bytes: 10485760
  max_request_body_bytes: 1048576
  key:
    hash: true
    disable_query: true
    headers: [Accept-Language]
  status_codes: [200]
```

`key.disable_query` should be enabled only when every query parameter is irrelevant to the upstream
response.

When `allowed_http_verbs` is omitted, only `GET` responses are stored and `HEAD` may reuse a
matching `GET` response. CONNECT, protocol upgrades, range and conditional requests, request
`no-store`, and responses carrying `Set-Cookie` or `Content-Range` bypass storage. Authorization
does not affect cache eligibility or key identity, and response `Cache-Control` directives are
ignored. Concurrent misses for one key share a single origin request. SQLite failures fail open to
the origin. Each Gateway limits the logical cached response payload to 1 GiB by default; use the
cluster-level `gateway.cache.max-size-bytes` setting to change it. It samples cache hits,
deduplicates recent touches with a small Bloom filter, and asynchronously updates a separate SQLite
access table so LRU tracking does not rewrite rows containing response bodies. Expired entries are
removed first; capacity pressure then evicts approximately least-recently-used entries to a 90%
low-water mark. Capacity-rejected writes use an in-memory logical-usage check before starting a
SQLite transaction. Read connections map at most 256 MiB of pages by default to reduce random-read system calls.
Periodic cleanup incrementally returns bounded batches of free pages when fragmentation reaches
25%. Cache schema changes switch immediately to a fresh SQLite file and delete the old file
asynchronously; secure-delete is disabled because response-cache data is disposable. Expired
entries are not served; stale refresh and stale-on-error behavior are not part of the current cache
phase.

Rule precedence is exact path, longest prefix, regex, then a rule without matches. `tls` accepts
`serve|disabled` and `http` accepts `redirect|serve|disabled`; `http: redirect` requires
`tls: serve`. Native cache fields are described by
[`cache-handler.schema.json`](https://raw.githubusercontent.com/gfreezy/swarmlite/main/crates/swarmlite-stack/schema/cache-handler.schema.json).

### Remote management over SSH

Management commands accept `ssh://[user@]host[:port]` as a Controller URL. The system `ssh`
executable honors aliases and options from `~/.ssh/config`, including `ProxyJump`. The Stack file
stays on the local workstation.

For repeated commands, the Controller URL and default Stack file can be supplied through the
environment. Explicit command-line options take precedence:

```bash
export SWARMLITE_CONTROLLER=ssh://ubuntu@server.example.com
export SWARMLITE_COMPOSE_FILE=swarmlite-prod.yaml
swarmlite deploy
swarmlite ps demo
```

The remote Swarmlite CLI must match the local version and be able to read
`/var/lib/swarmlite`. Root SSH works directly. For a dedicated SSH user, allow only the
machine-readable connection command without a password prompt:

```sudoers
deploy ALL=(root) NOPASSWD: /usr/local/bin/swarmlite connection-info --json
```

The CLI invokes `sudo -n` and transfers the cluster token only through encrypted SSH output. SSH
mode is for management commands; cluster nodes still maintain their normal HTTP connection to the
Controller.

## Run the cluster

This section is for the person responsible for machines, networking, membership, and recovery.
Swarmlite has no separate day-two operations subsystem: inspect the cluster, fix the external
dependency, and let reconciliation continue.

### Install, upgrade, and uninstall

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

Pass `--version` with an existing release tag to pin a release. The macOS ARM64 installer installs
the CLI only and should run without `sudo`:

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

### Initialize the Controller and join nodes

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

### Manage Gateways

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

The first replacement of a legacy bridge-network Gateway has one short stop/bind transition,
because Docker's old host-port proxy cannot share `80`/`443` with the new host-network listener.
After that one-time migration, replacements use the overlapping blue/green path. Custom images and
listeners are described in
[`caddy-storage/README.md`](caddy-storage/README.md).

Mutable cluster settings use dotted scopes. Optional settings are omitted until explicitly set; an
explicit `0` or `false` remains distinct from an unset value. Clear a value with
`swarmlite config unset KEY`; optional Proxy and Caddy settings become unset, while clearing
`gateway.image`, `gateway.listen`, or a deployment setting restores the Swarmlite default.

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
| `gateway.cache.hit-sample-ratio` | positive integer | Sample one in N hits for LRU metadata; default 32 |
| `gateway.cache.access-update-interval-seconds` | positive integer | Minimum persisted access-update interval; default 300 seconds |
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

### Configure labels and deployment policy

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

### Check and maintain the cluster

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

### Rebuild a lost Controller

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

### Secure the trusted network

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

## How Swarmlite works

This section explains why the system is shaped this way. It is not an additional operating guide.

### Components and data flow

Every machine runs the same `swarmlite serve` process. Its fixed role determines which components
are active:

| Component | Runs where | Responsibility |
| --- | --- | --- |
| Controller | The node created with `init` | Stores desired state, schedules tasks, and exposes the API |
| Agent | Every node | Reconciles assigned containers with Docker or Podman |
| Gateway | Enabled per node | Runs Caddy and publishes HTTP/HTTPS routes |
| Stack | Cluster-wide | Groups services from one Stack file |
| Service | Inside a Stack | Defines an image, replicas, placement, ports, and update behavior |

The Controller and Agents are the control plane. Containers, operating-system port mappings, and
Caddy are the serving data plane. A control process tells the data plane what should run, but it is
not in the request path after that state has been applied.

### Why there is one fixed Controller

One Controller serializes scheduling, deployment, and routing decisions against one authoritative
SQLite state. Avoiding consensus, quorums, leader election, replicated logs, and split-brain
recovery keeps a small cluster understandable and makes causal history easy for operators and AI
tools to inspect.

This is viable because the Controller is not a traffic proxy. Losing it pauses new decisions but
does not stop containers or Caddy. Swarmlite therefore chooses a replaceable control plane over a
highly available control plane: there is no promotion, demotion, election, or automatic failover.
Recovery rebuilds desired state from the surviving data plane and the original Stack files.

### Why tasks bind ports on the host

Swarmlite does not create a cross-node container network. Docker or Podman allocates a host port,
the Agent reports it to the Controller, and Gateways route directly to
`node-advertise-address:allocated-host-port`.

The mapping belongs to the operating system and container runtime, so restarting the Agent does
not interrupt packets. Dynamic ports let replicas and `start-first` replacements coexist on the
same node. The cost is that nodes and Gateways must be mutually reachable, volumes remain
node-local, and Swarmlite provides no service VIP, routing mesh, NAT traversal, overlay network, or
cross-node DNS.

### Why Controller-Agent connections use HTTP

Control coordination deliberately uses authenticated HTTP with JSON payloads. Bulk logs use an
authenticated WebSocket session. The intended environment is a small trusted network, and a plain
protocol is easy to inspect, reproduce with `curl`, record in logs, and reason about during
AI-assisted debugging.

TLS inside Swarmlite would add certificate bootstrap, trust distribution, renewal, hostname
validation, and another recovery dependency. Swarmlite leaves transport protection to the trusted
network, its firewall or VPN, or an external TLS endpoint. This is an explicit security tradeoff,
not an accidental omission: the bearer token authenticates requests but cannot hide them from, or
protect them against modification by, a machine on the same network path.

### Availability model and non-goals

Managed workload and Gateway containers use the runtime's `unless-stopped` restart policy. They are
not child processes of `swarmlite serve`. An active Caddy starts with `--resume` and keeps its last
accepted configuration in its slot-local config volume. Replacement Gateways always start with
fresh slot volumes and rebuild Controller-owned state before they accept traffic; retired volumes
are deleted after a successful handoff. Agents persist task identity, Stack, Service, slot, revision,
specification hash, ports, and config digests as container labels, then adopt matching containers
after restarting.

Replaying the same desired state is idempotent. Disconnection freezes the last applied state;
deletion happens only from an explicit desired-state change. This favors availability of existing
traffic over strict singleton execution and immediate reconciliation.

Swarmlite is a good fit for a trusted LAN or region that needs Compose-style definitions,
replicated services, placement constraints, rolling updates, logs, and optional HTTPS routing.
Choose another orchestrator when you require control-plane high availability, an overlay network,
service VIPs, cross-node DNS, autoscaling, global services, or the broader Kubernetes ecosystem.

## Reference

### Command reference

The CLI exposes 20 top-level commands and 16 actionable subcommands in the grouped command trees.
Run `swarmlite COMMAND --help` for complete arguments.

```text
init                 initialize a single-controller cluster
join                 configure another node from cluster settings
join-token           print the generated join command
connection-info      print the stored Controller address and cluster token
upgrade              install the latest or a selected GitHub Release
serve                run this node's fixed components
config get|set|unset|explain read, update, clear, or describe cluster-wide settings
gateway status|enable|disable
                     inspect all Gateways or update one node's Gateway switch
node label get|set|remove
                     read or update one node's placement labels
registry login       store private registry credentials
deploy               deploy or update a Stack
deployment status [STACK]
deployment history [STACK]
deployment attach|retry|rollback STACK
                     inspect, follow, or recover Stack deployments
ls [STACK]           list Services
ps [TARGET]          list tasks, optionally for one Stack or Service
inspect SERVICE      inspect a Service
logs SERVICE|TASK_NAME|TASK_ID
                     stream container logs
scale SERVICE=N      scale replicated Services
restart SERVICE      roll a Service
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

### Schemas and examples

| Resource | Purpose |
| --- | --- |
| [`stack.schema.json`](https://raw.githubusercontent.com/gfreezy/swarmlite/main/crates/swarmlite-stack/schema/stack.schema.json) | Complete Stack schema and editor completion |
| [`cache-handler.schema.json`](https://raw.githubusercontent.com/gfreezy/swarmlite/main/crates/swarmlite-stack/schema/cache-handler.schema.json) | Native Gateway cache settings |
| [`examples/services-all.yaml`](examples/services-all.yaml) | Service fields |
| [`examples/routing-all.yaml`](examples/routing-all.yaml) | HTTP and HTTPS routing |
| [`examples/configs.yaml`](examples/configs.yaml) | File-backed configs |
| [`docs/kv-api.md`](docs/kv-api.md) | Generic Controller KV API |
| [`caddy-storage/README.md`](caddy-storage/README.md) | Custom Gateway image and Caddy modules |

### Default ports and paths

| Value | Default | Purpose |
| --- | --- | --- |
| Controller API | TCP `17080` | Agent and management API |
| Gateway HTTP | TCP `80` | HTTP serving and redirect |
| Gateway HTTPS | TCP `443` | HTTPS serving |
| Caddy admin API | `127.0.0.1:2019` | Local atomic configuration |
| Staged Caddy admin API | `127.0.0.1:2020` | Temporary endpoint during Gateway replacement |
| Certificate sync admin API | `127.0.0.1:2021` | Temporary legacy-migration helper endpoint |
| CLI and node process | `/usr/local/bin/swarmlite` | System installation binary |
| Node data | `/var/lib/swarmlite` | Identity, SQLite state, and Agent config cache |
| Installed runtime settings | `/etc/swarmlite/runtime.env` | Data directory, runtime, and socket |
| systemd unit | `/etc/systemd/system/swarmlite.service` | Node service |

Foreground user mode stores data under `$XDG_STATE_HOME/swarmlite` or
`$HOME/.local/state/swarmlite`.

### Generic KV API

The authenticated Controller KV API stores opaque base64 values with last-write-wins ordering from
the single Controller's SQLite transaction sequence. It has no built-in Caddy, certificate, or TLS
semantics.

Available endpoints are:

- `GET`, `PUT`, and `DELETE /v1/kv`
- `GET /v1/kv/keys`
- `GET /v1/kv/stat`
- `POST /v1/kv/locks/{acquire,renew,release}`

Optional-cache consumers should continue locally while it is unavailable. Request and response
formats are in [`docs/kv-api.md`](docs/kv-api.md).

### Current limitations

- Linux Docker and Podman nodes are the intended production targets.
- Only replicated Services are supported; `deploy.mode: global` is rejected.
- Compose `build`, external `configs`, `secrets`, resource reservations, and autoscaling are not
  supported.
- `stats` and interactive `exec` are not implemented.
- Gateway routing supports the documented host, path, rewrite, backend, and cache model rather than
  arbitrary Caddy handlers.

## Development

The project pins Rust 1.97.0.

### Workspace architecture

The Rust implementation is a Cargo workspace with one thin `swarmlite` binary and explicit
dependency boundaries:

- `swarmlite-cli` owns argument parsing, command orchestration, connection handling, and output;
- `swarmlite-node` is the composition root for initialization, joining, serving, and supervising
  the Agent, optional Controller, and Gateway on one machine;
- `swarmlite-agent` owns heartbeats, assignments, reconciliation, commands, and data streams;
- `swarmlite-controller` owns the API, desired state, scheduling, deployments, and control-plane
  persistence;
- `swarmlite-core`, `swarmlite-protocol`, and `swarmlite-client` provide shared domain, wire, and
  client boundaries;
- `swarmlite-platform` contains Docker/Podman, SQLite local state, registry credentials, and config
  cache adapters;
- `swarmlite-registry` isolates image reference rewriting, the Controller pull-through cache, and
  the Agent's loopback relay;
- `swarmlite-stack` parses and validates Stack documents and renders routing structures.

Only `swarmlite-node` composes Agent and Controller. Those role crates do not depend on each other.

Build the Rust binary with:

```bash
cargo build --release --locked
```

Run the project checks with:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
(cd caddy-storage && go test ./...)
```

The real image-proxy E2E requires a Linux Docker daemon plus outbound access to
`registry.k8s.io`. It verifies the Controller Registry with real HTTP CONNECT and SOCKS5 proxies,
the Agent relay, Docker pull, cache-hit, temporary-tag cleanup, and direct-fallback path. Run it
explicitly with:

```bash
cargo test -p swarmlite-platform --test image_proxy_e2e --locked -- --ignored --nocapture
```

The project [`Dockerfile`](Dockerfile) builds Swarmlite. Gateway nodes pull the official image
matching the installed Swarmlite version, so Go is required only when developing or publishing the
Gateway image.

GitHub Actions builds release archives and SHA-256 checksums for Linux AMD64, Linux ARM64, and
macOS ARM64. Linux archives use musl and are verified as fully static ELF binaries. A release tag
publishes the matching multi-platform Gateway image before the archives, installer, and systemd
unit in the GitHub Release.
