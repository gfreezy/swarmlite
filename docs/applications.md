# Deploy applications

[← README](../README.md)

- [Stack file basics](#stack-file-basics)
- [Scheduled jobs](#scheduled-jobs)
- [Task environment templates](#task-environment-templates)
- [Deployment lifecycle](#deployment-lifecycle)
- [Inspect services, jobs, tasks, and logs](#inspect-services-jobs-tasks-and-logs)
- [Images and private registries](#images-and-private-registries)
- [Image proxy](#image-proxy)
- [Config files, volumes, and placement](#config-files-volumes-and-placement)
- [HTTP and HTTPS routes](#http-and-https-routes)
- [Remote management over SSH](#remote-management-over-ssh)

This section is for application owners. Cluster installation, node membership, and recovery are in
[Run the cluster](operations.md).

## Stack file basics

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

See [`examples/services-all.yaml`](../examples/services-all.yaml) for Service fields and
[`examples/routing-all.yaml`](../examples/routing-all.yaml) for routing fields.

## Scheduled jobs

Define one-shot workloads under `x-swarmlite-jobs`. A Stack can contain only jobs, or
both services and jobs with distinct names. See [`examples/jobs.yaml`](../examples/jobs.yaml).

```yaml
x-swarmlite:
  name: maintenance

x-swarmlite-jobs:
  cleanup:
    image: example/app:1.0
    command: ["/app/cleanup"]
    schedule: "0 2 * * *"
    timezone: Asia/Shanghai
    timeout: 30m
    stop_signal: SIGTERM
    stop_grace_period: 10s
    suspend: false
```

`schedule` is a five-field cron expression (minute, hour, day, month, weekday), evaluated
in the IANA `timezone` (default `UTC`). Omit `schedule` for a manual-only job. Each occurrence has its own `task_id`; `job_id`
identifies the Stack-qualified definition, such as `maintenance.cleanup`. Job container
labels use `io.swarmlite.task_kind=job`, `io.swarmlite.job_id`, the existing
`io.swarmlite.task_id`, and immutable schedule/start-deadline/timeout metadata. They do
not reuse Service identity labels.

Jobs reuse the existing image, command, entrypoint, environment, labels, configs,
volumes, pull policy, stop settings, and `deploy.placement` fields. Service inheritance,
ports, health checks, replicas, and rolling-update settings are not supported for jobs.
`stop_signal` is also supported on services; omitting it preserves the image's
`STOPSIGNAL`, falling back to the runtime's `SIGTERM` default.

Each scheduled occurrence makes **at most one container start attempt**. The Controller
persists the trigger cursor and fixed node assignment before dispatch, and the Agent
atomically claims the task in its local SQLite ledger before creating its container.
Ambiguous create/start results consume that attempt. Failed or lost executions are not
restarted or moved to another node. Containers use restart policy `no`, including after
host reboot. An attempt may therefore never start; this is deliberate.

At every new occurrence, the Controller revokes all unfinished earlier occurrences and
requests their termination. It allows up to the largest old `stop_grace_period` plus
five seconds before releasing the new assignment, capped at 30 seconds and half the
remaining interval before the next occurrence. Agents perform stops independently of reconciliation: send the
configured stop signal, wait the remaining grace period, then force termination if
necessary. An unreachable node never blocks the new occurrence. Old and new executions
can overlap during a partition; there is no strict singleton guarantee. Startup work
also expires at the next occurrence, so a slow image pull cannot start an obsolete run.

`timeout` defaults to `30m` and measures running time from the container's actual start,
excluding image pulls. It must be a positive whole-second duration. The Agent enforces
it even while disconnected from the Controller; graceful shutdown time is additional.
If the Agent itself is unavailable, enforcement resumes after it restarts. Stop intent
and its original timestamp are durable, so restarts do not reset the grace period.
`suspend: true` skips future occurrences without terminating a running execution;
resuming does not replay skipped occurrences.

Controller startup skips missed schedule times and resumes at a future occurrence.
Ordinary scheduling tolerates tick latency within the scheduled minute; older missed
occurrences are skipped. With no compatible live node, that occurrence is skipped too.
Only Agents advertising job support receive jobs. Keep node clocks synchronized.
Controller storage requires schema 12; older database formats are rejected without
modifying their contents. Automatic schema 11 migration has been removed. Upgrade an
existing schema-11 database through v0.1.41 before using this version. Upgrade Controllers,
Agents, and CLI together. New managed workload containers have `io.swarmlite.task_kind`
set to `service` or `job`. Older containers without this label are still recognized as
Services: upgrading the software or database does not rewrite existing container labels.
This compatibility remains necessary until those containers are replaced.
Older Controller binaries cannot read a schema-12 database.
Older storage layouts, embedded KV documents, slotless Gateways, `pull_policy: if_not_present`,
and removed cache fields are not supported. Use `pull_policy: missing`; cache keys
are always hashed and do not accept `key.hash`.
On lost-database recovery, redeploy the Stack files: historical jobs are never replayed,
and surviving unclaimed job containers are stopped rather than adopted as services.
The at-most-once guarantee depends on retaining the Agent attempt ledger and on there
being only one authoritative Controller. Do not restore an old Agent database while
retaining live assignments; use the documented full-cluster recovery procedure instead.

Deploy and manage jobs:

```bash
swarmlite deploy -c examples/jobs.yaml
swarmlite inspect maintenance.cleanup
swarmlite ls maintenance
swarmlite job run maintenance.cleanup
swarmlite job history maintenance.cleanup --json
swarmlite logs maintenance.cleanup
swarmlite job cancel <task-id>
```

Manual invocations are allowed while suspended, but reject unfinished earlier executions.
They receive a startup window of at most five minutes (or until the next cron occurrence).
Every `job run` request creates a new execution; do not blindly retry a request with an
unknown result. `job cancel` requests termination; use history to confirm it finished.
An automatic occurrence also replaces a still-running manual invocation.

Deployment completion means the schedule was registered; it does not wait for a job
execution. `inspect` includes the next trigger, execution timestamps, exit codes, and
stop reasons. The last 20 confirmed finished executions are retained for inspection and
logs; unresolved executions remain visible until reconciled. The Agent's compact start
claims are retained to reject stale assignments. `service scale` and `service restart` reject jobs;
edit and redeploy their definitions instead. Configuration changes affect future
occurrences, while existing executions keep their original container settings.

## Task environment templates

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

## Deployment lifecycle

`deploy`, `service scale`, `service restart`, and `rm` submit desired state to the Controller and wait for
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
`service restart` always increments the Service revision and performs its configured rolling replacement.
Old healthy tasks remain routable until their replacements are healthy and Gateways accept the new
upstreams.

## Inspect services, jobs, tasks, and logs

Shared queries live at the top level: `ls` lists Service and Job definitions (optionally
filtered by Stack), `inspect` reads either definition with its tasks, `ps` lists tasks for a
Stack, Service, or Job, and `logs` reads output for either workload or an individual Task.
`ls --json` includes Job scheduling settings; the table distinguishes `service` and `job`
and shows `-` for Job replica counts.

Type-specific operations are grouped: `service scale/restart` manage long-running Services;
`job run/history/cancel` manage Job executions. `deploy` and `rm` apply to entire Stacks.

This CLI layout replaces top-level `scale/restart` with `service scale/restart` and removes
`job ls/logs` in favor of top-level `ls/logs`. Update scripts to use the new paths.

Examples (optional arguments are shown in brackets):

```bash
swarmlite ls [STACK]
swarmlite ps [STACK|STACK.SERVICE|STACK.JOB]
swarmlite inspect STACK.SERVICE
swarmlite inspect STACK.JOB
swarmlite logs --tail 200 STACK.SERVICE
swarmlite logs --tail 200 STACK.JOB
swarmlite logs --follow STACK.SERVICE
swarmlite logs --follow STACK.SERVICE.SLOT
swarmlite service scale STACK.SERVICE=3
swarmlite service restart STACK.SERVICE
swarmlite rm STACK
```

During a rolling update, an old and new task may temporarily share a slot name. Use the task ID
from `swarmlite ps` to select one exactly. Log sessions select at most 64 tasks and cap `--tail` at
10,000 lines.

## Images and private registries

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

## Image proxy

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

## Config files, volumes, and placement

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
[`examples/configs.yaml`](../examples/configs.yaml).

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

## HTTP and HTTPS routes

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
    query_parameters: [embedded]
    headers: [Accept-Language]
  status_codes: [200]
```

`key.disable_query` should be enabled only when every query parameter is irrelevant to the upstream
response. To keep only selected query parameters, use the case-sensitive `key.query_parameters`
allowlist. It replaces only the query component, preserving scheme, host, method, path, content type,
and request-body identity. It cannot be combined with `key.disable_query`.

When `allowed_http_verbs` is omitted, only `GET` responses are stored and `HEAD` may reuse a
matching `GET` response. CONNECT, protocol upgrades, range and conditional requests, request
`no-store`, and responses carrying `Set-Cookie` or `Content-Range` bypass storage. Authorization
does not affect cache eligibility or key identity, and response `Cache-Control` directives are
ignored. Concurrent misses for one key share a single origin request. SQLite failures fail open to
the origin. Each Gateway limits the logical cached response payload to 1 GiB by default; use the
cluster-level `gateway.cache.max-size-bytes` setting to change it. By default, an uncached key is
stored on its third request within a five-minute admission window. The dynamic admission filter
uses one 64 KiB Bloom-filter level per preceding request, up to eight requests. Cached hits are
deduplicated with a separate Bloom filter and asynchronously update a SQLite access table so LRU
tracking does not rewrite rows containing response bodies. Expired entries are
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

## Remote management over SSH

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
