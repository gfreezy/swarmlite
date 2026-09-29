# Swarmlite UI

A local cluster console built with npm, React, TypeScript, Vite, Tailwind CSS,
and checked-in shadcn/ui components. Inspect workloads and tasks, troubleshoot routing,
compare retained deployments, and submit CLI operations without a Stack YAML file.

## Run the embedded console

```sh
swarmlite ui --controller ssh://root@server.example.com
swarmlite ui --port 17081 --no-open
```

The command binds only to `127.0.0.1`. The default port is chosen by the OS. It uses the
same `--controller`, `--token`, environment variables, and local settings as other management
commands. SSH connections and tunnels remain alive until the UI process exits.

Open the **complete URL** printed by the command. A random, process-scoped browser credential
is passed in its URL fragment, removed from the address bar, and kept in tab session storage.
The server checks Host, Origin and the browser credential. Inspect and deployment comparison
show complete environment values, command arguments, health checks and labels. Configuration
masks credentials embedded in proxy URLs. Local connection and node join details, available
from Nodes, read the UI machine’s stored node settings and can include cluster tokens.
Restarting the command invalidates previous browser credentials and clears operation history.
Do not publish this local server behind a reverse proxy; hosted login and Caddy integration
are separate future work.

## CLI coverage

Actions live in their owning resource modules, using contextual forms and result dialogs.
They reuse the CLI’s Controller connection, token, data directory and OS permissions. Arguments
are passed directly to the same executable without a shell; registry passwords use stdin.

| CLI functionality | Web entry |
| --- | --- |
| `status`, `ls`, `inspect`, `ps` | Overview, Workloads, rich Inspect tabs, Tasks |
| `logs` (target, tail, follow, raw) | Logs explorer and workload/task log panels |
| `deployment status/history/attach` | Deployment generation selector, full records and progress dialog |
| `service scale/restart` | Service detail actions |
| `job run/history/cancel` | Job detail and execution rows; task inspection |
| `deployment retry/rollback`, `rm` | Stack detail actions |
| `config get/explain/set/unset` | Configuration details, edit and reset dialogs |
| `gateway status/enable/disable` | Routes and node details |
| `node label get/set/remove` | Node details |
| `registry login` | Registries |
| `connection-info`, `join-token` | Nodes → local connection and join details |
| `deploy`, `init`, `join`, `serve`, `upgrade` | Excluded from the UI and rejected by its API |

Each action names its target before submission. Results and recent activity remain in the
owning module; completed actions refresh that page. Recent activity navigation lasts for the
current browser page, while the backend retains request IDs for the entire UI session.
Recovering a lost HTTP response with the same ID returns the existing action instead of
running it again. At most four requests run concurrently and 128 submissions are retained.
Each stdout/stderr buffer retains up to 256 KiB. Stop terminates the local request process;
it does not reverse changes already accepted by the Controller. Stopping the UI server
also stops its local request processes.

## Inspection

- **Routes** uses an interactive directed graph to connect hostnames and path rules to service targets, published upstreams,
  task replicas and node ports. Select a node to trace its connections and open full rule details;
  zoom, fit, search and a list view are available. Unpublished tasks stay in the details, and
  missing upstreams use dashed diagnostic edges. It reads the Controller's retained Gateway routing snapshot,
  including recovery state; each Gateway's desired/applied generation and errors are shown
  separately. These records do not prove live application reachability.
- **Jobs** shows schedules, timezones, suspension, next trigger, execution states, runtime,
  exit codes, stop reasons and per-execution logs. New Agents report container finish times;
  older records without one show unavailable duration rather than estimating completion.
- **Deployment comparison** selects two retained generations and compares their saved
  configuration, including environment values, commands, labels and health checks.
  Local YAML is not read or edited.
  Deleted or expired generation snapshots return an explicit error.

## Build

Use Node.js 24 LTS and npm to build from source:

```sh
cargo build --release --locked
```

The CLI build script runs `npm ci` and `npm run build` automatically, then embeds every Vite
output file into the binary. Source changes trigger a rebuild. Node.js, npm, and external web
assets are not required to run the resulting CLI.

For a prebuilt frontend (used by the Docker build):

```sh
cd ui
npm ci
npm run build
cd ..
SWARMLITE_UI_DIST="$PWD/ui/dist" cargo build --release --locked
```

`SWARMLITE_UI_DIST` is an explicit override: the caller is responsible for rebuilding those
assets after frontend changes. The build fails if that directory has no `index.html`.

## Frontend development

Start a local UI backend in one terminal:

```sh
cargo run -- ui --port 17081 --no-open --controller ssh://root@server.example.com
```

In another:

```sh
cd ui
npm ci
npm run dev
```

Open the Vite URL with the `#session=...` fragment printed by the backend appended. Vite's
loopback-only development server proxies `/api` to `http://127.0.0.1:17081`; set
`SWARMLITE_UI_PROXY` when using another backend port. Only the development proxy rewrites Host
and removes Origin for that connection. The embedded production server uses strict same-origin
checks and serves both assets and API on one origin.

```sh
npm run build  # TypeScript checking + production bundle
npm run lint
npm test      # Inspection and operation behavior tests
```

UI API failures preserve and label the last successful snapshot. Membership does not imply
node connectivity. Logs stay in bounded browser memory and disconnect when leaving the log tab.
