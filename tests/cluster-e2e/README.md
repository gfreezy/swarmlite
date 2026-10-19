# Isolated whole-cluster TCP E2E

This opt-in test uses three privileged Docker-in-Docker containers, **not the
host Docker socket**. Each has a separate Docker daemon, container inventory,
filesystem and network namespace. Two run real managed Gateways; only the third
can run application tasks. No host ports are published. Never run it against a
production Docker endpoint.

Prerequisites: a Docker daemon permitting privileged containers, Python 3.10+,
Bash, Go matching `caddy-storage/go.mod`, Git and an already built `ui/dist`.
Allow approximately 12 GB RAM and several GB of free Docker disk space. The
builder produces a static native-architecture Linux executable, with release
LTO disabled for test build speed. It does not publish binaries or images.

```bash
artifacts="$(mktemp -d /tmp/swarmlite-cluster-e2e-XXXXXX)"
bash tests/cluster-e2e/prepare.sh "$artifacts"
python3 tests/cluster-e2e/run.py \
  --binary "$artifacts/swarmlite-current" \
  --baseline-binary "$artifacts/swarmlite-baseline" \
  --output "$artifacts/run"
```

`prepare.sh` builds the working tree and a clean `git archive HEAD` baseline
using the same Rust toolchain, dependency cache and prebuilt static UI assets.
The baseline **backend code** and Caddy module come from HEAD, not the working
tree. Thus this tests an upgrade from the repository's current committed version;
record `baseline-commit.txt` when interpreting results. After this feature is
committed, choose the pre-feature release commit explicitly before preparing a
baseline instead of treating HEAD as a pre-feature release.

Each run produces its own `report.json`, generated Stack files, Controller DB,
node settings, Gateway configuration and logs. These artifacts include the
ephemeral cluster token: keep them local. HTTP is sampled throughout upgrades and
fault injection; report failed samples instead of claiming zero interruption.
TCP interruption on reload/restart is allowed.

Coverage:

- Old HTTP-only cluster → worker/Node/Controller binary replacement preserving
  state → real managed Gateway blue/green image replacement → enable TCP.
- Two Gateways → two healthy replicas on the third node, using dynamically
  published Docker ports; distinct responses verify round robin.
- Unknown extension fields ignored; missing TCP configuration keeps HTTP-only
  Caddy configuration free of the TCP app.
- Docker health failure with a still-listening backend; Controller exclusion and
  automatic task replacement.
- Actual occupied TCP host port on just one Gateway; unrelated HTTP deployment,
  another TCP route, Gateway restart/resume, and recovery after releasing it.
- Abrupt loss of every replica while the worker agent is stopped; Caddy active
  probes, connection failure, other routes, then replacement endpoints.
- Scale to zero/back up, Controller restart with running Gateways, TCP-only
  service restart and managed Gateway image replacement while TCP is enabled
  and another route still has an occupied host port.
- Stack removal releases ports, verified by binding fresh host-network listeners.

Cleanup removes only the three uniquely named node containers (including their
anonymous volumes) and their uniquely named network. Build cache volumes are
removed by `prepare.sh`; generated local images and diagnostic files are retained.
SIGKILL or machine shutdown can prevent cleanup; use the exact resource names and
`io.swarmlite.e2e` label recorded in the report to recover, never `docker prune`.

Limits: all three nodes share one Linux kernel/VM; this is not a physical
multi-host network, WAN/firewall-blackhole, load/OOM or soak test. HTTP uses local
test hostnames without ACME; certificate issuance/renewal is outside this test.
