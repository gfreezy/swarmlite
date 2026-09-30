# Development

[← README](../README.md)

- [Workspace architecture](#workspace-architecture)
- [Release workflow](#release-workflow)

The project pins Rust 1.97.0.

## Workspace architecture

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

Building the CLI also requires Node.js 24 LTS and npm. Cargo automatically builds and embeds
the React UI; CI and Docker builds include it in the same release binary.

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
npm ci --prefix ui --no-audit --no-fund
npm run lint --prefix ui
npm test --prefix ui
```

The real image-proxy E2E requires a Linux Docker daemon plus outbound access to
`registry.k8s.io`. It verifies the Controller Registry with real HTTP CONNECT and SOCKS5 proxies,
the Agent relay, Docker pull, cache-hit, temporary-tag cleanup, and direct-fallback path. Run it
explicitly with:

```bash
cargo test -p swarmlite-platform --test image_proxy_e2e --locked -- --ignored --nocapture
```

The project [`Dockerfile`](../Dockerfile) builds Swarmlite. Gateway nodes pull the official image
matching the installed Swarmlite version, so Go is required only when developing or publishing the
Gateway image.

GitHub Actions builds release archives and SHA-256 checksums for Linux AMD64, Linux ARM64, and
macOS ARM64. Linux archives use musl and are verified as fully static ELF binaries. A release tag
publishes the matching multi-platform Gateway image before the archives, installer, and systemd
unit in the GitHub Release.

## Release workflow

From a clean `main` branch containing the changes to publish, run:

```bash
scripts/release.sh <VERSION>
```

The script updates the shared Cargo version, creates the release commit and annotated tag,
atomically pushes `main` and the tag, waits for tag CI, and verifies the GitHub Release and
Gateway image platforms. It does not run local tests or builds. Also check the main-branch CI
run for formatting, linting and tests. See the [CI workflow](../.github/workflows/ci.yml).
