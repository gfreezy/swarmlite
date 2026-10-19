#!/usr/bin/env bash
set -euo pipefail

# Builds local images/binaries only; never pushes or publishes anything.
repo="$(cd "$(dirname "$0")/../.." && pwd)"
artifacts="${1:?Usage: bash tests/cluster-e2e/prepare.sh /absolute/artifact-directory}"
mkdir -p "$artifacts"
artifacts="$(cd "$artifacts" && pwd)"
test -f "$repo/ui/dist/index.html" || { echo 'Build ui/dist first (cd ui && npm ci && npm run build).' >&2; exit 1; }
test -z "$(ls -A "$artifacts")" || { echo 'Choose an empty artifact directory.' >&2; exit 1; }
build_scratch="$(mktemp -d /tmp/swarmlite-e2e-build-XXXXXX)"
suffix="$(basename "$build_scratch")"
registry_volume="$suffix-registry"
target_volume="$suffix-target"
build_container="$suffix-builder"
cleanup() {
  docker rm -f "$build_container" >/dev/null 2>&1 || true
  docker volume rm "$registry_volume" "$target_volume" >/dev/null 2>&1 || true
  rmdir "$build_scratch" >/dev/null 2>&1 || true
}
trap cleanup EXIT
docker volume create --label io.swarmlite.e2e="$suffix" "$registry_volume"
docker volume create --label io.swarmlite.e2e="$suffix" "$target_volume"
docker build -t swarmlite-e2e-builder:local -f "$repo/tests/cluster-e2e/Dockerfile.builder" "$repo/tests/cluster-e2e"
mkdir -p "$artifacts/baseline-src" "$artifacts/gateway-current" "$artifacts/gateway-baseline"
git -C "$repo" archive HEAD | tar -x -C "$artifacts/baseline-src"
git -C "$repo" rev-parse HEAD > "$artifacts/baseline-commit.txt"

for version in current baseline; do
  source_dir="$repo"
  if [[ "$version" == baseline ]]; then
    source_dir="$artifacts/baseline-src"
    # A git archive preserves old mtimes. Refresh this generated checkout so
    # Cargo cannot reuse current working-tree crates as the baseline executable.
    find "$source_dir" -type f -exec touch {} +
  fi
  docker run --rm --name "$build_container" --label io.swarmlite.e2e="$suffix" \
    -e RUSTUP_TOOLCHAIN=1.97.1 \
    -v "$source_dir:/src:ro" -v "$repo/ui/dist:/ui-dist:ro" \
    -v "$registry_volume:/usr/local/cargo/registry" -v "$target_volume:/target" \
    swarmlite-e2e-builder:local sh -c \
    'cargo clean --release -p swarmlite -p swarmlite-cli -p swarmlite-node -p swarmlite-agent -p swarmlite-controller -p swarmlite-core -p swarmlite-stack -p swarmlite-platform -p swarmlite-registry -p swarmlite-client -p swarmlite-protocol && cargo build --release --locked -j 6'
  docker run --rm -v "$target_volume:/target:ro" -v "$artifacts:/out" \
    --entrypoint sh swarmlite-e2e-builder:local \
    -c "cp /target/release/swarmlite /out/swarmlite-$version"
  caddy_dir="$repo/caddy-storage"
  [[ "$version" != baseline ]] || caddy_dir="$source_dir/caddy-storage"
  (cd "$caddy_dir" && CGO_ENABLED=0 GOOS=linux GOARCH="${GOARCH:-$(docker info --format '{{.Architecture}}' | sed 's/aarch64/arm64/;s/x86_64/amd64/')}" \
    go build -trimpath -o "$artifacts/gateway-$version/caddy" ./cmd/caddy)
  cp "$caddy_dir/bootstrap.json" "$artifacts/gateway-$version/bootstrap.json"
  docker build -t "swarmlite-e2e-gateway:$version" -f "$repo/tests/cluster-e2e/Dockerfile.gateway" "$artifacts/gateway-$version"
done
docker build -t swarmlite-e2e-gateway:replacement -f "$repo/tests/cluster-e2e/Dockerfile.replacement" "$repo/tests/cluster-e2e"
docker pull docker:29-dind
docker pull busybox:1.36
echo "Ready: python3 $repo/tests/cluster-e2e/run.py --binary $artifacts/swarmlite-current --baseline-binary $artifacts/swarmlite-baseline --output $artifacts/run"
