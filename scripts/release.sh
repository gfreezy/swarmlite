#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
    cat <<'EOF'
Create and publish a Swarmlite release.

Usage:
  scripts/release.sh <VERSION>

Examples:
  scripts/release.sh 0.1.20
  scripts/release.sh v0.1.20

The script requires a clean main branch. It updates the shared workspace
version, creates a release commit and annotated tag, atomically pushes main
and the tag, waits for the tag CI run, and verifies the GitHub release and
multi-platform Gateway image.
EOF
}

fail() {
    printf 'release error: %s\n' "$*" >&2
    exit 1
}

log() {
    printf 'release: %s\n' "$*" >&2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

if (($# != 1)); then
    usage >&2
    exit 2
fi

version=${1#v}
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "invalid version: $1"
tag="v$version"

require_command git
require_command python3
require_command gh
require_command docker

repository_root=$(git rev-parse --show-toplevel 2>/dev/null) || fail 'not inside a Git repository'
cd "$repository_root"

[[ "$(git branch --show-current)" == main ]] || fail 'releases must be created from main'
[[ -z "$(git status --porcelain)" ]] || fail 'working tree must be clean'

log 'fetching origin/main and tags'
git fetch origin main --tags
git merge-base --is-ancestor origin/main HEAD \
    || fail 'local main does not contain the latest origin/main'

if git show-ref --verify --quiet "refs/tags/$tag"; then
    fail "local tag already exists: $tag"
fi
if git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null 2>&1; then
    fail "remote tag already exists: $tag"
fi

python3 - "$version" <<'PY'
import pathlib
import re
import sys

target = sys.argv[1]
workspace_packages = [
    "swarmlite",
    "swarmlite-agent",
    "swarmlite-cli",
    "swarmlite-client",
    "swarmlite-controller",
    "swarmlite-core",
    "swarmlite-node",
    "swarmlite-platform",
    "swarmlite-protocol",
    "swarmlite-stack",
]

root_path = pathlib.Path("Cargo.toml")
root_lines = root_path.read_text().splitlines(keepends=True)
in_workspace_package = False
found = []
for index, line in enumerate(root_lines):
    stripped = line.strip()
    if stripped == "[workspace.package]":
        in_workspace_package = True
        continue
    if in_workspace_package and stripped.startswith("["):
        break
    match = re.fullmatch(r'version\s*=\s*"([^"]+)"', stripped)
    if in_workspace_package and match:
        found.append((index, match.group(1)))
if len(found) != 1:
    raise SystemExit("release error: expected one [workspace.package] version in Cargo.toml")
version_index, current = found[0]

manifest_paths = [root_path, *sorted(pathlib.Path("crates").glob("*/Cargo.toml"))]
for path in manifest_paths:
    lines = path.read_text().splitlines()
    in_package = False
    inherits_workspace_version = False
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "[package]":
            in_package = True
            continue
        if in_package and stripped.startswith("["):
            break
        if in_package and stripped == "version.workspace = true":
            inherits_workspace_version = True
    if not inherits_workspace_version:
        raise SystemExit(f"release error: {path} must inherit version.workspace")

if tuple(map(int, target.split("."))) <= tuple(map(int, current.split("."))):
    raise SystemExit(f"release error: target {target} must be newer than {current}")

lock_path = pathlib.Path("Cargo.lock")
lock_text = lock_path.read_text()
for package in workspace_packages:
    pattern = re.compile(
        rf'(^\[\[package\]\]\n(?:(?!^\[\[package\]\]).)*?'
        rf'^name = "{re.escape(package)}"\n(?:(?!^\[\[package\]\]).)*?'
        rf'^version = ")([^"]+)(")',
        re.MULTILINE | re.DOTALL,
    )
    matches = list(pattern.finditer(lock_text))
    if len(matches) != 1:
        raise SystemExit(f"release error: expected one {package} package in Cargo.lock")
    if matches[0].group(2) != current:
        raise SystemExit(
            f"release error: {package} Cargo.lock version {matches[0].group(2)} "
            f"does not match manifests {current}"
        )
    lock_text = pattern.sub(rf'\g<1>{target}\g<3>', lock_text, count=1)

newline = "\n" if root_lines[version_index].endswith("\n") else ""
root_lines[version_index] = f'version = "{target}"{newline}'
root_path.write_text("".join(root_lines))
lock_path.write_text(lock_text)
PY

git add Cargo.toml Cargo.lock
git diff --cached --quiet && fail 'version update produced no changes'
git commit -m "chore: release $tag"
release_commit=$(git rev-parse HEAD)
git tag -a "$tag" -m "$tag"

log "pushing main and $tag atomically"
git push --atomic origin HEAD:refs/heads/main "refs/tags/$tag"

log 'waiting for the tag CI run to appear'
run_id=
for _ in {1..30}; do
    run_id=$(gh run list \
        --workflow CI \
        --event push \
        --commit "$release_commit" \
        --limit 10 \
        --json databaseId,headBranch \
        --jq ".[] | select(.headBranch == \"$tag\") | .databaseId" \
        | head -n 1)
    [[ -z "$run_id" ]] || break
    sleep 2
done
[[ -n "$run_id" ]] || fail "could not find the $tag CI run"

gh run watch "$run_id" --exit-status

release_url=$(gh release view "$tag" --json url --jq .url)
repository=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
image="ghcr.io/${repository%/*}/swarmlite-caddy:$tag"
inspection=$(docker buildx imagetools inspect "$image")
grep -q 'Platform:.*linux/amd64' <<<"$inspection" \
    || fail "$image does not contain linux/amd64"
grep -q 'Platform:.*linux/arm64' <<<"$inspection" \
    || fail "$image does not contain linux/arm64"

log "published $tag"
printf 'Release: %s\nGateway image: %s\n' "$release_url" "$image"
