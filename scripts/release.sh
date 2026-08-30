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

The script requires a clean main branch. It updates the workspace package
versions, creates a release commit and annotated tag, atomically pushes main
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
manifest_paths = [
    pathlib.Path("Cargo.toml"),
    pathlib.Path("crates/swarmlite-stack/Cargo.toml"),
]


def read_manifest_version(path: pathlib.Path):
    lines = path.read_text().splitlines(keepends=True)
    in_package = False
    found = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "[package]":
            in_package = True
            continue
        if in_package and stripped.startswith("["):
            break
        match = re.fullmatch(r'version\s*=\s*"([^"]+)"', stripped)
        if in_package and match:
            found.append((index, match.group(1)))
    if len(found) != 1:
        raise SystemExit(f"release error: expected one [package] version in {path}")
    index, current = found[0]
    return path, lines, index, current


manifests = [read_manifest_version(path) for path in manifest_paths]
current_versions = [manifest[3] for manifest in manifests]
if len(set(current_versions)) != 1:
    raise SystemExit(
        "release error: workspace package versions differ: " + ", ".join(current_versions)
    )
current = current_versions[0]
if tuple(map(int, target.split("."))) <= tuple(map(int, current.split("."))):
    raise SystemExit(f"release error: target {target} must be newer than {current}")

lock_path = pathlib.Path("Cargo.lock")
lock_text = lock_path.read_text()
for package in ("swarmlite", "swarmlite-stack"):
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

for path, lines, index, _ in manifests:
    newline = "\n" if lines[index].endswith("\n") else ""
    lines[index] = f'version = "{target}"{newline}'
    path.write_text("".join(lines))
lock_path.write_text(lock_text)
PY

git add Cargo.toml Cargo.lock crates/swarmlite-stack/Cargo.toml
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
