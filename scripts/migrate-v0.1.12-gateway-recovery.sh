#!/usr/bin/env bash

set -Eeuo pipefail

umask 077

readonly RECOVERY_PATH=/config/swarmlite-recovery.json
readonly RECOVERY_TEMP_PATH=/config/.swarmlite-recovery.json.migrate.tmp

database=/var/lib/swarmlite/swarmlite.sqlite
runtime=docker
output=
write_snapshot=false

usage() {
    cat <<'EOF'
Create the first GatewayRecoverySnapshot when migrating a v0.1.12 Controller.

The script reads the legacy Controller SQLite document and current managed
containers. It does not modify the database or reload Caddy. Without --write it
only prints (or saves) the generated snapshot.

Usage:
  migrate-v0.1.12-gateway-recovery.sh [OPTIONS]

Options:
  --database PATH       Legacy/archived swarmlite.sqlite file.
                        Default: /var/lib/swarmlite/swarmlite.sqlite
  --runtime NAME        docker or podman. Default: docker
  --output PATH         Also save the generated snapshot to PATH (mode 0600).
  --write               Atomically install the snapshot in every matching,
                        running Gateway container on this node.
  -h, --help            Show this help.

Safe migration sequence:
  1. Restore and run v0.1.12, then confirm the existing routes still work.
  2. Run this script without --write and inspect the generated JSON.
  3. Run it again with --write. Existing recovery files are never overwritten.
  4. Stop v0.1.12, install the new binary, and run `swarmlite init --recover`.

Required commands: bash, sqlite3, jq, curl, and docker/podman.
EOF
}

fail() {
    printf 'migration error: %s\n' "$*" >&2
    exit 1
}

log() {
    printf 'migration: %s\n' "$*" >&2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

while (($# > 0)); do
    case "$1" in
        --database)
            (($# >= 2)) || fail '--database requires a path'
            database=$2
            shift 2
            ;;
        --runtime)
            (($# >= 2)) || fail '--runtime requires docker or podman'
            runtime=$2
            shift 2
            ;;
        --output)
            (($# >= 2)) || fail '--output requires a path'
            output=$2
            shift 2
            ;;
        --write)
            write_snapshot=true
            shift
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            fail "unknown argument: $1"
            ;;
    esac
done

case "$runtime" in
    docker | podman) ;;
    *) fail '--runtime must be docker or podman' ;;
esac

require_command sqlite3
require_command jq
require_command curl
require_command cmp
require_command install
require_command "$runtime"

[[ -f "$database" ]] || fail "database not found: $database"
[[ -r "$database" ]] || fail "database is not readable: $database"

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/swarmlite-recovery-migration.XXXXXX")
cleanup() {
    rm -rf -- "$work_dir"
}
trap cleanup EXIT

document_path=$work_dir/control-plane.json
containers_path=$work_dir/containers.json
snapshot_path=$work_dir/swarmlite-recovery.json

row_count=$(sqlite3 -readonly -batch -noheader "$database" \
    'SELECT COUNT(*) FROM control_plane WHERE singleton = 1;')
[[ "$row_count" == 1 ]] || fail 'legacy database must contain exactly one control_plane row'
IFS='|' read -r row_schema cluster_id generation < <(
    sqlite3 -readonly -batch -noheader -separator '|' "$database" \
        'SELECT schema_version, cluster_id, generation
           FROM control_plane
          WHERE singleton = 1;'
)
case "$row_schema" in
    7 | 8 | 9) ;;
    *) fail "unsupported legacy persistence schema $row_schema; expected 7, 8, or 9" ;;
esac
[[ "$cluster_id" =~ ^cluster-[A-Za-z0-9._-]+$ ]] || fail "invalid cluster ID in database: $cluster_id"
[[ "$generation" =~ ^[0-9]+$ ]] || fail "invalid Controller generation in database: $generation"
if ((generation == 0)); then
    generation=1
fi

sqlite3 -readonly -batch -noheader "$database" \
    'SELECT CAST(document AS TEXT) FROM control_plane WHERE singleton = 1;' \
    >"$document_path"

jq -e . "$document_path" >/dev/null || fail 'control_plane document is not valid JSON'
jq -e --arg cluster_id "$cluster_id" --argjson row_schema "$row_schema" '
    .cluster_id == $cluster_id
    and .cluster.cluster_id == $cluster_id
    and (.schema_version == $row_schema)
    and (.state.stacks | type == "object")
    and (.state.services | type == "object")
    and (.state.tasks | type == "object")
    and (.state.members | type == "object")
' "$document_path" >/dev/null || fail 'legacy control-plane identity or document structure is inconsistent'

mapfile -t gateway_ids < <(
    "$runtime" ps -q \
        --filter 'label=io.swarmlite.managed=true' \
        --filter "label=io.swarmlite.cluster_id=$cluster_id" \
        --filter 'label=io.swarmlite.system=true' \
        --filter 'label=io.swarmlite.component=gateway'
)
((${#gateway_ids[@]} > 0)) || fail "no running managed Gateway found for cluster $cluster_id"

mapfile -t managed_ids < <(
    "$runtime" ps -aq \
        --filter 'label=io.swarmlite.managed=true' \
        --filter "label=io.swarmlite.cluster_id=$cluster_id"
)
if ((${#managed_ids[@]} > 0)); then
    "$runtime" inspect "${managed_ids[@]}" >"$containers_path"
else
    printf '[]\n' >"$containers_path"
fi

jq -e --arg cluster_id "$cluster_id" '
    all(.[].Config.Labels["io.swarmlite.cluster_id"]; . == $cluster_id)
' "$containers_path" >/dev/null || fail 'runtime returned a managed container from another cluster'

jq -n \
    --slurpfile document_file "$document_path" \
    --slurpfile containers_file "$containers_path" \
    --arg cluster_id "$cluster_id" \
    --argjson generation "$generation" '
    def backend_key:
        "\(.service):\(.target_port):\(.protocol)";

    def actual_ports($container; $target_port):
        [
            ($container.NetworkSettings.Ports[(($target_port | tostring) + "/tcp")] // [])[]?
            | .HostPort
            | select(type == "string" and test("^[0-9]+$"))
            | tonumber
            | select(. > 0 and . <= 65535)
        ] | unique;

    def persisted_ports($task; $target_port):
        [
            ($task.ports // [])[]?
            | select(.target == $target_port and (.protocol // "tcp") == "tcp")
            | .published
            | select(type == "number" and . > 0 and . <= 65535)
        ] | unique;

    def runtime_is_ready($container):
        ($container.State.Running == true)
        and (($container.State.Health.Status // "healthy") == "healthy");

    ($document_file[0]) as $doc
    | ($containers_file[0]) as $containers
    | ($containers
        | map(select(.Config.Labels["io.swarmlite.task_id"] != null))
        | group_by(.Config.Labels["io.swarmlite.task_id"])
        | map(
            if length != 1 then
                error("duplicate managed containers use task ID " +
                      .[0].Config.Labels["io.swarmlite.task_id"])
            else
                {key: .[0].Config.Labels["io.swarmlite.task_id"], value: .[0]}
            end
        )
        | from_entries) as $runtime_tasks

    | {
        format_version: 1,
        cluster_id: $cluster_id,
        generation: $generation,
        stacks: (
            $doc.state.stacks
            | to_entries
            | map(
                . as $stack
                | ($stack.value.gateway // {
                    tls: "serve",
                    http: "redirect",
                    http_routes: []
                }) as $gateway
                | select(($gateway.http_routes // []) | length > 0)
                | ([
                    ($gateway.http_routes // [])[]?
                    | .rules[]?
                    | .backend
                    | select(.service != null)
                    | {
                        service: .service,
                        target_port: .port,
                        protocol: (.protocol // "http")
                    }
                ] | unique_by([.service, .target_port, .protocol])) as $backends
                | {
                    key: $stack.key,
                    value: {
                        gateway: $gateway,
                        upstreams: (
                            reduce $backends[] as $backend ({};
                                .[$backend | backend_key] = ([
                                    $doc.state.tasks
                                    | to_entries[]
                                    | . as $task_entry
                                    | $task_entry.value as $task
                                    | ($doc.state.services[$task.service_id] // null) as $service
                                    | select(
                                        $service != null
                                        and $service.deleted != true
                                        and $service.stack == $stack.key
                                        and $service.name == $backend.service
                                        and $task.desired == "running"
                                        and $task.revision == $service.revision
                                    )
                                    | ($doc.state.members[$task.node_id].address // null) as $address
                                    | select($address != null and ($address | length) > 0)
                                    | ($runtime_tasks[$task_entry.key] // null) as $container
                                    | (if $container == null then
                                           persisted_ports($task; $backend.target_port)
                                       elif runtime_is_ready($container) then
                                           (actual_ports($container; $backend.target_port)) as $actual
                                           | if ($actual | length) > 0 then
                                                 $actual
                                             else
                                                 persisted_ports($task; $backend.target_port)
                                             end
                                       else
                                           []
                                       end)[]
                                    | $address + ":" + (tostring)
                                ] | unique | sort)
                            )
                        )
                    }
                }
            )
            | from_entries
        )
    }
' >"$snapshot_path" || fail 'failed to construct the recovery snapshot'

jq -e '
    .format_version == 1
    and (.cluster_id | type == "string" and length > 0)
    and (.generation | type == "number" and . > 0)
    and (.stacks | type == "object")
    and all(
        .stacks | to_entries[];
        (.key | length > 0)
        and (.value.gateway.http_routes | type == "array" and length > 0)
        and (.value.upstreams | type == "object")
        and all(.value.upstreams | to_entries[]; (.value | type == "array" and length > 0))
    )
' "$snapshot_path" >/dev/null || fail 'generated snapshot has a routed backend without a usable upstream'

route_count=$(jq '[.stacks[].gateway.http_routes[]] | length' "$snapshot_path")
stack_count=$(jq '.stacks | length' "$snapshot_path")
upstream_count=$(jq '[.stacks[].upstreams[][]] | length' "$snapshot_path")
((stack_count > 0)) || fail 'legacy Controller has no Gateway-routed stacks to recover'

curl -fsS --max-time 5 http://127.0.0.1:2019/config/ >/dev/null \
    || fail 'Caddy admin API is unavailable; refusing to snapshot an unverified Gateway'

if [[ -n "$output" ]]; then
    install -m 0600 "$snapshot_path" "$output"
    log "saved generated snapshot to $output"
fi

log "validated cluster $cluster_id: $stack_count stack(s), $route_count route(s), $upstream_count upstream(s), generation $generation"

if [[ "$write_snapshot" != true ]]; then
    if [[ -z "$output" ]]; then
        jq . "$snapshot_path"
    fi
    log 'dry run complete; rerun with --write to install the snapshot'
    exit 0
fi

for gateway_id in "${gateway_ids[@]}"; do
    if "$runtime" exec "$gateway_id" test -e "$RECOVERY_PATH"; then
        existing_path=$work_dir/existing-$gateway_id.json
        "$runtime" cp "$gateway_id:$RECOVERY_PATH" "$existing_path"
        if cmp -s "$snapshot_path" "$existing_path"; then
            log "Gateway $gateway_id already contains the identical snapshot"
            continue
        fi
        fail "Gateway $gateway_id already has a different recovery snapshot; refusing to overwrite it"
    fi

    "$runtime" cp "$snapshot_path" "$gateway_id:$RECOVERY_TEMP_PATH"
    "$runtime" exec "$gateway_id" /bin/sh -ec \
        'chmod 0600 /config/.swarmlite-recovery.json.migrate.tmp; sync; mv /config/.swarmlite-recovery.json.migrate.tmp /config/swarmlite-recovery.json; sync'
    "$runtime" exec "$gateway_id" cat "$RECOVERY_PATH" \
        | cmp -s "$snapshot_path" - \
        || fail "Gateway $gateway_id did not persist the expected snapshot"
    log "atomically installed the recovery snapshot in Gateway $gateway_id"
done

log 'migration snapshot is ready; stop the old Controller before running the new init --recover'
