# HTTP CI deployment

CI pushes the new image to the registry, then sends one authenticated POST to an existing Service.
It needs no Swarmlite CLI or Stack file. The Service keeps its configured image reference and other
settings. Build/push a stable deployment tag before triggering; changing image references remains
an administrator operation.

## Restart semantics

```bash
# Restart existing containers sequentially; no pull, replacement, or revision change.
swarmlite service restart production.api

# Pull the configured image, then roll out replacement containers, even for identical content.
swarmlite service restart production.api --pull
```

Both commands wait for healthy completion by default; `--detach` returns after durable acceptance.
Ordinary restart preserves container/task/image IDs and the writable layer, like `docker restart`.
Single-replica services have downtime. Jobs and zero-replica services cannot be restarted.
The Controller and all eligible Agents must support service operations; old versions are not
silently given replacement semantics.

## Administrator preparation

```bash
swarmlite deploy-token create --name github-api --service production.api --expires-in never
swarmlite deploy-token list
swarmlite deploy-token list --show-secret
# Replace TOKEN_ID with the ID printed above.
swarmlite deploy-token revoke TOKEN_ID
```

`--expires-in` defaults to `90d`. It accepts a positive integer with `s`, `m`, `h`, or `d`, or
`never` for a permanent, manually revocable credential. Tokens bind to exactly one Service ID,
not its image repository; an administrator's image-reference changes do not invalidate the token.
Deploy tokens are HTTP-only and cannot be used as CLI administrator credentials.

Creation prints the complete token and a curl template. Default listing hides secrets;
`list --show-secret` reveals active tokens and curl templates again. Expired/revoked tokens are
not revealed, and revocation clears the stored secret. The Controller stores retrievable secrets:
protect its data directory and database backups as credentials.

All printed deployment URLs retain the literal `<GATEWAY_HOST>` placeholder:

```text
http://<GATEWAY_HOST>/v1/deploy/services/production.api/redeploy
```

Replace it with the Gateway's actual IP or domain, optionally including a port. HTTP 80 needs no
port suffix. No public address is discovered or stored. Copy the resulting URL into `DEPLOY_URL`
and the token into your CI `DEPLOY_TOKEN` secret. Replace the placeholder before using a printed
curl command; each newly rendered command gets a fresh request key, while retries of the same
command retain that key.

CLI output uses the existing color controls: active is green, expired/revoked red, and permanent
expiry/address placeholders yellow. Tokens and curl command values contain no ANSI escapes.
Piped/redirected output is plain in default `auto` mode; `--color never` disables styling.

## Network configuration

The default path is `CI → Gateway:80 → Controller:17082`. Controller and Gateway may be separate
nodes. The Controller automatically distributes an HTTP-only `/v1/deploy/` system route before
application routes and redirects. It does not proxy management APIs or inject the cluster token.
Unknown deployment paths return an error rather than falling through to an application.

The Controller deployment listener defaults to `0.0.0.0:17082`. Change it with
`swarmlite serve --deploy-listen 0.0.0.0:18082` or `SWARMLITE_DEPLOY_LISTEN`; Gateway upstreams use
the Controller's cluster-reachable advertised host and this port. Ensure custom bindings are
reachable by Gateway nodes. Gateway must listen on HTTP 80 (its default).

Keep Controller ports 17080 and 17082 private; allow 17082 only from Gateway nodes and expose
Gateway 80. Without Gateway, explicitly use the Controller's deployment address in CI; binding
it to port 80 requires an unused port and appropriate privileges. It never shares or steals a
Gateway listener.

HTTP sends credentials and deployment traffic in plaintext, including the Gateway-to-Controller
hop. Public use risks interception, replay, and tampering. Token scope, expiry, rate limits, and
idempotency do not encrypt the connection. Use a trusted network or add an HTTPS entry point
when those risks are unacceptable; this implementation does not provision certificates.

## GitHub Actions

After building and pushing the Service's configured tag, use a protected GitHub Environment
with `DEPLOY_TOKEN` in Secrets and the completed `DEPLOY_URL` in Variables:

```yaml
jobs:
  trigger-deployment:
    runs-on: ubuntu-latest
    environment: production
    concurrency:
      group: production-api
      cancel-in-progress: false
    steps:
      - name: Trigger deployment
        shell: bash
        env:
          DEPLOY_URL: ${{ vars.DEPLOY_URL }}
          DEPLOY_TOKEN: ${{ secrets.DEPLOY_TOKEN }}
        run: |
          set -euo pipefail
          status=$(curl --fail-with-body --silent --show-error \
            --proto '=http,https' --connect-timeout 10 --max-time 30 \
            --output deploy-response.json --write-out '%{http_code}' \
            --request POST \
            --header "Authorization: Bearer $DEPLOY_TOKEN" \
            --header "Idempotency-Key: github:$GITHUB_REPOSITORY:$GITHUB_RUN_ID:$GITHUB_RUN_ATTEMPT" \
            --header 'Content-Type: application/json' \
            --data '{}' "$DEPLOY_URL")
          test "$status" = 202
          cat deploy-response.json
```

For the full build workflow, serialize promotion of the stable deployment tag as well as the
trigger. Check that the commit is still eligible before promoting, so an older build finishing
late cannot overwrite a newer release. Never expose production secrets to untrusted PR code.

`202 Accepted` means durably accepted, not deployed or healthy. The operation continues if CI
disconnects or Controller restarts. Reuse the same key and JSON body to recover a lost response;
the response contains the original `operation_id`. New keys during an active Stack operation
return `409`, not implicit replacement or queuing. Keys are retained for at least 90 days.

Optionally poll `GET /v1/deploy/operations/OPERATION_ID` with the same bearer token. Status is
`queued`, `running`, `succeeded`, or `failed`; `succeeded` means healthy. `stalled` and `blocked`
are separate flags requiring inspection, not successful completion. Query parameters
`after_revision=N&wait_seconds=25` enable bounded long polling. The returned `Location` is
relative, so use the same public Gateway host. Another token cannot query this operation.

Requests require `Idempotency-Key` and accept only `{}` or a `source` object of string audit
metadata. They cannot change the image reference, environment, volumes, routes, replicas,
credentials, or arbitrary commands. New operations are limited to one per second per credential,
with a persistent one-operation-per-Stack slot. Revocation/expiry rejects submission and status
queries but does not cancel a previously accepted operation.

Pull preparation finishes on eligible nodes before old containers are replaced. Each node's
resolved image ID is persisted and passed to the Runtime; a missing prepared image fails rather
than pulling a potentially different tag. Migration to another node requires preparation there
first. Mutable tags do not guarantee identical image bytes across nodes or byte-exact rollback.
There is no automatic CI rollback.

The schema-12 upgrade creates a consistent, permission-restricted
`swarmlite.schema12.UUID.sqlite` backup in the Controller data directory before migration. Keep
it protected. Older Controllers reject the upgraded database; do not downgrade over it.
