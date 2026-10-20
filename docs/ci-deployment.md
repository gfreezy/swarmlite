# HTTP(S) CI deployment

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
Deploy tokens are only for the HTTP(S) deployment API and cannot be used as CLI administrator credentials.

Creation prints the complete token and a curl template. Default listing hides secrets;
`list --show-secret` reveals active tokens and curl templates again. Expired/revoked tokens are
not revealed, and revocation clears the stored secret. The Controller stores retrievable secrets:
protect its data directory and database backups as credentials.

For a public deployment entry point, configure a domain first:

```bash
swarmlite config set gateway.deploy.domain deploy.example.com
swarmlite config get gateway.deploy.domain
swarmlite gateway status
```

Use an exact DNS hostname, not a URL, IP, wildcard, port, or path. ASCII DNS names (including
punycode) are accepted and normalized to lowercase. Point its A/AAAA records at your reachable
Gateway nodes or a TCP load balancer in front of them; only publish working addresses. The
Controller does not discover public IPs. With multiple Gateways, all receive the same domain
and route policy; DNS round-robin alone is not a health check.

When the domain is set, token creation and listing print the real address, and printed curl
commands need no address replacement:

```text
https://deploy.example.com/v1/deploy/services/production.api/redeploy
```

Without a configured domain, the existing HTTP mode remains available and URLs retain the
literal address placeholder:

```text
http://<GATEWAY_HOST>/v1/deploy/services/production.api/redeploy
```

In HTTP mode, replace it with the Gateway's actual IP or domain, optionally including a port.
HTTP 80 needs no port suffix. Copy the completed URL into `DEPLOY_URL` and the token into your
CI `DEPLOY_TOKEN` secret. Each newly rendered curl command gets a fresh request key, while
retries of the same command retain that key. URL rendering reads the current cluster config;
a failed config read reports an error rather than silently selecting HTTP.

Tokens are not bound to the domain. Changing it keeps existing credentials valid, but you must
update the URL in CI. Listing again shows the new address. To deliberately restore plaintext
HTTP deployment, run `swarmlite config unset gateway.deploy.domain`; the CLI prints a warning.
After any change, use `swarmlite gateway status` to confirm every public Gateway has applied the
desired generation before relying on the new policy. Updates are not atomic across nodes.

CLI output uses the existing color controls: active is green, expired/revoked red, and permanent
expiry/address placeholders yellow. Real HTTPS URLs, tokens and curl command values contain no ANSI escapes.
Piped/redirected output is plain in default `auto` mode; `--color never` disables styling.

## Network configuration

With a domain, the path is `CI → HTTPS Gateway:443 → private HTTP Controller:17082`.
Without a domain, it is `CI → HTTP Gateway:80 → private HTTP Controller:17082`. Controller and
Gateway may be separate nodes. The Controller automatically distributes system deployment routes
before application routes and redirects. They do not proxy management APIs or inject the cluster
token. Unknown deployment paths and wrong HTTPS hosts cannot fall through to an application.

Domain mode accepts deployment requests only over HTTPS with the configured Host. HTTP requests
to `/v1/deploy` and its descendants receive `403` for every Host, including direct IP requests;
they are not redirected or forwarded. A forged `X-Forwarded-Proto` does not bypass this rule.
Only the deployment API is disabled on HTTP: Gateway keeps its port 80 listener for business
routes, certificate validation and Gateway ownership probes. Certificate failures never
automatically re-enable HTTP deployment.

Caddy manages certificates using the existing Gateway storage coordination. Provide reachable
ports 80/443, correct DNS, and persistent certificate storage. A front-end load balancer should
forward TCP/TLS to the Gateways rather than terminating TLS and forwarding plaintext to this
HTTPS-only route. Custom listener settings are not overwritten; ensure they expose the required
HTTPS listener. The automated multi-Gateway test uses isolated local CAs and verifies TLS, routing
and hot reload; public-CA issuance/renewal across multiple public DNS targets still needs an
environment-specific validation before production use.

The Controller deployment listener defaults to `0.0.0.0:17082`. Change it with
`swarmlite serve --deploy-listen 0.0.0.0:18082` or `SWARMLITE_DEPLOY_LISTEN`; Gateway upstreams use
the Controller's cluster-reachable advertised host and this port. Ensure custom bindings are
reachable by Gateway nodes. Domain mode does not add TLS to this internal listener.

Keep Controller ports 17080 and 17082 private; allow 17082 only from Gateway nodes and expose
Gateway 80/443 as needed. Without Gateway, the legacy alternative is to explicitly use the
Controller's HTTP deployment address in a trusted network; binding it to port 80 requires an
unused port and appropriate privileges. It never shares or steals a Gateway listener. A configured
Gateway domain does not secure direct Controller access; keep that port private.

HTTP sends credentials and deployment traffic in plaintext, including the Gateway-to-Controller
hop. Public use risks interception, replay, and tampering. Token scope, expiry, rate limits, and
idempotency do not encrypt the connection. Use domain mode for the public CI connection and a
trusted network for the private hop. An HTTP rejection cannot undo credentials already sent in
plaintext: CI must start with the printed HTTPS URL, not rely on an HTTP redirect.

## GitHub Actions

After building and pushing the Service's configured tag, use a protected GitHub Environment
with `DEPLOY_TOKEN` in Secrets and the printed HTTPS `DEPLOY_URL` in Variables:

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
            --proto '=https' --connect-timeout 10 --max-time 30 \
            --output deploy-response.json --write-out '%{http_code}' \
            --request POST \
            --header "Authorization: Bearer $DEPLOY_TOKEN" \
            --header "Idempotency-Key: github:$GITHUB_REPOSITORY:$GITHUB_RUN_ID:$GITHUB_RUN_ATTEMPT" \
            --header 'Content-Type: application/json' \
            --data '{}' "$DEPLOY_URL")
          test "$status" = 202
          cat deploy-response.json
```

The example verifies TLS certificates, does not follow redirects, and cannot downgrade to HTTP.
For deliberately selected HTTP mode on a trusted network, replace `--proto '=https'` with
`--proto '=http'` and fill in the address placeholder; do not use `--insecure` for HTTPS.

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
