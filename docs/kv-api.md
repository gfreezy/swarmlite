# Generic KV API

Every request requires `Authorization: Bearer <cluster-token>`. The single controller serves all
reads and writes from its SQLite database. Consumers may treat an unavailable response as a cache
miss when KV is only an optimization.

## Values

Write an opaque value with `PUT /v1/kv`:

```json
{
  "key": "my-app/items/example",
  "value_base64": "dmFsdWU=",
  "version": {
    "physical_unix_ms": 1785289000000,
    "logical": 0,
    "replica_id": "writer-7f22"
  },
  "modified_at_unix_ms": 1785289000000
}
```

Delete an exact key, or a complete prefix when `recursive` is true, with `DELETE /v1/kv`:

```json
{
  "key": "my-app/items",
  "version": {
    "physical_unix_ms": 1785289001000,
    "logical": 0,
    "replica_id": "writer-7f22"
  },
  "modified_at_unix_ms": 1785289001000,
  "recursive": true
}
```

Both mutations return the winning version and whether this mutation won:

```json
{
  "applied": true,
  "version": {
    "physical_unix_ms": 1785289001000,
    "logical": 0,
    "replica_id": "writer-7f22"
  }
}
```

Read an exact key with `GET /v1/kv?key=my-app%2Fitems%2Fexample`. Missing values return `404`.
Versions are compared lexicographically by `(physical_unix_ms, logical, replica_id)`, so stale
writes and stale tombstones cannot replace newer data.

List keys with `GET /v1/kv/keys?prefix=my-app%2Fitems&recursive=true`. Direct listing returns
only the next path component; recursive listing returns all descendant components. Read metadata
with `GET /v1/kv/stat?key=my-app%2Fitems%2Fexample`; `is_value` distinguishes an exact value from
a prefix.

Keys use non-empty slash-separated components without a leading or trailing slash. A key is
limited to 1024 bytes and a decoded value to 4 MiB.

## Locks

Acquire an expiring lock with `POST /v1/kv/locks/acquire`:

```json
{
  "name": "my-app/jobs/example",
  "owner_id": "writer-7f22",
  "lease_millis": 30000
}
```

The response status is `acquired` or `busy`. An acquired response includes a monotonically
increasing `fencing_token`. Renew with `POST /v1/kv/locks/renew` and release with
`POST /v1/kv/locks/release`:

```json
{
  "name": "my-app/jobs/example",
  "owner_id": "writer-7f22",
  "fencing_token": 42,
  "lease_millis": 30000
}
```

Release accepts the same body with `lease_millis` omitted. A stale owner or fencing token returns
`409`. Leases may be from 1 second through 5 minutes. Clients should renew periodically, for
example after each third of the lease, and must stop assuming ownership if renewal fails.
