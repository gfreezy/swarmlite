# swarmlite-gateway

`swarmlite-gateway` owns Swarmlite's HTTP routing domain:

- the `x-swarmlite` routing data model;
- normalization and validation;
- internal service port discovery from route references;
- deterministic rule ordering;
- Caddy JSON generation.

The crate has no dependency on controllers, scheduling, Raft, redb, or a container runtime. Its
renderer accepts a callback which resolves an internal `(stack, service, port)` into healthy dial
addresses. The main `swarmlite` crate implements that adapter from replicated cluster state.

The editor schema is at [schema/stack.schema.json](schema/stack.schema.json). During deployment, the
main process extracts the service names from the same Stack file and passes them to
`validate_and_normalize`; no external validation service or network request is involved. This
in-process check is authoritative because the standard JSON Schema implementation used by VS Code
cannot compare `backend.service` with arbitrary keys in the sibling `services` map.
