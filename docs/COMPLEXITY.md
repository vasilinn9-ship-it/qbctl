# Complexity budget

qbctl is a safety-sensitive local controller, not an application framework.

The architecture therefore separates **dependency boundaries** while deliberately avoiding speculative infrastructure.

## Keep

The following boundaries are justified because they isolate different dependencies/failure modes:

- `qb-domain` — pure invariants and state machines;
- `qb-application` — use cases and ports;
- `qb-proto` — public wire schema;
- `qb-ipc` — Windows Named Pipe/framing;
- `qb-metainfo` — local BitTorrent metadata parsing;
- `qb-qbit` — qBittorrent HTTP/JSON;
- `qb-journal` — SQLite durability;
- `qb-win` — Win32/filesystem/platform primitives;
- thin `qbctl` and `qbctld` binaries.

These are dependency boundaries, not layers that require wrappers around every function.

## Avoid

Do not add without concrete evidence:

- generic command/event buses;
- plugin frameworks;
- dependency-injection frameworks;
- repository/service/facade wrappers around already-small ports;
- generic state-machine engines;
- generic workflow DSLs;
- parallel mutation schedulers;
- cache layers over authoritative state;
- future database tables before the owning behavior exists;
- traits with only one production implementation unless tests/boundary isolation justify them;
- micro-crates for helpers/config/logging/jobs/policies.

## First-use rule

A concept is implemented when its first real use case needs it.

Examples:

- RequestId fingerprint persistence arrives with the first external mutation;
- Clock/IdGenerator ports arrive when deterministic operation IDs/time are actually consumed;
- queue policy persistence arrives with queue policy;
- job/plan tables arrive with jobs/plans;
- SecretStore arrives with qBittorrent credentials;
- migration backup machinery arrives with the first non-trivial transforming migration.

This prevents "architecture-shaped dead code".

## File/module rule

Split a module when at least one is true:

- it has a distinct dependency;
- it has a distinct test/failure boundary;
- it is becoming difficult to understand as one file.

Do not split merely to make the directory tree mirror a diagram.

## Reliability vs complexity

Reliability comes from a small set of explicit invariants:

- one authoritative daemon writer;
- durable intent before external effect;
- fresh observation before receipt;
- Unknown is not retry;
- no overwrite;
- publish-before-delete;
- exact RequestId semantics;
- deterministic recovery tests.

Adding more indirection does not make these guarantees stronger.

## Review question

For every new abstraction, ask:

> What concrete complexity, dependency, or failure mode does this remove?

If the answer is unclear, do not add it.
