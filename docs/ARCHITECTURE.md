# qbctl Rust architecture v1.1

This document is the **source of truth** for the Rust architecture of qbctl.

GitHub issues record decisions, implementation slices, and review history. If an issue and this document disagree, the architecture docs win unless a newer ADR explicitly supersedes them.

The Python `qbctl 0.2.17` implementation remains a behavioral reference until Rust v1 acceptance. Its former architecture description is preserved under [legacy/python-architecture.md](legacy/python-architecture.md).

## Design goals

The system must remain:

- **simple** — few concepts, explicit responsibilities, no framework-like indirection;
- **modular** — business behavior is isolated from transport, storage, Windows, and qBittorrent;
- **extensible** — new clients, adapters, and operation kinds can be added without rewriting the core;
- **reliable** — every external mutation is journaled and recoverable;
- **agent-friendly** — stable typed commands, ids, statuses, and error codes;
- **developer-friendly** — ownership is obvious from the crate/module name and dependency direction.

## System shape

~~~text
                       external clients
                  CLI / agent / future UI
                            |
                            v
                 +----------------------+
                 |   protocol layer     |
                 | qb-proto + adapter   |
                 +----------+-----------+
                            |
                 +----------v-----------+
                 |   transport layer    |
                 |       qb-ipc         |
                 |  Windows Named Pipe  |
                 +----------+-----------+
                            |
                      protocol adapter
                    wire <-> application
                            |
                 +----------v-----------+
                 |    qb-application    |
                 | use cases + ports    |
                 +----+--------+--------+
                      |        |
          +-----------+        +----------------+
          |                                     |
    +-----v------+     +-------------+     +-----v------+
    |  qb-qbit   |     | qb-journal  |     |   qb-win   |
    | HTTP/JSON  |     |   SQLite    |     | filesystem |
    +------------+     +-------------+     +------------+

    +-------------+
    | qb-metainfo |
    | local parse |
    +-------------+

                 +----------------------+
                 |      qb-domain       |
                 | identities/invariants|
                 +----------------------+
~~~

The diagram is conceptual: adapters depend inward on application/domain; the application does **not** depend outward on them.

## Production workspace

Rust v1 uses **8 library crates + 2 binaries**:

~~~text
apps/
  qbctl/          thin CLI client
  qbctld/         daemon composition/runtime root

crates/
  qb-domain/      pure identities, invariants, state machines
  qb-application/ use cases, ports, semantic outcomes
  qb-proto/       public Protobuf wire schema/generated types
  qb-ipc/         Windows Named Pipe framing/connection mechanics
  qb-metainfo/    local .torrent/bencode parsing and manifest extraction
  qb-qbit/        qBittorrent Web API HTTP/JSON adapter
  qb-journal/     SQLite persistence, migrations, durable journal
  qb-win/         Windows filesystem/platform/credential/service primitives
~~~

The number of crates is not a goal by itself. A crate exists only when it owns a distinct dependency boundary. Do not create micro-crates for config, logging, jobs, policies, or helpers without evidence that the boundary is useful.

## Dependency direction

Allowed production dependencies:

~~~text
qb-domain
  -> no workspace dependencies

qb-application
  -> qb-domain

qb-proto
  -> no domain/application dependency

qb-ipc
  -> transport/framing; may use qb-proto only for wire helpers

qb-metainfo
  -> qb-domain and/or qb-application port contracts

qb-qbit
  -> qb-domain + qb-application

qb-journal
  -> qb-domain + qb-application

qb-win
  -> qb-domain + qb-application

qbctl.exe
  -> qb-proto + qb-ipc

qbctld.exe
  -> composition of application + adapters + protocol/IPC
~~~

Forbidden:

- domain -> application/infrastructure;
- application -> qbit/journal/win/ipc/proto/metainfo implementation;
- adapter -> another adapter;
- CLI -> qBittorrent/SQLite/payload;
- Protobuf types in domain/application APIs;
- SQL/Win32/HTTP DTOs leaking inward.

## Protocol is an outer layer

The public protocol is deliberately separate from the application model.

~~~text
qb-proto          = what is sent on the wire
qb-ipc            = how bytes are transported locally
protocol adapter  = wire request/response <-> application command/outcome
qb-application    = what the program does
qb-domain         = rules that make the behavior valid
~~~

**Protobuf messages are not domain entities.**

The daemon must contain an explicit protocol adapter. Named Pipe server code must not become an application controller.

Recommended daemon structure:

~~~text
apps/qbctld/src/
  main.rs
  bootstrap.rs
  runtime.rs
  server.rs
  protocol/
    mod.rs
    handshake.rs
    decode.rs
    dispatch.rs
    encode.rs
~~~

Responsibilities:

- `server.rs`: accept/read/write/disconnect only;
- `protocol/*`: validate wire messages and map them to/from application types;
- `runtime.rs`: daemon lifecycle, scheduling, shutdown;
- `bootstrap.rs`: configuration and dependency wiring;
- business workflows: **qb-application**, never qbctld.

This boundary allows a future GUI, different local transport, or remote authenticated transport to reuse the same application core.

## Metainfo is not qBittorrent

`qb-metainfo` is a separate dependency boundary.

It owns:

- bencode/metainfo parsing;
- BitTorrent v1 info-hash calculation;
- v2/hybrid identity extraction where supported;
- file manifest/path/size extraction;
- total payload size;
- rejection of malformed/unsupported metainfo.

It does not:

- perform HTTP;
- know qBittorrent Web API DTOs;
- persist tracker/passkey data without an explicit future requirement;
- perform torrent transfer logic.

`qb-qbit` owns only qBittorrent communication and normalization.

## Persistence is not the public protocol

SQLite is the authoritative durable journal, but public wire Protobuf and persistence representation are independent.

~~~text
qbctl.v1            public protocol namespace
qbctl.persistence.* internal persistence schema if Protobuf BLOBs are used
~~~

Changing the CLI/IPC protocol must not automatically force a database migration. Searchable and recovery-critical fields remain normalized SQL columns.

## Reliability model

Every destructive or externally visible mutation follows:

~~~text
preconditions
    ->
durable intent
    ->
external effect
    ->
fresh observation
    ->
durable receipt
~~~

Core rules:

- RequestId makes externally initiated mutations idempotent.
- UNKNOWN means an effect may have happened and **is not a retry state**.
- Unknown effects are resolved by observation/recovery before reissue.
- Destination overwrite is forbidden.
- Cross-volume handoff is copy -> flush -> verify -> no-replace publish -> receipt -> source-delete intent -> delete.
- qBittorrent record removal in core workflows always uses file deletion disabled.
- one daemon is the authoritative writer;
- one serialized mutation lane is the v1 default;
- background automation is opt-in.

See [recovery.md](architecture/recovery.md) and [windows-storage.md](architecture/windows-storage.md).

## Daemon rule

`qbctld` is a composition/runtime host, not a business-logic crate.

Allowed responsibilities:

- bootstrap/configuration/secrets;
- adapter construction;
- IPC server;
- protocol mapping;
- mutation scheduler;
- lifecycle/readiness/shutdown;
- logging/tracing setup.

Forbidden in the daemon binary:

- torrent completion workflow implementation;
- payload handoff policy;
- admission/reconcile business decisions;
- direct SQL business decisions;
- qBittorrent DTO interpretation.

If a function could be tested without starting the daemon process, it usually belongs in a library crate.

## Simplicity rules

1. Prefer typed commands over generic maps/JSON.
2. No `common`, `utils`, or `manager` dumping grounds.
3. Keep ports small and capability-oriented.
4. Split crates by dependency boundary, modules by responsibility.
5. Avoid speculative extension APIs; preserve seams, not unused methods.
6. Human messages are presentation; stable machine behavior uses typed status/problem codes.
7. Do not introduce parallel mutation workers until correctness tests show a real need.

## Runtime roots

Installed/service mode:

~~~text
%ProgramData%\qbctl\
  config.toml
  state.sqlite
  logs\
  backups\
  quarantine\
~~~

User/development mode:

~~~text
%LOCALAPPDATA%\qbctl\
~~~

The source checkout is never the production runtime root.

## Implementation slices

Implementation remains organized as large vertical slices:

1. Foundation & control plane.
2. qBittorrent observation/direct control + metainfo implementation.
3. Windows storage & admission.
4. Completion handoff & recovery.
5. Reconcile/jobs/full agent workflow.
6. Production hardening/Windows service/release acceptance.

Each slice must produce end-to-end behavior, not merely fill one crate.

## Industrial invariants

All implementation is governed by [INVARIANTS.md](INVARIANTS.md). Its MUST/MUST NOT rules are release and review constraints, not suggestions.

## Detailed documents

- [INVARIANTS.md](INVARIANTS.md) — non-negotiable production invariants.
- [layers.md](architecture/layers.md) — layer/crate ownership and dependency rules.
- [protocol.md](architecture/protocol.md) — wire/transport/protocol-adapter boundary.
- [persistence.md](architecture/persistence.md) — SQLite and internal persistence contract.
- [recovery.md](architecture/recovery.md) — mutation/recovery semantics.
- [windows-storage.md](architecture/windows-storage.md) — Windows path and handoff safety.
- [PROTOCOL.md](PROTOCOL.md) — operational invariants inherited from the controller.

## Change rule

Changes that alter dependency direction, mutation/recovery semantics, public protocol compatibility, or persistence compatibility require an ADR-level update before implementation.
