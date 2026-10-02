# Rust v1 implementation

Rust v1 implements the architecture defined in [ARCHITECTURE.md](ARCHITECTURE.md).

The Python `qbctl 0.2.17` controller remains alongside the Rust rewrite until parity/release acceptance. Its architecture is legacy documentation, not the template for Rust module boundaries.

## Workspace target

Production architecture is **8 library crates + 2 binaries**:

~~~text
apps/qbctl
apps/qbctld

crates/qb-domain
crates/qb-application
crates/qb-proto
crates/qb-ipc
crates/qb-metainfo
crates/qb-qbit
crates/qb-journal
crates/qb-win
~~~

## Protocol boundary

Protocol is separate from the application:

~~~text
qb-proto          = wire schema
qb-ipc            = Named Pipe transport
qbctld/protocol   = wire <-> application mapping
qb-application    = use cases
qb-domain         = invariants/state machines
~~~

`qbctld` must remain a composition/runtime host; application workflows must live in `qb-application`.

## Large implementation slices

Implementation issue #13 is organized into six large logical vertical slices:

1. #14 Foundation & control plane.
2. #15 qBittorrent observation/direct control + qb-metainfo implementation.
3. #16 Windows storage & admission pipeline.
4. #17 Completion handoff & recovery.
5. #18 Reconcile, durable jobs & full agent workflow.
6. #19 Production hardening, Windows Service & release acceptance.

## Slice 1 target

Slice 1 establishes:

- the 8-crate workspace boundary, including an initially minimal `qb-metainfo` crate;
- Protobuf v1 handshake/system commands;
- explicit protocol adapter inside qbctld;
- Windows Named Pipe transport;
- SQLite schema/migration foundation;
- ProgramData/LocalAppData runtime-root model;
- single daemon instance;
- `qbctld run`;
- `qbctl capabilities/status/daemon status/doctor`;
- human/proto/fields output foundations.

qBittorrent HTTP and actual metainfo parser behavior belong to Slice 2.

## Development

On Windows:

~~~powershell
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
~~~

The daemon never implies automatic watch/reconcile. Automation remains opt-in.
