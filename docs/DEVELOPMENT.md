# Development guide

Rust v1 is the active architecture target. Python 0.2.17 remains a behavioral/reference implementation until Rust release acceptance.

Start with:

- [ARCHITECTURE.md](ARCHITECTURE.md)
- [INVARIANTS.md](INVARIANTS.md)
- [RUST_V1.md](RUST_V1.md)

## Rust workspace

Production target:

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

Dependency direction and responsibilities are defined in [architecture/layers.md](architecture/layers.md).

## Rust change checklist

Before changing implementation:

1. Identify the owning layer/crate.
2. Identify affected invariant IDs from [INVARIANTS.md](INVARIANTS.md).
3. Keep Protobuf/IPC/SQL/Win32/qBittorrent DTOs outside domain/application boundaries.
4. For every new external mutation, define:
   - preconditions;
   - durable intent;
   - external effect;
   - fresh postcondition observation;
   - durable receipt;
   - Unknown/recovery behavior.
5. Add failure-path tests, not only happy-path tests.
6. Do not introduce a new abstraction/crate/queue/cache unless it creates a real boundary or solves demonstrated complexity.

Every review of mutation code should be able to answer:

- What is authoritative state if the process dies immediately before the effect?
- What is authoritative state if it dies immediately after the effect?
- How does restart distinguish applied, not-applied, and ambiguous?
- Why can retry not duplicate/destructively repeat the effect?

## Rust commands

On Windows:

~~~powershell
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
~~~

A slice is not complete until its required Windows integration/fault tests are green.

## Runtime/data safety

For Rust development, use a temporary or dedicated `QBCTL_RUNTIME_DIR`.

Do not point development/test instances at personal payload roots or the normal qBittorrent profile. E2E work uses disposable qBittorrent profiles and synthetic torrent fixtures.

Never commit:

- credentials;
- live SQLite state;
- torrent files from real users;
- tracker passkeys;
- raw client snapshots;
- logs containing private operational data.

## Python reference implementation

Legacy Python modules remain under `qbctl/` and are used as behavior/reference evidence while Rust is incomplete.

Python tests:

~~~powershell
py -3 -m unittest discover -s tests -v
~~~

The Python implementation should not be structurally copied into Rust. In particular, the former large controller/common-module pattern is explicitly not the Rust architecture.

The historical Python architecture is preserved under [legacy/python-architecture.md](legacy/python-architecture.md).

## Architecture changes

A change requires ADR-level review before implementation if it changes:

- dependency direction;
- public protocol compatibility;
- persistence compatibility;
- mutation/recovery semantics;
- source-delete/no-overwrite guarantees;
- single-writer/concurrency model;
- local security/trust boundary.

Local implementation details that preserve these contracts do not require architecture redesign.
