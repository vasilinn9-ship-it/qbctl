# Rust v1 implementation

The Rust rewrite is developed alongside Python `qbctl 0.2.17` until parity and release acceptance are complete.

## Architecture

The approved architecture is tracked in GitHub issues #1–#12. Implementation is tracked as large vertical slices under issue #13.

Production workspace:

```text
apps/qbctl      thin CLI client
apps/qbctld     daemon/composition root

crates/qb-domain
crates/qb-application
crates/qb-proto
crates/qb-ipc
crates/qb-qbit
crates/qb-journal
crates/qb-win
```

## Slice 1

Slice 1 establishes the control plane:

- Protobuf v1 handshake and system commands;
- Windows Named Pipe transport;
- SQLite schema v1 with WAL + FULL durability;
- ProgramData/LocalAppData runtime-root model;
- single daemon instance guard;
- `qbctld run`;
- `qbctl capabilities`, `status`, `daemon status`, and `doctor`;
- human/proto/fields output foundations.

Torrent HTTP control and payload mutation are intentionally absent until later slices.

## Development

On Windows:

```powershell
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Run the daemon in user mode:

```powershell
cargo run -p qbctld -- run
```

Then in another shell:

```powershell
cargo run -p qbctl-rs -- status
cargo run -p qbctl-rs -- capabilities
cargo run -p qbctl-rs -- doctor
```

For isolated development, set `QBCTL_RUNTIME_DIR` to a temporary directory. The default user runtime root is `%LOCALAPPDATA%\qbctl`.

The Rust daemon does not automatically watch, reconcile, or mutate qBittorrent in Slice 1.
