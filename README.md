# qbctl

`qbctl` is a Windows controller for a local qBittorrent Web API.

The repository currently contains two generations side by side:

- **Python 0.2.17** — the current behavioral/reference implementation.
- **Rust v1** — the replacement being implemented in large vertical slices.

The Rust architecture is the forward-looking source of truth for new development. Start with [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Rust v1 architecture

Rust v1 is designed to be simple, modular, extensible, reliable, agent-friendly, and understandable to a new developer.

Core boundaries:

~~~text
qbctl.exe
  -> qb-proto / qb-ipc
  -> qbctld protocol adapter
  -> qb-application
  -> qb-domain

adapters:
  qb-metainfo
  qb-qbit
  qb-journal
  qb-win
~~~

Important rules:

- Protobuf is a public wire contract, not the application/domain model.
- Named Pipe transport is separate from protocol mapping.
- `qbctld` is a composition/runtime host, not the business controller.
- qBittorrent JSON exists only in `qb-qbit`.
- .torrent parsing exists only in `qb-metainfo`.
- SQLite is an authoritative durable journal.
- external mutations follow intent -> effect -> observation -> receipt.
- UNKNOWN is not blindly retried.
- v1 uses one serialized mutation lane.

Implementation is tracked under GitHub epic #13 as six large logical vertical slices.

## Current Python reference implementation

### Requirements

- Windows 10 or later
- Python 3.12 or later and the `py` launcher
- qBittorrent with its Web UI/API enabled on `127.0.0.1`

### Setup

1. Copy `config.example.toml` to `config.toml` and edit the four directories for your workflow.
2. Set `QBCTL_USERNAME` and `QBCTL_PASSWORD` in the PowerShell session that will run the CLI. Keep credentials out of files and command history.
3. Run `qbctl.cmd --help`.

The Python CLI refuses API URLs outside loopback. It does not enable its watcher or background executor by default. Read-only `status`/`doctor` commands inspect state; mutating commands require an explicit `--apply`.

### Common Python commands

~~~powershell
.\qbctl.cmd --version
.\qbctl.cmd doctor --json
.\qbctl.cmd status --json
.\qbctl.cmd run --apply --wait --request-id my-unique-run --json
.\qbctl.cmd downloads set 10 --apply --wait --request-id set-limit-10 --json
~~~

Use a new request ID for a genuinely new operation. If a command reports an unknown outcome or pending work, inspect its saved job/operation state and use the documented recovery command before issuing a new mutation.

## Development

### Rust

On Windows:

~~~powershell
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
~~~

See [docs/RUST_V1.md](docs/RUST_V1.md).

### Python reference

~~~powershell
py -3 -m unittest discover -s tests -v
~~~

The Python tests use temporary directories and fake/local HTTP APIs. They do not require a running qBittorrent instance.

## Data and safety

Runtime state must not be committed to Git.

Python 0.2.17 still uses the checkout-local runtime convention. Rust v1 moves production runtime state to `%ProgramData%\qbctl` or `%LOCALAPPDATA%\qbctl` according to mode.

Do not commit live SQLite databases, torrent files, credentials, logs, or client snapshots.

## Developer documentation

- [Rust architecture source of truth](docs/ARCHITECTURE.md)
- [Industrial invariants](docs/INVARIANTS.md)
- [Architecture layers](docs/architecture/layers.md)
- [Protocol architecture](docs/architecture/protocol.md)
- [Persistence architecture](docs/architecture/persistence.md)
- [Recovery model](docs/architecture/recovery.md)
- [Windows storage architecture](docs/architecture/windows-storage.md)
- [Operational invariants](docs/PROTOCOL.md)
- [Rust v1 implementation plan](docs/RUST_V1.md)
- [Legacy Python architecture](docs/legacy/python-architecture.md)
- [Historical reports](docs/reports/)

The Python compatibility target remains the historical 0.2.17 baseline. Rust qBittorrent compatibility is determined by the adapter compatibility matrix and contract/real-client tests rather than a permanent hard-coded upper WebAPI version.
