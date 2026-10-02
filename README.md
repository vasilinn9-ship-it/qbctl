# qbctl

`qbctl` is a small Windows command-line controller for a local qBittorrent Web API. It uses Python's standard library and keeps its operation journal in SQLite beside the program.

## Requirements

- Windows 10 or later
- Python 3.12 or later and the `py` launcher
- qBittorrent with its Web UI/API enabled on `127.0.0.1`

## Setup

1. Copy `config.example.toml` to `config.toml` and edit the four directories for your workflow.
2. Set `QBCTL_USERNAME` and `QBCTL_PASSWORD` in the PowerShell session that will run the CLI. Keep credentials out of files and command history.
3. Run `qbctl.cmd --help`.

The CLI refuses API URLs outside loopback. It does not enable its watcher or background executor by default. Read-only `status`/`doctor` commands inspect state; mutating commands require an explicit `--apply`.

## Common commands

```powershell
.\qbctl.cmd --version
.\qbctl.cmd doctor --json
.\qbctl.cmd status --json
.\qbctl.cmd run --apply --wait --request-id my-unique-run --json
.\qbctl.cmd downloads set 10 --apply --wait --request-id set-limit-10 --json
```

Use a new request ID for a new operation. If a command reports an unknown outcome or pending work, inspect its saved job/operation state and use the documented recovery command before issuing a new mutation.

## Development

Run the isolated unit suite with:

```powershell
py -3 -m unittest discover -s tests -v
```

The tests use temporary directories and fake/local HTTP APIs. They do not require a running qBittorrent instance.

## Data and safety

The root `config.toml`, SQLite state, logs, backups, plans, quarantine, and generated reports are local runtime data and are ignored by Git. Start with `config.example.toml`; do not commit live state or client snapshots. Read `docs/reports/` for a sanitized implementation and acceptance summary. The reports describe the recorded 0.2.17 work; they are not a claim of formal certification or a substitute for running the suite against a change.
