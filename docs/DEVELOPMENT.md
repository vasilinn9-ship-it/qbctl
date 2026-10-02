# Development guide

## Layout

- `qbctl/`: application modules
- `qbctl_entry.py` and `qbctl.cmd`: Windows entry points
- `config.example.toml`: safe template; copy it to ignored local `config.toml`
- `tests/test_*.py`: isolated unit suite
- `docs/reports/`: curated, sanitized reports; raw client snapshots remain local
- `result.schema.json`: JSON output contract

## Local setup

Install Python 3.12+ on Windows, clone the repository, copy `config.example.toml` to `config.toml`, and edit the directory settings. Set API credentials in the process environment; do not put passwords in TOML, command lines, test fixtures, or commits.

The standard-library test command is:

```powershell
py -3 -m unittest discover -s tests -v
```

Tests should use temporary directories and fake/local HTTP APIs. They must not require the user's qBittorrent session, real torrent metadata, or data folders. Keep generated test output under ignored runtime or sandbox folders.

## Change checklist

1. Identify the relevant invariant in `PROTOCOL.md` and the code path in `ARCHITECTURE.md`.
2. Keep filesystem intents, mutations, receipts, and recovery behavior consistent. Preserve no-overwrite semantics and request-ID idempotency.
3. Add or update isolated tests for code changes. Never use live torrent/client data as a test fixture.
4. Run the unit suite and report the exact command and result. Distinguish automated tests from live acceptance and historical reports.
5. Update `result.schema.json` and its documentation when the result contract changes.
6. Update a curated report under `docs/reports/` when implementation or acceptance status changes. Do not copy raw operational JSON, local databases, backup folders, torrent hashes, or personal file names into the public repository.

## Before a live operation

A live `--apply` operation can change qBittorrent state and move files. Use a separate disposable qBittorrent profile and temporary folders for end-to-end development. Never point a development checkout at production paths unless the operator explicitly authorizes that live operation.
