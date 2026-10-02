# Architecture

`qbctl` is a local controller around qBittorrent's Web API. It uses only the Python standard library. The checkout root is also the runtime root: configuration, SQLite state, locks, logs, plans, and generated reports are stored there and excluded from Git.

## Components

- `qbctl/cli.py` parses commands, creates plans, enforces explicit apply flags, and selects read-only or mutating flows.
- `qbctl/api.py` owns HTTP communication and authentication. It restricts the endpoint to loopback and uses credentials supplied through environment variables.
- `qbctl/engine.py` implements torrent admission, completion, recovery, filesystem checks, and journaled mutations.
- `qbctl/executor.py` coordinates saved jobs, bounded worker steps, foreground waits, and the explicitly started background executor.
- `qbctl/common.py` contains configuration validation, SQLite storage, locking, path guards, and filesystem helpers.
- `qbctl/registry.py`, `queue.py`, `directory.py`, and `ownership.py` support torrent identity, queue policy, directory indexing, and operation ownership.
- `qbctl/resources.py` samples supported host resource measurements. Resource guarding is a separate opt-in policy and is not enabled by executor startup.
- `qbctl/output.py` and `presentation.py` produce machine-readable and concise human-readable results.
- `result.schema.json` describes the public JSON result envelope. `tests/` contains isolated unit tests using temporary directories and fake/local APIs.

## State and concurrency

SQLite is the durable journal for operations and jobs. Filesystem changes are preceded by a recorded intent and followed by receipts so recovery can observe an already-applied change without blindly repeating it. Request IDs make repeated submissions refer to the same saved request. An unknown outcome is surfaced for diagnosis rather than automatically resending a mutation.

Payload mutations remain serialized through the existing controller and writer lock. The asynchronous coordinator schedules and observes bounded steps; it does not create a second payload mover. Watch and the background executor share exclusion and are never started implicitly.

## Runtime files

The following are created beside the CLI when it is used: `config.toml`, `state.sqlite*`, lock files, `logs/`, `plans/`, `reports/`, `backups/`, and `quarantine/`. They are operational state, not source files. Do not copy a live database into a public issue or commit it to Git.
