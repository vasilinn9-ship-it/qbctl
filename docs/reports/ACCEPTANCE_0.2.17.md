# Acceptance report: qbctl 0.2.17

Date recorded: 2026-10-02. This is a sanitized summary of the local acceptance record, not a fresh test run in this repository.

## Result

- The recorded acceptance completed two explicitly scoped foreground runs.
- 22 completed tasks were processed across those runs.
- 22 torrent files were archived and 37 payload files were transferred.
- The final job summaries reported no pending operations or issues.
- Watch and background execution remained disabled.
- New completions outside each frozen scope were left for a later explicit run.

## Checks reported

The recorded runs used the CLI to perform operations and read fresh client status afterward. Completion processing used per-task manifest preflight, journaled file moves, no-overwrite behavior, and removal of the client entry with data deletion disabled. A known file belonging to another incoming torrent remained in the working directory when metainfo confirmed its relative path and size.

## Verification boundaries

This report does not claim a full automatic test run for version 0.2.17. The source notes that its acceptance was a live operational check; earlier automated test counts apply only to the versions stated in their original local reports. Power loss, storage failure, all API versions, and long-running watcher behavior were not established by this acceptance.

Live torrent names, hashes, file paths, client snapshots, and raw JSON receipts were excluded to avoid publishing personal operational data. See the test sources in `tests/` for isolated scenarios.
