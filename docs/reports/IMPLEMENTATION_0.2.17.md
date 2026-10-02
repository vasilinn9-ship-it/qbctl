# Implementation report: qbctl 0.2.17

This report summarizes the 0.2.17 implementation recorded on 2026-10-02. It intentionally omits live torrent identifiers, filenames, absolute user paths, and raw client snapshots.

## Changes

- A run freezes its initial completion scope and processes one fully preflighted completion at a time. This prevents a large batch preflight from being repeated after each bounded worker step.
- Each completion still follows the journaled sequence: verify the selected files and manifest, stop only that task, archive its `.torrent`, move payload files without overwriting existing files, persist file receipts, remove the client entry without deleting data, then refill the queue.
- A file belonging to a different incoming torrent can remain in the working directory when its exact relative path and size are declared by that torrent's metainfo.
- Watch and background execution remain opt-in. New completions that appear after a run's frozen scope are left for a later explicit run.

## Recorded live acceptance

The 0.2.17 acceptance record reports two sequential scopes processing 22 completions in total. The report records 22 archived torrent files, 37 payload files transferred, and 0 pending operations or issues at the end of those scopes. A fresh status later showed additional completions outside the frozen scopes; those were left for a subsequent run.

The acceptance report says the client's downloaded data was not read after handoff. The source folder contained live operational records; identifying torrent names, hashes, paths, and per-client snapshots are deliberately not reproduced here.

## Limits

These observations cover the recorded Windows/qBittorrent setup and the specific completed runs. They do not establish formal industrial certification, power-loss safety across all filesystems, or correctness for every qBittorrent/API version. Changes should be checked with the isolated suite and reviewed against the protocol in the source before deployment.
