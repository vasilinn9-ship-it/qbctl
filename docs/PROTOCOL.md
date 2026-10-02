# Operational protocol and invariants

> Rust v1 normative safety rules are defined in [INVARIANTS.md](INVARIANTS.md). This document describes workflow-level operational invariants inherited from the Python reference controller.

All four data folders are configurable. In examples below, *incoming*, *archive*, *working*, and *completed* mean the configured directories; no machine-specific paths are assumed.

## Admission

- Read new `.torrent` files only from the top level of *incoming*. Do not treat the archive directory as an input queue.
- Identify torrents by their metainfo info-hash, not by filename. Before admission, compare against archived and already-known hashes. A torrent already archived must not be re-added; a redundant copy in *incoming* can be removed only through the journaled duplicate-cleanup path.
- Add accepted work with the configured *working* save path and automatic torrent management disabled. Incomplete payload stays in *working*.
- Apply the configured queue/download limit separately from the number of saved client records. Preserve user-paused tasks.

## Completion handoff

A task is eligible only after the client reports complete and the controller confirms all files are selected, the expected manifest is known, file sizes and identities are valid, and destination paths do not conflict. The preflight for each selected task must finish before that task is stopped.

The handoff order is:

1. Stop only the completed task and observe that stop.
2. Move its `.torrent` from *incoming* to *archive* without overwriting.
3. Move each verified payload file from *working* to *completed* without overwriting. Persist the intent and receipt for every file.
4. Remove the qBittorrent record with file deletion disabled.
5. Refill the queue only within configured limits and after admission checks.

If a step has an uncertain outcome, recover from its saved journal and observe the filesystem/client state. Do not send the same uncertain mutation again under a new request ID. A destination conflict blocks the affected task and preserves the files for review.

After a completed file has a durable handoff receipt, normal status and audit must not reopen or revalidate that payload in *completed*. A narrowly targeted recovery may inspect only the destination of a rename whose receipt was not committed yet. Handoff confirms the move at that time; it does not guarantee future storage health.

## Jobs, watch, and limits

`--wait` runs one saved job to its terminal result and does not wait for unfinished downloads to finish. `--enqueue` records a job but does not mean it has run. The background executor processes explicitly submitted jobs; it does not create new work by itself. Continuous discovery through watch must be explicitly enabled and remains disabled by default.

Changing a target limit must be followed by a fresh client observation. Stopping excess incomplete tasks must preserve their partial files and input torrent. Decreasing a limit must not reset progress. Report record count, allowed download slots, active transfers, pending operations, and newly completed work as distinct quantities.

## Guarantees and limits

The protocol protects against routine duplicate admission, path mistakes, accidental overwrite, and interrupted application steps when its journal and files remain available. It cannot guarantee data against disk failure, external deletion, filesystem or controller defects, or unrelated software modifying the same paths. Do not describe it as formally certified or promise a hard percentage ceiling for every operating-system resource.
