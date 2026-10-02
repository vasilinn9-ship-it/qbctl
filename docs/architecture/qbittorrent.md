# qBittorrent adapter contract

This document is the Rust v1 source of truth for the qBittorrent boundary.

## Ownership

`qb-qbit` owns:
- loopback HTTP/WebUI API communication;
- login/session handling;
- WebAPI version/capability checks;
- qBittorrent JSON DTOs;
- normalization into application/domain types;
- mutation transport certainty classification.

It does not own:
- .torrent parsing;
- filesystem mutations;
- SQLite;
- CLI/protobuf;
- workflow decisions.

## Supported family

Rust v1 targets qBittorrent 5.x with WebAPI >= 2.13.0.

Compatibility is feature-based. Read-only status may remain available for a newer unknown compatible API, but mutation readiness fails closed until affected semantics are contract-tested.

Relevant compatibility points:
- 2.13.x: tracker response model used by diagnostics;
- 2.14.x: torrents/add response/status changes;
- 2.15.x: Basic authentication exists but is not the baseline;
- 2.16.x: add fields changed and combined speed-limit API appeared.

Baseline authentication remains auth/login + SID cookie.

## Security

- URL must be loopback HTTP only.
- URL credentials are rejected.
- redirects are rejected.
- environment proxy inheritance is disabled.
- Origin/Referer must match the configured qB endpoint where required.
- password/SID are never logged or persisted in operational SQLite.
- DTOs deserialize only fields required by a use case.
- tracker URLs are redacted before leaving qb-qbit.

## Mutation contract

Every qB mutation is application-journaled before the HTTP effect.

HTTP success is not semantic success. A fresh qB observation must prove the postcondition before receipt.

Timeout/disconnect after a request may have been sent produces Unknown and is never blindly retried.

## Queue concepts

These are distinct:
- qbctl target_client_count;
- qBittorrent max active downloads;
- transfer speed limits.

Changing one must not silently rewrite the others.

## Transfer limits

Public/application units are bytes/s.

Use transfer/setDownloadLimit and transfer/setUploadLimit for mutations, followed by readback.

Preferences using KiB/s are diagnostic evidence only and are explicitly normalized.

## Observation efficiency

Correctness does not depend on undocumented optimizations.

For file manifests:
- baseline is explicit torrents/files;
- fetch at most once per touched torrent within one immutable freshness snapshot;
- after mutation, observe afresh;
- no generic cache.

Any combined/batch file optimization must be independently capability-tested and optional.
