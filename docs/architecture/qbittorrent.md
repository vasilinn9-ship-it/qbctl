# qBittorrent adapter contract

This document is the Rust v1 source of truth for the qBittorrent boundary.

## Ownership

`qb-qbit` owns:
- loopback HTTP/WebUI API communication;
- login/session handling;
- WebAPI version/capability checks;
- qBittorrent JSON DTOs;
- normalization into application/domain types;
- canonical qBittorrent torrent selectors (including full v2 info-hash -> 160-bit qBittorrent TorrentID normalization);
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

Baseline authentication remains `auth/login` session-cookie auth. The adapter accepts the legacy `SID` cookie and the qBittorrent 5.2 `QBT_SID_<webui-port>` cookie, and treats only the documented legacy `200 Ok`/`Ok.` or modern `204 No Content` login success shapes as authenticated.

## Security

- URL must be loopback HTTP only.
- URL credentials are rejected.
- redirects are rejected.
- environment proxy inheritance is disabled.
- Origin/Referer must match the configured qB endpoint where required.
- password/session cookies are never logged or persisted in operational SQLite.
- DTOs deserialize only fields required by a use case.
- required observation fields fail closed when absent; incompatible responses are not silently defaulted to zero/false evidence.
- tracker URLs are reduced to scheme/host/port before leaving qb-qbit; credentials, query values and secret-like path tokens derived from the tracker URL are also redacted from diagnostic messages.

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


## Torrent selectors

qBittorrent's WebAPI `hash` parameter identifies its internal 160-bit `TorrentID`. For v1/hybrid torrents that is the 40-hex v1-compatible ID. For a v2-only torrent, qBittorrent derives the same 160-bit ID from the first 160 bits of the SHA-256 info-hash.

The domain selector therefore accepts either:
- a 40-hex qBittorrent TorrentID; or
- a full 64-hex v2 info-hash, which is canonicalized to the corresponding first-40-hex qBittorrent TorrentID before WebAPI use and RequestId fingerprinting.

Full v1/v2 identity aliases remain the responsibility of metainfo/identity models; mutation and read selectors use the canonical engine ID.

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


## Durable direct mutations

Slice 2 uses one independently observable effect per RequestId.

Supported direct effects:
- stop one exact torrent;
- start one exact torrent;
- set qBittorrent max active downloads;
- set global download limit;
- set global upload limit;
- set qbctl target client count locally.

The durable lifecycle is:

```text
Prepared
-> EffectPending
-> ObservedApplied
-> Finished
```

Exceptional states are `Blocked`, `Unknown` and `Failed`.

After restart, `EffectPending` and `Unknown` are observed before any retry. If the desired state is already present, the existing OperationId is completed without sending the effect again. If a fresh observation proves the effect did not happen, the same operation may become retry-ready.

A blocked preflight is not automatic restart work. Repeating the same RequestId explicitly revalidates it.

## Queue policy

`target_client_count` is qbctl-owned policy, not a qBittorrent preference. It is stored atomically in SQLite with a monotonically increasing policy revision.

qBittorrent `max_active_downloads` remains a separate setting. If qBittorrent queueing is disabled, changing the active-download count blocks instead of silently enabling queueing.

## Transfer-limit units and quantization

Application and protocol units are bytes/s.

The canonical mutation/readback path is the transfer API, whose rate-limit values are bytes/s. qBittorrent preferences/UI represent global limits as whole KiB/s; preferences are diagnostic evidence only.

Postcondition matching therefore keeps requested and effective values distinct and accepts a non-zero effective value when its difference from the requested value is less than 1024 bytes/s. Zero remains exact because it represents unlimited. This prevents whole-KiB/s persistence from being misreported as configuration drift while still rejecting larger mismatches.
