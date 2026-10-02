# Industrial invariants

This document defines the **non-negotiable invariants** for Rust qbctl v1.

The keywords **MUST**, **MUST NOT**, **SHOULD**, and **SHOULD NOT** are normative. A change that violates a MUST/MUST NOT requires an explicit architecture decision before implementation; it is not a local refactor.

The goal is a production application that is **simple, modular, extensible, understandable, and recoverable after failure**.

## 1. Architecture invariants

### INV-ARCH-001 — dependencies point inward

`qb-domain` MUST NOT depend on application, transport, protocol, persistence, qBittorrent, Windows, CLI, or logging.

`qb-application` MUST depend on domain contracts only. It MUST NOT import Protobuf, Named Pipe, SQL, qBittorrent HTTP/JSON DTOs, Win32 APIs, or Clap.

Adapters MUST implement application ports. Adapters MUST NOT call each other directly.

### INV-ARCH-002 — wire types are not application types

Protobuf messages MUST NOT appear in public APIs of `qb-domain` or `qb-application`.

The daemon protocol adapter MUST translate:

~~~text
wire request -> typed application input
application outcome -> wire response
~~~

### INV-ARCH-003 — daemon is a host, not the controller

`qbctld` MUST own composition, lifecycle, IPC hosting, protocol mapping, scheduling, readiness, and shutdown.

Business workflows such as admission, completion, reconcile, queue release, and recovery decisions MUST live in `qb-application` / `qb-domain`, not in the binary.

### INV-ARCH-004 — one responsibility per dependency boundary

`qb-qbit` MUST own qBittorrent Web API communication only.

`qb-metainfo` MUST own local .torrent/bencode parsing only.

`qb-journal` MUST own SQLite persistence only.

`qb-win` MUST own Windows-specific platform/storage primitives only.

### INV-ARCH-005 — typed contracts, no generic command bus

Core application and protocol commands MUST be typed.

A generic `name + map/json args` command format MUST NOT be introduced for core operations.

Raw qBittorrent Web API passthrough MUST NOT be exposed.

## 2. Simplicity invariants

### INV-SIMPLE-001 — no speculative abstraction

A new crate, trait, framework, queue, cache, or abstraction SHOULD exist only when it establishes a real dependency/testability boundary or removes demonstrated duplication.

Do not create unused extension APIs "for the future".

### INV-SIMPLE-002 — no dumping-ground modules

Modules named `common`, `utils`, `helpers`, or `manager` MUST NOT become cross-domain dumping grounds.

A module MUST have a narrow, explainable responsibility.

### INV-SIMPLE-003 — one authoritative state owner

Operational state MUST have one authoritative owner:

- durable operation/job/request state -> SQLite journal;
- qBittorrent current state -> fresh qBittorrent observation;
- filesystem current state -> fresh storage observation.

Logs, caches, CLI output, and process memory MUST NOT become alternative authorities.

## 3. Mutation and recovery invariants

### INV-MUT-001 — every external mutation is journaled

Before any externally visible/destructive effect, the application MUST durably record an intent.

After the effect, the application MUST freshly observe the defined postcondition before recording a receipt.

Required order:

~~~text
preconditions
-> durable intent
-> external effect
-> fresh observation
-> durable receipt
~~~

### INV-MUT-002 — UNKNOWN is not retry

If an effect may have happened but is not proven, the operation MUST enter `Unknown`.

The same effect MUST NOT be automatically resent merely because a timeout, disconnect, or process crash occurred.

Recovery MUST observe external state first.

### INV-MUT-003 — RequestId is durable idempotency

Every externally initiated mutation MUST have a RequestId.

Same RequestId + same semantic command MUST resolve to the same authoritative operation/result.

Same RequestId + different semantic command MUST fail with `REQUEST_ID_CONFLICT` before side effects.

After transport uncertainty, the caller MUST reuse the same RequestId for the same semantic action.

### INV-MUT-004 — confirmed checkpoints are monotonic

An operation MUST NOT move its last confirmed checkpoint backward.

`Finished` MUST be terminal.

Recovery MAY change `Unknown/Blocked -> Active` only through an explicit evidence-backed transition.

### INV-MUT-005 — process memory is disposable

Correct recovery MUST NOT depend on an in-memory future, task, lock owner, or scheduler cursor surviving restart.

After a process crash, authoritative recovery data MUST come from the journal plus fresh external observations.

## 4. Concurrency/runtime invariants

### INV-RUN-001 — one daemon writer

Exactly one daemon instance MUST own a runtime root and authoritative writes.

On Windows, runtime ownership MUST be enforced by a kernel-backed exclusive handle. The current v1 mechanism keeps `daemon.lock` open with `CreateFile` sharing disabled (`share_mode(0)`), so a second process cannot acquire the same runtime root while the owner is alive.

The CLI MUST NOT open the operational SQLite database or mutate qBittorrent/payload directly.

### INV-RUN-002 — one mutation lane in v1

External mutations MUST be serialized through one mutation lane in v1.

Parallel mutation workers require a later ADR plus evidence that claim ordering/recovery remain correct.

### INV-RUN-003 — automation is opt-in

Starting the daemon MUST NOT implicitly start watch/reconcile/resource automation.

Background automation MUST be explicitly enabled by policy.

### INV-RUN-004 — readiness gates mutations

The daemon MUST NOT admit ordinary mutations while required migration/recovery/classification is incomplete.

Diagnostic/read-only operations MAY remain available when safely possible.

### INV-RUN-005 — graceful shutdown never invents success

Shutdown MUST stop admitting new effects, classify any in-flight effect, persist a safe checkpoint/Unknown state, and then exit.

Caller disconnect or timeout MUST NOT cancel already durably admitted work.

### INV-RUN-006 — queues and concurrency are bounded

IPC connections, queued jobs/mutations, concurrent read tasks, response sizes, and other externally driven resource pools MUST have explicit bounds.

When a bound is reached, the daemon MUST apply backpressure or reject admission with a stable problem code. It MUST NOT accept work and silently drop it.

### INV-RUN-007 — external I/O has bounded waits

qBittorrent requests, IPC waits, shutdown waits, and other external I/O MUST have explicit deadlines/timeouts appropriate to their semantics.

A timeout MUST classify certainty; it MUST NOT imply that an external mutation did not happen.

### INV-RUN-008 — untrusted input must not panic the daemon

Malformed CLI/protocol/config/metainfo/qBittorrent data MUST produce a controlled error or connection rejection.

Panics are reserved for internal programmer invariants and MUST NOT be a normal response to external input.

## 5. Persistence invariants

### INV-DB-001 — SQLite is authoritative and durable

SQLite is the authoritative durable journal.

Production settings MUST preserve the agreed durability model (WAL, FULL synchronous, foreign keys) unless superseded by measured ADR.

### INV-DB-002 — public protocol and persistence schema are independent

Public `qbctl.v1` Protobuf MUST NOT be used as the primary persisted state model.

Internal Protobuf BLOBs, if used, MUST use a separate persistence namespace/version.

Recovery-critical/searchable fields MUST remain explicit SQL columns.

### INV-DB-003 — transition writes are atomic

A semantic state transition, its event, and related receipt/projection updates that must agree MUST be committed atomically.

Optimistic revision checks SHOULD detect stale internal state even with one writer.

### INV-DB-004 — migrations fail safe

A database newer than the binary MUST fail closed for mutation.

Non-trivial destructive migrations MUST create a consistent backup first.

Corrupt authoritative state MUST NOT be silently replaced with an empty database.

## 6. Filesystem/storage invariants

### INV-FS-001 — only managed paths may be mutated

Payload/metainfo mutations MUST operate on validated managed-root references.

Arbitrary caller-provided absolute paths MUST NOT be accepted by ordinary mutation commands.

### INV-FS-002 — no overwrite

Destination overwrite MUST NOT occur.

Preflight `exists` checks are diagnostic only; the actual filesystem primitive MUST enforce no-replace semantics.

### INV-FS-003 — path escape fails closed

Traversal, ADS, reserved device names, root overlap, and reparse-point traversal MUST fail closed for managed mutation paths.

Containment MUST NOT rely only on string-prefix comparison.

### INV-FS-004 — cross-volume handoff is publish-before-delete

Cross-volume handoff MUST follow:

~~~text
exclusive temp
-> copy
-> flush
-> verify
-> no-replace publish final
-> durable destination receipt
-> source-delete intent
-> delete source
-> observe source absent
~~~

Source deletion MUST NOT happen before verified publication and durable destination evidence.

### INV-FS-005 — ambiguous filesystem state blocks

If recovery evidence is contradictory or insufficient, the operation MUST Block/Remain Unknown.

It MUST NOT guess based on timestamps, filenames, or "most likely" outcomes.

## 7. qBittorrent invariants

### INV-QBIT-001 — qBittorrent remains the transfer engine

qbctl MUST NOT reimplement torrent transfer/piece/cache logic.

### INV-QBIT-002 — JSON is isolated

qBittorrent HTTP/JSON DTOs MUST remain inside `qb-qbit`.

Unknown qBittorrent state values MUST fail closed for safety-sensitive mutations.

### INV-QBIT-003 — mutation success requires observation

HTTP 2xx alone MUST NOT be treated as final semantic success for add/stop/start/remove.

The application MUST observe the defined qBittorrent postcondition.

### INV-QBIT-004 — core record removal never deletes payload

Core completion/release record removal MUST send `deleteFiles=false`.

A future delete-with-files capability, if ever added, MUST be a separate explicitly destructive design.

### INV-QBIT-005 — local secure boundary

Production qBittorrent endpoint MUST be loopback/local according to the approved threat model.

Credentials MUST NOT appear in URL, CLI arguments, logs, public Protobuf responses, or committed config.

Redirects/proxy behavior MUST NOT silently move requests outside the approved endpoint boundary.

## 8. Protocol/CLI invariants

### INV-PROTO-001 — protocol is versioned and bounded

No application command is accepted before protocol handshake.

Breaking changes require a major version change.

Frames MUST be size-bounded; malformed/oversized input MUST NOT crash the daemon.

### INV-PROTO-002 — transport failure is not mutation status

IPC disconnect/timeout MUST NOT be translated into "mutation failed" or "mutation succeeded".

Mutation certainty comes from durable admission plus external observation.

### INV-PROTO-003 — exact mutation selectors

Mutating commands MUST use exact stable identifiers.

Fuzzy/name/substring matching MUST NOT select destructive targets.

### INV-PROTO-004 — machine output is deterministic

Machine decisions MUST use typed status/problem/error codes.

Human-readable prose MUST NOT be parsed as control logic.

`--output proto` stdout MUST contain only the protobuf payload.

### INV-PROTO-005 — no hidden mutation identity

The CLI MUST NOT silently generate and hide a new RequestId for an agent mutation.

The caller must be able to repeat the same semantic request with the same idempotency key.

## 9. Security invariants

### INV-SEC-001 — least privilege and local trust boundary

Named Pipe access MUST be restricted to the intended local identities.

Remote pipe clients MUST be rejected.

Service identity/ACL configuration MUST be explicit.

### INV-SEC-002 — secrets never become ordinary data

Secrets MUST NOT be persisted in ordinary TOML, SQLite operational payloads, logs, command lines, crash reports, or protocol responses.

### INV-SEC-003 — untrusted boundaries are validated

CLI input, Protobuf input, qBittorrent responses, metainfo paths, config, and filesystem observations MUST be treated as untrusted until validated at their boundary.

## 10. Configuration invariants

### INV-CONFIG-001 — configuration is validated before readiness

The daemon MUST fully parse and validate the configuration required for a capability before advertising that capability as mutation-ready.

Invalid configuration MUST fail closed for affected mutations and remain diagnosable through status/doctor where safe.

### INV-CONFIG-002 — operations use a stable validated policy snapshot

A durable operation MUST record or reference the policy/config revision that authorized it.

Configuration/policy changes MUST NOT silently change the meaning of an already-admitted operation. Continuation MUST revalidate the relevant revision/preconditions explicitly.

### INV-CONFIG-003 — secrets and operator config are separate concerns

Validated operator configuration may reference credentials, but secret material MUST be obtained through the secret provider boundary and MUST NOT become ordinary config serialization.

## 11. Observability invariants

### INV-OBS-001 — logs are non-authoritative

Failure to write a secondary log MUST NOT invalidate a successfully committed semantic journal transition.

### INV-OBS-002 — every mutation is correlatable

Mutation logs/events SHOULD include RequestId, OperationId, JobId where applicable, component, and stable event/problem code.

### INV-OBS-003 — status exposes blockers

Daemon status/doctor MUST expose whether mutation admission is enabled and whether Unknown/Blocked/recovery conditions prevent progress, without exposing secrets.

## 12. Test/release invariants

### INV-TEST-001 — mutation code requires failure-path tests

A new external mutation is not complete with happy-path tests only.

It MUST test relevant boundaries around:

- after intent commit;
- before effect;
- uncertain effect;
- after effect before receipt;
- restart/recovery.

### INV-TEST-002 — invariant traceability

Every MUST/MUST NOT invariant affecting executable behavior MUST map to at least one review check, unit/contract/integration/fault test, or static dependency rule.

### INV-TEST-003 — tests never require personal production data

Tests MUST use synthetic fixtures, temporary paths, fake adapters, or disposable qBittorrent profiles.

### INV-TEST-004 — CI is a release gate

Format, clippy, tests, and required Windows integration/fault suites MUST be green for the relevant slice before merge/release.

A failing mandatory safety test MUST NOT be waived as "flaky" without root-cause analysis and an explicit decision.

### INV-TEST-005 — production dependencies are reproducible

The committed `Cargo.lock` MUST match the workspace.

CI MUST use Cargo `--locked` for dependency-resolving build/test checks so a build cannot silently select different dependency versions.

## 13. Review invariant

Every implementation change SHOULD answer four questions:

1. Which layer owns this behavior?
2. Which invariant(s) does it preserve or implement?
3. What is the authoritative state after a crash at each external-effect boundary?
4. How is that behavior tested?

If these answers are unclear, the implementation is probably too coupled or too implicit.
