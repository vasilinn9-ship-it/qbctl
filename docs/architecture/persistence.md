# Persistence architecture

SQLite is the authoritative durable journal.

## Separation from public protocol

Public Protobuf and persistence representation are independent.

Do not persist `qbctl.v1.Response` as the primary state model.

If Protobuf BLOBs are useful, use an internal namespace such as:

~~~text
qbctl.persistence.v1
~~~

and keep decision-critical fields normalized in SQL.

## Core durability

Initial settings:

~~~text
journal_mode = WAL
synchronous = FULL
foreign_keys = ON
~~~

The daemon is the only writer.

## Incremental schema rule

The conceptual model lists the durable entities required by the completed v1, but physical tables are introduced at first real use. Slice 1 intentionally persists only schema metadata/health. Mutation tables arrive with mutation semantics; job/plan tables arrive with jobs/plans.

This avoids freezing speculative columns before the owning state machine exists.

## Durable entities

Core logical state includes:

- requests;
- operations;
- operation_events;
- operation_files;
- jobs;
- job_operations;
- torrent_registry;
- torrent_aliases;
- policies;
- plans;
- schema metadata/migrations.

## Transaction boundary

Before an external mutation:

~~~text
commit request/operation + pending effect intent
~~~

Then perform the effect.

After a fresh observation proves the postcondition:

~~~text
commit evidence + receipt + next checkpoint
~~~

A crash between effect and receipt intentionally leaves a pending effect that recovery observes.

## Migration

- newer-than-binary schema fails closed;
- older schemas migrate sequentially;
- non-trivial migration requires a consistent backup;
- corrupted authoritative state is never silently replaced by an empty database.
