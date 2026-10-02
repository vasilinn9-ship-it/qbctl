# Architecture layers

This document expands the crate/layer ownership defined in [../ARCHITECTURE.md](../ARCHITECTURE.md).

## qb-domain

Pure business semantics:

- strong IDs and torrent identity aliases;
- operation states and legal transitions;
- manifests and file claims;
- plan guard semantics;
- queue/storage policy value objects;
- no-overwrite and terminal handoff invariants.

Must not depend on Tokio, Protobuf, SQL, HTTP, Win32, Clap, environment variables, or logging.

## qb-application

Owns behavior:

- use cases;
- ports;
- application outcomes/problem codes;
- RequestId idempotency semantics;
- intent/effect/observation/receipt orchestration;
- plans, jobs, recovery decisions.

It depends on qb-domain only.

Representative ports:

- TorrentClient;
- MetainfoReader;
- Journal;
- Storage;
- PolicyStore;
- Clock;
- IdGenerator.

## qb-proto

Owns only the public wire contract:

- schemas;
- generated wire types;
- protocol version constants.

No business validation lives here.

## qb-ipc

Owns local transport mechanics:

- Windows Named Pipe client/server;
- framing;
- frame limits;
- connection lifecycle;
- transport errors.

It must not dispatch application use cases.

## protocol adapter

The protocol adapter is an **outer adapter hosted by qbctld**, not part of qb-application.

It:

- validates wire requests;
- maps protobuf request -> typed application input;
- invokes application services;
- maps application outcome -> protobuf response.

It must not query SQLite/qBittorrent/filesystem directly.

## qb-metainfo

Local .torrent parser implementing MetainfoReader.

It is separate from qb-qbit because bencode/identity/manifest parsing is a BitTorrent metadata concern, not a qBittorrent Web API concern.

## qb-qbit

Implements TorrentClient and owns:

- Web API auth/session;
- HTTP/JSON DTOs;
- compatibility/capability matrix;
- state normalization;
- external mutation certainty classification.

JSON must not escape this crate.

## qb-journal

Implements durable repository/journal ports:

- SQLite setup/migrations;
- normalized schema;
- request replay/idempotency persistence;
- operations/events/jobs/plans/registry;
- atomic transition transactions;
- internal persistence BLOBs when needed.

It does not decide legal business transitions.

## qb-win

Implements Windows-specific ports and primitives:

- managed roots/paths;
- volume/free-space discovery;
- file identity;
- same-volume no-replace rename;
- cross-volume copy/flush/verify/publish/delete primitives;
- runtime root;
- credentials;
- later Windows Service primitives.

## qbctld

Composition/runtime host only.

No use-case workflow should be implemented directly in the binary.

## qbctl

Thin client:

- Clap;
- request construction;
- IPC;
- rendering;
- exit-code mapping.

It never accesses qBittorrent, SQLite, or payload storage directly.
