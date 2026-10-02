# Protocol architecture

The protocol is an outer contract, independent from domain/application internals.

## Three distinct concepts

~~~text
wire contract      qb-proto
transport          qb-ipc
translation        qbctld protocol adapter
~~~

Do not merge them.

## Wire contract

Namespace: `qbctl.v1`.

The wire defines:

- ClientHello / ServerHello;
- Request / Response envelopes;
- typed command payloads;
- Status;
- Problem;
- RetryGuidance;
- MutationCertainty;
- capability/version negotiation.

Typed commands are preferred:

~~~text
PauseTorrentRequest
ResumeTorrentRequest
ApplyPlanRequest
RecoverOperationRequest
~~~

Never use a generic `Command{name,args:map}` protocol.

## Transport

Local Windows transport:

~~~text
\\.\pipe\qbctl
~~~

Frame:

~~~text
u32 little-endian length
protobuf payload
~~~

Maximum frame size: 16 MiB for v1.

Transport failure does not determine mutation certainty.

## Protocol adapter

The daemon protocol adapter is responsible for:

~~~text
protobuf Request
      |
validate
      v
application command/input
      |
application service
      v
application Outcome
      |
map
      v
protobuf Response
~~~

The adapter is allowed to know both qb-proto and qb-application. Neither qb-proto nor qb-application depends on the other.

## Versioning

- major: breaking semantics/wire compatibility;
- minor: backward-compatible optional additions;
- never reuse protobuf field numbers;
- reserve removed field numbers;
- zero enum value is UNSPECIFIED;
- unknown mutation-critical enum values fail closed.

## Application independence

Application tests must be able to invoke use cases directly without Protobuf or Named Pipes.

A future UI/transport can reuse application services by providing another outer adapter.
