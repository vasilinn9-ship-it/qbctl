# Recovery model

Recovery is a first-class behavior, not exception handling around retries.

## Universal mutation sequence

~~~text
validate preconditions
    |
record durable intent
    |
perform external effect
    |
observe actual state
    |
record durable receipt
~~~

## UNKNOWN

UNKNOWN means the effect may have happened.

Rules:

- never automatically resend the same effect merely because a timeout occurred;
- preserve the last confirmed checkpoint;
- retain the pending effect kind;
- observe qBittorrent/filesystem state;
- either confirm the effect, prove non-effect and authorize a new explicit attempt, remain Unknown, or Block on contradictory evidence.

## RequestId

Externally initiated mutations carry a RequestId.

- same ID + same semantic command -> replay existing authoritative result;
- same ID + different semantic command -> REQUEST_ID_CONFLICT;
- after transport uncertainty, repeat the same semantic command with the same RequestId.

## Single writer

Rust v1 uses one daemon and one serialized mutation lane.

This is deliberately conservative. Parallel mutation execution may be added only after claim/recovery tests demonstrate that it is safe and necessary.

## Completion terminality

After a completed payload has durable handoff receipts, normal audit/status/reconcile does not reopen or rehash it in Completed.

Targeted recovery may inspect a destination only when a prior handoff receipt was not durably committed.
