# Windows storage architecture

Windows storage is isolated behind qb-win.

## Managed roots

Roles:

- Incoming;
- Archive;
- Working;
- Completed;
- separate Runtime root.

Data roots must not overlap each other or Runtime.

MVP uses local volumes only.

## Path safety

Mutation paths reject:

- absolute/drive-relative input where a relative path is expected;
- `..` / traversal;
- ADS syntax;
- reserved Win32 device names;
- trailing dot/space;
- reparse-point traversal.

Containment is not established by string prefix alone.

## Same-volume handoff

Use a no-replace rename/move primitive.

Preflight destination absence is diagnostic; correctness depends on the no-replace OS operation.

After the effect, observe source/destination identity before recording receipt.

## Cross-volume handoff

~~~text
source
  -> exclusive operation-owned temp
  -> copy
  -> flush
  -> cryptographic verification
  -> no-replace publish temp -> final
  -> durable destination receipt
  -> source-delete intent
  -> delete source
  -> observe absent
~~~

Source deletion is forbidden before verified final publication.

## Recovery evidence

Observe source/temp/final existence plus expected file/volume identity, size, and digest where required.

Never resolve ambiguity using filename timestamp alone.

## Windows API boundary

Use std/tokio filesystem APIs for ordinary safe operations and the `windows` crate where Win32 semantics are required.

Do not hand-write raw FFI where generated bindings suffice.
