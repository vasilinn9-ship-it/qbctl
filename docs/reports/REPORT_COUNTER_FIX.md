# Report counter correction

Version 0.2.18 corrects the summary for client records removed after successful completion. A verified completion already records `client_removed=true`, but the summary previously counted only queue-trim `release` actions. This made the displayed removal total zero after completions even when each completion receipt confirmed removal.

The report now counts verified completion removals and verified queue releases only when the receipt confirms that the client record was removed with payload deletion disabled. New completion receipts also state `delete_files=false` explicitly; older completion receipts remain readable because that operation has always used the non-deleting API request.

The JSON contract now accepts the `job_receipts` report basis and documents the removal counter. `tests/test_presentation.py` covers new and historical completion receipts, queue releases, blocked actions, and cases that must not increment the counter.

## Validation

- Presentation regression test: 1 passed.
- HTTP adapter tests: 8 passed.
- JSON-schema contract test: 1 passed; the schema accepts the `job_receipts` basis.
- Rebuilding the previous saved run summary offline now reports 5 removals, matching the five verified completion receipts. This check did not contact qBittorrent or touch payload files.
- A full-suite run before correcting the newly added regression test and a test fixture path reported 261 tests, 7 failures, and 2 errors. Separate older remove-confirmation and dedupe assertions also failed. Those unrelated cases were not changed, and the full suite was not rerun after the targeted corrections; a clean full-suite result is not established.
