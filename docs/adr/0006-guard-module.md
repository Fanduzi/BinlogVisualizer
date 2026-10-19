# The flashback apply guard is one module whose message cut exists only in SQL

`internal/guard` owns the apply guard (ADR 0012 step 3; this used to be `internal/analyzer/flashback_guard.go`). ADR 0013 supersedes `Seal` / `SealWriter` and the per-block `COMMIT` lock: the emit path is `Write`, and `Verify` checks the script. `Header`, `BlockLock` (once, inside the header), `BlockGate`, and `RowCond` remain. A token mismatch on an undo statement is ERROR 1242. The zero guard returns "" or its input unchanged. The renderer no longer knows the lock SQL, the token condition or how the token is made. The walk that collects omitted columns stays in the planner and passes `[]Col`.

The 200-byte cut of the mismatch message (whole names, by bytes, `|` shown as `/`) is implemented once, in the SQL that `Header` emits. The Go copy (`trimGuardList`, `guardErrorValue`) was called only by tests and is deleted.

## Why

The unit tests exercised the Go copy, while production shipped the SQL. #192 and #208 were both regressions in the SQL that only live QA caught. A test of a copy proves the copy.

## Considered options

- Keep the Go copy and add a test that compares it to the SQL. That still means two implementations, plus a test whose only job is to keep them in step.
- Compute the message in Go and embed it. The list of mismatched columns is only known on the target (information_schema), so the cut has to run there.

## Consequences

- The guard tests (`internal/guard/guard_test.go`) run the emitted header on real servers named by `BINLOGVIZ_GUARD_MYSQL` (one or more client commands separated by `|`, for example MySQL 8.0 and 5.7). They check the exact ERROR 1231 value for the #192/#208 cases, sweep the cut point, and check that a matching target stays writable. Without that variable they are skipped. The offline tests check the zero guard and the token seal.
- The script output is unchanged byte for byte.
