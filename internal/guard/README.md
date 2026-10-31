# Apply guard

The flashback apply guard (ADR 0006, ADR 0013, ADR 0014): the SQL a
generated undo script runs so that applying it to the wrong target, in the
wrong session, or after the file was tampered with fails instead of
writing. `New` builds the generated-column header, `Write` seals one
script (session token, running digest, a trailer that does not commit),
and `Verify` checks that script and produces the form that may commit.
Flashback-only: analyze does not call it, and `internal/redo` does not
either — a forward replay is not sealed.

## Members

| File | Responsibility |
|------|----------------|
| `guard.go` | `Col` (one omitted generated column) and `Guard`: `New`, `Header`, `BlockLock` (the once-per-script COMMIT + read-only lock inside the header), `BlockGate` (the per-block read-only gate that does not COMMIT), `RowCond` (the token condition appended to every undo statement — a mismatched session token is ERROR 1242 and sets `@binlogviz_bad`, not a silent zero-row match), and `seal` (token substitution). The header SQL itself does the `information_schema.COLUMNS` generated-column check, the server-version check (below MySQL 5.7.0 or MariaDB 10.2 is refused), the session read-only flag capture, and the one 200-byte `sql_mode` mismatch-message cut to whole names (`guardSafeLabel` keeps a column name from ever forming the ` | ` separator). The session token is set only on a match that ran on a supported, previously writable server; the lock is fails-closed so a header-less block, a reconnect, or another script's block stays read-only. The zero `Guard` emits nothing. |
| `script.go` | The script seal and verify. `Piece{Kind, Text}` (`PieceComment` is not digested, `PieceStmt` is prepared from the same string that is hashed, `PieceRaw` executes its bytes as written because XA START, the finish procedure, and the guard header cannot be prepared). `Write` runs the caller's `render` twice — the first pass hashes every non-comment payload to derive the 16-hex session token, the second writes with `\x00binlogviz-token\x00` substituted — then emits the marker protocol (`-- binlogviz-exec`, `-- binlogviz-raw`, `-- binlogviz-trailer`), the `SET @binlogviz_sql`/digest-step pair per piece (a running `SHA2` chain the server recomputes in `@binlogviz_digest`), and the trailer: the `@binlogviz_sealed` comparison plus the published finish (`want = 0`) ending in `noCommitSQL`, so the published script never commits. `ProcedureSQL`/`UndoOpenSQL`/`MariaDBStopSQL` are the raw sections every script carries; `Verify`, `ApplySQL`, and `CopyApply` are the read side. |
| `*_test.go` | `guard_test.go` runs the emitted header SQL on real MySQL 8.0/5.7 clients (`BINLOGVIZ_GUARD_MYSQL`), including the message cut. `script_test.go`/`seal_test.go` pin the seal and verify contract (a zero guard still publishes a digest and trailer but no session token); `script_mem_test.go` holds verify's streaming-memory bound in a child process; `script_race_test.go`/`script_norace_test.go` expose the race-build flag. |

## Interfaces

| API | Contract |
|-----|----------|
| `New(cols []Col) Guard` | Builds the guard for the omitted generated columns, in order. Empty means no guard: every method then returns `""` or its input unchanged. |
| `(Guard).Write(w, render) error` | Seals one script. `render` is called twice and must emit the same pieces in the same order. The token names the XA id and the finish procedure; it is the guard session token when the script has a guard. The published script does not commit. |
| `Verify(r io.Reader) (Result, error)` | Recomputes the apply digest from a script stream — one pass, unbuffered, no MySQL. Returns `Result{Statements, Digest}` when the file matches its trailer. |
| `ApplySQL(script) (string, error)` / `CopyApply(w, r, n) error` | Verify first, then rewrite the script for apply: the finish procedure is renamed with a per-call nonce (a leftover routine is not dropped) and `want = 1`, so the finish commits only when every statement succeeded. The XA id stays the session token. |
| `ProcedureSQL()` / `UndoOpenSQL()` | Creates the `binlogviz.binlogviz_fb_end_<token>` finish procedure before the undo transaction, and opens the one undo transaction through it (mode 1 `XA START`; a failure leaves the session read-only). |
| `MariaDBStopSQL()` / `MariaDBStopMessage` | The piece that stops a script on MariaDB before the MySQL-only finish syntax runs — the run is marked failed and raised as ERROR 1231; on MySQL it is `DO 0`. |
| `Piece{Kind, Text}` | One comment, prepared statement, or raw section of a flashback script. |
| `VerifyError{Reason, Version}` | A script verify will not commit: `not_flashback`, `truncated`, `missing_trailer`, `appended`, `digest_mismatch`, or `old_format` with `Version` naming the release that wrote it (`v0.31.0 or earlier`, `v0.32.0`, `an unreleased build`). |

## The `E\n`/`G\n`/`T\n` statement tags

`internal/flashback` prefixes each prepared undo's digested text with a
two-character tag (`tagUndo`); the finish procedure (mode 0) strips it and
dispatches:

- `E\n` — keyed undo: `ROW_COUNT()` must be exactly 1, including a no-PK
  `LIMIT 1` UPDATE/DELETE; anything else fails the run (errno 45000).
- `G\n` — `--conflict` guard undo: 1 row counts as applied, 0 rows is a
  skip (`@binlogviz_m`), any other count rolls the undo back.
- `T\n` — tally without a row check (`@binlogviz_n`), used for an INSERT
  undo.

Deleting the tag changes the digest, so the seal covers it.

## Notes

- The seal binds the whole file: the server replays each payload's `SHA2` step into `@binlogviz_digest`, and the trailer sets `@binlogviz_sealed` only when it equals the trailer digest and `@binlogviz_bad` is unset. `Verify` re-derives the same chain in Go; trailing bytes after the no-commit line are `appended`.
- The published file ends with `SET SESSION sql_mode = 'binlogviz: flashback script has no commit trailer; run binlogviz flashback verify';` so a client that skips verify cannot treat the run as success. The success line is assembled from pieces so `mysql --force -v` echoing text is not a commit.
- A raw piece is followed by `GET DIAGNOSTICS`, so a failure outside the procedure is kept; an undo `CALL` is followed by an `@@error_count` check (statements carrying `@binlogviz_next_mode` keep the diagnostics check instead, because a sql_mode warning 3090 is not a failure on MySQL 5.7).
- The mismatch message cut exists only in the emitted SQL — there is no Go copy — and `guard_test.go` exercises it on a real server.
- Session state uses the `@binlogviz_*` prefix and prepared statements the `binlogviz_fb_*_x9q` names, kept apart from names a DBA might already use.

## Dependencies

- Upstream: standard library only (`crypto/sha256`, `crypto/rand`, `bufio`, `io`).
- Downstream: `internal/flashback` — the planner walks spilled rows and collects the omitted `Col`s, `render` emits pieces through `Write`, and `--output-dir` split files carry per-file cols; `cmd/binlogviz flashback verify` calls `Verify`/`CopyApply`.
