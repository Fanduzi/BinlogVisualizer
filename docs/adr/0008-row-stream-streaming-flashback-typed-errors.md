# Row stream, streaming flashback, stateless packages and typed errors

Status: accepted (2026-10-14). Tracks #257. Driven by the v0.23.34 production-readiness review (BinlogQA, 2026-10-14), issues #250–#257 and two P1s: flashback silently overwrites later changes with exit 0, and flashback memory is about 10× the binlog size.

## Context

`internal/analyzer` is a god package. One `*Analyzer` owns transaction building, every aggregator, the schema catalog (ADR 0005), the guard (ADR 0006), the expression evaluator (ADR 0007) and flashback. Their state is shared fields on that struct, so none of them is a **deep module** with its own interface: tests have to drive the whole analyzer to reach any one of them, and each new consumer (redo SQL, split output, remote read, #256) would add more fields.

Three concrete failures come from that shape:

1. `FlashbackSQL() (string, error)` keeps every selected row image (`flashGroups`) and then the whole script in memory. 255 MB of binlog peaks at 2.5 GB RSS; analyze of the same file peaks at 36 MB.
2. Flashback sees every later event in the scanned range but never compares it with the rows it is undoing, so an undo overwrites later application writes and exits 0.
3. Errors are plain strings. Usage errors, a missing file, a corrupt binlog and a flashback refusal all exit 1, and an agent has to scrape localized text.

## Decision

### 1. Row stream (seam: committed transactions)

A `rowstream` module turns parsed events into an iterator of committed transactions with their row images: `Next() (Txn, error)` with `Txn{Key, GTID, Path, Pos, Rows []FlashRow}`. It owns transaction boundaries and the window, GTID, object and DML filters, and it marks each transaction **selected** or **not selected** instead of dropping the unselected ones. Analyze, flashback and later redo/split output are consumers. Memory is one in-flight transaction.

The first adapter is the existing analyzer path. The seam becomes real with the second consumer (flashback, step 2 below); until then it is extracted only as far as flashback needs it, per "one adapter is a hypothetical seam".

### 2. Streaming flashback writer (deep module)

Interface: `flashback.Planner` receives transactions, and `WriteTo(w io.Writer) (Plan, error)` writes the script. The caller learns three things: feed transactions, write, read the plan. Hidden inside:

- **Spill.** Each selected transaction is appended to a temp file as one length-suffixed record, so the writer can read records last-to-first without an index. Resident memory is one transaction, independent of binlog size. The temp file lives under `$TMPDIR` and is removed on every exit path.
- **Validate before writing.** A forward pass over the spill finishes deferred schema checks, the generated-column review (ADR 0005/0007) and the guard column list (ADR 0006). Any refusal happens before the first byte reaches stdout. A short write during generation is ADR 0013: the sealed script is copied to stdout only after a temp file is complete, and a prefix that a failed copy leaves behind has no commit trailer.
- **Seal without holding the script.** The guard token is a hash of the script with a placeholder. The writer renders the script twice in reverse order: once into the hash only, once to `w` with the token substituted. That costs CPU and no memory. The output is byte-identical to v0.23.34.
- **Conflict detection.** While the stream runs, the planner keeps a set of 64-bit hashes of `(schema, table, primary-key image)` for the selected rows (or the whole image when there is no primary key). Any later row event in the scanned range, selected or not, from another transaction that touches one of those keys is a conflict. A hash collision can only add a false conflict, which is the safe direction. This set grows with the selected rows, not the binlog.
- **Conflict policy.** Default: refuse, write no script, exit 7 (below), and name the first conflicts (table, key, the later GTID and position). `--conflict guard` writes the script with a post-image guard: each UPDATE and DELETE undo matches every logged column of the after image (`<=>`), so a row changed later is left alone, and an UPDATE undo that moves the primary key back also requires that no other row holds the old key (#270); the plan reports how many statements are guarded. `--plan` writes a JSON plan (transactions, rows per table and operation, conflicts, guard mode) instead of SQL.

### 3. Stateless packages

`mysqlexpr` (ADR 0007), `schemacatalog` (ADR 0005) and `guard` (ADR 0006) move out of `internal/analyzer` as packages with no `*Analyzer` receiver: input in, value or error out. Their tests are their interface. A `report/view` model shared by text, JSON, markdown and HTML is the last move. These moves are mechanical and come after the behaviour fixes, one package per PR, each with no output change.

### 4. Typed errors, exit codes and JSON errors

One `clierr` package defines a small taxonomy. Every command error is one of these kinds, and the exit code is a function of the kind:

| Exit | Kind | Examples |
|---|---|---|
| 0 | ok | |
| 1 | error | any failure with no other kind (an open BEGIN or unclassified QUERY stays here, as ADR 0001 documents) |
| 2 | no_data | a complete binlog with nothing in range (ADR 0001, unchanged) |
| 3 | usage | unknown flag, invalid `--format` (#251), bad time or GTID set |
| 4 | input | file missing or unreadable |
| 5 | corrupt | truncated or corrupt binlog, not a binlog, unsupported format |
| 6 | refused | flashback cannot be exact: DDL in range, missing images, generated-column mismatch |
| 7 | conflict | flashback: later changes to the same rows (default policy) |
| 8 | output | stdout or file write failed (ENOSPC); SIGPIPE keeps 141 |

With `--error-format json` (or `BINLOGVIZ_ERROR_FORMAT=json`), stderr carries one line: `{"error":{"kind":"corrupt","code":5,"message":"…","details":{…}}}`. `message` is localized; `kind` and `details` keys are stable. Go `%#v` dumps of parser internals never reach the user (#254). Credential text in any error (CREATE USER, GRANT, SET PASSWORD) passes through one redactor (#250).

### 5. Terminal hygiene

Progress and ANSI erase codes are written only when stderr is a TTY; `--quiet` silences progress and info lines everywhere (#253).

## Plan

Stacked PRs, each opened with a failing test, each QA'd by BinlogQA before merge:

1. Streaming flashback with bounded memory (P1-b).
2. Flashback conflict detection, `--conflict guard`, `--plan` (P1-a).
3. Typed errors, exit codes 3–8, JSON errors (#254); reject an invalid `--format` (#251).
4. No progress or ANSI on non-TTY output; `--quiet` (#253).
5. Redact the password hash in the DDL refusal (#250).
6. Remove the O(N²) top-transaction insert (#252): a bounded heap for a limit, append-then-sort when unlimited.

Later, not in this stack:

7. Extract `rowstream` fully and move `mysqlexpr`, `schemacatalog`, `guard`, then `report/view` (#257).
8. Agent docs (AGENTS.md, cobra Examples, docs in the tarball), an MCP server, and `--tz` for display time (#255).
9. Feature gaps vs pt-query-digest, binlog2sql and my2sql: redo SQL, split flashback output, row-value and thread filters, remote read, fingerprints, CSV (#256). All of these consume the row stream.

## Considered options

- **Keep rows in memory and cap with an error.** Fails safe, but a 1 GB incident binlog is exactly when flashback is needed.
- **Reverse with an in-memory offset index.** Grows by one integer per transaction; the length-suffixed record avoids it for free.
- **Warn on conflicts but keep exit 0.** That is today's documented behaviour, and agents and scripts ignore warnings; wrong data with exit 0 is the P1.
- **Always guard with the post image.** Silently skips rows the operator may want overwritten; it stays opt-in and visible in the plan.
- **Exit codes per message.** Too many codes to document; kinds keep the table short and stable.

## Consequences

- Flashback writes a temp file about the size of the selected row images; `$TMPDIR` must have room. The generated-column review still holds the images of tables that have schema-file generated columns (a `--schema-file` path only).
- A flashback that used to exit 0 now exits 7 when later changes exist; scripts must choose `--conflict guard` or narrow the range.
- Exit 1 is now only failures with no other kind. Scripts that matched 1 for usage, missing or corrupt input, or a flashback refusal must read the new codes; the JSON error is the stable contract. The table lives in `docs/reference/exit-codes.md`.
