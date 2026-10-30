# ADR 0025: Split SQL output for flashback and redo

Status: accepted (2026-11-01). Roadmap #256: a 1 GB incident currently produces one SQL script — no per-table files like `my2sql -file-per-table`, no `--max-statements-per-file` bound. Recovery cannot be parallelized per table, resumed after a mid-file failure, or applied incrementally.

## Context

Both writers already stage everything before publishing, so splitting is an output-layer concern, not a parser concern:

- **Flashback** emits a *sealed* script, not a stream. `internal/guard` `Guard.Write` wraps a render callback: it initializes a digest (`digestInitSQL`), runs `guard.ProcedureSQL()`, opens one XA (`UndoOpenSQL`), digests every emitted statement and comment piece, and `writeTrailer` seals the script — the `binlogviz-trailer-v1` marker, a final digest step, and `SET @binlogviz_sealed = IF(@binlogviz_digest = '<final>' ...)` — followed by `publishedSuffix`/`noCommitSQL`. The all-or-nothing unit is the whole script: one XA transaction, one procedure, the `--E:`, `--G:`, `--T:` expectations and the digest all scoped to a single `@binlogviz_sealed` token. `guard.Verify` and `CopyApply` depend on that framing: `parsePublishedSuffix` rejects anything whose trailer does not end the script, and apply refuses to commit unless the digest matched the run.
- **Redo** emits a stream (`internal/redo/emit.go`): `writeScriptHead` opens three `SET` lines, then `emitRecord` walks staged records in binlog order — lazy `BEGIN;`, mirrored `COMMIT;`/`ROLLBACK;`, verbatim DDL/ADMIN/QUERY with `USE` context switching, trailer `COMMIT;` for still-open groups. Records are either statement-bearing (`recRows`, `recStmt`, `recXAPair`, `recBegin`, `recClose`) or comments (`recGTID`, `recComment`).
- `flashback.Publish` (shared by redo) already defines per-*output* publication atomicity: the staged script is copied to stdout only after generation succeeds, so a refusal leaves nothing behind.

Constraints that shaped the decisions:

- Naively slicing the sealed script into fragments would break every guarantee at once — procedure, XA, digest, expectations — and would still *look* like a flashback script. That is the failure mode this ADR must rule out, not implement.
- Redo's mirroring contract means a split boundary cannot fall just anywhere: a file cut inside a transaction group must end and restart cleanly.
- Per-table partitioning for *undo* is semantically safe — undo order between different tables does not matter (the monolith emits whole transactions in reverse; within one table, reverse order still applies per file). For *redo* it destroys the mirrored transaction structure and cannot attribute DDL/QUERY records to any table.
- `workflow run --output-dir` already names a directory destination; `--output` is analyze's report-to-file flag. The split destination is a directory of SQL files — same vocabulary as workflow, not the file flag.

## Decision

### Flag surface: `--output-dir` + `--file-per-table` + `--max-statements-per-file`

Three composable flags, matching `my2sql` vocabulary (`-file-per-table`, statement-per-file):

- `--output-dir DIR` — write the split file set to `DIR` instead of stdout. Mutually exclusive with stdout output: when set, stdout carries nothing. Requires at least one split flag; conversely `--file-per-table`/`--max-statements-per-file` require `--output-dir` (no degenerate one-file mode — shell redirection already does that).
- `--file-per-table` — one output stream per `(schema, table)`. **`flashback` only**; the flag is not registered on `redo` (unknown flag, usage exit 3). See the redo section for why.
- `--max-statements-per-file N` — bound the statements per file at `N` (`N ≥ 1`; statement counting defined below). Preserves chronology: file *k* always precedes file *k+1* in stream order (redo) or undo order (flashback). Both commands.
- Composition: `--file-per-table --max-statements-per-file N` chunks each table's file set at `N` undo rows — an undo-heavy table still splits.
- `flashback --plan` ⊥ all three (usage): the plan JSON is not split output.
- Input selection, selectors, `--where`, `--conflict`, remote/stdin are unchanged — splitting happens after staging, on the same record/row sets the monolithic writers would emit.

### The guard problem: per-file sealed scripts, never fragments

Each output file is a **complete sealed guard script**, produced by re-entering the same writer per unit: `writeFlashbackScript` already takes a `backward func(func(*flashGroup) error) error` plus the `[]guard.Col` for that script's header — a per-unit filtered backward pass and per-unit column set are the entire seam. Every file therefore contains its own:

- session preamble and guard header (`--H:` generated-column checks scoped to the columns *that file* omits — a per-table file checks only its own table's columns),
- `binlogviz_finish` procedure and one `XA START <token>` transaction,
- `--E:`/`--G:`/`--T:` expectations covering exactly the statements in that file,
- digest trailer + `publishedSuffix`, ending in `noCommitSQL` like the monolith.

Consequences, stated plainly:

- `flashback verify <file>` works on each file unchanged — it parses a sealed script and does not know or care that siblings exist.
- The atomic-apply unit shrinks from "the whole run" to "each file". Cross-file atomicity is the applier's orchestration: files of different tables are independent (undo order between tables does not matter except foreign keys — apply parent files first), and `--max-statements-per-file` chunks of one stream **must** apply in `MANIFEST` order. This is the deliberate semantic boundary: per-file sealed all-or-nothing, not per-run.
- A transaction's undo split across files (only possible under `--max-statements-per-file`) warns per GTID on stderr — the `--E:` checks in each file still hold, but apply order matters.

Rejected alternatives:

- **Fragment/raw-DML split for flashback.** Rejected: a file containing bare `SET @binlogviz_sql` pieces or raw undo DML keeps none of the seal — no digest, no procedure, no expectations, `verify` rejects it — while wearing flashback's vocabulary. A silent degrade of the security model.
- **Refuse split unless `--conflict` mode X / "guard off".** Rejected as unnecessary: there is no guard-off mode to gate on; the per-file seal preserves every per-statement check. The only thing that changes is the transaction scope, which this ADR makes explicit.
- **One giant transaction across files (XA chaining).** Rejected: distributed atomicity across separate `mysql` invocations is not implementable — each file is a distinct session.

### Redo splitting: statement-count only, chronology preserved

`redo` accepts `--output-dir` + `--max-statements-per-file N`. `emitRecord` gains a rotation seam evaluated at record boundaries: before a statement-bearing record that would push the current file past `N`, the file closes and the next opens. Semantics:

- A file cut inside open transaction groups ends with `COMMIT;` per open group; the next file re-emits `writeScriptHead` (the three `SET` lines), the last `USE` context if one was emitted, `--` comments naming the continuing GTIDs, and `BEGIN;` per still-open group. Every file is independently `mysql < file`-appliable; a group spanning files becomes several consistent segments, each committed — a real semantic difference from the monolith, reported per spanning GTID as a stderr warning (`warning.redoFileSplit`).
- Boundary grace: a `recClose` whose group is open always lands in the same file as its last statement (a file may exceed `N` by one `COMMIT;`), and a single row *record* never splits mid-record (one file may exceed `N` by one record's rows). `N` bounds binlog statements, not lines — `sqlModeWrap` triples, comments, and re-emitted scaffolding do not count.
- Rotation is decided on the emitted stream (`recGTID`/`recComment` produce no statements and never trigger it), so counting exactly mirrors `Prepare`'s `Statements()` basis.

**`--file-per-table` is not offered on `redo`.** Forward replay derives its correctness from mirrored order and transaction structure: per-table partition scatters one source transaction's statements across files, reorders cross-table apply (FK order lost), and has no honest answer for DDL/ADMIN/QUERY records that carry no table key. Operators wanting one table's redo compose `--include-table db.t --max-statements-per-file N` — which keeps chronology and needs no new semantics.

**Per-transaction splitting is not implemented.** For redo, `--max-statements-per-file` already keeps whole transactions in one file whenever they fit; a dedicated mode adds a third partitioning rule for marginal benefit. For flashback it would need per-transaction chunk collections retained in memory (the spill groups a group into `ChunkRows` parts; a "one file per transaction" writer cannot stream them). If asked for, it is a separate change — not blocked by this design.

### Atomic publication of the file set

Split output reuses the staged-publication contract at directory granularity, via a new leaf `internal/splitout`:

1. Flag validation (usage, exit 3, before any parse): `DIR` must be absent or an existing *empty* directory, and its parent must exist — a non-empty `DIR` is a usage error, not a merge.
2. `splitout.Open` creates a staging sibling `.binlogviz-split-*` next to `DIR`. Writers stream each file inside it; every file is fsynced at close.
3. `Publish` writes `MANIFEST` last (apply order, one name per line), fsyncs, then `os.Rename` moves the staging directory onto `DIR` — atomic on POSIX; an existing empty `DIR` is removed first.
4. Any refusal or error before `Publish` — or a process exit — leaves only the staging directory; `DIR` never exists in a half-written state. `Abort`/`Close` removes the staging dir; orphaned `.binlogviz-split-*` dirs after a crash are inspectable and safe to delete.

`Prepare` runs before `Open`, so a refusal or empty selection (exit 2) never creates the directory at all.

### Naming, ordering, determinism

- Per-table files: `<esc(schema)>.<esc(table)>.sql` where `esc` percent-escapes every byte outside `[A-Za-z0-9_-]` (`%XX` uppercase) — including `.`, so the schema/table boundary is unambiguous. Empty schema renders `_`. Case-insensitive collisions (macOS) get a `-2`, `-3` suffix assigned in a fixed order.
- Per-table + `--max`: every file of every table is numbered `<base>-NNNNNN.sql` (1-indexed per table) — uniform numbering avoids any collision between a table whose name ends in `-NNNNNN` and another table's chunk files.
- Count-only: `part-NNNNNN.sql` (6-digit, zero-padded) for both commands.
- `MANIFEST` lists file names in apply order: `part-*` ascending for count mode; for `--file-per-table`, tables in backward-first-seen order (matching the monolith's undo order — newest-touched tables first), chunks ascending within a table.
- Per-file content is fully deterministic — each sealed script's digest covers only its own pieces, so the same input yields byte-identical file sets.

### Statement counting

- **Flashback**: one undo row = one statement — the same count `Prepare` returns and the trailer reports. Scaffolding (preamble, procedure, `SET @binlogviz_txn`, mode wraps) is not counted.
- **Redo**: one staged *statement* = `recRows` → row count; `recStmt`, `recXAPair`, `recBegin`, `recClose` → 1; `recGTID`/`recComment` → 0 — the same basis `Statements()` uses.

### Error taxonomy (no new kinds)

| Condition | Kind | Exit |
|---|---|---|
| Split flag without `--output-dir`; `--output-dir` without a split flag; `N < 1`; `--plan` + split; `--file-per-table` on `redo`; `DIR` exists non-empty or is a file; missing parent | usage | 3 |
| Existing refusal/conflict/unsupported/no_data kinds | unchanged | 6/7/9/2 |
| Staging-create, file-write, fsync, or rename failure | output | 8 |

## Considered options

- **Per-transaction split mode.** Deferred, not rejected: redo count-split preserves whole transactions in the common case; flashback would need retained per-transaction chunk collections. A third partitioning rule earns its own ADR if requested.
- **Refuse any split under flashback's sealed contract.** Rejected: per-file seals preserve the verify/apply/expectation machinery; only the transaction scope narrows, and that is declared.
- **Raw-DML split files for flashback.** Rejected: silently drops the seal while keeping flashback's name — worse than refusing.
- **Per-table for redo.** Rejected: destroys mirrored order/atomicity and cannot place DDL/QUERY records; `--include-table` + `--max-statements-per-file` covers the need.
- **`--output DIR` / `-o`.** Rejected: analyze already owns `-o` for a report *file*; a directory destination is a different contract and gets its own flag (`--output-dir`, consistent with `workflow run`).
- **Splitting inside `guard.Write`.** Rejected: the seal must wrap each file's complete statement set; rotation belongs at the writer seam that re-enters `writeFlashbackScript`/`emitRecord`, not inside the digester.
- **`manifest.json` metadata file.** Rejected for now: a `#`-commented line list is directly consumable by `xargs`/`while read` apply loops; structured metadata can be added additively later.

## Consequences

- `flashback --output-dir D --file-per-table` writes one sealed, verifiable, individually-appliable script per selected table; `--max-statements-per-file` bounds file size for both commands and composes with `--file-per-table`.
- `redo --output-dir D --max-statements-per-file N` writes chronologically-ordered `part-NNNNNN.sql` files, each independently appliable; mid-transaction cuts are sealed with `COMMIT;`/`BEGIN;` and warned about.
- `MANIFEST` defines apply order; `xargs -a D/MANIFEST -I{} sh -c 'mysql < "D/{}"'` is the documented apply loop shape.
- Each flashback file re-runs the render for its unit (one backward spill pass per file, digested twice like the monolith); redo re-reads the staged record spill once per rotation layout check plus the emit pass. Both stay spill-backed — resident memory does not grow with the incident.
- Split composes orthogonally with `--where`, object/DML/time/position/GTID selectors, `--conflict`, stdin and remote input — all resolved before output.
- Live e2e proves the apply contract: a two-table incident is split per table, each file `flashback verify`ed and applied separately; redo parts replay in order. New kinds: none.
- Not in this change: per-transaction split mode, `analyze` report splitting, compression/piping, parallel apply inside binlogviz, JSON manifest.
