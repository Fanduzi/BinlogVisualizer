# ADR 0022: Forward (redo) SQL output

Status: accepted (2026-10-31). Roadmap #256: the `2sql` work-type gap — `binlog2sql`'s default and `my2sql -work-type 2sql` — replayable SQL read from binlog events.

## Context

`flashback` emits undo SQL: it reverses selected row changes into a single guarded XA transaction that does not commit (ADR 0013, 0014). There is no way to produce *forward* SQL — the changes themselves, replayed in source order — for point-in-time recovery, selective replay, or an audit trail. An operator who needs "replay these rows on that table" today must run `binlog2sql` or `my2sql` beside BinlogViz.

Remote binlog read (ADR 0021) just landed: `analyze` reads four input modes — positional files, stdin `-`, `--from-dir` discovery, and `--host` replication stream — all through the same checked decode. A forward-SQL feature on that surface gets bounded remote PITR replay for free.

Constraints that shaped the decisions:

- `internal/binlog` already decodes checked row events and, when flashback capture is on, produces exact SQL literals: charset introducers, binary padding, ENUM/SET ordinals, temporal quoting, binary-JSON rebuild. The table-map facts those literals need (signedness, collation, enum/set members, column names, primary key) all come from `binlog_row_metadata=FULL`. Under `MINIMAL` the names *and* the type facts are absent — a column-name strategy must supply both, not just names.
- `schemacatalog` + `mysqlexpr` already parse `CREATE TABLE`/`ALTER TABLE` into ordered `SchemaCol` lists with charset, signedness, member lists, and generated-column sets — built from `--schema-file` and/or DDL observed in the stream. That is exactly the fallback source of names and type facts; it does not track primary keys.
- The flashback planner's contract is undo-shaped: reverse order, retained spill groups, conflict hashes, refusal-before-publish, guard procedure output, no commit. Forward replay is stream-shaped: same order, no retention, no conflicts, commit boundaries mirrored. Sharing the planner would fork its semantics on every branch; sharing the emit layer does not.
- Redo output is designed to be piped to `mysql` like `binlog2sql` output. Anything reaching stdout before a refusal would be applied as a truncated prefix — the same partial-commit hazard ADR 0014 fixed for undo — so the whole script is staged until the stream completes cleanly.
- `use db` context in QUERY events changes which schema an unqualified statement hits. `mysqlbinlog` reproduces that context; verbatim passthrough without it can replay a statement into the wrong schema.

## Decision

### Command surface: `binlogviz redo`

A new subcommand, not `flashback --mode undo|redo` and not an `analyze --format`:

- `flashback` is the undo brand. Its flags (`--conflict`, `--conflict-limit`, `--plan`, `verify`), its refusal taxonomy (conflict exit 7, DDL-in-range refusal), and its output contract (reverse order, one XA, no commit, apply markers) are undo-specific. A `--mode` flag would fork every one of those surfaces — help text, flag validity, output shape — on a single value.
- `analyze` formats render a *report* (text/json/markdown/html). Redo output is a SQL script with its own refusals and staging contract; it is not a presentation of `AnalysisResult`.
- `redo` is the natural pair to `flashback` in DBA vocabulary (undo/redo), and maps directly onto the `2sql` work-type incumbents.

`redo` shares `flashback`'s input resolution and selection flags and stops before report rendering, the same place `flashback` returns early.

### Row-event emission: real `UPDATE`, PK-or-all-columns WHERE

Per logical row:

- `INSERT INTO `s`.`t` (`cols`) VALUES (<after-image literals>);`
- `UPDATE `s`.`t` SET <after-image> WHERE <match>;`
- `DELETE FROM `s`.`t` WHERE <match>;`

`<match>` is the primary-key columns compared with `<=>` (NULL-safe) when FULL metadata names a PK; otherwise every column `<=>`-matched plus `LIMIT 1` and an inline `-- no primary key` comment — the same contract `undoWhereImage` uses for undo.

`UPDATE` is a real `UPDATE`, not a my2sql-style `DELETE` + `INSERT` pair. The pair fires `DELETE` and `INSERT` triggers, cascades, foreign-key actions, and auto-increment allocation that the source statement did not fire, and a two-row lock profile the source did not take. A `SET`-after/`WHERE`-before `UPDATE` replays the same transition the server logged. The pair's only advantage — surviving a key moved between the images — is already handled: the `WHERE` clause matches the before-image's key (or every before-image column when there is none).

### DDL and QUERY events: verbatim passthrough of kept statements

Kept QUERY events (DDL, independent ADMIN, unclassified) are emitted verbatim followed by `;`, in stream position. A `USE `schema`;` statement is emitted when a passthrough statement's event default-schema differs from the last emitted context, so unqualified text lands in the schema it ran under. ROWS_QUERY annotations are emitted as `--` comments; GTID events appear only as `-- gtid: <id>` block-head comments with `-- binlog: <file>:<pos>`.

- Ignored QUERY events (session `SET`, `SET TIMESTAMP`, and friends) are **not** emitted — analyze drops them before normalization and this ADR does not pay the normalize-variant plumbing to recover them. Consequence: replayed DDL runs under the replay session's variables rather than the source's; for the kept statement classes (DDL/ADMIN/unclassified) this is cosmetic. `binlog2sql` behaves the same way.
- Statement-format DML (QUERY text matching `IsQueryDML` — INSERT/UPDATE/DELETE/REPLACE in `STATEMENT`/`MIXED` binlogs) anywhere in the parsed input refuses `unsupported` (exit 9, `details.reason: statement_format`). It cannot be bound to the table filter or the window (normalization drops it before selection), so emitting it would silently produce a script that replays changes outside the selection. `ROW`-only input is the fix and the message says so.
- An unclassified QUERY whose stored text was truncated at the model bound refuses `truncated_query` — an emitted prefix would be a different statement.
- GTID, XID, ROTATE, TABLE_MAP, format-description, and payload-wrapper artifacts never emit SQL.
- Filtered-out QUERY events are dropped like filtered rows — the selection contract applies to statements as well. Redo deliberately does not reuse undo's DDL-in-range refusal (ADR 0015): that refusal guards an *undo* applied to a possibly-changed schema; verbatim passthrough replays the DDL itself.

### Transaction boundaries: mirrored

Boundaries mirror the source stream:

- `BEGIN;` is emitted lazily ahead of the first emitted statement of a group that logged BEGIN; `COMMIT;` on XID or plain COMMIT; `ROLLBACK;` on plain ROLLBACK.
- Rows outside any logged group emit bare — the autocommit equivalent of their source.
- XA QUERY verbs (`XA START/END/PREPARE/COMMIT/ROLLBACK`) emit verbatim. The server-generated `XA_PREPARE` marker (physical Format-0 XID, not a QUERY) emits the pair `XA END <xid>; XA PREPARE <xid>;` — `XA END` was never logged but the protocol requires it before PREPARE.
- An open transaction at the end of the parsed range emits `COMMIT;` and a stderr warning names it a cut boundary: the selected prefix replays, but it may be a prefix the source never committed as such.
- Groups empty under the selection still emit their `BEGIN;`/`COMMIT;` pair — the mirror is of the stream, not of retained rows.

### Column names: table-map first, schema catalog second, refuse third

Per table, per event, in order:

1. `binlog_row_metadata=FULL` supplies names, signedness, collation, members, and PK — the flashback path.
2. `MINIMAL` + a resolvable table definition — from `--schema-file` or `CREATE`/`ALTER` observed in-stream (`schemacatalog`, `Exact` or `Fold` resolution, column-count match) — supplies names and fills the missing type facts. The WHERE clause falls back to all-columns + `LIMIT 1` because the catalog does not track primary keys. A schema definition that contradicts a comparable binlog fact (base type, prec/scale/FSP, charset, members) refuses `schema`.
3. `MINIMAL` + no resolvable definition refuses `names` (exit 6). There is no positional-`VALUES` degrade: the literals themselves need the missing type facts, so a name-free INSERT cannot be made exact in the general case. The message names the remediation — `--schema-file` (which does supply names for redo, unlike undo) or `binlog_row_metadata=FULL` (MySQL 8.0.14+, MariaDB 10.5+).

Generated columns (catalog-known) are omitted from INSERT column lists and UPDATE SETs; the server recomputes them — the same rule undo applies via `assignColumns`.

### Session context and sql_mode

The script opens with statements undo already proved necessary for byte-exact apply:

- `SET SESSION sql_mode = REPLACE(REPLACE(@@SESSION.sql_mode, 'PAD_CHAR_TO_FULL_LENGTH',''), 'NO_BACKSLASH_ESCAPES','');` — literals use backslash escapes; CHAR `<=>` matching must not pad.
- `SET NAMES utf8mb4;` — non-utf8mb4 literals carry explicit charset introducers.
- `SET time_zone = '+00:00';` — TIMESTAMP literals are the UTC wall clock of the stored instant.

A row that stored an ENUM index-0 or a zero date (flags `captureFlashbackRows` already computes) is wrapped in a save/relax/restore sql_mode triple sharing `relaxedModes`' drop list and `sqlModeWrap`'s expression — three statements around one, only for those rows. Unlike undo, no MariaDB stop line is emitted: redo runs no guard procedures, and its statements are ordinary DML/XA that MariaDB applies.

### Input modes: all four, remote included

`redo` accepts `analyze`'s complete input surface: positional files, `-` stdin (materialized), `--from-dir` with `--prefix` or `--index-file`, and `--host` remote with all nine remote flags through `remoteOpenSource`/`NewRemoteParser`. Remote redo is `my2sql`'s live-server parity mode; the bounded-read semantics ADR 0021 defined carry over unchanged — the read ends at the connect-time manifest end.

### Filters: the analyze matrix, unchanged

`--include-schema`/`--exclude-schema`/`--include-table`/`--exclude-table`, `--dml`, `--start`/`--end`, `--start-position`/`--stop-position` (single explicit file only), `--include-gtids`/`--exclude-gtids` (complete-group semantics). A table or `--dml` filter that splits a transaction warns on stderr, same as undo. No new filter types.

### No verify/apply machinery

`redo` has no `verify` subcommand, no guard procedures, no conflict hashes, no apply markers. Replay is the source's own transaction structure; a failed statement stops the pipe at that statement and the operator fixes forward — the `mysqlbinlog`/`binlog2sql` apply model. Deliberately not in scope: `--follow`, reconnect/resume, per-table or per-transaction output splitting.

### Snapshots: not an input

`analyze` snapshot JSON carries aggregates and bounded samples, not row images or literals — forward SQL cannot be reconstructed from it. `redo` takes the same raw-event inputs as `analyze`, never `snapshot` names or report files.

### Error taxonomy (no new kinds)

| Condition | Kind | Exit | `details.reason` |
|---|---|---|---|
| Flag combinations/values (below) | usage | 3 | — |
| Unreadable/missing binlog, stdin, remote connect/TLS/auth/manifest/privilege/start/stream | input | 4 | remote reasons per ADR 0021 |
| Checked-decode failure | corrupt | 5 | — |
| Unrenderable row or statement: `names`, `column_type`, `row_image`, `capture`, `schema`, `truncated_query` | refused | 6 | per reason |
| Statement-format DML in parsed input | unsupported | 9 | `statement_format` |
| Staging/write/copy failure | output | 8 | — |
| `BINLOGVIZ_SPILL_BYTES`/`BINLOGVIZ_MEMORY_BYTES` ceilings (staged script counts as spill) | resource | 10 | `spill_limit`/`memory_limit` |
| Zero statements emitted for a valid parse | no_data | 2 | — |

Mutual-exclusion matrix (all `usage`): positional args ⊥ `--from-dir`/`--prefix`/`--index-file`; `--from-dir` needs exactly one of `--prefix`/`--index-file` (`--index-file` only with `--from-dir`); remote flags require `--host`; remote ⊥ positional/stdin/discovery/`--start-position`/`--stop-position`; `--start-pos` requires `--start-file`; `--start-gtid` ⊥ `--start-file`/`--start-pos`; position bounds are non-negative, `stop > start`, and valid on one explicit file only; `--schema-file-db` requires `--schema-file`.

### Shared machinery (anti-patch-stack)

- `internal/binlog` gains `captureRedoRows` beside `captureFlashbackRows`: same row walk and skip rules, but it keeps the **raw decoded images** plus a per-column descriptor (`model.FlashCol` extended with `Typ`, `Meta`, `Binary`) instead of rendering literals eagerly — names are not required at capture because schema resolution happens later in the planner. `SetCaptureRedo(bool)` mirrors `SetCaptureFlashback`; `RawEvent.RedoRows` flows into `NormalizedEvent.RedoRows` by one `fillNormalizedEvent` line.
- The literal machinery is split into fact-resolution (stays in `flashback.go`, reading table-map maps) and a shared `emitLiteral(emitCol, value, doc)` dispatch — `flashLiteral` delegates to it, so undo and redo render every cell through one formatter. The refactor lands in its own commit.
- Statement assembly shared pieces — `quoteIdent`, `quoteColumnList`, `keyMatch`, `undoWhereImage`, `assignColumns`/`isGenerated`/`omittedGeneratedNull`, `relaxedModes`/`sqlModeWrap` — move to a new leaf `internal/sqlemit`; `internal/flashback` delegates. One commit, no behavior change.
- `internal/redo` is a new leaf: a streaming `Planner` fed by the analyzer's existing `Observe`/`Note`/`NoteSkipped` seam, a `schemacatalog.Catalog` for name resolution, per-row `emitLiteral` calls, `sqlemit` statement assembly, and a private staging temp file — nothing reaches stdout until `Finish` reports the stream complete.
- `internal/analyzer` gains `Options.Redo` and wires `a.redo` at the same three callsites as `a.flash` (mutually exclusive); `cmd/binlogviz` gains `redo*.go` reusing `analyzeOptions` fields, `analyzePaths`/`materializeAnalyzePaths`, and the remote seam.

## Considered options

- **`flashback --mode redo`.** Rejected: the command's flags, refusals, and output contract are undo-shaped end to end; one flag would fork every surface.
- **`analyze --format sql`.** Rejected: analyze formats render `AnalysisResult`; redo is a refused/staged SQL stream, not a report.
- **my2sql `DELETE`+`INSERT` for UPDATE.** Rejected: wrong trigger/cascade/lock semantics — it replays a different change.
- **Positional `INSERT ... VALUES` under MINIMAL.** Rejected: literals need type facts MINIMAL also lacks, so the degrade cannot be exact in general; a uniform `names` refusal with `--schema-file` remediation is deterministic and diagnosable.
- **Emitting session `SET` (Ignored QUERY).** Rejected for this change: they die in normalization before selection; the kept statement classes are unaffected in practice. Documented, not hidden.
- **Refusing filtered DDL like undo (ADR 0015).** Rejected: that refusal protects an undo applied to a changed schema; passthrough replays the DDL itself, so the selection contract applies.
- **New `RedoRow` model vs reusing `FlashRow`.** `FlashRow` stores rendered literal strings; redo needs raw images so schema-merged facts can drive the shared emitter — hence `model.RedoRow` carrying `[]any` images + per-cell JSON docs.
- **Redo planner inside `internal/flashback`.** Rejected: forward streaming and retained-undo are different shapes; `internal/redo` + `internal/sqlemit` keeps undo's package boundary (ADR 0012 direction).

## Consequences

- `binlogviz redo <input>` prints replayable SQL on stdout and nothing else; nothing reaches stdout until the input parses cleanly.
- `--schema-file` gains a second meaning for redo: it supplies names *and* type facts under MINIMAL — it must describe the table at incident time, same rule as undo.
- `MINIMAL`-metadata tables replay fully when a definition resolves; otherwise the run refuses with a named remediation.
- Remote bounded reads make `redo --host` a live PITR-replay tool with no new protocol code.
- `model.FlashCol` gains `Typ`/`Meta`/`Binary` — additive fields only consumed by the redo path.
- Unit tests seam the network (`FrameSource`) and drive the real parser for fixtures; the live e2e replays emitted SQL through `mysql` under `BINLOGVIZ_FLASHBACK_E2E`.
- Not in this change: `--follow`/reconnect, session-SET passthrough, snapshot input, apply/verify procedures, output splitting, MariaDB-specific replay gates.
