# ADR 0026: Analyze output surfaces — CSV/TSV stats and statement fingerprints

Status: accepted (2026-11-01). Roadmap #256: `my2sql -work-type stats` writes machine-readable stats *files*; binlogviz emits only text/json/markdown/html reports. Separately, `binlog_rows_query_log_events=ON` binlogs carry per-statement text that today surfaces only as one bounded `query_summary` per transaction — there is no pt-query-digest-style statement ranking.

## Context

Code evidence that shaped the decisions:

- **Statement text is already captured, unconditionally.** `internal/binlog/parser.go` copies `RowsQueryEvent.Query` (and `MariadbAnnotateRowsEvent.Query`) onto `raw.QuerySQL`; `normalize.go` emits a `ROWS_QUERY` event with the text bounded at `model.MaxStoredSQLBytes` (4096). `TransactionBuilder.handleRowsQuery` (`transactions.go`) stores it on the in-flight transaction for every event, inside or outside the window — no `--show-rows` or `--sql-context` flag gates capture; those flags only gate *rendering*.
- **Only the last statement per transaction survives.** `accumulateRowEvent` overwrites `retainedQuerySQL` on every retained row event, so `Transaction.QueryContext`/`QuerySummary` describe the *final* retained statement. A fingerprint feature computed over `result.Transactions` would miss every earlier statement of a multi-statement transaction.
- **`result.Transactions` is a bounded ranking, not the stream.** `ReportAggregator` keeps top-N (`TopTransactions`) plus bounded incomplete evidence; the remaining transactions are aggregated, not retained. Any post-hoc fingerprinting over the result is a biased sample.
- **Event-level capture would silently break under filters.** `filteredWorkloadEvent` maps `ROWS_QUERY` through `withDDLIdentity`; DML text never parses as DDL, so a Rows_query event is never a "workload" event. Under `--include-table`/`--dml` it is skipped before `reportAgg.ConsumeEvent` runs (`analyzer.go` guards), and it carries no `Schema`/`Table`/`Operation` of its own to filter by. Fingerprinting must therefore hook the moment the query text is *adopted* as a transaction's retained context — exactly where `retainedQuerySQL` is assigned, which is also where object/DML filters and window boundaries have already been applied (`clearCurrentQueryContext` drops the pending text when an event is filtered out).
- **`PatternStats` is a transaction-shape aggregate, not text.** `patternIdentity` keys on sorted table set + operation set + rows-per-event bucket (`patterns.go`). It deliberately knows nothing about statement text — two `UPDATE users` shapes with different WHERE clauses are one pattern. Fingerprinting is the orthogonal text axis and must not be bolted onto `PatternStats` (different key space, different granularity: per-statement vs per-transaction).
- **Report datasets differ in bounding.** JSON emits `tables` and `patterns` unbounded, `transactions` bounded by `--top-transactions` with `transactions_omitted`, `hot_rows` bounded with `hot_rows_omitted`. CSV must mirror the same bounding or it silently changes meaning per dataset.
- **`--output` is report-file only, `--output-dir` is script-parts only.** `resolveOutputDestination` accepts `--output` only with `--format html`; `--output-dir` (ADR 0025) is registered on `flashback`/`redo`, not `analyze`, and its contract is "parts of one script + MANIFEST + staged atomic publish" — not independent named exports.
- **`report.Write` is the format dispatch** (`write.go`): `ParseFormat` validates the string early at `analyze.go` (unknown `--format` is a usage error before input is read, #251), and each accepted format needs a `Write` case.
- **Bounded-map precedent exists.** `HotRowTrackLimit = 8192` caps distinct primary keys with an explicit approximate path; a distinct-fingerprint map has the same unbounded-key hazard and needs the same honesty contract.

## Decision

### CSV/TSV: one flat dataset per run on stdout, `--csv-dataset` selects it

`--format` gains two values, `csv` and `tsv`. Each run emits **one** tabular dataset on stdout with a header row, RFC 4180 quoting via `encoding/csv` (TSV = the same writer with `Comma = '\t'`). Line endings are `\r\n` per `encoding/csv` — the documented contract, and what unix CSV tooling expects.

Dataset selection is a flag, not extra format values:

- `--csv-dataset` ∈ `tables` (default), `transactions`, `patterns`, `fingerprints`.
- `--csv-dataset` set with any `--format` other than `csv`/`tsv` is a usage error (exit 3), matching the `--output`-is-html-only precedent (reject, never silently ignore).
- Bounding mirrors JSON exactly, so `--format csv` is an *export of the report's datasets*, not a second computation: `tables` and `patterns` emit the full sets; `transactions` emits the same listed transactions JSON emits (bounded by `--top-transactions`, `0` = all); `fingerprints` is bounded by the new `--top-fingerprints`.
- Map-valued cells (`tables`, `operations`) serialize as `name=count;name=count`, names sorted — deterministic, single-cell, parseable with a split on `;`.
- An empty dataset writes the header row and nothing else. A run with no Rows_query text writes a header-only `fingerprints` CSV — the header is the schema contract; emptiness is explained by the existing `sql_context.available` in JSON and by the explicit note in text mode.

**Rejected: a stats *directory* (`--output-dir` on analyze).** my2sql writes stats files into `-work-dir`, and ADR 0025's `splitout` staging sink exists, so the temptation is real. It is rejected for three reasons: (a) the datasets are independent named exports, not parts of one artifact — there is no ordering contract for a `MANIFEST` to carry; (b) three files = three shell-redirection invocations, which is the unix-composable surface this feature exists for; (c) `--output-dir` on `analyze` would collide semantically with the script-parts contract on `flashback`/`redo`. A directory mode remains additive later if demanded (a `--csv-dataset all` writing files is a strict extension, not a redesign).

**Rejected: `--csv-sep` / `--delimiter`.** Two format values are smaller than a format + separator flag, are self-documenting in `--help`, and the emitter is one `encoding/csv` code path with a different `Comma` — no separate validation surface.

### Statement fingerprints: aggregate at the retained-statement seam

**Model.** `model.StatementFingerprint`: `Fingerprint` (normalized text), `Sample` (bounded first-seen original, `FormatQuerySummary` rules — 160 chars plus the standard truncation marker), `Statements` (occurrences), `Rows` (rows written by retained events of those statements), `ShareOfRows`, `Tables map[string]int` (row counts per `schema.table`, same semantics as `PatternStats.Tables`). `AnalysisResult` gains `StatementFingerprints []StatementFingerprint` and `FingerprintOverflow int` (statement occurrences dropped when the group map was full).

**Capture seam.** `TransactionBuilder` owns a builder-lifetime `querySeq` counter, incremented in `handleRowsQuery`. On every *inside-window retained* row event with non-empty `querySQL`, the builder calls a sink `consumeRetainedStatement(seq, sql, schema, table, rowCount)` wired in `Analyzer.reset` to `ReportAggregator`. The aggregator dedups by `lastSeq` (sequences are monotonic and one statement's row events are contiguous): a new seq bumps `Statements` and records the sample; every call merges `Rows` and `Tables[schema.table] += rowCount`. This is the only seam that is simultaneously: per-statement (not last-of-transaction), covering *all* transactions (not the top-N result), and exactly aligned with the rows the report counted (filtered statements never become `retainedQuerySQL`, so they never fingerprint — the same scoping `QuerySummary` already has). A multi-table statement merges every retained table; a multi-row-event statement counts once.

**Normalization** (`internal/analyzer/fingerprint.go`, `FingerprintSQL`). pt-query-digest-style, applied left-to-right over the stored (≤4096-byte) text:

1. Strip comments: `--`/`#` to end-of-line, `/* ... */` including `/*!versioned*/` in full.
2. Replace literals with `?`: single/double-quoted strings (backslash escapes and doubled-quote escapes honored), numbers (integer, decimal, float, `0x`, `0b`, leading-dot, scientific), `X'..'`, `B'..'`, `N'..'`, and `?`/`%s`/`:name` parameter placeholders.
3. Fold whitespace to single spaces; trim; drop a trailing `;`.
4. Collapse a parenthesized group whose contents are only `?`, `null`, commas → `(?)`; then collapse a run of `(?)` groups separated by commas → one `(?)`. This turns `IN (1, 2, 3)` → `in (?)` and `VALUES (...), (...)` → `values (?)`.
5. Case-fold a curated SQL keyword set (statement verbs, clauses, operators, `null`) to lowercase; a word immediately followed by `(` is a function name and folds too. Identifiers are preserved *including case*; backtick quotes are stripped (`` `u` `` → `u`) so quoted and bare spellings group together.

Documented deviations from pt-query-digest: identifier case is preserved (per the roadmap item), `?+` repetition notation is not used (single `(?)` instead), backticks are stripped, versioned comments are dropped whole, and the input is the ≤4096-byte stored text — a statement truncated at capture fingerprints its stored prefix (`query_truncated` remains visible per transaction).

**Grouping and ranking.** Group key is the full normalized string. Rank: `Rows` desc, `Statements` desc, `Fingerprint` asc — the same deterministic style as the pattern sort. Groups are capped at **8192** distinct fingerprints (the `HotRowTrackLimit` precedent): a new distinct fingerprint past the cap increments `FingerprintOverflow` instead of creating a group, so tracked groups keep exact counts and the report can name the dropped volume. Sample = `FormatQuerySummary` of the first-seen original text.

**Relation to `PatternStats`.** Patterns answer "which transaction shapes dominate" (table set + ops + batch size); fingerprints answer "which statement texts dominate" (literal-normalized text). They cross-cut deliberately: one pattern may contain many fingerprints (`UPDATE t SET v=?` vs `DELETE FROM t WHERE id=?` in one small-batch shape), and one fingerprint may span patterns. Neither derives from the other; fingerprints do not reuse `PatternStats` fields.

**`--sql-context off`.** A fingerprint is literal-free by construction — it is statement *shape*, closer to the pattern label (table names + ops) that already renders under `off` than to raw query text. Groups render under `off` with `Fingerprint`/`Statements`/`Rows`/`Tables` intact and `Sample` suppressed, matching how pattern `sample_query_summary` is suppressed.

### Rendering surface: text opt-in + JSON + CSV/TSV dataset

- **JSON** (report v4 stays v4 — additive keys, ADR 0010): `statement_fingerprints` array bounded by `--top-fingerprints`, `statement_fingerprints_omitted` for tracked groups beyond the list, `statement_fingerprints_overflow` when the map cap dropped occurrences. Emitted always (empty array when no groups); the existing `sql_context.available` already says whether the binlog carried Rows_query text at all.
- **Text**: opt-in `=== STATEMENT FINGERPRINTS ===` section via a new `--show-fingerprints` flag, included by `--details` like `--show-minutes`/`--show-patterns`. Never silently empty: when the flag is on and no groups exist, the section prints an explicit note (no Rows_query text captured — the binlog likely lacked `binlog_rows_query_log_events`) versus "no retained statements".
- **CSV/TSV**: `fingerprints` dataset — same bounded list as JSON.
- **Markdown/HTML: deferred.** They are the long-form incident narrative; the ranking is bulk tabular data whose machine form is JSON and whose spreadsheet form is CSV. Adding a section later is purely additive.

### Flag surface

- `--format csv|tsv` — new `Format` values in `knownFormats`; `ParseFormat`'s error names all six.
- `--csv-dataset <name>` — `tables|transactions|patterns|fingerprints`, default `tables`. Explicit use with a non-CSV format is a usage error; an unknown name is a usage error listing the names.
- `--show-fingerprints` — text section opt-in; `--details` implies it.
- `--top-fingerprints N` — list bound inheriting `--top`, `0` = unlimited; follows the `--top-tables`/`--top-threads`/`--top-rows` `Changed`-bit pattern. Negative is a usage error via `validateNonNegativeCounts`.

### Error taxonomy (no new kinds)

| Condition | Kind | Exit |
|---|---|---|
| Unknown `--format` (now incl. csv/tsv in the list); `--csv-dataset` with non-CSV format; unknown dataset name; negative `--top-fingerprints` | usage | 3 |
| CSV/TSV write failure | output | 8 |
| Absent Rows_query text | not an error — explicit empty state | 0 |

## Considered options

- **Fingerprint from `result.Transactions` / `QuerySummary`.** Rejected: covers only top-N transactions and only the last retained statement each — a biased, wrong-granularity sample.
- **Fingerprint at `reportAgg.ConsumeEvent` on `ROWS_QUERY` events.** Rejected: the event carries no schema/table/operation, is not a "workload" event, and under object/DML filters either disappears (breaking capture silently) or counts statements whose rows were filtered out (lying about the workload).
- **Per-transaction fingerprint list.** Rejected as v1 surface: the useful aggregation is cross-transaction grouping; per-transaction detail remains in `query_sql`/`query_summary`.
- **Fingerprint DDL/ADMIN/UNCLASSIFIED text too.** Out of scope: those texts live in the DDL timeline / diagnostics; the roadmap item is row-change statements.
- **`--output-dir` stats files.** Rejected above; additive later.
- **Markdown/HTML fingerprint sections.** Deferred above.
- **`statement_fingerprints` unbounded in JSON.** Rejected: the 8192-group cap still permits a multi-MB document; the `--top-fingerprints` + `_omitted` pair matches `transactions`/`hot_rows` bounding.

## Consequences

- `analyze --format csv > tables.csv` and `--csv-dataset fingerprints` give spreadsheet/script consumers the stats surface my2sql provides, with bounding identical to the JSON datasets.
- `--show-fingerprints` (or `--details`) on a `binlog_rows_query_log_events=ON` binlog ranks normalized write statements by rows written, with per-fingerprint table attribution — the pt-query-digest answer inside analyze, complementing `PatternStats` on the text axis.
- No `--show-rows`, `--sql-context`, or parser flag is needed for fingerprints: capture rides the existing unconditional `ROWS_QUERY` path. Under `--sql-context off` the raw `sample` text is suppressed; fingerprints still render.
- Filter consistency: a fingerprint counts a statement only when its rows were retained — identical scoping to `query_summary`, object filters, `--dml`, and window boundaries.
- Bounded memory: ≤8192 groups × (normalized key + small aggregates); `statement_fingerprints_overflow` honestly reports dropped occurrences.
- MariaDB `ANNOTATE_ROWS` text fingerprints through the same `QuerySQL` field — no MariaDB-specific work.
- New kinds: none. New exit codes: none.
