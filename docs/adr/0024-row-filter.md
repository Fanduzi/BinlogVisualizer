# ADR 0024: Row-level WHERE filter for flashback and redo

Status: accepted (2026-11-01). Roadmap #256: surgical recovery — undo or replay only the rows a predicate selects (`flashback --where "user_id < 400"`), instead of every row the object/DML/time/position/GTID selectors retained.

## Context

Selection today stops at the event boundary: schema/table include-exclude, `--dml`, time window, position bounds, GTID sets. A damaged `DELETE FROM users` reverts or replays *all* its rows, even when the incident touched one id range. Operators need a predicate over the row images themselves — the same ask `binlog2sql`-style tooling answers with its own `--where`-ish filter.

Constraints that shaped the decisions:

- `internal/mysqlexpr` is a generated-column checker, not a WHERE evaluator. Its public API (`EvalGenExpr`, `EvalDecExprInc`) takes an env of `GenVal`s whose fields are private; its scalar grammar (`pexAdd`/`pexAtom`/`Scan`, one parse tree per expression) covers literals, column refs, arithmetic, JSON arrows, and a small function set, but has no comparison or boolean layer — `AND`, `OR`, `NOT`, `IN`, `BETWEEN`, `LIKE` are explicitly syntax keywords it does not parse. Extending it in-package — where `GenVal`'s private fields and the charset/collation machinery are reachable — is far less code and far more faithful to MySQL semantics than a parallel expression engine. The value model's philosophy ("unverified is never a match") maps directly onto a conservative predicate.
- `FlashRow.Before`/`After` already hold exact SQL literals (`b'..'`, `X'..'`, `_charset 0x..`, `CAST(.. AS JSON)`, quoted temporal text) plus per-column `model.FlashCol` facts — the env a predicate needs. `RedoRow` keeps raw decoded images; the shared emitter (`binlog.EmitLiteral`) renders the same literals on demand once names/facts are bound.
- `binlog_row_metadata=MINIMAL` rows have no column names: undo stamps `FlashProblemNames` at capture and refuses; redo resolves names (and type facts) from `--schema-file`/in-stream DDL through `schemacatalog`, or refuses `names`. A name-keyed predicate inherits that vocabulary for free.
- A filtered-out transaction is already a first-class concept: both planners record kept/skipped marks per transaction key and warn "partly kept" (`noteFlashSplit`/`noteSplit`). Row-level drops ride that machinery instead of inventing new diagnostics.
- The guard script counts emitted statements (E/G/T tags); the correct contract under filtering is not "expectations tolerate missing rows" but "filtered rows never become statements, so nothing is expected of them."

## Decision

### Flag surface: one `--where`, on `flashback` and `redo` only

`--where <expr>` compiles once at flag-validation time into a predicate the analyzers pass to both planners (`analyzer.Options.RowWhere`). It composes by AND with every existing selector: an event must survive schema/table/DML/time/position/GTID selection *and* each of its rows must satisfy the predicate.

- Not on `analyze`: the report's row counts are event-level aggregates, not an emitted set; scoping them by predicate is a different feature. Not on `verify`/`workflow`: neither reads row images. Cobra rejects the flag elsewhere as a normal usage error.
- One flag, no `--where-before`/`--where-after` pair (rejected below).
- Invalid or unsupported predicate syntax is a `usage` error (exit 3) before any input is read — the same treatment `--dml` gets.

### Image semantics: the image the statement's own WHERE saw

- `DELETE`, `UPDATE`: the predicate evaluates the **before-image** — the row state the logged statement's `WHERE` matched on.
- `INSERT`: the **after-image** — the row the statement created.

This is "the rows this event touched as the server saw them": an UPDATE that moves `user_id` from 399 to 500 stays selected (`before.user_id < 400` is true), and its undo restores the in-scope before-image; an UPDATE that moves 500 to 399 does not match, so undo does not move it back out. The rejected alternative — before-OR-after for UPDATE — would revert an event that moved a row *into* the scope, dragging the row back out of scope on apply: self-defeating. Single-image semantics also keep `--where` explainable in one sentence.

### Predicate language: the supported subset

Scalar operands are the `mysqlexpr` scalar grammar: bare or backticked column refs, integer/decimal/exponent literals (extended to decimals for this feature), `'..'`-quoted strings, `X'..'`/`0x..` hex, `b'..'` bit literals (added), `_charset '..'` introducers, `NULL`, `TRUE`, `FALSE`, arithmetic `+ - * / DIV % MOD` and unary sign, JSON `->`/`->>` constant paths, and this function whitelist — `UPPER`, `LOWER`, `CONCAT`, `CONCAT_WS`, `LENGTH`, `OCTET_LENGTH`, `CHAR_LENGTH`, `CHARACTER_LENGTH`, `JSON_EXTRACT`, `JSON_UNQUOTE`, `MOD`. A function outside the whitelist, a quoted-function call, a `COLLATE` clause, or a qualified `t.col` reference is a compile error (usage), never a silent per-row failure — a predicate that cannot compile must not silently filter everything.

The boolean layer is deliberately the WHERE-shaped core:

- Comparisons `=`, `<=>`, `<>`, `!=`, `<`, `<=`, `>`, `>=`.
- `IS [NOT] NULL`, `IS [NOT] TRUE`, `IS [NOT] FALSE` (`UNKNOWN` accepted as `NULL`).
- `expr [NOT] IN (scalar, scalar, ...)`.
- `AND`, `OR`, `XOR`, `NOT`, parentheses, MySQL precedence (`NOT` < comparison < `AND` < `XOR` < `OR`... i.e., `NOT` binds looser than comparisons, tighter than `AND`).
- A bare scalar is a valid predicate: nonzero is true, like MySQL.

Outside the subset — usage error: `BETWEEN`, `LIKE`/`RLIKE`/`REGEXP`, `CASE`, subqueries, `INTERVAL`/casts, qualified references, `!`. A MySQL user can always write the `AND`/`OR` expansion of `BETWEEN`/`NOT BETWEEN`, so the subset covers the realistic recovery vocabulary without a second comparison engine.

### Comparison semantics: only provably true rows emit

The evaluator is three-valued (true / false / unknown); a row is kept only when the predicate is *true*. `unknown` — a NULL operand, an unparseable cell literal, an undecodable charset, or a comparison the model declines — excludes the row. The script therefore contains only rows where the predicate provably holds; it never emits a row MySQL's own evaluation would reject. This is the checker's own rule ("unverified is never a match") applied to filtering.

Comparisons mirror MySQL coercion where it is modelled exactly:

- number × number: exact `big.Rat` compare.
- number × text: the text must be a *strict* numeric literal (full-string, no trailing junk); anything else is unknown. MySQL's leading-prefix coercion (`'123abc'` → 123) is a warning path this evaluator declines.
- text × text: the claim must agree under **both** byte order and case-folded order (the package's `latin1`/`utf8mb4` fold). `'a' < 'b'` agrees under `_bin` and `_ci` collations and is claimed; `'Z' < 'a'` does not, so the row is unknown/excluded rather than guessed under a collation the binlog does not carry. `BINARY`-charset columns compare raw bytes only.
- ENUM/SET columns carry their member list (`FlashCol.Members`): comparing to text goes through the member label (`status = 'active'` matches stored index 2); to a number it is the index/bitmask.
- JSON compares only through `->`/`->>` paths and `JSON_UNQUOTE`-equivalent unquoting; whole-document ordering is unknown.
- `expr = NULL` is NULL — write `IS NULL`. `<=>` is the null-safe equal.

Decimal and exponent literals join the scalar grammar (`400.5`, `1e3`); `.5` still does not parse (write `0.5`).

### Error taxonomy: three doors, one new refusal reason

| Condition | Kind | Exit | `details.reason` |
|---|---|---|---|
| Unparseable or unsupported `--where` (incl. qualified refs, non-whitelist functions, `COLLATE`) | usage | 3 | — |
| Predicate references a column the selected row event does not have | refused | 6 | `where_column` |
| Referenced cell cannot be rendered as a literal (redo) | refused | 6 | `column_type` (existing) |
| Comparison unverifiable for a row | — | — | row is excluded (not selected) |

A missing-column check runs per row *event* against the event's column set — either the FULL table-map names or the schema-resolved names under MINIMAL — mirroring MySQL's `ERROR 1054 Unknown column`.

MINIMAL metadata inherits the existing refusal chain unchanged: flashback's `names` refusal fires at row-problem check before the predicate runs (a `--schema-file` never supplies names to undo); redo either resolves names from the catalog — the predicate then sees the resolved names — or refuses `names` first. No `--where`-specific names error exists; `where_column` covers only a name absent from a *resolved* column set.

### Planner seam: filter at staging, after all existing checks

The predicate runs inside each planner's `Note` path — `noteFlashback` for undo, `noteRows` for redo — **after** every check that already ran on the raw event (capture problem kinds, the `names` refusal, `generated_unknown`, schema validation) and **before** rows are appended/staged. Checks that refuse keep refusing on the full event, predicate or not: a row the predicate would drop can still be part of a binlog/schema contradiction the script must not wave through. The predicate then decides which checked rows emit:

- Rows failing the predicate are simply not staged — no statement, no `noteSelectedKeys` registration (a later change to a filtered-out row is correctly not a conflict), no guard expectation.
- Per transaction key, dropped rows record the existing `skipped` mark and kept rows the `kept` mark — a partially-filtered group produces the same "partly kept" stderr warning a `--dml` or table filter produces today.
- An event whose rows all fail the predicate leaves only the `skipped` mark — identical to a fully-excluded event; a `BEGIN`/`COMMIT` pair with no rows is exactly what a dropped ROWS event produces today.
- Redo renders literals only for referenced cells (via `binlog.EmitLiteral` on the merged `FlashCol` facts); flashback evaluates the literals `FlashRow` already holds. No parser pushdown: the binlog decoder is untouched, per the contract.
- Non-row statements are not filtered: `redo` still passes kept DDL/ADMIN/XA/QUERY text through verbatim — `--where` scopes row payloads, not statement passthrough.

### Output contracts

- Undo: filtered rows produce no `SET @binlogviz_sql` payloads and no E/G/T expectations — the guard script is valid by construction because expectations only ever describe emitted statements.
- `flashback --plan`: the plan JSON reflects the post-filter set (staged rows only); no schema or `plan_version` change.
- `redo`: `Prepare`/`Statements()` count staged rows; zero matching rows still maps to exit 2 (`no_analyzable_events`), same as an empty selection.
- `analyze` counts are **not** predicate-scoped — the command takes no `--where`.

### Deferred

`--thread-id` filtering is explicitly out of scope per the orchestration contract: thread id lives on the event, not the row image, and belongs to a separate selector ADR if it is ever taken up. Also not here: parser pushdown, guard-script semantic changes, MariaDB replay gates, output splitting, CSV/TSV, fingerprinting.

## Considered options

- **Separate `--where-before`/`--where-after` flags.** Rejected: two flags invite contradictory pairs, double the help/refusal surface, and the single-image rule already covers the recoverable intent — for UPDATE the before-image is what the statement's own WHERE matched.
- **UPDATE matched on before-OR-after.** Rejected: an UPDATE that moves a row *into* scope would be reverted, moving it back out on apply — the predicate must describe the rows the event's WHERE saw, not the rows it produced.
- **Filter at the analyzer selection seam (`consume`) instead of inside each planner.** Rejected: selection runs before `FlashRows`/`RedoRows` exist and before name/fact binding; the planners are where literals, resolved names, and refusal machinery already live. The analyzer only passes the compiled predicate through `Options`.
- **Positional `@N` references for MINIMAL rows.** Rejected: positional labels are placeholders, not names — allowing `--where "@1 < 400"` would bless a column identity the binlog does not carry and contradict the `names` refusal story.
- **Emulating `_ci` collation for text order.** Rejected: the binlog does not carry the column collation. Byte-and-fold agreement claims only what holds under `_bin` and `_ci` alike; everything else is unverifiable and excluded — under-matching is safe, over-matching emits rows the predicate rejected.
- **`--where` on `analyze`.** Rejected: report counts are event-level aggregates; row-scoped counting is a different feature with its own report-contract questions.

## Consequences

- `binlogviz flashback binlog --where "user_id < 400"` emits undo only for rows the predicate proves; `binlogviz redo --where "status = 'open'"` replays only matching row payloads.
- `internal/mysqlexpr` gains a second job beside generated-column checking: WHERE-predicate compile/eval over logged literals. Its module README, CONTEXT.md, and this ADR now describe both; the scalar grammar and `GenVal` machinery are shared, not duplicated.
- A predicate can only shrink the emitted set, never change what kept statements contain: guard scripts, redo scripts, warnings, plan JSON, and the exit taxonomy are otherwise untouched. The one new refusal reason is `where_column`.
- Usage errors for `--where` are decided before input files are read — a typo'd predicate never scans a binlog.
- Live verification of "only matching rows were reverted" runs in the `flashback-e2e` CI job (gated on `BINLOGVIZ_FLASHBACK_E2E`); there is no local MySQL requirement for the unit/CLI level.
- Not in this change: `--thread-id`, parser pushdown, analyze-time row filtering, BETWEEN/LIKE/CASE/subqueries, collation-aware comparison beyond the byte/fold agreement rule.
