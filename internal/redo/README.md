# Redo planner

Forward (redo) SQL for selected row changes and kept QUERY events (ADR 0022). The script replays what the binlog logged — INSERT, UPDATE, DELETE, kept DDL/ADMIN/QUERY verbatim — so it can be applied to a copy of the data. `internal/analyzer` holds one `*Planner` and calls it from the event loop, the same seam `internal/flashback` uses. `cmd` uses `RedoPlanner()`.

## Members

| File | Responsibility |
|------|----------------|
| `planner.go` | One `Planner`: the schema catalog, the staged-record temp file, name binding, deferred MINIMAL resolution, refusal/warning state, split-transaction bookkeeping, and the `Prepare` counting pass. Schema-file SQL loads at `New`; a catalog refusal fires at `Note`, a deferred one at `Prepare` — a refusal writes nothing. |
| `emit.go` | The write pass. Transaction boundaries mirror the logged stream: explicit BEGIN and XID emit `BEGIN;`/`COMMIT;`, a ROLLBACK close emits `ROLLBACK;`, implicit-commit work (DDL, ADMIN, unclassified QUERY) is never wrapped in a fabricated pair, and a group still open at end-of-input gets a trailer `COMMIT;` plus the cut warning. ROWS_QUERY becomes a `--` comment; `USE` is emitted only when a verbatim statement's default database changes. XA verbs pass through; a server-generated XA PREPARE marker expands to the `XA END`/`XA PREPARE` pair replay needs. |
| `spill.go` | Staged-record storage on an unlinked temp file: rows carry typed cells (decimal, binary, JSON documents, opaque values) serialized beside the record. Resident memory is one record plus the intern tables. |
| `refusal.go` | The refusal taxonomy: `Refusal` exit-6 codes with `RefusalDetails` — `capture`, `column_type`, `names` (MINIMAL metadata with no resolving definition), `schema` (a definition that contradicts the row), `generated_unknown`, `where_column` (a `--where` name the row does not have, ADR 0024), `truncated_query`. |
| `resource.go` | `ResourceError`: `spill_limit` (`BINLOGVIZ_SPILL_BYTES`) and `memory_limit` (`BINLOGVIZ_MEMORY_BYTES`). `ValidateResourceEnv` checks the variables before parse; the CLI exits 10. |
| `where.go` | The `Options.Where` row filter (ADR 0024): evaluates the compiled `mysqlexpr.WhereExpr` over each row's matched image after names and column facts resolve and before staging. Predicate column names share the row's resolution, so a `names` refusal under MINIMAL still wins; an unknown predicate column refuses with `where_column`. A dropped row feeds the same split-transaction bookkeeping as a table or DML skip and is never rendered. |

## Interfaces

| API | Contract |
|-----|----------|
| `New(Options) *Planner` | Loads the schema catalog from `SchemaSQL` / `SchemaFileDB`. `Options.Where` is the compiled `*mysqlexpr.WhereExpr` row predicate, or nil (ADR 0024). |
| `(*Planner).Observe` | Catalog observe for one scanned event, selected or not — in-stream DDL teaches the catalog before the rows that need it. |
| `(*Planner).Note`, `NoteSkipped` | Records a retained event in stream order, or a skipped row event for the partial-transaction warning. |
| `(*Planner).Prepare() (int64, error)` | Runs every refusal and counts statements. A refusal returns before `WriteTo` is meaningful; zero statements is exit 2. |
| `(*Planner).WriteTo(io.Writer) (int64, error)` | Streams the sealed script: session preamble, comments, and statements. Every cell literal renders through `internal/binlog.EmitLiteral`; statement shapes come from `internal/sqlemit`. |
| `(*Planner).Warnings()`, `WarningList()` | Localized stderr warnings: unknown definitions, split transactions, cut-transaction trailers, deferred-schema notes. |
| `(*Planner).Statements() int64` | Emitted statement count after `Prepare`. |
| `(*Planner).Close()` | Removes the staged records; the analyzer calls it on reset. |
| `RefusalDetails(err) (reason, table string, ok bool)` | Maps a refusal error onto its stable code and optional table for `details.reason` / `details.table`. |

## Dependencies

- Upstream: `internal/model` (`RedoRow`, `FlashCol`, `NormalizedEvent`, `FlashbackWarning`), `internal/i18n`.
- `internal/binlog` for `EmitLiteral` (the shared cell formatter; ADR 0022 keeps one formatter for both directions).
- `internal/sqlemit` for quoted identifiers, `<=>` WHERE clauses, generated-column omission, and the sql_mode drop expression.
- `internal/schemacatalog` + `internal/mysqlexpr` for definition resolution, the MINIMAL comparable-fact check (`CheckSchemaCol`), and the fact merge (`FillFlashCol`).
- Downstream: `internal/analyzer` owns the planner; `cmd/binlogviz` reads it for `redo`.

## Update Rule

If members, interfaces, or dependencies change, update this file in the same change.
