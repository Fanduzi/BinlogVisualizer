# Flashback planner

Undo SQL for selected ROW changes (ADR 0012 step 4). `internal/analyzer` holds one `*Planner` and calls it from the event loop. `cmd` uses `Planner()`.

## Members

| File | Responsibility |
|------|----------------|
| `planner.go` | Collects filtered rows and renders reverse-order undo SQL, or one error and no script. Omits generated columns from INSERT and UPDATE assignments. A schema-file mismatch prints no SQL. A generated value that contradicts the expression refuses the script and names every contradicting column. An expression that cannot be checked is omitted, stderr warns, and the header says the guard checks the target. A no-primary-key WHERE keeps generated columns. An ENUM index of 0 is wrapped in a sql_mode save and restore that also drops TRADITIONAL. A selected table with no definition warns and still prints. Warns when a table or DML filter splits a transaction. `Prepare` runs every refusal and counts rows. `WriteTo` / `WriteFlashback` renders twice from the spill (into the token hash, then to the writer), so no partial script is written and memory does not grow with the binlog (ADR 0008). |
| `conflict.go` | Conflicts (#259): one 8-byte hash per selected row key; every later row event from another transaction, selected or not, that touches one is a conflict. Default refuses (`FlashbackConflictError`, CLI exit 7); `guard` matches each undo against the after image, and a key-moving UPDATE undo runs only when the old key is free (#270); `WritePlan` prints the JSON plan (`plan_version` 2), including on a refusal (`refusal`); `--conflict-limit` bounds the listed conflicts. |
| `spill.go` | Row storage (ADR 0008, #258): finished transaction parts (at most `ChunkRows` rows each) go to an unlinked temp file as length-framed records, read forward for checks and last-first for rendering. Binds and column type lists are interned once. Resident memory is one part, not the binlog. |

## Interfaces

| API | Contract |
|-----|----------|
| `New(Options) *Planner` | Loads the schema catalog from `SchemaSQL` / `SchemaFileDB`. |
| `(*Planner).Observe` | Conflict check and catalog observe for one event, before the transaction builder consumes it. |
| `(*Planner).Note`, `NoteSkipped` | Records a retained event, or a skipped one for the split-transaction warning. |
| `(*Planner).Prepare() (int, error)` | Every refusal and the undo row count. Zero rows is an empty script. |
| `(*Planner).WriteTo(w) (int64, error)` | Streams the script. The count is undo rows, not bytes. |
| `(*Planner).WritePlan` | `--plan` JSON. |
| `(*Planner).SQL` | The whole script in memory. Tests use it; the CLI uses `WriteTo`. |
| `ChunkRows` | Spill part size (2048). |

## Dependencies

- Upstream: `internal/model`, `internal/mysqlexpr`, `internal/schemacatalog`, `internal/guard`, `internal/redact`, `internal/clierr`, `internal/i18n`.
- Downstream: `internal/analyzer` and `cmd/binlogviz`. This package does not import `analyzer`.
