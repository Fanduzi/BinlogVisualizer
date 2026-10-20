# ADR 0012: Split `internal/analyzer` into deep modules, one seam at a time

Status: accepted (2026-10-19). Tracks #257. ADR 0008 named these moves and left them until the behaviour fixes landed. Those fixes are in (streaming spill, conflict detection, `internal/clierr`). This ADR is the package layout and the order of the moves. It does not change output.

## Context

At `4cb6cc2`, `internal/analyzer` is 36 non-test files and about 15.4k lines. One package still holds the report aggregator, the transaction model, flashback SQL, the expression evaluator (ADR 0007), the schema catalog (ADR 0005), the apply guard (ADR 0006), and the DuckDB detail store. They compile together, so they share unexported types and the flashback fields on `*Analyzer`.

Those fields, in `analyzer.go`, are the spill, the conflict key set and the pending conflict list, the catalog pointer, the split-transaction map, and the generated-column review flags: `flashCur`, `flashSpill`, `flashKeys`, `flashConflicts`, `flashPending`, `flashPendingSeq`, `flashConflictsSettled`, `flashConflictList`, `flashGroupID`, `flashSeq`, `flashClearSeq`, `flashPendingNames`, `flashChecked`, `flashErr`, `flashCat`, `flashUnknown`, `flashSchemaWarn`, `flashGenNotes`, `flashReviewed`, `flashReviewErr`, `flashSplit`, `flashSplitOrder`, `flashSplitDone`. Conflict detection added keys and pending state on `*Analyzer` instead of a planner that owns them. The October 16 recheck on #257 asked for that planner before more flashback features land.

The expression evaluator and the schema catalog are not fields. They are functions and a `schemaCatalog` value. They still cannot be imported by a second caller, because `sqlScan`, `schemaCol`, and `intVal` are unexported and the files that use them sit in `analyzer`. `sqlScan` is the one SQL cursor. Methods on it live in `flashback_generated.go`, `flashback_eval.go`, and `flashback_expr.go`, and the column parser in `flashback_schema.go` reads `sc.i` and `sc.s` directly. A Go method has to live in the type's package, so those files are one closed set. The catalog walker (`note`, `noteCreate`, `noteAlter`, `noteDrop`, `noteRename`) walks that same cursor. The generated-column review reads `schemaCol` fields and calls `evalGenExpr` / `evalDecExprInc`.

`internal/clierr` already exists (ADR 0008 step 4, ADR 0009). `flashGroups` is gone; rows sit in the spill. Per-format transaction selection is still separate (`renderTopTransactions` in `internal/report/text.go`, `mdTopTransactions` in `internal/report/markdown.go`), and the format switch still lives in `cmd/binlogviz/analyze.go`.

## Decision

Leaf packages do not import `internal/analyzer`. `analyzer` orchestrates: it calls the leaves, and later a flashback planner. No package in the graph imports a package that imports it. No compatibility wrapper stays behind in `analyzer`; a step moves the callers and deletes the old file in the same change.

`FlashbackWarning` (the `code` + `message` value on `flashback --plan`) moves to `internal/model`. The catalog, the evaluator, and the planner all build one, and none of those leaves may import `analyzer` to share it. `model` does not import them. JSON field names stay `code` and `message`.

### Target layout

| Package | Owns | Does not own |
|---|---|---|
| `internal/mysqlexpr` | The expression evaluator (ADR 0007): one grammar, one cached tree, `EvalGenExpr` and `EvalDecExprInc`. The value rules (`genVal`, `decVal`, JSON, charset, integer and DECIMAL arithmetic, including `intVal` / `applyMulOp` / `applyIntDiv`). The SQL cursor `Scan` and every method on it. The column value `SchemaCol` and the CREATE/ALTER column parser, row check, and generated-column review, because they share that cursor and those fields. | Which `db.table` a definition binds to. Undo SQL. The guard. Reports. |
| `internal/schemacatalog` | The schema catalog (ADR 0005): load a schema file, observe table maps and DDL, resolve or peek `db.table`, return warnings. Binding rules stay here. | Parsing a column list or evaluating an expression. It calls `mysqlexpr` for both. |
| `internal/guard` | The apply guard (ADR 0006). ADR 0013 replaces `Seal` / `SealWriter` with `Write` and `Verify`. The zero value returns `""` or its input unchanged. | Walking spilled row groups to collect omitted columns. The planner does that and passes `[]Col`. |
| `internal/flashback` | The planner: selection, refusals, spill, conflict set, review flags, streaming writer, `--plan`. | The grammar, the binding rules, the guard SQL. |
| `internal/report/view` | One ordered, bounded view of an `AnalysisResult`. | SQL text, HTML chrome, JSON encoding. The existing renderers stay, and they read the view. |
| `internal/detailstore` | The DuckDB adapter (`store_duckdb.go` / `store_nocgo.go`) behind the existing `detailStore` interface. `cmd` wires it. | Aggregation. The default `none` store can stay a tiny type in `analyzer` or move with the interface; either way `analyzer` does not import `go-duckdb`. |
| `internal/rowstream` | An iterator of committed transactions with row images, selected or not. | Aggregation and undo SQL. Extract it when a second consumer needs it (redo, split output). One caller is not a seam (ADR 0008). |
| `internal/analyzer` | Streaming aggregation: transactions, tables, minutes, DDL, alerts, spikes, diagnostics, the report aggregator. It holds a `*schemacatalog.Catalog` and, until the planner step, the flashback fields. After that step it holds one planner. | The leaves above. |
| `internal/clierr` | Already shipped. Not moved again here. | |

`internal/model`, `internal/i18n`, `internal/binlog`, `internal/jsonout`, `internal/snapshot`, `internal/compare`, `internal/trend`, `internal/workflow`, and `internal/version` stay where they are.

### Dependency direction

```
cmd
 └─ analyzer ─┬─ schemacatalog ── mysqlexpr ── model, i18n
              ├─ flashback ─┬─ schemacatalog
              │             ├─ mysqlexpr
              │             └─ guard
              ├─ detailstore (cmd injects the DuckDB store; analyzer sees the interface)
              ├─ report ── report/view
              └─ model, clierr

rowstream ── model
flashback and analyzer both consume rowstream once it exists
```

`mysqlexpr` imports `model` and `i18n` only. It does not import `schemacatalog`, `analyzer`, `flashback`, or `guard`. `schemacatalog` imports `mysqlexpr`, `model`, and `i18n`. `guard` imports the standard library only. Nothing imports `analyzer` except `cmd`, tests, and the packages that already depend on the analysis result through `model` rather than through `analyzer`.

### Public interface at each seam

`mysqlexpr`, evaluator (ADR 0007, unchanged behaviour):

- `EvalGenExpr(expr string, env map[string]GenVal) (GenVal, bool)`
- `EvalDecExprInc(expr string, env map[string]GenVal, inc int) (DecVal, bool)`

`GenVal` and `DecVal` are `genVal` and `decVal` with an exported name. Callers outside the package do not touch the cached tree. The review helpers below build the environment from logged literals; a caller does not assemble `GenVal` by hand.

`mysqlexpr`, column model. `SchemaCol` is the one column type (name, generated flag, expression, base type, unsigned, charset, ENUM/SET members, precision, scale). The parser, the review, and the catalog all use it. There is no second struct and no mapper. `Name`, `Generated`, and `Expr` are exported because the planner reads them when it omits a generated column and when it builds the guard column list. The other fields stay unexported next to the parser.

- `ParseSchemaColumns`, `CloneSchemaCols`, `ApplyTableCharset`, `AlterSlice`, `AlterNoColumnChange`, `GeneratedNameSet`, `ValidateSchemaRow`, `DumpSegments`, `DumpDatabase`, `SchemaColNames`
- Generated review, called by the planner: `PrepareGenImages`, `(*GenImages).Outcome`, `(*GenImages).DivIncrementFit`, `DivIncrementMismatch`, `DivIncrementHint`, `GeneratedMismatch`, `SQLTableName`, `UnverifiedGeneratedComment`, `UnverifiedGeneratedWarning`

`mysqlexpr`, cursor. The catalog walker is the other caller. Exported surface is the type plus the operations that walker already uses, not a new parser:

- `NewScan(s string) *Scan`
- `Skip`, `Word`, `WordIs`, `Ident`, `Rest`, `UnreadWord`, `Qualified`, `ParenBody`, `PeekWord`, `PeekByte`
- `S` and `I`, the text and the index. The walker compares the current byte with `,` in two places. Exporting those two fields keeps that code as it is. `SplitSQL`, `SplitComma`, `IndexCreateTable`, and `CopyNames` live here for the same reason: the walker and the column parser both call them.

`schemacatalog` (ADR 0005):

- `New(flagDB string) *Catalog`
- `LoadFile(sql string)`
- `ObserveTable(schema, table string) (abandoned []string)`
- `ObserveDDL(session, sql string) (abandoned []string)`
- `Resolve(schema, table string) Resolution`
- `Peek(schema, table string) Resolution`
- `Warnings() []model.FlashbackWarning`

`Resolution` carries `Kind` (`Missing`, `Exact`, `Fold`, `Ambiguous`, `Refused`), `Cols []mysqlexpr.SchemaCol`, `CheckRows`, `Deferred`, `FileTable`, `Candidates`, and `Reason` (`UnreadableCreate`, `CreateSelect`, `LikeSource`, `AlterUnknown`, `AlterUnreadable`). Callers still build every message from that value. The catalog does not rewrite error text.

`guard`:

- `type Col struct { Schema, Table, Column string }`
- `New(cols []Col) Guard`
- `Header() string`, `BlockLock() string`, `RowCond() string`, `Seal(script string) string`
- `SealWriter(w io.Writer, sum []byte) (func(string) error, error)`

`Seal` replaces the placeholder in a finished script. The planner does not call it. It renders twice (ADR 0008): once into a sha256, then once through `SealWriter`, so the script is not buffered. `sum` is that digest, the same one `Seal` computes. A digest shorter than 8 bytes is an error (#319). The placeholder stays inside the guard. The first draft of this list had only `Seal`, which would make the planner buffer the script or learn the placeholder.

`flashback.Planner` (the type that takes the `flash*` fields off `*Analyzer`):

- `Observe`, `Note`, `NoteSkipped` are what the event loop calls
- `Prepare() (int, error)` runs every refusal and the generated-column review before any byte is written, and returns the undo row count
- `WriteTo(w io.Writer) (int64, error)` renders from the spill. The count is undo rows, not bytes
- `Plan() Plan` / `WritePlan` is the `--plan` document (`plan_version` stays 2)

`cmd` uses `Planner()` for `Prepare`, `WriteTo`, and `Plan`. Spill layout, conflict hashes, and guard sealing stay inside the planner. Sealing calls `guard.SealWriter`.

`report/view`: one function from `model.AnalysisResult` plus render limits to a view the text, Markdown, JSON, and HTML renderers share. Selection of top transactions happens once, in the view.

`detailstore`: the existing `NewDuckDBStore` moves with the adapter. `analyzer.NewWithStore` keeps its signature until the call site in `cmd` passes the interface; the signature change is part of that step, not a second wrapper.

### What leaves `*Analyzer`

| Step | State |
|---|---|
| `mysqlexpr` | Nothing on the struct. The evaluator is functions plus the process-wide expression cache. The column parser's maps are not analyzer fields. |
| `schemacatalog` | The catalog's own maps (`defs`, pending binds, fold notes) already sit inside the catalog value. The step changes `flashCat` from `*schemaCatalog` to `*schemacatalog.Catalog`. The pointer stays on `*Analyzer` until the planner step. |
| `guard` | No new analyzer field. The planner, still in `analyzer` until the next step, builds the guard from omitted columns. |
| `flashback` planner | Every `flash*` field above moves onto `Planner`. `*Analyzer` keeps one `*flashback.Planner`. |
| `detailstore` | `store detailStore` stays. The DuckDB struct type leaves the package. |
| `rowstream` | Transaction-boundary state moves when the second consumer exists. Not before. |

### Migration steps

Each step is one PR. It ends with `go test ./...`, `go vet ./...`, and the flashback e2e green, and with analyze JSON, the text report, and flashback SQL / `--plan` byte-identical on the repo's testdata binlogs. No golden file is edited to make a test pass. No wrapper package is added.

1. **`internal/mysqlexpr`.** Move `flashback_eval.go`, `flashback_expr.go`, `flashback_schema.go`, `flashback_generated.go`, and the tests that only call those files. Export the surface above. `analyzer` calls it. The catalog file still lives in `analyzer` and is the first caller of `Scan` and the column parser.
2. **`internal/schemacatalog`.** Move `schema_catalog.go` and the catalog tests. `flashCat` becomes `*schemacatalog.Catalog`. Delete the old file.
3. **`internal/guard`.** Move `flashback_guard.go` except the walk over `flashGroup` that collects omitted columns. That walk stays with the planner and calls `guard.New`.
4. **`internal/flashback`.** Move `flashback.go`, `flashback_spill.go`, `flashback_conflict.go`, and the `flash*` fields onto `Planner`. `analyzer` holds one planner and calls it from the event loop. `cmd` uses `Planner()`. There is no forwarding wrapper. This is the step the October 16 recheck asked to do before more flashback features.
5. **`internal/report/view`.** Build the view once. Point text, Markdown, JSON, and HTML at it, including `renderTopTransactions` and `mdTopTransactions`. Move the format switch out of `cmd/binlogviz/analyze.go` so an unknown `--format` cannot skip the view.
6. **`internal/detailstore`.** Move the DuckDB adapter out of `analyzer` behind `detailStore`. `CGO_ENABLED=0` still builds.
7. **`internal/rowstream`.** Extract only when redo SQL or split flashback output is the second consumer.

Steps 1 and 2 are the first PR. They are the closed set with the smallest shared state: no `*Analyzer` receiver, and every test of the evaluator and the catalog already calls the functions above rather than the aggregator.

## Generated-column memory and resource exhaustion

The plain path spills row images and renders from that spill, so RSS stays flat as the script grows. The generated-column review used to copy every before-image and after-image into `GenImages` (the logged values and an evaluation environment per image) before it could refuse or warn. RSS then grew with the script: tens of megabytes of SQL held hundreds of megabytes, and a larger input died in the Go runtime. That fatal call is `exit(2)`, the same status as `no_data`, and it writes no JSON error, so a caller can treat an out-of-memory crash as an empty result (#317).

The planner reviews from the row spill and does not retain images:

1. One forward pass evaluates each generated column at the default `div_precision_increment`. It keeps the first mismatch example, an unverified flag, and the set of column names. The image is discarded.
2. A column that needs the identical-inputs check writes `(bucket, column, sequence, reference key, literal)` records to a second unlinked temp file. Records are sorted in bounded chunks and merged at a fixed fan-in. The clash that is reported is the one `GenImages.dependencyClash` returns: the earliest contradicting image, the example from the first image of that key, and the later literal. The example is rebuilt from that one image on a replay. A column that already mismatched and only needs to know that a clash exists does not store the example.
3. A third pass runs only when a column mismatched at the default, its expression contains `/`, and it did not clash. It evaluates increments 0 through 30. Which increments fit does not depend on order.

Resident memory is one row-spill part plus one clash chunk, the same shape as the plain path, however large the script is.

Two ceilings are `resource`, exit 10, with `details.reason` `spill_limit` or `memory_limit`. Stderr is the usual JSON error when `--error-format json` is set. Exit 10 is never 2.

- `spill_limit`: `BINLOGVIZ_SPILL_BYTES` is a positive integer and the row spill plus the review spill grow past it. Unset means no byte ceiling. ENOSPC while writing a spill stays `output` (8). A full disk and a ceiling the operator set are different fixes.
- `memory_limit`: `GOMEMLIMIT` is finite. If `HeapAlloc` is still above 90% of that limit after one GC, the planner returns before the runtime calls `exit(2)`.

The innermost kind wins, so a later wrap does not turn this into `refused` or `error`. A resource failure writes no plan and no script.

## Considered options

- **Export only `EvalGenExpr` and keep the column parser in `analyzer`.** The parser and the evaluator share `Scan` and `SchemaCol`. Leaving the parser behind means `mysqlexpr` cannot see `schemaCol`, or `analyzer` imports `mysqlexpr` while `mysqlexpr` imports `analyzer`.
- **Put the column parser in `schemacatalog` and export every cursor field.** That also works, and it puts CREATE/ALTER parsing next to the binding rules. It makes `Scan.S` and `Scan.I` a public cursor for a package that does not need to own parsing. Keeping the parser next to the cursor, with the catalog calling `ParseSchemaColumns` and `AlterSlice`, leaves the catalog's interface as the six methods from ADR 0005 plus the resolution. The walker still uses `Scan`, because that walk is the binding logic (USE, qualified names, LIKE, RENAME), not the column grammar.
- **One `internal/flashsql` package for the evaluator and the catalog.** It compiles, and it is still a grab bag. Two packages with one import arrow are the point of the split.
- **Leave forwarding funcs in `analyzer` (`func evalGenExpr(...) { return mysqlexpr.EvalGenExpr(...) }`).** Callers would keep compiling without moving. The old names would rot. Each step updates the callers and deletes the old code.
- **Extract `rowstream` first.** ADR 0008 already rejected a seam with one caller. Flashback still reads the analyzer's own event loop.

## Consequences

- Steps 1 and 2 do not change analyze or flashback output. `report_version` and `plan_version` stay. Golden files stay.
- ADR 0005 and ADR 0007 still describe the binding rules and the evaluator. The file paths in those ADRs are historical; the packages in this ADR are the location. Step 3 moved the guard to `internal/guard` and added `SealWriter` (see the guard interface). ADR 0006's behaviour is unchanged; its path now names `internal/guard`.
- Step 4 moved the planner. `*Analyzer` keeps one `*flashback.Planner` and no `flash*` fields. The event loop calls the planner directly (`Observe`, `Note`, `NoteSkipped`); `cmd` uses `Planner()` (`Prepare`, `WriteTo`, `Plan` / `WritePlan`). There is no forwarding wrapper on `Analyzer`. `Prepare` returns the undo row count as `(int, error)` because exit 2 is "no undo rows". `WriteTo` returns that count widened to `int64`, not a byte count. Spill chunk size is `ChunkRows`. Generated-column review streams; see the section below.
- Step 5 adds `internal/report/view`. `view.Build(result, Limits) View` is the only transaction selection. The renderers read that value. They do not slice the transaction lists themselves, and nothing in `analyzer` or `cmd` forwards the old helpers.

  `Limits.TopN` is the normalized report top (`--top`, never left at 0 by `normalizeOptions`). `Limits.DiagnosticLists` is set only for full diagnostics. `Limits.HasQuery` is how text decides that a transaction has a query line; the view does not know SQL-context wording.

  `View` holds:

  - `Listed` and `Omitted`. `Listed` is `AnalysisResult.Transactions` in analyzer order. `Omitted` is `max(0, TotalTransactions-len(Listed))`. Markdown, JSON `transactions`, and the HTML lookup consume `Listed`. The analyzer already applied `--top-transactions`. The view does not cut this list again by `--top`.
  - `Text.Largest`, `Text.Longest`, `Text.Widest`. The first `min(3, TopN)` of `Diagnostics.LargestTransactions`, the same cap of `LongestTransactions`, and the first `min(1, TopN)` of `WidestTransactions`. `Text.QueryFallback` is the first `Listed` transaction for which `HasQuery` is true, and only when none of those three slices has query text. Text prints that fallback as a query line and nothing else. `Text.ByteLeaders` is the byte ranking, stopped at `min(3, TopN)` or the first non-positive byte count.
  - `Evidence`. The first transaction of each full diagnostic list (largest, then longest, then widest), merged when `TxnKey` is non-empty, with one reason code per win. HTML maps the codes onto the existing labels.
  - `Largest`, `Longest`, `Widest`, `LargestBytes`. The full diagnostic lists when `DiagnosticLists` is set. Nil otherwise, so JSON summary still emits `[]` for the three row/duration/width lists and still omits an empty byte list, and JSON off still omits `diagnostics`.

  The view does not sort. Those lists are already ranked. Table, thread, hot-row, minute, DDL, pattern, and apply-delay caps stay in the renderers. They already share one helper each, and they are not the per-format transaction drift this step closes.

  Text and Markdown do not select the same transactions. Text prints the capped diagnostic champions. Markdown prints every `Listed` row. This step keeps both so the bytes stay the same. Unifying them is #352.

  `report.Write` normalizes options, builds one view, then switches on `ParseFormat`. An unknown format returns that error and writes nothing. There is no default arm, so an unknown format cannot render text. `cmd/binlogviz/analyze.go` calls `Write` for stdout. A JSON snapshot and an HTML file call the same render functions, which build the same view. The CLI still rejects an unknown `--format` with `ParseFormat` before it reads input (usage, exit 3).
- A later step that wants the column parser to live inside `schemacatalog` moves those functions and keeps `SchemaCol` as the single column type. It does not add a parallel struct.
