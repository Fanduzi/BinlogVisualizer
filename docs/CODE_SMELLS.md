# Code smells audit (v0.23.3 / `main` @ `61c4bb4`)

Audit-only pass requested by Adrian Van via BinlogQA. No product refactor in this change.

**Method:** read current parser → normalize → `TransactionBuilder` → analyzer retain path, CLI flags, i18n catalogs, DuckDB/CGO wiring, and tests. Prefer code + tests over delivery notes. Closed issues #61–#63 are cited only where the *structure* that produced those bugs is still in the tree.

**Rank:** P0 = next MariaDB dialect or go-mysql bump can fail analyze or hide GTIDs again. P1 = maintainers/operators will misread behavior or fight the build. P2 = duplication / docs drift that slows every change. P3 = local clutter.

---

## Top 5

### 1. P0 — GTID / XA / DDL boundaries are still a pile of special cases

**Still the #61–#63 structure.** Those bugs are fixed in behavior (v0.23.1–v0.23.3). The state machine that produced them is not.

**Where**

| Symbol | File |
|--------|------|
| `TransactionBuilder.consumeWindowed` | `internal/analyzer/transactions.go` |
| `handleGTID`, `handleBegin`, `handleCommit`, `mergeProvenance` | same |
| `Analyzer.persistCompletedTransactions` | `internal/analyzer/analyzer.go` |
| `parseXAQuery`, `normalizeQueryEventInto` | `internal/binlog/normalize.go` |
| `applyBinlogEventMetadata` + `mariaDBXAPrepareXID` | `internal/binlog/parser.go` |

**What the code actually does**

1. A GTID opens an implicit group (`handleGTID`). A second GTID while the group still has a canonical GTID is a hard `conflicting GTID` (`mergeProvenance`).
2. #61: a GTID-started, non-explicit `DDL` sets `hasEndBoundary` and `finalizeTransaction` so the next MariaDB DDL GTID is a new group, not a conflict.
3. #62: normalized `XA_PREPARE` (and `XA_COMMIT` / `XID` / `COMMIT`) is treated as `handleCommit`. That is the only reason a physical MariaDB `XAPrepareLogEvent` after `XA END` closes the prepared group.
4. #63: after groups close, retain is a second, easy-to-break predicate:

```509:511:internal/analyzer/analyzer.go
		if txn.TotalRows > 0 || txn.XAXID != "" && txn.PositionEnd > txn.PositionStart {
			persisted = append(persisted, txn)
		}
```

Go precedence makes this `TotalRows > 0 || (XAXID != "" && span)`. Zero-row XA COMMIT is kept only because `XAXID` was copied from the Query or from the physical PREPARE payload. Zero-row DDL groups built by the same builder are dropped here on purpose (they live on the DDL timeline). There is no named helper and no comment at the call site.

`parseXAQuery` only maps `XA START|PREPARE|COMMIT` and requires a non-empty remainder as the XID. `XA END`, `XA ROLLBACK`, and `XA BEGIN` (MySQL synonym) return `ok=false`.

**Why it bites**

The next legal MariaDB/MySQL sequence that is not “DDL closes” or “PREPARE/COMMIT closes” will fail analyze (EXIT 1, empty stdout) or drop a GTID from the report / `--include-gtids` (EXIT 2). That is exactly the #61 → #62 → #63 cascade: each fix unblocked the next unmapped boundary. #63 delivery notes already recorded a parser-adapter test gap found in review.

Operators doing XA recovery need the prepare GTID, the commit GTID, and `xa_xid`. Those three facts are assembled in three layers (parser payload, Query SQL, retain predicate) instead of one boundary table.

**Suggested default / fix**

- Make a single `boundaryKind` table: `{opens, continues, closes, ignored}` for `GTID`, `BEGIN`, `DDL` (GTID-started implicit only), `XA_START`, `XA_END`, `XA_PREPARE`, `XA_COMMIT`, `XA_ROLLBACK`, `XID`, `COMMIT`.
- Replace the retain line with a named function, e.g. `retainCompletedTransaction(txn)`, with explicit cases: row work; in-window XA identity; everything else omitted (DDL stays on `Diagnostics.DDLEvents`).
- Treat every `XA *` Query as an XA event, including `END` / `ROLLBACK`. Do not rely on “PREPARE happens to be a commit.”
- Keep the conflicting-GTID guard. Do not weaken it to paper over missing close events.

Related: [#61](https://github.com/Fanduzi/BinlogVisualizer/issues/61), [#62](https://github.com/Fanduzi/BinlogVisualizer/issues/62), [#63](https://github.com/Fanduzi/BinlogVisualizer/issues/63).

---

### 2. P0 — Normalize and the parser skip or swallow without a counter

**Where**

- `NormalizeRawEvent` / `NormalizeRawEventInto` / `normalizeQueryEventInto` / `parseXAQuery` — `internal/binlog/normalize.go`
- `applyBinlogEventMetadata` (`GTIDNext` `err == nil`), `mariaDBXAPrepareXID`, `queryEventActor`, `ParseFilesWithProgress` (`os.Stat` ignored) — `internal/binlog/parser.go`
- `TestNormalizeSkipsNonTransactionalQueryWithoutAllocation` — `internal/binlog/normalize_test.go` (the skip is *tested* as a feature)

**Evidence**

`NormalizeRawEvent` documents `Returns nil for events that should be skipped`. Skip paths return `(nil, nil)` / `(false, nil)` with no kind, no count, no stderr:

- `FORMAT_DESCRIPTION` and any type not in `isSupportedNormalizedEvent`
- Query that is not `BEGIN` / `COMMIT` / mapped XA / `LOAD DATA` / DDL prefix — including **`XA END …` and `XA ROLLBACK …`**
- Empty `EventType`
- Fall-through of the first-letter `switch` in `NormalizeRawEventInto`

Parser:

- `e.GTIDNext()` errors are discarded; `raw.GTID` stays empty → group is anonymous → active GTID selectors never match (`gtid_selector.go` contract).
- `mariaDBXAPrepareXID` returns `""` on short/malformed payload; #63 retain then drops the zero-row commit.
- `queryEventActor` hits `default: return "", ""` on an unknown status-var code and *aborts the rest of the buffer*.
- `os.Stat` failure leaves `fileSize == 0`; progress clamps but never reports the stat error.

**Why it bites**

#62 was “normalize discarded `XAPrepareLogEvent`, next GTID conflicted.” The same shape is still the default for unknown or half-mapped types. Operators get `conflicting GTID`, `window matched 0 events`, or a report with no `xa_xid`, and there is no `skipped_events` / `skipped_xa_query` diagnostic to tell them the parser saw the bytes.

**Suggested default / fix**

- Keep cheap skips for `SET timestamp=…` and FORMAT_DESCRIPTION.
- Do **not** silently skip any Query whose first word is `XA`.
- On `GTIDNext` failure, fail parse (or attach a warning + increment). Empty GTID under `--include-gtids` is how a real group becomes EXIT 2.
- Surface skip counts on `AnalysisResult.Diagnostics` (or stderr once per kind). `TestNormalizeSkipsNonTransactionalQueryWithoutAllocation` should still pass for `SET`, not for `XA END`.

Related structure: #62 (discarded physical event), #63 (missing XID → dropped commit).

---

### 3. P1 — Two event-type vocabularies, plus dead first-letter branches

**Where:** `internal/binlog/normalize.go` (`isSupportedNormalizedEvent`, `NormalizeRawEventInto`, `hasRowsPrefix`); `internal/binlog/parser.go` (`ev.Header.EventType.String()`); tests mix both spellings (`QUERY_EVENT` vs `QueryEvent`). Comment in `TestNormalizeQueryEventBegin`: `go-mysql returns "QueryEvent" not "QUERY_EVENT"`.

**Evidence**

Every supported kind is listed twice (go-mysql `String()` camel name and historical `SCREAMING` alias). Rows are worse: `WriteRows*` is accepted by `isSupportedNormalizedEvent`, then dispatched by `et[0]`:

- `case 'W'` / `'U'` / `'D'` handle `WriteRows` / `WRITE_ROWS` / …
- `case 'R'` still calls `hasRowsPrefix(et, 'W'|'U'|'D')`, which can never match a string that starts with `R`.

That `case 'R'` rows path is dead. A future go-mysql name that starts with neither letter silently returns `false, nil` (smell 2).

**Why it bites**

#62 was a third name (`XAPrepareLogEvent` / `XA_PREPARE_LOG_EVENT`) that the parser emitted and normalize dropped. The dual list is how that class of bug returns.

**Suggested default / fix**

Canonicalize once in the parser (`RawEvent.EventType` = `GTID|QUERY|ROWS|…` or a typed enum). Normalize matches one string per kind. Tests should use only the canonical name.

---

### 4. P1 — CGO / DuckDB is compiled into every `binlogviz` binary; the mode name lies

**Where**

| Symbol | File |
|--------|------|
| `import duckdb "github.com/marcboeker/go-duckdb"` | `internal/analyzer/store.go` (same package as `Analyzer`) |
| `New` vs `NewWithStore` | `internal/analyzer/analyzer.go` |
| `createDuckDBTempStore` | `cmd/binlogviz/analyze.go` |
| `CGO_ENABLED=1` | `.goreleaser.yml`, `.github/workflows/release.yml` |
| No `//go:build` tags | entire repo |

**Evidence**

Default CLI is `--detail-store none` (`DetailStoreNone`). Reports are assembled from `ReportAggregator.Snapshot()`, not DuckDB (`assembleResult`). There is still **no** build tag: importing `internal/analyzer` always pulls `go-duckdb` and CGO.

`New(opts)` with `DetailStoreMode == duckdb` does **not** open DuckDB; it attaches `newInMemoryStore()`. Real DuckDB exists only via `NewWithStore` + `createDuckDBTempStore`. The README states this, then `Finalize` in the same file still says “Flushes in-flight state to DuckDB, queries persisted transactions…”.

`analysisStore` (`store.go`) and `detailStore` (`detail_store.go`) overlap. `model.Transaction`, `persistedTransaction`, and `transactionRow` are three near-copies of the same evidence record (~40 fields).

**Why it bites**

- `go build` / `go test` without CGO fail even though the supported operator path never opens a `.duckdb` file.
- Release is locked to native CGO runners (`.goreleaser.yml` comment).
- A test that sets `DetailStoreMode: duckdb` and calls `New` is not testing DuckDB.

**Suggested default / fix**

Move DuckDB into `internal/analyzer/duckdbstore` with `//go:build cgo`, or a stub file for `!cgo`. Keep `none` as the only default-package store. Rename `DetailStoreDuckDB` on `New()` to `memory` / `buffered` so it does not collide with `--detail-store duckdb`. Collapse the three transaction DTOs to one mapper.

---

### 5. P1 — No committed MariaDB XA / consecutive-DDL binlog; CI cannot regress #61–#63 on bytes

**Where**

- Real fixtures in git: `cmd/binlogviz/testdata/minimal.binlog`, `internal/binlog/testdata/minimal.binlog`, `cmd/binlogviz/testdata/sample-binlog/mysql-bin.000001` — MySQL 5.7 ROW, ~1500 bytes, `CREATE TABLE` + DML only (`cmd/binlogviz/testdata/README.md`).
- XA/DDL coverage: synthetic `model.NormalizedEvent` / `binlog.RawEvent` in `internal/analyzer/transactions_test.go`, `internal/analyzer/selection_test.go`, `cmd/binlogviz/integration_test.go` (`TestRunAnalysisJSONPreservesMariaDBXAAndLoadDataBoundaries`).
- #62 / #63 delivery notes: disposable `mariadb:11.8.3` containers; fixtures moved to Trash, not committed.

**Why it bites**

Unit tests never execute `EventType.String()`, `GenericEvent` + `XA_PREPARE_LOG_EVENT`, or `mariaDBXAPrepareXID` against a real MariaDB 11.4+ file (`LogPos=0` reconstruction in `deriveEventPositionRange` is the other MariaDB-specific hazard). A go-mysql upgrade that changes those names or the PREPARE payload layout will stay green until someone re-runs a laptop container.

`parseXAQuery` has no test for `XA END` / `XA ROLLBACK` (they are silently skipped today).

**Suggested default / fix**

Commit one redacted MariaDB 11.x ROW file: consecutive DDL GTIDs + `XA START` / rows / `XA END` / physical PREPARE / `XA COMMIT`. Drive `go test` through `NewParser().ParseFiles`, not a hand-built `RawEvent` list. Keep the synthetic tests as fast unit coverage.

Related: #61–#63 (behavior fixed; golden bytes not in repo).

---

## P2 — naming, docs vs flags, duplication

### 6. Three product names

| Name | Used as |
|------|---------|
| `Fanduzi/BinlogVisualizer` | GitHub repo, badge URLs |
| `BinlogViz` | README title, HTML `<title>`, pages.dev |
| `binlogviz` | Go module, CLI, Homebrew, `~/.binlogviz`, goreleaser `project_name` |

**Why it bites:** new docs, cask names, and support answers disagree. Operators search GitHub for `binlogviz` and land on the display name.

**Suggested default:** user-facing **BinlogViz**, binary/module **binlogviz**, repo stays `BinlogVisualizer` with one sentence at the top of README: “Repository ID `BinlogVisualizer`; CLI `binlogviz`.” Do not rename the repo in a drive-by.

### 7. Flag catalog, i18n, and `--help` do not share one source

**Where:** `cmd/binlogviz/analyze.go` `newAnalyzeCommand`; `internal/i18n/locales/en.json` + `zh-CN.json`; `docs/reference/cli.md`; README “`--help` output currently remains in English”.

Newer analyze flags are **hardcoded English** and have **no** i18n keys: `--start-position`, `--stop-position`, `--include-gtids`, `--exclude-gtids`, `--snapshot-name`, `--snapshot-dir`. Older flags use `i18n.T("cmd.analyze.flag.*")` even though help is generated before `i18n.Init` (so those keys are English-only at `--help` time anyway).

`docs/reference/cli.md` describes `--top` as “default number of ranked items for text detail sections (minutes, patterns).” `buildAnalyzerOptions` / `buildReportOptions` also copy `--top` onto `TopTables` / `TopTransactions` unless those flags were `Changed`. `cmd/binlogviz/README.md` documents that inheritance; the CLI reference table does not.

`internal/analyzer/README.md` `Finalize` still claims a DuckDB query path; `assembleResult` reads `reportAgg.Snapshot()`.

`README.md` has two Architecture sections (top module table and a second table before License).

**Suggested default:** one flag table generated from cobra (or a test that diffs `cmd.Flags()` vs `docs/reference/cli.md`). Stop calling `i18n.T` for flag `Usage` until help runs after language init. Document `--top` inheritance in the reference table or stop inheriting.

### 8. Parser loops and HTML reports are copy-paste

- `parser.ParseFilesFromOffset` and `ParseFilesWithProgress` (`internal/binlog/parser.go`) both construct `RawEvent`, call `deriveEventPositionRange`, `applyBinlogEventMetadata`, and `handler`. Offset vs progress should be one loop with options.
- Self-contained HTML: `internal/report/html_template.go` (2214 lines), `internal/compare/html.go` (1690), `internal/trend/html.go` (1363). Shared ECharts/CSS/guard chrome. `internal/compare/recommendations.go` vs `internal/trend/recommendations.go` is the same pattern at a smaller scale. Trend already aliases compare input types (`internal/trend/model.go`); HTML did not follow.

**Suggested default:** one `parseLoop(paths, startOffset, onProgress, handler)`. Extract report chrome (guard banner, ECharts init, table sort) once; leave compare/trend data binding local.

### 9. `--top` / `--top-tables` / `--top-transactions` inheritance

**Where:** `analyzeOptions.topTablesChanged` / `topTransactionsChanged`; `buildAnalyzerOptions`; `buildReportOptions`; tests in `cmd/binlogviz/analyze_test.go`.

**Why it bites:** `--top 6` without `--top-tables` changes analyzer *and* renderer limits. `--top-tables 0` is unlimited only if the flag was explicitly set. Easy to “fix help text” and change JSON membership. v0.23.0 already had to stop `--top` from rewriting JSON shares (#54).

**Suggested default:** `--top` presentation-only; `--top-tables` / `--top-transactions` are the only analyzer bounds. Or keep inheritance but put it in `docs/reference/cli.md` with a test that the table mentions it.

---

## P3 — local hazards

### 10. GTID-selector event buffer is unbounded

`Analyzer.pendingGroupEvents` (`analyzer.go`) appends every in-window event for an in-flight group when selectors are active. Comment: `ponytail: buffer one in-flight group; spool if real transaction sizes make this a memory ceiling.` A large selected transaction holds a second copy of every `NormalizedEvent` until close. Default path does not do this.

**Suggested default:** spool to the detail store, or retain only identity + counters needed to match, then aggregate without buffering raw events.

### 11. Status-var and XA payload parsers fail closed to empty

`queryEventActor` / `mariaDBXAPrepareXID` (`parser.go`) use magic offsets (`case 11` = invoker, header size 13). Unknown or short input → empty strings, no error. Empty `XAXID` is enough for smell 1’s retain predicate to drop a zero-row XA commit.

**Suggested default:** return `(xid, err)`; log/count decode failures. Add a parser test with a captured MariaDB PREPARE payload (even if the full binlog is not committed yet).

### 12. Informal leftover

`ponytail:` in `Analyzer.consume` is not a domain term. Replace with a ticket or delete.

---

## What this audit is not

- Not a claim that #61–#63 are still open. Consecutive MariaDB DDL GTIDs, physical `XAPrepareLogEvent`, and zero-row XA COMMIT + `--include-gtids` are covered by unit/integration tests on **synthetic** events.
- Not a request to turn DuckDB back on by default (`none` is the right operator default).
- Not a style nit about file headers or bilingual release notes.

---

## Suggested fix order (when someone *does* change code)

1. Canonical event types + never skip `XA *` Queries; add skip counters (smells 2–3).
2. Named retain + XA END/ROLLBACK in the boundary table (smell 1).
3. Check in one MariaDB 11.x golden binlog (smell 5).
4. Split DuckDB behind CGO (smell 4).
5. Flag/docs single source and `--top` contract (smells 7, 9).
