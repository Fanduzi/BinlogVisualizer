# Orchestration: split `buildHTMLData` into per-section builders

Date: 2026-11-02. Post-v0.38.0 structural hardening unit.

## Why this, evidence first

`internal/report/html.go:buildHTMLData` — 386 lines, complexity 58, 178 statements — is the only function tracked on **all three** codehealth ceilings (length, complexity, statements). It is the single assembly point for every HTML report feature; each new section makes it worse. Every other function >120 lines in `internal/` is ≤154. Splitting it pays down all three ceilings at once and makes per-section unit tests possible for the first time (codegraph: no direct covering tests).

Not a cosmetic refactor: the function is already organized as ~15 sequential sections with natural seams; extraction is mechanical. No ADR (codehealth paydown, precedent #461).

## Structure map (orchestrator recon)

`buildHTMLData(result, opts, echartsJS, built) htmlReportData` — sequential sections:

1. summary/header fields + txn lookup + StartTime/EndTime (325–356)
2. tables + `tableActivitySeries` map (358–396)
3. threads + show-column flags (397–441)
4. alerts + TopAlerts cap-4 (443–464)
5. DDLEvents (466–487)
6. HotRows (489–524)
7. DMLFilter + primaryKey + rowValues notes (525–545)
8. TransactionEvidence (546–547)
9. HotIntervals (549–571)
10. FileCoverage (573–590)
11. FileSegments + throughput chart JSON (592–615)
12. minute chart + TPS series (617–642)
13. top-tables bar + ops pie (644–667)
14. PatternDrilldowns (669–705)
15. ApplyDelay (707)

**Cross-section couplings that must be hoisted, not hidden:**
- `tables` (display-limited via `limitTablesForDisplay`) is reused by the bar-chart section (line 645 `top := tables`). Compute the limiting in the orchestrator and pass to both, or fold bar+pie into the tables builder. Do NOT recompute per-section.
- `chartLayout` (from `opts.chartMinuteLayout`) shared by minute chart + TPS.
- `built view.View` used only for `built.Key` txn lookup and `built.Evidence`.
- `mustJSON` and `opts.formatTime*` used everywhere — builders keep same signatures.
- `d.Alerts` TopAlerts slicing depends on full Alerts list — keep in same builder.

## Refactor shape

Per-section builders following the codebase's existing convention (`buildHTMLTransactionEvidence`, `buildHTMLApplyDelay` already extracted). Either:
- `func buildHTMLTables(d *htmlReportData, ...) ` mutate-style, or
- return-value style where the section maps to one d field.

Worker chooses the leaner of the two per section; consistency matters more than uniformity. `buildHTMLData` becomes a thin orchestrator. Output must be **byte-identical**: same field values, same JSON keys, same ordering.

## Worker task

1. Extract the ~15 sections into named builders in `internal/report/` (same file or `html_data.go` — pick per file-size sanity).
2. Hoist the shared inputs (`tables` limiting, `chartLayout`) to the orchestrator.
3. Keep `htmlReportData` struct + field order + template contract untouched.
4. Add focused unit tests on a few extracted builders where intent is testable (e.g., TopAlerts cap-4, HotRows hidden-key path, throughput JSON shape) — tests encode intent, don't mirror implementation.
5. Gates: `go build`, `CGO_ENABLED=0 go build`, `go vet`, `go test -count=1 ./...`, `go run ./internal/codehealth/cmd/codehealth -base <main> -paydown -update compare`, golangci-lint (incl. `gofmt -s`), doc checker, i18n symmetry.
6. Baseline: this is a paydown — run codehealth `-paydown -update` and include the updated `baseline.json` in the commit.
7. Fixture smoke: generate an HTML report from `test/fixtures/dml-full-row.binlog` before/after and diff — must be identical except `GeneratedAt` timestamp.

## Delivery

- Worktree `.worktrees/report-html-split`, branch `refactor/report-html-data`, herdr tab.
- Orchestrator reviews diff + ceiling deltas + fixture byte-diff before PR.
- PR → CI → rebase-merge → cleanup.
