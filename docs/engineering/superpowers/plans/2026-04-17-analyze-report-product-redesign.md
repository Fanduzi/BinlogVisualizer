# Analyze Report Product Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild `binlogviz analyze` reports into a DBA-oriented product with concise terminal output, coherent HTML reading flow, explicit metric definitions, stronger corpus coverage, and a 1 GB / 10 second performance gate.

**Architecture:** Keep analyzer result semantics stable where possible, but add a thin report-product contract layer that defines top-N defaults, metric labels, and text-vs-HTML presentation policy. Text output becomes a fast diagnostic summary; HTML becomes the full visual evidence surface. Corpus and benchmark work gates the redesign so future additions cannot reintroduce unclear metrics or unbounded output.

**Tech Stack:** Go, Cobra CLI, existing `internal/analyzer`, `internal/model`, `internal/report`, `internal/i18n`, ECharts HTML, Go benchmarks, GitNexus impact analysis, three-level documentation checks.

---

## Scope Decisions

- Default top-N value: `10`.
- Default text output does not render minute details or write-pattern details.
- Detail flags to add: `--details`, `--show-minutes`, `--show-patterns`, and shared `--top`.
- Existing `--top-tables`, `--top-transactions`, and `--top-minutes` remain accepted for compatibility during this iteration.
- TPS definition for this iteration: `avg_tps_per_minute = transactions_in_minute / 60`.
- No ASCII sparklines in this iteration. Text output shows peak average TPS/min and points users to HTML for charts.
- CI uses deterministic synthetic corpus and benchmarks. Private 1 GB binlog validation remains a manual smoke gate.

## File Structure

### New Or Heavily Modified Files

- `internal/report/product.go`
  - Owns report-product constants and small helpers such as default top-N, metric labels, and text visibility policy.

- `internal/report/text.go`
  - Replaces the six-section dump with `Summary`, `Top Findings`, `Top Tables`, and `Next Actions`.
  - Keeps detail renderers behind `report.Options`.

- `internal/report/text_test.go`
  - Locks default text output shape, table alignment, hidden minute/pattern defaults, top-N behavior, and detail flags.

- `internal/report/html.go`
  - Builds a clearer HTML view model: executive summary, timeline series, hotspots, DDL, transaction rankings, analyzed files, and write-shape patterns.

- `internal/report/html_template.go`
  - Reorders sections and chart containers according to the DBA reading path.
  - Adds TPS chart and fixes tooltip/help behavior.

- `internal/report/html_drilldown_test.go`
  - Guards HTML structure, ranking labels, tooltip content, and section order.

- `internal/report/html_i18n_test.go`
  - Guards English and Chinese labels for new sections and tooltips.

- `internal/report/options.go`
  - Adds presentation-only flags: top-N and detail visibility controls.

- `cmd/binlogviz/analyze.go`
  - Adds CLI flags and maps them into `report.Options` and existing analyzer options.

- `internal/analyzer/timeseries.go`
  - Changes `TPSSeries` values from transactions per minute to average TPS per minute.

- `internal/analyzer/timeseries_test.go`
  - Locks TPS semantics.

- `internal/i18n/locales/en.json`
  - Adds labels for the productized report sections and help text.

- `internal/i18n/locales/zh-CN.json`
  - Adds matching Chinese labels.

- `cmd/binlogviz/testdata/sql-corpus/**`
  - Adds deterministic scenario inputs and golden expectations for realistic DBA report paths.

- `cmd/binlogviz/analyze_product_test.go`
  - Adds CLI-level tests for default output, detail flags, top-N consistency, and corpus coverage.

- `cmd/binlogviz/benchmark_test.go`
  - Adds near-1 GB benchmark shape and text-vs-HTML benchmark split.

### Documentation Files

- `README.md`
  - Documents analyze report contract and performance target.

- `README_ZH.md`
  - Chinese version of the same user-facing contract.

- `internal/report/README.md`
  - Notes text/HTML responsibility split and report-product constants.

- `cmd/binlogviz/README.md`
  - Documents new flags and compatibility with existing flags.

## Task 1: Add Report Product Contract

**Files:**
- Create: `internal/report/product.go`
- Modify: `internal/report/README.md`
- Test: `internal/report/options_test.go`

- [ ] **Step 1: Run impact analysis before editing report option symbols**

Run:

```text
gitnexus_impact({target: "Options", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "DefaultOptions", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: record direct callers and risk. If either returns HIGH or CRITICAL, warn before editing.

- [ ] **Step 2: Write failing tests for normalized report defaults**

Add to `internal/report/options_test.go`:

```go
func TestNormalizeOptionsAppliesProductDefaults(t *testing.T) {
	opts := normalizeOptions(Options{})

	if opts.TopN != DefaultTopN {
		t.Fatalf("expected default top N %d, got %d", DefaultTopN, opts.TopN)
	}
	if opts.ShowMinutes {
		t.Fatal("default text output must not show minute details")
	}
	if opts.ShowPatterns {
		t.Fatal("default text output must not show write pattern details")
	}
}

func TestNormalizeOptionsDetailsEnablesDetailedTextSections(t *testing.T) {
	opts := normalizeOptions(Options{Details: true})

	if !opts.ShowMinutes {
		t.Fatal("--details should enable minute details")
	}
	if !opts.ShowPatterns {
		t.Fatal("--details should enable write pattern details")
	}
}

func TestNormalizeOptionsRejectsInvalidTopN(t *testing.T) {
	opts := normalizeOptions(Options{TopN: -5})

	if opts.TopN != DefaultTopN {
		t.Fatalf("expected invalid top N to fall back to %d, got %d", DefaultTopN, opts.TopN)
	}
}
```

- [ ] **Step 3: Run tests and verify they fail**

Run:

```bash
go test ./internal/report -run 'TestNormalizeOptions(AppliesProductDefaults|DetailsEnablesDetailedTextSections|RejectsInvalidTopN)' -count=1
```

Expected: FAIL because `Options.TopN`, `Options.Details`, `Options.ShowMinutes`, `Options.ShowPatterns`, and `DefaultTopN` do not exist yet.

- [ ] **Step 4: Add the product contract file**

Create `internal/report/product.go`:

```go
// Package report defines user-facing report presentation contracts.
// input: analyzer results and CLI-selected report presentation options.
// output: stable defaults and labels shared by text, JSON, and HTML renderers.
// pos: report product contract layer used by renderer view-model builders.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

const (
	// DefaultTopN is the product-wide default number of ranked items shown in reports.
	DefaultTopN = 10
)

const (
	MetricAvgTPSPerMinute = "avg_tps_per_minute"
	MetricRows            = "rows"
	MetricDuration        = "duration"
	MetricTouchedTables   = "touched_tables"
)
```

- [ ] **Step 5: Extend `Options` and `normalizeOptions`**

Modify `internal/report/options.go`:

```go
type Options struct {
	SQLContextMode SQLContextMode
	Format         Format
	TopN           int
	Details        bool
	ShowMinutes    bool
	ShowPatterns   bool
}
```

Update `DefaultOptions`:

```go
func DefaultOptions() Options {
	return Options{SQLContextMode: SQLContextSummary, TopN: DefaultTopN}
}
```

Update `normalizeOptions`:

```go
func normalizeOptions(opts Options) Options {
	mode, err := ParseSQLContextMode(string(opts.SQLContextMode))
	if err != nil {
		opts.SQLContextMode = SQLContextSummary
	} else {
		opts.SQLContextMode = mode
	}
	if opts.TopN <= 0 {
		opts.TopN = DefaultTopN
	}
	if opts.Details {
		opts.ShowMinutes = true
		opts.ShowPatterns = true
	}
	return opts
}
```

- [ ] **Step 6: Update module README**

Add to `internal/report/README.md` under Notes:

```markdown
- `product.go` owns presentation defaults such as `DefaultTopN` so text, HTML, and command flags share one report contract.
```

- [ ] **Step 7: Run tests and commit**

Run:

```bash
go test ./internal/report -run 'TestNormalizeOptions|TestParse' -count=1
```

Expected: PASS.

Commit:

```bash
git add internal/report/product.go internal/report/options.go internal/report/options_test.go internal/report/README.md
git commit -m "report: define analyze product presentation contract"
```

## Task 2: Fix TPS Semantics

**Files:**
- Modify: `internal/analyzer/timeseries.go`
- Modify: `internal/analyzer/timeseries_test.go`
- Modify: `internal/model/timeseries.go`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "BuildTimeseries", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "TimeseriesAggregator", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: record affected report, JSON, compare, and trend consumers. Warn if HIGH or CRITICAL.

- [ ] **Step 2: Write failing TPS test**

Add to `internal/analyzer/timeseries_test.go`:

```go
func TestBuildTimeseriesUsesAverageTPSPerMinute(t *testing.T) {
	minute := time.Date(2026, 4, 17, 9, 0, 0, 0, time.UTC)
	series := BuildTimeseries(TimeseriesInput{
		Minutes: []model.MinuteBucket{{
			Minute:   minute,
			TxnCount: 120,
		}},
	})

	if len(series.TPSSeries) != 1 {
		t.Fatalf("expected one TPS point, got %d", len(series.TPSSeries))
	}
	if got := series.TPSSeries[0].Value; got != 2 {
		t.Fatalf("expected avg TPS/min 2.0, got %.2f", got)
	}
}
```

- [ ] **Step 3: Run test and verify it fails**

Run:

```bash
go test ./internal/analyzer -run TestBuildTimeseriesUsesAverageTPSPerMinute -count=1
```

Expected: FAIL with current value `120`.

- [ ] **Step 4: Implement TPS calculation**

Modify `internal/analyzer/timeseries.go` where `TPSSeries` is appended:

```go
avgTPS := float64(bucket.TxnCount) / 60.0
series.TPSSeries = append(series.TPSSeries, model.TimeseriesPoint{Minute: minute, Value: avgTPS})
```

- [ ] **Step 5: Clarify model comment**

Update `internal/model/timeseries.go` comment:

```go
// TPSSeries stores average TPS per minute, calculated as minute transaction count divided by 60.
TPSSeries []TimeseriesPoint
```

- [ ] **Step 6: Run focused and affected tests**

Run:

```bash
go test ./internal/analyzer ./internal/report ./internal/compare ./internal/trend -count=1
```

Expected: PASS or intentional golden failures where labels need updating in later tasks. Do not update unrelated goldens in this task.

- [ ] **Step 7: Commit**

```bash
git add internal/analyzer/timeseries.go internal/analyzer/timeseries_test.go internal/model/timeseries.go
git commit -m "analyzer: define TPS as average per minute"
```

## Task 3: Redesign Default Text Report

**Files:**
- Modify: `internal/report/text.go`
- Modify: `internal/report/text_test.go`
- Modify: `internal/i18n/locales/en.json`
- Modify: `internal/i18n/locales/zh-CN.json`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "RenderTextWithOptions", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "renderWorkloadSummary", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "renderTopTables", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: record blast radius. This is user-facing CLI output; warn if HIGH or CRITICAL.

- [ ] **Step 2: Write failing default-output test**

Replace the old section-order expectation in `internal/report/text_test.go` with:

```go
func TestRenderTextDefaultIsConciseDiagnosticSummary(t *testing.T) {
	result := productTextFixture()

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{"Summary", "Top Findings", "Top Tables", "Next Actions", "Peak avg TPS/min"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected default text report to contain %q\n%s", token, out)
		}
	}
	for _, hidden := range []string{"Minute Activity", "Top Patterns", "2026-04-17 09:01"} {
		if strings.Contains(out, hidden) {
			t.Fatalf("default text report should hide %q\n%s", hidden, out)
		}
	}
}
```

Add fixture helper:

```go
func productTextFixture() model.AnalysisResult {
	start := time.Date(2026, 4, 17, 9, 0, 0, 0, time.UTC)
	return model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 120,
			TotalRows:         10000,
			TotalEvents:       500,
			StartTime:         start,
			EndTime:           start.Add(10 * time.Minute),
			Duration:          10 * time.Minute,
		},
		Timeseries: model.Timeseries{
			TPSSeries:  []model.TimeseriesPoint{{Minute: start.Add(time.Minute), Value: 22.5}},
			RowsSeries: []model.TimeseriesPoint{{Minute: start.Add(time.Minute), Value: 9000}},
		},
		Tables: []model.TableStats{
			{Schema: "shop", Table: "orders", TotalRows: 9000, TxnCount: 100, EventCount: 300},
			{Schema: "shop", Table: "users", TotalRows: 1000, TxnCount: 20, EventCount: 80},
		},
		Minutes: []model.MinuteBucket{
			{Minute: start, TotalRows: 1000, TxnCount: 10},
			{Minute: start.Add(time.Minute), TotalRows: 9000, TxnCount: 100},
		},
		Diagnostics: model.Diagnostics{
			HotIntervals: []model.MinuteBucket{{Minute: start.Add(time.Minute), TotalRows: 9000, TxnCount: 100}},
			DDLEvents: []model.DDLEvent{{
				Timestamp: start.Add(2 * time.Minute),
				Operation: "ALTER",
				Schema: "shop",
				Table: "orders",
				Statement: "ALTER TABLE shop.orders ADD COLUMN marker INT",
			}},
			LongestTransactions: []model.Transaction{{
				TxnKey: "txn-long",
				TotalRows: 500,
				Duration: 45 * time.Second,
				BinlogPathStart: "mysql-bin.000044",
				PositionStart: 100,
				PositionEnd: 200,
			}},
		},
	}
}
```

- [ ] **Step 3: Write failing table-alignment test**

Add:

```go
func TestRenderTextTopTablesUsesAlignedTableAndTopLimit(t *testing.T) {
	result := productTextFixture()
	out, err := RenderTextWithOptions(result, Options{TopN: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "#  Table") || !strings.Contains(out, "Rows") || !strings.Contains(out, "Share") {
		t.Fatalf("expected aligned top tables header\n%s", out)
	}
	if !strings.Contains(out, "shop.orders") {
		t.Fatalf("expected top table\n%s", out)
	}
	if strings.Contains(out, "shop.users") {
		t.Fatalf("expected top limit to hide second table\n%s", out)
	}
}
```

- [ ] **Step 4: Write failing detail flags test**

Add:

```go
func TestRenderTextDetailsCanShowMinuteAndPatternSections(t *testing.T) {
	result := productTextFixture()
	result.Patterns = []model.PatternStats{{
		PatternKey: "shop.orders|UPDATE|medium",
		Label: "shop.orders / UPDATE / medium batch",
		TotalRows: 9000,
		TxnCount: 100,
		AvgRowsPerTxn: 90,
	}}

	out, err := RenderTextWithOptions(result, Options{Details: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{"Minute Details", "Write Shape Patterns", "shop.orders / UPDATE / medium batch"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected detail output to contain %q\n%s", token, out)
		}
	}
}
```

- [ ] **Step 5: Run tests and verify failures**

Run:

```bash
go test ./internal/report -run 'TestRenderText(DefaultIsConcise|TopTablesUsesAligned|DetailsCanShow)' -count=1
```

Expected: FAIL because current renderer still prints old sections.

- [ ] **Step 6: Implement text report flow**

Change `RenderTextWithOptions` to this structure:

```go
func RenderTextWithOptions(result model.AnalysisResult, opts Options) (string, error) {
	opts = normalizeOptions(opts)
	var buf strings.Builder

	renderDiagnosticSummary(&buf, result)
	renderTopFindings(&buf, result, opts)
	renderTopTablesTable(&buf, result.Tables, opts.TopN)
	renderNextActions(&buf, result)

	if opts.ShowMinutes {
		renderMinuteDetails(&buf, result.Minutes, opts.TopN)
	}
	if opts.ShowPatterns {
		renderWriteShapePatterns(&buf, result.Patterns, result.PatternDrilldowns, opts.TopN)
	}
	return buf.String(), nil
}
```

Implement helper functions in `internal/report/text.go`:

```go
func renderDiagnosticSummary(buf *strings.Builder, result model.AnalysisResult) {
	buf.WriteString("=== " + i18n.T("report.text.summary") + " ===\n")
	buf.WriteString(fmt.Sprintf("  %s: %s - %s\n", i18n.T("report.label.timeRange"), formatTime(result.Summary.StartTime), formatTime(result.Summary.EndTime)))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalTransactions"), result.Summary.TotalTransactions))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalRows"), result.Summary.TotalRows))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalEvents"), result.Summary.TotalEvents))
	buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.text.peakAvgTPS"), formatPeakSeries(result.Timeseries.TPSSeries)))
	buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.text.peakRowsPerMinute"), formatPeakSeries(result.Timeseries.RowsSeries)))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.html.analyze.ddlTimeline"), len(result.Diagnostics.DDLEvents)))
	buf.WriteString("\n")
}
```

Use deterministic helper:

```go
func formatPeakSeries(points []model.TimeseriesPoint) string {
	if len(points) == 0 {
		return i18n.T("time.notAvailable")
	}
	peak := points[0]
	for _, point := range points[1:] {
		if point.Value > peak.Value || (point.Value == peak.Value && point.Minute.Before(peak.Minute)) {
			peak = point
		}
	}
	return fmt.Sprintf("%.1f at %s", peak.Value, peak.Minute.Format("2006-01-02 15:04"))
}
```

Implement aligned table:

```go
func renderTopTablesTable(buf *strings.Builder, tables []model.TableStats, topN int) {
	buf.WriteString("=== " + i18n.T("report.text.topTables") + " ===\n")
	if len(tables) == 0 {
		buf.WriteString("  " + i18n.T("report.text.noTableActivity") + "\n\n")
		return
	}
	limit := minInt(topN, len(tables))
	totalRows := 0
	for _, table := range tables {
		totalRows += table.TotalRows
	}
	buf.WriteString("  #  Table                         Rows       Txns     Events    Share\n")
	for i := 0; i < limit; i++ {
		table := tables[i]
		name := table.Schema + "." + table.Table
		share := 0.0
		if totalRows > 0 {
			share = float64(table.TotalRows) * 100 / float64(totalRows)
		}
		buf.WriteString(fmt.Sprintf("  %-2d %-28s %10d %8d %8d %6.1f%%\n",
			i+1, name, table.TotalRows, table.TxnCount, table.EventCount, share))
	}
	buf.WriteString("\n")
}
```

Add `minInt` if the package does not already have one:

```go
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
```

- [ ] **Step 7: Add i18n keys**

Add to `internal/i18n/locales/en.json`:

```json
"report.text.summary": "Summary",
"report.text.topFindings": "Top Findings",
"report.text.topTables": "Top Tables",
"report.text.nextActions": "Next Actions",
"report.text.minuteDetails": "Minute Details",
"report.text.writeShapePatterns": "Write Shape Patterns",
"report.text.peakAvgTPS": "Peak avg TPS/min",
"report.text.peakRowsPerMinute": "Peak Rows/min",
"report.text.openHTML": "Open HTML for minute charts and transaction evidence.",
"report.text.noFindings": "No high-signal findings detected.",
"report.text.noTableActivity": "No table activity detected."
```

Add matching Chinese keys to `internal/i18n/locales/zh-CN.json`.

- [ ] **Step 8: Run tests**

Run:

```bash
go test ./internal/report -count=1
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add internal/report/text.go internal/report/text_test.go internal/i18n/locales/en.json internal/i18n/locales/zh-CN.json
git commit -m "report: make analyze text output diagnostic-first"
```

## Task 4: Add Analyze CLI Presentation Flags

**Files:**
- Modify: `cmd/binlogviz/analyze.go`
- Modify: `cmd/binlogviz/integration_test.go`
- Modify: `cmd/binlogviz/README.md`
- Modify: `internal/i18n/locales/en.json`
- Modify: `internal/i18n/locales/zh-CN.json`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "newAnalyzeCommand", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "buildReportOptions", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: command-level blast radius. Warn if HIGH or CRITICAL.

- [ ] **Step 2: Write failing CLI flag test**

Add to `cmd/binlogviz/integration_test.go`:

```go
func TestAnalyzeTextOutputHidesMinutesUntilRequested(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("en")
	t.Cleanup(i18n.ResetForTesting)

	fixture := minimalBinlogFixture(t)
	root := newRootCommand()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"analyze", fixture, "--format", "text"})

	if err := root.Execute(); err != nil {
		t.Fatalf("analyze text failed: %v\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "Minute Details") {
		t.Fatalf("default output should hide minute details\n%s", out.String())
	}
}
```

If `minimalBinlogFixture` does not exist, use the existing fixture helper used by other integration tests instead of inventing a new parser path.

- [ ] **Step 3: Write failing `--show-minutes` test**

Add:

```go
func TestAnalyzeShowMinutesFlagEnablesMinuteDetails(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("en")
	t.Cleanup(i18n.ResetForTesting)

	fixture := minimalBinlogFixture(t)
	root := newRootCommand()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"analyze", fixture, "--format", "text", "--show-minutes"})

	if err := root.Execute(); err != nil {
		t.Fatalf("analyze text failed: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "Minute Details") {
		t.Fatalf("expected minute details with --show-minutes\n%s", out.String())
	}
}
```

- [ ] **Step 4: Run tests and verify failures**

Run:

```bash
go test ./cmd/binlogviz -run 'TestAnalyze(TextOutputHidesMinutesUntilRequested|ShowMinutesFlagEnablesMinuteDetails)' -count=1
```

Expected: FAIL because flags are not wired.

- [ ] **Step 5: Extend analyze options and flags**

Modify `analyzeOptions`:

```go
details      bool
showMinutes  bool
showPatterns bool
top           int
```

Add flags in `newAnalyzeCommand`:

```go
cmd.Flags().BoolVar(&opts.details, "details", false, i18n.T("cmd.analyze.flag.details"))
cmd.Flags().BoolVar(&opts.showMinutes, "show-minutes", false, i18n.T("cmd.analyze.flag.showMinutes"))
cmd.Flags().BoolVar(&opts.showPatterns, "show-patterns", false, i18n.T("cmd.analyze.flag.showPatterns"))
cmd.Flags().IntVar(&opts.top, "top", report.DefaultTopN, i18n.T("cmd.analyze.flag.top"))
```

Update `buildReportOptions` to include:

```go
reportOpts.TopN = opts.top
reportOpts.Details = opts.details
reportOpts.ShowMinutes = opts.showMinutes
reportOpts.ShowPatterns = opts.showPatterns
```

Ensure analyzer top limits are still set from `--top-tables` and `--top-transactions`, but if those flags are untouched and `--top` is set, use `--top` as the default for both table and transaction presentation.

- [ ] **Step 6: Add i18n and README text**

Add English keys:

```json
"cmd.analyze.flag.details": "Show detailed text sections",
"cmd.analyze.flag.showMinutes": "Show minute-level activity in text output",
"cmd.analyze.flag.showPatterns": "Show write shape patterns in text output",
"cmd.analyze.flag.top": "Default number of ranked items to show"
```

Add Chinese equivalents.

Update `cmd/binlogviz/README.md` with:

```markdown
- `--top N` sets the default ranked output size for productized text/HTML report sections.
- `--details`, `--show-minutes`, and `--show-patterns` expand the default concise text report.
```

- [ ] **Step 7: Run tests and commit**

Run:

```bash
go test ./cmd/binlogviz -run 'TestAnalyze' -count=1
go test ./internal/report -count=1
```

Expected: PASS.

Commit:

```bash
git add cmd/binlogviz/analyze.go cmd/binlogviz/integration_test.go cmd/binlogviz/README.md internal/i18n/locales/en.json internal/i18n/locales/zh-CN.json
git commit -m "cmd: add analyze report presentation controls"
```

## Task 5: Rebuild HTML Report Information Architecture

**Files:**
- Modify: `internal/report/html.go`
- Modify: `internal/report/html_template.go`
- Modify: `internal/report/html_drilldown_test.go`
- Modify: `internal/report/html_i18n_test.go`
- Modify: `internal/report/README.md`
- Modify: `internal/i18n/locales/en.json`
- Modify: `internal/i18n/locales/zh-CN.json`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "RenderHTMLWithOptions", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "buildHTMLData", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: HTML render path blast radius. Warn if HIGH or CRITICAL.

- [ ] **Step 2: Write failing section-order test**

Add to `internal/report/html_drilldown_test.go`:

```go
func TestAnalyzeHTMLUsesDBAReadingPath(t *testing.T) {
	out, err := RenderHTML(productHTMLFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedOrder := []string{
		`id="executive-summary"`,
		`id="timeline"`,
		`id="hotspots"`,
		`id="ddl-timeline"`,
		`id="transaction-evidence"`,
		`id="analyzed-files"`,
		`id="write-shape-patterns"`,
	}
	last := -1
	for _, token := range expectedOrder {
		idx := strings.Index(out, token)
		if idx < 0 {
			t.Fatalf("expected HTML to contain %s", token)
		}
		if idx < last {
			t.Fatalf("expected %s after previous section", token)
		}
		last = idx
	}
}
```

Add `productHTMLFixture` if missing, reusing the text fixture structure but with DDL, hot intervals, file coverage, transactions, and pattern drilldowns.

- [ ] **Step 3: Write failing TPS chart test**

Add:

```go
func TestAnalyzeHTMLIncludesReadableTPSChart(t *testing.T) {
	out, err := RenderHTML(productHTMLFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{`id="chart-tps"`, `avg TPS/min`, `min-height: 420px`} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected TPS chart token %q\n%s", token, out)
		}
	}
}
```

- [ ] **Step 4: Write failing transaction-label test**

Add:

```go
func TestAnalyzeHTMLTransactionEvidenceLabelsRankingMetric(t *testing.T) {
	out, err := RenderHTML(productHTMLFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"Top 5 Largest Transactions by Rows",
		"Top 5 Longest Transactions by Duration",
		"Top 5 Widest Transactions by Touched Tables",
		"touched tables",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected transaction evidence label %q\n%s", token, out)
		}
	}
}
```

- [ ] **Step 5: Write failing analyzed-files and tooltip test**

Add:

```go
func TestAnalyzeHTMLExplainsAnalyzedFilesAndPatternMetrics(t *testing.T) {
	out, err := RenderHTML(productHTMLFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"Analyzed Files",
		"selected: overlaps requested time window",
		"filesystem mtime",
		"Row share",
		"Transaction share",
		"Avg rows per transaction",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected explanatory token %q\n%s", token, out)
		}
	}
}
```

- [ ] **Step 6: Run tests and verify failures**

Run:

```bash
go test ./internal/report -run 'TestAnalyzeHTML(UsesDBAReadingPath|IncludesReadableTPSChart|TransactionEvidenceLabels|ExplainsAnalyzedFiles)' -count=1
```

Expected: FAIL against current HTML.

- [ ] **Step 7: Extend HTML data model**

Modify `htmlReportData` in `internal/report/html.go` to add:

```go
PeakAvgTPS          string
PeakRowsPerMinute  string
SelectedFileCount  int
SkippedFileCount   int
TPSLabels          template.JS
TPSValues          template.JS
EventsValues       template.JS
InsertEvents       template.JS
UpdateEvents       template.JS
DeleteEvents       template.JS
DDLEventsSeries    template.JS
BinlogBytesValues  template.JS
TopN               int
```

Build these fields from `result.Timeseries` and normalized `opts.TopN`.

- [ ] **Step 8: Reorder template sections**

Modify `internal/report/html_template.go` so section order is:

```html
<section id="executive-summary">...</section>
<section id="timeline">...</section>
<section id="hotspots">...</section>
<section id="ddl-timeline">...</section>
<section id="transaction-evidence">...</section>
<section id="analyzed-files">...</section>
<section id="write-shape-patterns">...</section>
```

Ensure the TPS chart container exists:

```html
<div class="chart-panel chart-panel-large">
  <div class="chart-title">{{t "report.html.analyze.avgTPSPerMinute"}}</div>
  <div id="chart-tps" class="chart chart-large"></div>
</div>
```

CSS requirement:

```css
.chart-large {
  min-height: 420px;
}
.chart-controls,
.chart-legend-note {
  position: static;
}
```

- [ ] **Step 9: Fix pattern help behavior**

Replace decorative question marks with real title/aria labels:

```html
<span class="metric-help" title="{{t "report.html.pattern.rowShareHelp"}}" aria-label="{{t "report.html.pattern.rowShareHelp"}}">?</span>
```

If the UI cannot show useful help for a metric, remove the icon for that metric in this task.

- [ ] **Step 10: Add i18n keys**

Add English keys:

```json
"report.html.analyze.executiveSummary": "Executive Summary",
"report.html.analyze.timeline": "Timeline",
"report.html.analyze.hotspots": "Hotspots",
"report.html.analyze.analyzedFiles": "Analyzed Files",
"report.html.analyze.avgTPSPerMinute": "avg TPS/min",
"report.html.analyze.topLargestByRows": "Top 5 Largest Transactions by Rows",
"report.html.analyze.topLongestByDuration": "Top 5 Longest Transactions by Duration",
"report.html.analyze.topWidestByTouchedTables": "Top 5 Widest Transactions by Touched Tables",
"report.html.analyze.analyzedFilesHelp": "These are the binlog files selected or skipped for this analyze window. Filesystem mtime may be used as a coarse pre-filter before event timestamp probing.",
"report.html.pattern.rowShareHelp": "This pattern's rows divided by total rows in the analyzed window.",
"report.html.pattern.txnShareHelp": "This pattern's transactions divided by total transactions in the analyzed window.",
"report.html.pattern.avgRowsHelp": "Total rows for this pattern divided by transaction count for this pattern."
```

Add Chinese equivalents.

- [ ] **Step 11: Run tests and commit**

Run:

```bash
go test ./internal/report -count=1
```

Expected: PASS.

Commit:

```bash
git add internal/report/html.go internal/report/html_template.go internal/report/html_drilldown_test.go internal/report/html_i18n_test.go internal/report/README.md internal/i18n/locales/en.json internal/i18n/locales/zh-CN.json
git commit -m "report: restructure analyze HTML around DBA workflow"
```

## Task 6: Build Strong SQL Corpus And Goldens

**Files:**
- Create: `cmd/binlogviz/testdata/sql-corpus/README.md`
- Create: `cmd/binlogviz/testdata/sql-corpus/baseline-small/events.json`
- Create: `cmd/binlogviz/testdata/sql-corpus/tps-spike/events.json`
- Create: `cmd/binlogviz/testdata/sql-corpus/rows-spike/events.json`
- Create: `cmd/binlogviz/testdata/sql-corpus/ddl-incident/events.json`
- Create: `cmd/binlogviz/testdata/sql-corpus/large-transaction/events.json`
- Create: `cmd/binlogviz/testdata/sql-corpus/long-transaction/events.json`
- Create: `cmd/binlogviz/testdata/sql-corpus/wide-transaction/events.json`
- Create: `cmd/binlogviz/testdata/sql-corpus/multi-file-window/events.json`
- Create: `cmd/binlogviz/analyze_product_test.go`

- [ ] **Step 1: Run impact analysis for test helpers if modifying existing helpers**

If modifying existing command test helpers, run:

```text
gitnexus_impact({target: "newRootCommand", direction: "upstream", repo: "BinlogVisualizer"})
```

If only creating new test helper functions in `analyze_product_test.go`, no production symbol edit is needed.

- [ ] **Step 2: Add corpus README**

Create `cmd/binlogviz/testdata/sql-corpus/README.md`:

```markdown
# Analyze SQL Corpus

This corpus validates DBA-facing analyze report behavior. Each scenario is deterministic and targets one report contract:

| Scenario | Contract |
|---|---|
| `baseline-small` | normal workload with no high-signal anomaly |
| `tps-spike` | TPS timeline and hot interval detection |
| `rows-spike` | rows/minute spike without necessarily high TPS |
| `ddl-incident` | DDL Timeline and DDL series |
| `large-transaction` | Largest by Rows ranking |
| `long-transaction` | Longest by Duration ranking |
| `wide-transaction` | Widest by Touched Tables ranking |
| `multi-file-window` | Analyzed Files selected/skipped semantics |
```

- [ ] **Step 3: Add scenario event JSON shape**

Use this JSON format for every `events.json`:

```json
[
  {
    "timestamp": "2026-04-17T09:00:00Z",
    "event_type": "QUERY_EVENT",
    "query": "BEGIN"
  },
  {
    "timestamp": "2026-04-17T09:00:01Z",
    "event_type": "WRITE_ROWS_EVENT",
    "schema": "shop",
    "table": "orders",
    "row_count": 100
  },
  {
    "timestamp": "2026-04-17T09:00:02Z",
    "event_type": "XID_EVENT"
  }
]
```

Each scenario must include enough data to trigger its intended contract. For example, `long-transaction/events.json` must contain one transaction with a much longer BEGIN-to-XID duration than the others but fewer rows than the largest transaction scenario.

- [ ] **Step 4: Write corpus loader tests**

Create `cmd/binlogviz/analyze_product_test.go` with:

```go
package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

func loadCorpusEvents(t *testing.T, scenario string) []model.NormalizedEvent {
	t.Helper()
	path := filepath.Join("testdata", "sql-corpus", scenario, "events.json")
	rawBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read corpus %s: %v", scenario, err)
	}
	var raw []binlog.RawEvent
	if err := json.Unmarshal(rawBytes, &raw); err != nil {
		t.Fatalf("decode corpus %s: %v", scenario, err)
	}
	events := make([]model.NormalizedEvent, 0, len(raw))
	for _, ev := range raw {
		normalized, err := binlog.NormalizeRawEvent(ev)
		if err != nil {
			t.Fatalf("normalize corpus %s: %v", scenario, err)
		}
		if normalized != nil {
			events = append(events, *normalized)
		}
	}
	return events
}

func analyzeCorpus(t *testing.T, scenario string) model.AnalysisResult {
	t.Helper()
	a := analyzer.New(analyzer.DefaultOptions())
	result, err := a.Analyze(loadCorpusEvents(t, scenario))
	if err != nil {
		t.Fatalf("analyze corpus %s: %v", scenario, err)
	}
	return *result
}

func TestAnalyzeCorpusCoversRequiredScenarios(t *testing.T) {
	for _, scenario := range []string{
		"baseline-small",
		"tps-spike",
		"rows-spike",
		"ddl-incident",
		"large-transaction",
		"long-transaction",
		"wide-transaction",
		"multi-file-window",
	} {
		t.Run(scenario, func(t *testing.T) {
			if len(loadCorpusEvents(t, scenario)) == 0 {
				t.Fatalf("scenario %s has no normalized events", scenario)
			}
		})
	}
}
```

- [ ] **Step 5: Add behavior tests for each scenario**

Add tests:

```go
func TestAnalyzeCorpusTPSAndRowsSpikes(t *testing.T) {
	tps := analyzeCorpus(t, "tps-spike")
	if len(tps.Timeseries.TPSSeries) == 0 {
		t.Fatal("expected TPS series")
	}
	if len(tps.Diagnostics.HotIntervals) == 0 {
		t.Fatal("expected hot intervals for TPS spike")
	}

	rows := analyzeCorpus(t, "rows-spike")
	if len(rows.Diagnostics.LargestTransactions) == 0 {
		t.Fatal("expected largest transaction evidence for rows spike")
	}
}

func TestAnalyzeCorpusDDLAndTransactionRankings(t *testing.T) {
	ddl := analyzeCorpus(t, "ddl-incident")
	if len(ddl.Diagnostics.DDLEvents) == 0 {
		t.Fatal("expected DDL timeline events")
	}

	large := analyzeCorpus(t, "large-transaction")
	if len(large.Diagnostics.LargestTransactions) == 0 || large.Diagnostics.LargestTransactions[0].TxnKey == "" {
		t.Fatal("expected largest transaction rank 1")
	}

	long := analyzeCorpus(t, "long-transaction")
	if len(long.Diagnostics.LongestTransactions) == 0 || long.Diagnostics.LongestTransactions[0].Duration < 30*time.Second {
		t.Fatal("expected long transaction rank 1 by duration")
	}

	wide := analyzeCorpus(t, "wide-transaction")
	if len(wide.Diagnostics.WidestTransactions) == 0 || len(wide.Diagnostics.WidestTransactions[0].Tables) < 3 {
		t.Fatal("expected wide transaction touching at least three tables")
	}
}

func TestAnalyzeCorpusTextAndHTMLContracts(t *testing.T) {
	result := analyzeCorpus(t, "tps-spike")
	textOut, err := report.RenderText(result)
	if err != nil {
		t.Fatalf("render text: %v", err)
	}
	if strings.Contains(textOut, "Minute Details") || strings.Contains(textOut, "Write Shape Patterns") {
		t.Fatalf("default text output leaked detail sections\n%s", textOut)
	}

	htmlOut, err := report.RenderHTML(result)
	if err != nil {
		t.Fatalf("render html: %v", err)
	}
	for _, token := range []string{`id="chart-tps"`, `id="timeline"`, `id="hotspots"`} {
		if !strings.Contains(htmlOut, token) {
			t.Fatalf("expected HTML token %q", token)
		}
	}
}
```

- [ ] **Step 6: Run tests and verify failures before adding all fixtures**

Run:

```bash
go test ./cmd/binlogviz -run TestAnalyzeCorpus -count=1
```

Expected: FAIL until all scenario JSON files are present.

- [ ] **Step 7: Add deterministic scenario JSON files**

Create each `events.json` using the shape above. Keep files small but semantically clear:

- `baseline-small`: 3 small transactions across `shop.orders` and `shop.users`.
- `tps-spike`: at least 120 one-row transactions in the same minute.
- `rows-spike`: 2 transactions, one with `row_count` 100000.
- `ddl-incident`: at least one `ALTER TABLE`, one `CREATE TABLE`, one `DROP TABLE`, one `TRUNCATE TABLE`.
- `large-transaction`: one transaction with largest `TotalRows`.
- `long-transaction`: one transaction with BEGIN and XID at least 45 seconds apart.
- `wide-transaction`: one transaction writing at least 5 distinct tables.
- `multi-file-window`: events with source metadata if supported by `RawEvent`; otherwise leave file coverage assertions to planner tests.

- [ ] **Step 8: Run tests and commit**

Run:

```bash
go test ./cmd/binlogviz -run TestAnalyzeCorpus -count=1
go test ./internal/analyzer ./internal/report -count=1
```

Expected: PASS.

Commit:

```bash
git add cmd/binlogviz/analyze_product_test.go cmd/binlogviz/testdata/sql-corpus
git commit -m "test: add DBA analyze product corpus"
```

## Task 7: Enforce Top-N Consistency

**Files:**
- Modify: `internal/report/text.go`
- Modify: `internal/report/html.go`
- Modify: `internal/report/html_template.go`
- Modify: `internal/report/text_test.go`
- Modify: `internal/report/html_drilldown_test.go`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "buildHTMLData", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "renderTopTablesTable", direction: "upstream", repo: "BinlogVisualizer"})
```

If `renderTopTablesTable` does not exist yet because Task 3 was not completed, finish Task 3 first.

- [ ] **Step 2: Write failing HTML top-N test**

Add:

```go
func TestAnalyzeHTMLUsesReportTopNForTopTables(t *testing.T) {
	result := productHTMLFixture()
	for i := 0; i < 12; i++ {
		result.Tables = append(result.Tables, model.TableStats{
			Schema: "shop",
			Table: fmt.Sprintf("table_%02d", i),
			TotalRows: 1000 - i,
		})
	}

	out, err := RenderHTMLWithOptions(result, Options{TopN: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Count(out, `data-table-row`) > 3 {
		t.Fatalf("expected top 3 table rows in HTML\n%s", out)
	}
}
```

If the template does not yet have `data-table-row`, add it to table row markup in implementation.

- [ ] **Step 3: Write failing text/HTML parity test**

Add to `internal/report/text_test.go`:

```go
func TestRenderTextAndHTMLShareTopNDefault(t *testing.T) {
	if DefaultOptions().TopN != DefaultTopN {
		t.Fatalf("default options top N drifted from product default")
	}
}
```

- [ ] **Step 4: Run tests and verify failures**

Run:

```bash
go test ./internal/report -run 'TestAnalyzeHTMLUsesReportTopNForTopTables|TestRenderTextAndHTMLShareTopNDefault' -count=1
```

Expected: FAIL until HTML uses `opts.TopN`.

- [ ] **Step 5: Apply `opts.TopN` in HTML**

Modify `buildHTMLData`:

```go
opts = normalizeOptions(opts)
d.TopN = opts.TopN
```

When building `d.Tables`, limit using `opts.TopN`:

```go
tables := result.Tables
if len(tables) > opts.TopN {
	tables = tables[:opts.TopN]
}
for _, t := range tables {
	// existing table row build
}
```

For table bar data, use the same limited `tables` slice.

- [ ] **Step 6: Mark table rows in template**

Update table row markup:

```html
<tr data-table-row="{{.Key}}">
```

- [ ] **Step 7: Run tests and commit**

Run:

```bash
go test ./internal/report -count=1
```

Expected: PASS.

Commit:

```bash
git add internal/report/html.go internal/report/html_template.go internal/report/html_drilldown_test.go internal/report/text.go internal/report/text_test.go
git commit -m "report: apply consistent top-n presentation limits"
```

## Task 8: Add Text Fast Path Benchmarks

**Files:**
- Modify: `cmd/binlogviz/benchmark_test.go`
- Modify: `internal/report/text.go`
- Modify: `internal/report/html.go`

- [ ] **Step 1: Run impact analysis before optimizing render paths**

Run:

```text
gitnexus_impact({target: "RenderTextWithOptions", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "RenderHTMLWithOptions", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: render path blast radius. Warn if HIGH or CRITICAL.

- [ ] **Step 2: Add text-vs-HTML benchmark**

Add to `cmd/binlogviz/benchmark_test.go`:

```go
func BenchmarkAnalyzeRenderTextVsHTML(b *testing.B) {
	base := time.Date(2026, 4, 17, 10, 0, 0, 0, time.UTC)
	events := makeSyntheticTransactionEvents(base, 5000, 120)
	result := benchmarkAnalysisResult(b, events)

	b.Run("text", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := report.RenderText(result); err != nil {
				b.Fatalf("RenderText: %v", err)
			}
		}
	})

	b.Run("html", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := report.RenderHTML(result); err != nil {
				b.Fatalf("RenderHTML: %v", err)
			}
		}
	})
}

func benchmarkAnalysisResult(b *testing.B, raw []binlog.RawEvent) model.AnalysisResult {
	b.Helper()
	a := analyzer.New(analyzer.DefaultOptions())
	for _, ev := range raw {
		normalized, err := binlog.NormalizeRawEvent(ev)
		if err != nil {
			b.Fatalf("NormalizeRawEvent: %v", err)
		}
		if normalized == nil {
			continue
		}
		if err := a.Consume(*normalized); err != nil {
			b.Fatalf("Consume: %v", err)
		}
	}
	result, err := a.Finalize()
	if err != nil {
		b.Fatalf("Finalize: %v", err)
	}
	return *result
}
```

Add imports for `internal/report` and `internal/model`.

- [ ] **Step 3: Run benchmark and record baseline**

Run:

```bash
go test ./cmd/binlogviz -run '^$' -bench BenchmarkAnalyzeRenderTextVsHTML -benchmem -count=1
```

Expected: benchmark runs. Record `text` and `html` allocations in the task notes.

- [ ] **Step 4: Prevent text path from touching HTML-only data**

Review `RenderTextWithOptions`. It must not call:

- `buildHTMLData`
- `ReadEmbeddedECharts`
- JSON conversion helpers for HTML chart series
- pattern drilldown rendering unless `opts.ShowPatterns` is true
- minute detail rendering unless `opts.ShowMinutes` is true

If any of those calls exist, remove them from default text path.

- [ ] **Step 5: Add benchmark guard test for detail visibility**

Add to `internal/report/text_test.go`:

```go
func TestRenderTextDefaultDoesNotRenderPatternDrilldowns(t *testing.T) {
	result := productTextFixture()
	result.PatternDrilldowns = []model.PatternDrilldown{{
		PatternKey: "shop.orders|UPDATE|medium",
		Label: "shop.orders / UPDATE / medium batch",
		WhySelected: "high row share",
	}}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out, "high row share") {
		t.Fatalf("default text path rendered pattern drilldown\n%s", out)
	}
}
```

- [ ] **Step 6: Run benchmark and tests**

Run:

```bash
go test ./internal/report -run TestRenderTextDefaultDoesNotRenderPatternDrilldowns -count=1
go test ./cmd/binlogviz -run '^$' -bench BenchmarkAnalyzeRenderTextVsHTML -benchmem -count=1
```

Expected: PASS. Text render benchmark should be materially cheaper than HTML render benchmark.

- [ ] **Step 7: Commit**

```bash
git add cmd/binlogviz/benchmark_test.go internal/report/text.go internal/report/html.go internal/report/text_test.go
git commit -m "perf: keep analyze text rendering on fast path"
```

## Task 9: Add 1 GB Performance Gate Documentation And Benchmark Shape

**Files:**
- Modify: `cmd/binlogviz/benchmark_test.go`
- Modify: `README.md`
- Modify: `README_ZH.md`

- [ ] **Step 1: Add benchmark shape for near-real large input**

Add to `cmd/binlogviz/benchmark_test.go`:

```go
func BenchmarkAnalyzeNearOneGBSyntheticMix(b *testing.B) {
	base := time.Date(2026, 4, 17, 11, 0, 0, 0, time.UTC)
	events := makeSyntheticTransactionEvents(base, 20000, 120)
	events = append(events, makeDDLHeavyEvents(base.Add(24*time.Hour), 2000)...)
	opts := analyzer.DefaultOptions()
	opts.DetectSpikes = true
	benchmarkStreamingPipeline(b, []string{"near-1gb-synthetic-mix"}, &mockParser{events: events}, opts)
}
```

This is not a claim that the fixture is exactly 1 GB. It is a public CI-safe stress shape that protects relative regressions.

- [ ] **Step 2: Run benchmark once**

Run:

```bash
go test ./cmd/binlogviz -run '^$' -bench 'BenchmarkStreamingSyntheticLargeInputMix|BenchmarkAnalyzeNearOneGBSyntheticMix' -benchmem -count=1
```

Expected: both benchmarks complete. Record numbers in implementation notes.

- [ ] **Step 3: Document real 1 GB manual gate**

Add to `README.md`:

```markdown
### Analyze performance gate

For incident triage, the target for a 1 GB single-binlog `analyze` run is 10 seconds on the target DBA environment. Runs above 15 seconds should be treated as performance failures and profiled with `pprof`.

Recommended manual check:

```bash
time binlogviz analyze /path/to/mysql-bin.000044 --format text > /tmp/binlogviz-text.txt
time binlogviz analyze /path/to/mysql-bin.000044 --format html > /tmp/binlogviz.html
```

Text output is intended to stay on a fast diagnostic path. HTML output builds the full visual evidence report.
```

Add Chinese equivalent to `README_ZH.md`.

- [ ] **Step 4: Run docs-sensitive checks**

Run:

```bash
go test ./cmd/binlogviz -run '^$' -bench BenchmarkAnalyzeNearOneGBSyntheticMix -benchmem -count=1
```

Expected: benchmark completes.

- [ ] **Step 5: Commit**

```bash
git add cmd/binlogviz/benchmark_test.go README.md README_ZH.md
git commit -m "perf: document analyze one gigabyte performance gate"
```

## Task 10: Final Product Regression Sweep

**Files:**
- Modify only if tests reveal a blocker.

- [ ] **Step 1: Run full tests**

Run:

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 2: Run race tests**

Run:

```bash
go test -race ./...
```

Expected: PASS.

- [ ] **Step 3: Run documentation check**

Run:

```bash
/Users/fan/.codex/skills/check-three-level-doc/scripts/check_three_level_doc.sh
```

Expected: PASS or only non-blocking L1 reminder. If it reports missing L3 headers or module README drift for files touched in this plan, fix those docs before continuing.

- [ ] **Step 4: Run focused benchmarks**

Run:

```bash
go test ./cmd/binlogviz -run '^$' -bench 'BenchmarkStreamingSyntheticLargeInputMix|BenchmarkAnalyzeRenderTextVsHTML|BenchmarkAnalyzeNearOneGBSyntheticMix' -benchmem -count=1
go test ./internal/analyzer -run '^$' -bench 'BenchmarkMinuteAggregatorDrainBefore|BenchmarkTransactionBuilderConsumeRows|BenchmarkDuckDBStoreRecordTransactions' -benchmem -count=1
```

Expected: benchmarks complete. Record numbers in final implementation notes.

- [ ] **Step 5: Run GitNexus detect changes before final commit or handoff**

Run:

```text
gitnexus_detect_changes({repo: "BinlogVisualizer", scope: "all"})
```

Expected: changed scope matches report/cmd/analyzer/test/docs work. If risk is HIGH or CRITICAL, summarize why and verify it is expected for this product-level redesign.

- [ ] **Step 6: Manual smoke commands for user environment**

Do not claim this passed unless it runs on real data. Provide these commands for manual validation:

```bash
time binlogviz analyze /data/mysql_4010/data/mysql-bin.000044 --format text > /tmp/mysql-bin.000044.txt
time binlogviz analyze /data/mysql_4010/data/mysql-bin.000044 --format html --lang zh-CN > /tmp/mysql-bin.000044.zh.html

time binlogviz analyze --from-dir /data/mysql_4010/data/ --prefix mysql-bin. \
  --start "2026-04-05T09:00:00Z" \
  --end "2026-04-05T10:00:00Z" \
  --format text > /tmp/incident_current.txt

time binlogviz analyze --from-dir /data/mysql_4010/data/ --prefix mysql-bin. \
  --start "2026-04-05T09:00:00Z" \
  --end "2026-04-05T10:00:00Z" \
  --format html \
  --lang zh-CN > /tmp/incident_current.zh.html
```

Manual acceptance checks:

- 1 GB text analyze target: `<= 10s`.
- 1 GB text analyze failure threshold: `> 15s`.
- Text output fits roughly one to two terminal screens.
- Text output includes peak avg TPS/min and aligned top table rows.
- HTML Timeline starts with TPS and charts are readable.
- HTML labels largest/longest/widest with ranking metric.
- Analyzed Files explains selected/skipped files and reasons.
- Pattern help icons show real explanations or are absent.
- Chinese HTML has no unintended English in redesigned sections.

## Self-Review Checklist

- Spec coverage:
  - Terminal report redesign: Tasks 1, 3, 4, 7, 8.
  - HTML reading path and chart readability: Task 5.
  - Metric definitions: Tasks 1, 2, 3, 5.
  - Corpus redesign: Task 6.
  - Performance gate: Tasks 8, 9, 10.
  - i18n: Tasks 3, 4, 5.

- Red-flag scan:
  - This plan intentionally avoids vague instructions such as deferred implementation markers.
  - Any implementation worker must replace old tests rather than leaving contradictory old section-order assertions.

- Type consistency:
  - `Options.TopN`, `Options.Details`, `Options.ShowMinutes`, and `Options.ShowPatterns` are introduced in Task 1 and consumed later.
  - `DefaultTopN` is introduced in Task 1 and used by CLI/report tasks.
  - `TPSSeries` remains the same field name, but its value semantics change in Task 2.
