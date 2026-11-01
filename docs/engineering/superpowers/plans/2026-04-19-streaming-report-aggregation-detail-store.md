# Streaming Report Aggregation And Optional Detail Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make default `binlogviz analyze` assemble JSON/HTML/Text reports from bounded streaming report state, then make DuckDB an optional detail store instead of a required report-generation dependency.

**Architecture:** Add a report aggregation layer inside `internal/analyzer` that consumes completed transactions, minute buckets, DDL events, and filtered normalized events during streaming. Refactor `Analyzer.assembleResult()` to build `model.AnalysisResult` from that bounded state, then split the current store role into an optional detail-store path with `none` and `duckdb` modes.

**Tech Stack:** Go, existing `internal/analyzer`, `internal/model`, `internal/report`, Cobra CLI, DuckDB via `go-duckdb`, Go benchmarks, GitNexus impact analysis, three-level documentation checks.

---

## Ground Rules

- Run GitNexus impact before editing every existing function, method, or type.
- If impact is HIGH or CRITICAL, stop and report the blast radius before editing.
- Use TDD for every behavior change.
- Keep commits small and reviewable.
- Do not replace or fork `go-mysql` in this plan.
- Do not remove DuckDB support.
- Do not remove public JSON fields.
- Preserve current report behavior unless a task explicitly states the approved difference.

## File Structure

### New Files

- `internal/analyzer/report_aggregator.go`
  - Owns streaming report aggregation and `ReportSnapshot`.

- `internal/analyzer/report_aggregator_test.go`
  - Tests report aggregation parity for summary, warnings, top transactions, patterns, timeseries, alerts, findings, and drilldowns.

- `internal/analyzer/detail_store.go`
  - Defines the optional detail-store interface and no-op implementation after the store split.

- `internal/analyzer/detail_store_test.go`
  - Tests no-op detail store and DuckDB detail-store compatibility.

### Modified Files

- `internal/analyzer/analyzer.go`
  - Adds `reportAgg`.
  - Feeds report aggregation during streaming.
  - Refactors `assembleResult()` away from `QueryAllTransactions()`.

- `internal/analyzer/store.go`
  - Splits required report behavior from optional detail behavior.
  - Keeps DuckDB implementation.

- `internal/analyzer/options.go`
  - Adds detail-store mode configuration.

- `cmd/binlogviz/analyze.go`
  - Adds `--detail-store none|duckdb`.
  - Maps CLI option into `analyzer.Options`.

- `internal/analyzer/README.md`
  - Documents streaming report aggregation and optional detail store.

- `cmd/binlogviz/README.md`
  - Documents `--detail-store`.

- `internal/i18n/locales/en.json`
  - Adds flag help text.

- `internal/i18n/locales/zh-CN.json`
  - Adds matching Chinese flag help text.

- `cmd/binlogviz/integration_test.go`
  - Adds CLI mode coverage for `--detail-store`.

- `cmd/binlogviz/benchmark_test.go`
  - Adds real-binlog benchmark variants for detail-store mode.

## Task 1: Add Report Aggregator Parity Tests

**Files:**
- Create: `internal/analyzer/report_aggregator_test.go`
- Modify: none in production code yet

- [ ] **Step 1: Run GitNexus context checks**

Run:

```text
gitnexus_context({name: "Analyzer", repo: "BinlogVisualizer"})
gitnexus_context({name: "BuildPatterns", repo: "BinlogVisualizer"})
gitnexus_context({name: "BuildPatternDrilldowns", repo: "BinlogVisualizer"})
```

Expected: record current callers and confirm the test target is analyzer internals.

- [ ] **Step 2: Write the failing parity tests**

Create `internal/analyzer/report_aggregator_test.go`:

```go
// Package analyzer verifies streaming report aggregation parity with finalize-time builders.
// input: synthetic normalized events, completed transactions, and minute buckets.
// output: assertions that ReportAggregator snapshots match existing analyzer builder semantics.
// pos: regression coverage for replacing QueryAllTransactions-based report finalization.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestReportAggregatorMatchesExistingTransactionDerivedOutputs(t *testing.T) {
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	txns := []model.Transaction{
		{
			TxnKey:       "txn-1",
			StartTime:    base,
			EndTime:      base.Add(2 * time.Second),
			Duration:     2 * time.Second,
			TotalRows:    10,
			EventCount:   3,
			BinlogBytes:  1000,
			Tables:       map[string]int{"shop.orders": 10},
			Operations:   map[string]int{"INSERT": 10},
			QuerySummary: "insert orders",
		},
		{
			TxnKey:       "txn-2",
			StartTime:    base.Add(time.Minute),
			EndTime:      base.Add(time.Minute + 45*time.Second),
			Duration:     45 * time.Second,
			TotalRows:    2000,
			EventCount:   5,
			BinlogBytes:  9000,
			Tables:       map[string]int{"shop.orders": 1500, "shop.items": 500},
			Operations:   map[string]int{"UPDATE": 2000},
			QuerySummary: "update orders",
			QueryContext: &model.QueryContext{Summary: "update orders", Truncated: true},
		},
	}
	minutes := []model.MinuteBucket{
		{Minute: base, TotalRows: 10, TxnCount: 1, EventCount: 3, BinlogBytes: 1000},
		{Minute: base.Add(time.Minute), TotalRows: 2000, TxnCount: 1, EventCount: 5, BinlogBytes: 9000},
	}

	agg := NewReportAggregator(DefaultOptions())
	for _, txn := range txns {
		agg.ConsumeTransaction(txn)
	}
	for _, bucket := range minutes {
		agg.ConsumeMinuteBucket(bucket)
	}
	snapshot := agg.Snapshot()

	expectedPatterns := BuildPatterns(txns)
	expectedLargest, expectedLongest := SelectDiagnosticTransactions(txns, 5)

	if snapshot.Summary.TotalTransactions != len(txns) {
		t.Fatalf("total transactions = %d, want %d", snapshot.Summary.TotalTransactions, len(txns))
	}
	if snapshot.Summary.TotalRows != 2010 {
		t.Fatalf("total rows = %d, want 2010", snapshot.Summary.TotalRows)
	}
	if snapshot.Warnings != 1 {
		t.Fatalf("warnings = %d, want 1", snapshot.Warnings)
	}
	if len(snapshot.Patterns) != len(expectedPatterns) {
		t.Fatalf("patterns = %d, want %d", len(snapshot.Patterns), len(expectedPatterns))
	}
	if snapshot.Patterns[0].PatternKey != expectedPatterns[0].PatternKey {
		t.Fatalf("top pattern = %q, want %q", snapshot.Patterns[0].PatternKey, expectedPatterns[0].PatternKey)
	}
	if snapshot.Diagnostics.LargestTransactions[0].TxnKey != expectedLargest[0].TxnKey {
		t.Fatalf("largest txn = %q, want %q", snapshot.Diagnostics.LargestTransactions[0].TxnKey, expectedLargest[0].TxnKey)
	}
	if snapshot.Diagnostics.LongestTransactions[0].TxnKey != expectedLongest[0].TxnKey {
		t.Fatalf("longest txn = %q, want %q", snapshot.Diagnostics.LongestTransactions[0].TxnKey, expectedLongest[0].TxnKey)
	}
	if len(snapshot.Timeseries.TxnSizeSeriesSummary.Buckets) == 0 {
		t.Fatal("expected transaction size histogram buckets")
	}
}

func TestReportAggregatorDoesNotRetainAllTransactions(t *testing.T) {
	agg := NewReportAggregator(DefaultOptions())
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 1000; i++ {
		agg.ConsumeTransaction(model.Transaction{
			TxnKey:     generateTxnKey(uint64(i + 1)),
			StartTime:  base,
			EndTime:    base,
			TotalRows:  i + 1,
			EventCount: 1,
			Tables:     map[string]int{"shop.orders": i + 1},
			Operations: map[string]int{"UPDATE": i + 1},
		})
	}

	snapshot := agg.Snapshot()
	if len(snapshot.Transactions) > DefaultOptions().TopTransactions {
		t.Fatalf("snapshot retained %d top transactions, want <= %d", len(snapshot.Transactions), DefaultOptions().TopTransactions)
	}
	if len(snapshot.Diagnostics.LargestTransactions) > 5 {
		t.Fatalf("largest transactions retained %d, want <= 5", len(snapshot.Diagnostics.LargestTransactions))
	}
}
```

- [ ] **Step 3: Run tests and verify they fail**

Run:

```bash
go test ./internal/analyzer -run 'TestReportAggregator' -count=1
```

Expected: FAIL because `ReportAggregator`, `NewReportAggregator`, and `ReportSnapshot` do not exist.

- [ ] **Step 4: Keep the red tests unstaged**

Run:

```bash
git status --short
```

Expected: `internal/analyzer/report_aggregator_test.go` is modified or untracked. Do not commit failing tests on `main`; the next task makes them pass and commits the complete slice.

## Task 2: Implement Streaming ReportAggregator

**Files:**
- Create: `internal/analyzer/report_aggregator.go`
- Modify: `internal/analyzer/report_aggregator_test.go`
- Modify: `internal/analyzer/README.md`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "BuildPatterns", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "SelectDiagnosticTransactions", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "SelectWidestTransactions", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: no production behavior is edited in these symbols during this task. If later edits become necessary, report the risk first.

- [ ] **Step 2: Add minimal production implementation**

Create `internal/analyzer/report_aggregator.go`:

```go
// Package analyzer incrementally builds report-ready projections without retaining all transactions.
// input: completed transactions, minute buckets, DDL events, normalized events, and file coverage.
// output: bounded ReportSnapshot values used to assemble model.AnalysisResult.
// pos: streaming report aggregation layer that replaces QueryAllTransactions-dependent finalization.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"sort"
	"time"

	"binlogviz/internal/model"
)

type ReportSnapshot struct {
	Summary           model.WorkloadSummary
	Tables            []model.TableStats
	Transactions      []model.Transaction
	Patterns          []model.PatternStats
	Minutes           []model.MinuteBucket
	Timeseries        model.Timeseries
	Diagnostics       model.Diagnostics
	Alerts            []model.Alert
	Warnings          int
	PatternDrilldowns []model.PatternDrilldown
}

type ReportAggregator struct {
	opts Options

	totalTransactions int
	totalRows         int
	totalEvents       int
	startTime         time.Time
	endTime           time.Time
	warnings          int

	topTransactions []model.Transaction
	largest         []model.Transaction
	longest         []model.Transaction
	widest          []model.Transaction
	minutes         []model.MinuteBucket
	alerts          []model.Alert
	ddlEvents       []model.DDLEvent
	fileCoverage    model.FileCoverage
	patterns        map[string]*model.PatternStats
	patternOrder    []string
	txnSizeBuckets  model.TxnSizeSeriesSummary
}

func NewReportAggregator(opts Options) *ReportAggregator {
	return &ReportAggregator{
		opts:     opts,
		patterns: make(map[string]*model.PatternStats),
	}
}

func (a *ReportAggregator) ConsumeEvent(ev model.NormalizedEvent) {
	if a == nil {
		return
	}
	a.totalEvents++
	if a.startTime.IsZero() || ev.Timestamp.Before(a.startTime) {
		a.startTime = ev.Timestamp
	}
	if a.endTime.IsZero() || ev.Timestamp.After(a.endTime) {
		a.endTime = ev.Timestamp
	}
}

func (a *ReportAggregator) ConsumeTransaction(txn model.Transaction) {
	if a == nil {
		return
	}
	a.totalTransactions++
	a.totalRows += txn.TotalRows
	a.topTransactions = insertTopTransaction(a.topTransactions, txn, a.opts.TopTransactions, transactionRowsBetter)
	a.largest = insertTopTransaction(a.largest, txn, 5, transactionRowsBetter)
	a.longest = insertTopTransaction(a.longest, txn, 5, transactionDurationBetter)
	a.widest = insertTopTransaction(a.widest, txn, 5, transactionWidthBetter)
	if txn.QueryContext != nil && txn.QueryContext.Truncated {
		a.warnings++
	}
	a.consumePattern(txn)
	a.consumeTxnSize(txn)
	a.alerts = append(a.alerts, DetectLargeTransactionAlerts([]model.Transaction{txn}, a.opts)...)
}

func (a *ReportAggregator) ConsumeMinuteBucket(bucket model.MinuteBucket) {
	if a == nil {
		return
	}
	a.minutes = append(a.minutes, bucket)
}

func (a *ReportAggregator) ConsumeDDLEvents(events []model.DDLEvent) {
	if a == nil || len(events) == 0 {
		return
	}
	a.ddlEvents = append(a.ddlEvents, events...)
}

func (a *ReportAggregator) SetFileCoverage(coverage model.FileCoverage) {
	if a == nil {
		return
	}
	a.fileCoverage = coverage
}

func (a *ReportAggregator) Snapshot() ReportSnapshot {
	if a == nil {
		return ReportSnapshot{}
	}
	minutes := append([]model.MinuteBucket(nil), a.minutes...)
	sort.Slice(minutes, func(i, j int) bool { return minutes[i].Minute.Before(minutes[j].Minute) })
	patterns := a.snapshotPatterns()
	alerts := append([]model.Alert(nil), a.alerts...)
	alerts = append(alerts, DetectSpikeAlerts(minutes, a.opts)...)

	summary := model.WorkloadSummary{
		TotalTransactions: a.totalTransactions,
		TotalRows:         a.totalRows,
		TotalEvents:       a.totalEvents,
		StartTime:         a.startTime,
		EndTime:           a.endTime,
	}
	if !summary.StartTime.IsZero() && !summary.EndTime.IsZero() {
		summary.Duration = summary.EndTime.Sub(summary.StartTime)
	}

	diagnostics := model.Diagnostics{
		FileCoverage:        a.fileCoverage,
		DDLEvents:           append([]model.DDLEvent(nil), a.ddlEvents...),
		LargestTransactions: append([]model.Transaction(nil), a.largest...),
		LongestTransactions: append([]model.Transaction(nil), a.longest...),
		WidestTransactions:  append([]model.Transaction(nil), a.widest...),
		FileSegments:        BuildFileSegments(minutes, 5),
		HotIntervals:        SelectHotIntervals(minutes, 5),
		Findings:            BuildFindingsFromAlerts(alerts, minutes, append([]model.Transaction(nil), a.largest...), a.ddlEvents),
	}

	return ReportSnapshot{
		Summary:           summary,
		Transactions:      append([]model.Transaction(nil), a.topTransactions...),
		Patterns:          patterns,
		Minutes:           minutes,
		Timeseries:        buildStreamingTimeseries(minutes, a.txnSizeBuckets),
		Diagnostics:       diagnostics,
		Alerts:            alerts,
		Warnings:          a.warnings,
		PatternDrilldowns: BuildPatternDrilldowns(patterns, minutes, append([]model.Transaction(nil), a.largest...), alerts),
	}
}
```

- [ ] **Step 3: Add helper functions**

In the same file, add helpers equivalent to existing ordering and aggregation semantics:

```go
func insertTopTransaction(current []model.Transaction, txn model.Transaction, limit int, better func(left, right model.Transaction) bool) []model.Transaction {
	if limit <= 0 {
		return nil
	}
	insertAt := len(current)
	for index := range current {
		if better(txn, current[index]) {
			insertAt = index
			break
		}
	}
	if insertAt == len(current) {
		if len(current) < limit {
			return append(current, txn)
		}
		return current
	}
	if len(current) < limit {
		current = append(current, model.Transaction{})
	}
	copy(current[insertAt+1:], current[insertAt:])
	current[insertAt] = txn
	if len(current) > limit {
		current = current[:limit]
	}
	return current
}

func transactionRowsBetter(left, right model.Transaction) bool {
	if left.TotalRows != right.TotalRows {
		return left.TotalRows > right.TotalRows
	}
	if left.BinlogBytes != right.BinlogBytes {
		return left.BinlogBytes > right.BinlogBytes
	}
	return left.TxnKey < right.TxnKey
}

func transactionDurationBetter(left, right model.Transaction) bool {
	if left.Duration != right.Duration {
		return left.Duration > right.Duration
	}
	if left.TotalRows != right.TotalRows {
		return left.TotalRows > right.TotalRows
	}
	return left.TxnKey < right.TxnKey
}

func transactionWidthBetter(left, right model.Transaction) bool {
	if len(left.Tables) != len(right.Tables) {
		return len(left.Tables) > len(right.Tables)
	}
	if left.TotalRows != right.TotalRows {
		return left.TotalRows > right.TotalRows
	}
	return left.TxnKey < right.TxnKey
}

func (a *ReportAggregator) consumePattern(txn model.Transaction) {
	key, label := patternIdentity(txn)
	p := a.patterns[key]
	if p == nil {
		p = &model.PatternStats{
			PatternKey: key,
			Label:      label,
			Tables:     make(map[string]int),
			Operations: make(map[string]int),
		}
		a.patterns[key] = p
		a.patternOrder = append(a.patternOrder, key)
	}
	p.TotalRows += txn.TotalRows
	p.TxnCount++
	p.EventCount += txn.EventCount
	for k, v := range txn.Tables {
		p.Tables[k] += v
	}
	for k, v := range txn.Operations {
		p.Operations[k] += v
	}
	if p.SampleQuerySummary == "" && txn.QuerySummary != "" {
		p.SampleQuerySummary = txn.QuerySummary
	}
}

func (a *ReportAggregator) snapshotPatterns() []model.PatternStats {
	if len(a.patterns) == 0 {
		return []model.PatternStats{}
	}
	out := make([]model.PatternStats, 0, len(a.patterns))
	for _, key := range a.patternOrder {
		p := a.patterns[key]
		if p == nil || p.TxnCount == 0 {
			continue
		}
		cp := *p
		cp.Tables = cloneStringIntMap(p.Tables)
		cp.Operations = cloneStringIntMap(p.Operations)
		cp.AvgRowsPerTxn = float64(cp.TotalRows) / float64(cp.TxnCount)
		if a.totalTransactions > 0 {
			cp.ShareOfTransactions = float64(cp.TxnCount) / float64(a.totalTransactions)
		}
		if a.totalRows > 0 {
			cp.ShareOfRows = float64(cp.TotalRows) / float64(a.totalRows)
		}
		out = append(out, cp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TotalRows != out[j].TotalRows {
			return out[i].TotalRows > out[j].TotalRows
		}
		if out[i].TxnCount != out[j].TxnCount {
			return out[i].TxnCount > out[j].TxnCount
		}
		return out[i].PatternKey < out[j].PatternKey
	})
	return out
}

func cloneStringIntMap(src map[string]int) map[string]int {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (a *ReportAggregator) consumeTxnSize(txn model.Transaction) {
	a.txnSizeBuckets = addTxnSizeBucket(a.txnSizeBuckets, txn)
}

func addTxnSizeBucket(summary model.TxnSizeSeriesSummary, txn model.Transaction) model.TxnSizeSeriesSummary {
	label := txnSizeBucketLabel(txn.TotalRows)
	for i := range summary.Buckets {
		if summary.Buckets[i].Label == label {
			summary.Buckets[i].TxnCount++
			summary.Buckets[i].Rows += txn.TotalRows
			summary.Buckets[i].BinlogBytes += txn.BinlogBytes
			return summary
		}
	}
	summary.Buckets = append(summary.Buckets, model.TxnSizeBucket{
		Label:       label,
		TxnCount:    1,
		Rows:        txn.TotalRows,
		BinlogBytes: txn.BinlogBytes,
	})
	return summary
}

func txnSizeBucketLabel(rows int) string {
	switch {
	case rows >= 1000:
		return "1000+"
	case rows >= 100:
		return "100-999"
	case rows >= 10:
		return "10-99"
	default:
		return "1-9"
	}
}

func buildStreamingTimeseries(minutes []model.MinuteBucket, txnSize model.TxnSizeSeriesSummary) model.Timeseries {
	series := BuildTimeseries(TimeseriesBuildInput{Minutes: minutes})
	series.TxnSizeSeriesSummary = txnSize
	return series
}
```

- [ ] **Step 4: Run tests**

Run:

```bash
go test ./internal/analyzer -run 'TestReportAggregator' -count=1
```

Expected: PASS.

- [ ] **Step 5: Update module README**

Modify `internal/analyzer/README.md` to add:

```markdown
- `report_aggregator.go` maintains bounded streaming state for report assembly so default analyze output does not require full transaction rehydration.
```

- [ ] **Step 6: Run focused analyzer tests**

Run:

```bash
go test ./internal/analyzer -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add internal/analyzer/report_aggregator.go internal/analyzer/report_aggregator_test.go internal/analyzer/README.md
git commit -m "analyzer: add streaming report aggregator"
```

Expected: commit succeeds.

## Task 3: Feed ReportAggregator During Streaming

**Files:**
- Modify: `internal/analyzer/analyzer.go`
- Modify: `internal/analyzer/analyzer_test.go`
- Modify: `internal/analyzer/report_aggregator_test.go`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "Analyzer", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "Consume", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "persistCompletedTransactions", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "persistMinuteBuckets", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: `Analyzer` or `Consume` may be HIGH because command and tests depend on it. Report the blast radius before editing.

- [ ] **Step 2: Write failing integration test**

Add to `internal/analyzer/analyzer_test.go`:

```go
func TestAnalyzerReportAggregatorReceivesDrainedState(t *testing.T) {
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "QUERY_EVENT", Query: "BEGIN"},
		{Timestamp: base, EventType: "WRITE_ROWS_EVENT", Operation: "INSERT", Schema: "shop", Table: "orders", RowCount: 3, BinlogBytes: 300},
		{Timestamp: base.Add(time.Second), EventType: "XID_EVENT", RowCount: 0},
	}

	result, err := New(DefaultOptions()).Analyze(events)
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}
	if result.Summary.TotalTransactions != 1 {
		t.Fatalf("transactions = %d, want 1", result.Summary.TotalTransactions)
	}
	if result.Summary.TotalRows != 3 {
		t.Fatalf("rows = %d, want 3", result.Summary.TotalRows)
	}
	if len(result.Transactions) != 1 {
		t.Fatalf("top transactions = %d, want 1", len(result.Transactions))
	}
}
```

- [ ] **Step 3: Run test**

Run:

```bash
go test ./internal/analyzer -run TestAnalyzerReportAggregatorReceivesDrainedState -count=1
```

Expected: PASS may already occur via existing DuckDB/in-memory finalize. If it passes, keep it as a guard and continue with implementation.

- [ ] **Step 4: Add `reportAgg` to Analyzer**

Modify `internal/analyzer/analyzer.go`:

```go
type Analyzer struct {
	opts   Options
	store  analysisStore
	filter *EventFilter

	txnBuilder    *TransactionBuilder
	tableAgg      *TableAggregator
	minuteAgg     *MinuteAggregator
	ddlAgg        *DDLAggregator
	timeseriesAgg *TimeseriesAggregator
	reportAgg     *ReportAggregator
}
```

Update `reset()`:

```go
a.reportAgg = NewReportAggregator(a.opts)
```

- [ ] **Step 5: Feed events**

In `consume`, after time bounds and filter acceptance:

```go
if a.filter.Allow(ev.Schema, ev.Table) {
	a.reportAgg.ConsumeEvent(ev)
	a.tableAgg.Consume(ev)
	a.minuteAgg.Consume(ev)
	a.ddlAgg.ConsumeEvent(ev)
	a.timeseriesAgg.Consume(ev)
}
```

- [ ] **Step 6: Feed completed transactions before persistence**

Change `persistCompletedTransactions()` so it drains once and fans out to report and store:

```go
func (a *Analyzer) persistCompletedTransactions() error {
	drained := a.txnBuilder.DrainCompleted()
	if len(drained) == 0 {
		return nil
	}
	for _, txn := range drained {
		a.reportAgg.ConsumeTransaction(txn)
	}
	return a.store.RecordTransactions(toPersistedTransactions(drained))
}
```

- [ ] **Step 7: Feed minute buckets before persistence**

Change `persistMinuteBuckets()`:

```go
func (a *Analyzer) persistMinuteBuckets(buckets []model.MinuteBucket) error {
	if len(buckets) == 0 {
		return nil
	}
	for _, bucket := range buckets {
		a.reportAgg.ConsumeMinuteBucket(bucket)
	}
	return a.store.RecordMinuteBuckets(buckets)
}
```

- [ ] **Step 8: Run tests**

Run:

```bash
go test ./internal/analyzer -run 'TestAnalyzerReportAggregatorReceivesDrainedState|TestReportAggregator' -count=1
```

Expected: PASS.

- [ ] **Step 9: Commit**

Run:

```bash
git add internal/analyzer/analyzer.go internal/analyzer/analyzer_test.go internal/analyzer/report_aggregator_test.go
git commit -m "analyzer: feed report aggregation during streaming"
```

Expected: commit succeeds.

## Task 4: Assemble Default Result Without QueryAllTransactions

**Files:**
- Modify: `internal/analyzer/analyzer.go`
- Modify: `internal/analyzer/analyzer_test.go`
- Modify: `internal/analyzer/store_test.go`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "assembleResult", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "QueryAllTransactions", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "QueryTopTransactions", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: `assembleResult` is central. If HIGH/CRITICAL, report before editing.

- [ ] **Step 2: Write failing test that QueryAll is not required**

Add a test store wrapper in `internal/analyzer/analyzer_test.go`:

```go
type queryAllFailStore struct {
	analysisStore
}

func (s queryAllFailStore) QueryAllTransactions() ([]model.Transaction, error) {
	return nil, errors.New("QueryAllTransactions should not be called")
}

func TestAnalyzerFinalizeDoesNotRequireQueryAllTransactions(t *testing.T) {
	base := time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC)
	store := queryAllFailStore{analysisStore: newInMemoryStore()}
	a := &Analyzer{opts: DefaultOptions(), store: store}
	a.reset()

	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "QUERY_EVENT", Query: "BEGIN"},
		{Timestamp: base, EventType: "UPDATE_ROWS_EVENT", Operation: "UPDATE", Schema: "shop", Table: "orders", RowCount: 5, BinlogBytes: 500},
		{Timestamp: base.Add(time.Second), EventType: "XID_EVENT"},
	}
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatalf("Consume returned error: %v", err)
		}
	}
	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("Finalize returned error: %v", err)
	}
	if result.Summary.TotalTransactions != 1 {
		t.Fatalf("transactions = %d, want 1", result.Summary.TotalTransactions)
	}
}
```

Ensure `errors` is imported.

- [ ] **Step 3: Run test and verify failure**

Run:

```bash
go test ./internal/analyzer -run TestAnalyzerFinalizeDoesNotRequireQueryAllTransactions -count=1
```

Expected: FAIL with `QueryAllTransactions should not be called`.

- [ ] **Step 4: Refactor assembleResult**

Modify `assembleResult()` to use `reportAgg.Snapshot()`:

```go
func (a *Analyzer) assembleResult() (*model.AnalysisResult, error) {
	snapshot := a.reportAgg.Snapshot()
	topTransactions := snapshot.Transactions
	if err := a.attachTopTransactionSQL(topTransactions); err != nil {
		return nil, err
	}
	snapshot.Transactions = topTransactions
	snapshot.Tables = limitTables(a.tableAgg.Snapshot(), a.opts.TopTables)
	snapshot.Diagnostics.DDLEvents = a.ddlAgg.Snapshot()

	return &model.AnalysisResult{
		Summary:           snapshot.Summary,
		Timeseries:        snapshot.Timeseries,
		Tables:            snapshot.Tables,
		Transactions:      snapshot.Transactions,
		Patterns:          snapshot.Patterns,
		Minutes:           snapshot.Minutes,
		Diagnostics:       snapshot.Diagnostics,
		Alerts:            snapshot.Alerts,
		Warnings:          snapshot.Warnings,
		PatternDrilldowns: snapshot.PatternDrilldowns,
	}, nil
}
```

If DDL events are already fed into `ReportAggregator`, remove the direct `ddlAgg.Snapshot()` assignment and keep one source of truth.

- [ ] **Step 5: Run focused tests**

Run:

```bash
go test ./internal/analyzer -run 'TestAnalyzerFinalizeDoesNotRequireQueryAllTransactions|TestAnalyzerStreamingAndAnalyzeProduceSameResult|TestReportAggregator' -count=1
```

Expected: PASS.

- [ ] **Step 6: Run broader analyzer tests**

Run:

```bash
go test ./internal/analyzer -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add internal/analyzer/analyzer.go internal/analyzer/analyzer_test.go internal/analyzer/store_test.go
git commit -m "analyzer: assemble reports from streaming state"
```

Expected: commit succeeds.

## Task 5: Add Detail Store Mode To Options

**Files:**
- Create: `internal/analyzer/detail_store.go`
- Create: `internal/analyzer/detail_store_test.go`
- Modify: `internal/analyzer/options.go`
- Modify: `internal/analyzer/README.md`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "Options", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "DefaultOptions", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: likely MEDIUM/HIGH because command and tests construct options. Report if HIGH/CRITICAL.

- [ ] **Step 2: Write failing option tests**

Create `internal/analyzer/detail_store_test.go`:

```go
// Package analyzer verifies optional detail-store mode selection.
// input: analyzer Options values and no-op detail store calls.
// output: assertions for mode defaults and no-op store behavior.
// pos: regression coverage for making DuckDB optional in analyze.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import "testing"

func TestDefaultOptionsUsesNoDetailStore(t *testing.T) {
	opts := DefaultOptions()
	if opts.DetailStoreMode != DetailStoreNone {
		t.Fatalf("DetailStoreMode = %q, want %q", opts.DetailStoreMode, DetailStoreNone)
	}
}

func TestNoopDetailStoreAcceptsAnalyzerWrites(t *testing.T) {
	store := noopDetailStore{}
	if err := store.Reset(); err != nil {
		t.Fatalf("Reset returned error: %v", err)
	}
	if err := store.RecordTransactions(nil); err != nil {
		t.Fatalf("RecordTransactions returned error: %v", err)
	}
	if err := store.RecordMinuteBuckets(nil); err != nil {
		t.Fatalf("RecordMinuteBuckets returned error: %v", err)
	}
	if err := store.RecordAlerts(nil); err != nil {
		t.Fatalf("RecordAlerts returned error: %v", err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}
```

- [ ] **Step 3: Run tests and verify failure**

Run:

```bash
go test ./internal/analyzer -run 'TestDefaultOptionsUsesNoDetailStore|TestNoopDetailStoreAcceptsAnalyzerWrites' -count=1
```

Expected: FAIL because `DetailStoreMode`, `DetailStoreNone`, and `noopDetailStore` do not exist.

- [ ] **Step 4: Add detail-store types**

Create `internal/analyzer/detail_store.go`:

```go
// Package analyzer defines optional transaction detail persistence backends.
// input: completed transactions, minute buckets, alerts, and transaction keys needing SQL hydration.
// output: no-op or DuckDB-backed detail persistence for optional drilldown use.
// pos: storage boundary that keeps default report generation independent of DuckDB.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import "binlogviz/internal/model"

type DetailStoreMode string

const (
	DetailStoreNone   DetailStoreMode = "none"
	DetailStoreDuckDB DetailStoreMode = "duckdb"
)

type detailStore interface {
	Reset() error
	RecordTransactions([]persistedTransaction) error
	RecordMinuteBuckets([]model.MinuteBucket) error
	RecordAlerts([]model.Alert) error
	Flush() error
	ResolveTransactionQuerySQL([]string) (map[string]string, error)
	Close() error
}

type noopDetailStore struct{}

func (noopDetailStore) Reset() error { return nil }
func (noopDetailStore) RecordTransactions([]persistedTransaction) error { return nil }
func (noopDetailStore) RecordMinuteBuckets([]model.MinuteBucket) error { return nil }
func (noopDetailStore) RecordAlerts([]model.Alert) error { return nil }
func (noopDetailStore) Flush() error { return nil }
func (noopDetailStore) ResolveTransactionQuerySQL([]string) (map[string]string, error) {
	return map[string]string{}, nil
}
func (noopDetailStore) Close() error { return nil }
```

- [ ] **Step 5: Extend Options**

Modify `internal/analyzer/options.go`:

```go
type Options struct {
	DetailStoreMode DetailStoreMode
	// existing fields...
}
```

Update `DefaultOptions()`:

```go
DetailStoreMode: DetailStoreNone,
```

- [ ] **Step 6: Run tests**

Run:

```bash
go test ./internal/analyzer -run 'TestDefaultOptionsUsesNoDetailStore|TestNoopDetailStoreAcceptsAnalyzerWrites' -count=1
```

Expected: PASS.

- [ ] **Step 7: Update README and commit**

Modify `internal/analyzer/README.md`:

```markdown
- `detail_store.go` defines optional detail persistence. The default mode is `none`; DuckDB remains available for explicit detail storage.
```

Run:

```bash
git add internal/analyzer/detail_store.go internal/analyzer/detail_store_test.go internal/analyzer/options.go internal/analyzer/README.md
git commit -m "analyzer: define optional detail store modes"
```

Expected: commit succeeds.

## Task 6: Make Analyzer Use Noop Detail Store By Default

**Files:**
- Modify: `internal/analyzer/analyzer.go`
- Modify: `internal/analyzer/store.go`
- Modify: `internal/analyzer/analyzer_test.go`
- Modify: `cmd/binlogviz/analyze.go`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "New", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "NewWithStore", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "createDuckDBTempStore", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "runAnalysisWithReportAndSnapshotOptions", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: command-level flow may be HIGH. Report before editing if HIGH/CRITICAL.

- [ ] **Step 2: Write failing test that default analyzer does not need DuckDB**

Add to `internal/analyzer/analyzer_test.go`:

```go
func TestAnalyzerDefaultDetailStoreModeIsReportOnly(t *testing.T) {
	opts := DefaultOptions()
	if opts.DetailStoreMode != DetailStoreNone {
		t.Fatalf("DetailStoreMode = %q, want %q", opts.DetailStoreMode, DetailStoreNone)
	}
	a := New(opts)
	if _, ok := a.store.(noopDetailStore); !ok {
		t.Fatalf("default analyzer store = %T, want noopDetailStore", a.store)
	}
}
```

- [ ] **Step 3: Run test and verify failure**

Run:

```bash
go test ./internal/analyzer -run TestAnalyzerDefaultDetailStoreModeIsReportOnly -count=1
```

Expected: FAIL while `New` still uses `newInMemoryStore()` or another non-noop store.

- [ ] **Step 4: Change Analyzer store type to detail store**

Modify `Analyzer`:

```go
store detailStore
```

Ensure `DuckDBStore` satisfies `detailStore`.

- [ ] **Step 5: Update New**

Modify `New(opts Options)`:

```go
func New(opts Options) *Analyzer {
	a := &Analyzer{opts: opts}
	if opts.DetailStoreMode == DetailStoreDuckDB {
		a.store = newInMemoryStore()
	} else {
		a.store = noopDetailStore{}
	}
	a.reset()
	return a
}
```

If tests require in-memory behavior, keep `newInMemoryStore()` only in test-specific constructors or add `NewWithDetailStore`.

- [ ] **Step 6: Preserve NewWithStore**

Modify `NewWithStore`:

```go
func NewWithStore(opts Options, store *DuckDBStore) *Analyzer {
	opts.DetailStoreMode = DetailStoreDuckDB
	a := &Analyzer{opts: opts, store: store}
	a.reset()
	return a
}
```

- [ ] **Step 7: Run analyzer tests**

Run:

```bash
go test ./internal/analyzer -count=1
```

Expected: PASS after tests are updated to explicitly request DuckDB/in-memory behavior where needed.

- [ ] **Step 8: Commit**

Run:

```bash
git add internal/analyzer/analyzer.go internal/analyzer/store.go internal/analyzer/analyzer_test.go cmd/binlogviz/analyze.go
git commit -m "analyzer: use report-only detail store by default"
```

Expected: commit succeeds.

## Task 7: Add CLI `--detail-store`

**Files:**
- Modify: `cmd/binlogviz/analyze.go`
- Modify: `cmd/binlogviz/integration_test.go`
- Modify: `cmd/binlogviz/README.md`
- Modify: `internal/i18n/locales/en.json`
- Modify: `internal/i18n/locales/zh-CN.json`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "analyzeOptions", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "newAnalyzeCommand", direction: "upstream", repo: "BinlogVisualizer"})
gitnexus_impact({target: "buildAnalyzerOptions", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: command-level callers. Report HIGH/CRITICAL first if returned.

- [ ] **Step 2: Write failing CLI tests**

Add to `cmd/binlogviz/integration_test.go`:

```go
func TestAnalyzeAcceptsDetailStoreModes(t *testing.T) {
	cmd := newAnalyzeCommand()
	flags := cmd.Flags()
	if flags.Lookup("detail-store") == nil {
		t.Fatal("expected --detail-store flag")
	}
}

func TestBuildAnalyzerOptionsMapsDetailStoreMode(t *testing.T) {
	opts := &analyzeOptions{detailStore: "duckdb"}
	got := buildAnalyzerOptions(opts, nil, nil)
	if got.DetailStoreMode != analyzer.DetailStoreDuckDB {
		t.Fatalf("DetailStoreMode = %q, want %q", got.DetailStoreMode, analyzer.DetailStoreDuckDB)
	}
}
```

- [ ] **Step 3: Run test and verify failure**

Run:

```bash
go test ./cmd/binlogviz -run 'TestAnalyzeAcceptsDetailStoreModes|TestBuildAnalyzerOptionsMapsDetailStoreMode' -count=1
```

Expected: FAIL because `detailStore` option and flag do not exist.

- [ ] **Step 4: Add option and flag**

Modify `analyzeOptions`:

```go
detailStore string
```

Register flag in `newAnalyzeCommand()`:

```go
cmd.Flags().StringVar(&opts.detailStore, "detail-store", string(analyzer.DetailStoreNone), i18n.T("cmd.analyze.flag.detailStore"))
```

- [ ] **Step 5: Validate mode**

Add to `validateAnalyzeOptions`:

```go
switch analyzer.DetailStoreMode(opts.detailStore) {
case analyzer.DetailStoreNone, analyzer.DetailStoreDuckDB:
default:
	return fmt.Errorf("invalid --detail-store %q: expected none or duckdb", opts.detailStore)
}
```

- [ ] **Step 6: Map mode into analyzer options**

In `buildAnalyzerOptions`:

```go
analyzerOpts.DetailStoreMode = analyzer.DetailStoreMode(opts.detailStore)
```

- [ ] **Step 7: Add i18n keys**

Add to `internal/i18n/locales/en.json`:

```json
"cmd.analyze.flag.detailStore": "Detail store backend for optional transaction lookup: none or duckdb"
```

Add to `internal/i18n/locales/zh-CN.json`:

```json
"cmd.analyze.flag.detailStore": "可选事务明细存储后端：none 或 duckdb"
```

- [ ] **Step 8: Update CLI README**

Add to `cmd/binlogviz/README.md`:

```markdown
- `--detail-store none|duckdb`: controls optional transaction detail persistence. The default `none` generates reports from streaming aggregates without DuckDB; `duckdb` keeps the detail backend enabled for future lookup workflows.
```

- [ ] **Step 9: Run tests**

Run:

```bash
go test ./cmd/binlogviz -run 'TestAnalyzeAcceptsDetailStoreModes|TestBuildAnalyzerOptionsMapsDetailStoreMode' -count=1
go test ./cmd/binlogviz -count=1
```

Expected: PASS.

- [ ] **Step 10: Commit**

Run:

```bash
git add cmd/binlogviz/analyze.go cmd/binlogviz/integration_test.go cmd/binlogviz/README.md internal/i18n/locales/en.json internal/i18n/locales/zh-CN.json
git commit -m "cmd: expose optional analyze detail store"
```

Expected: commit succeeds.

## Task 8: Add DuckDB/None Equivalence Tests

**Files:**
- Modify: `cmd/binlogviz/analyze_product_test.go`
- Modify: `cmd/binlogviz/benchmark_test.go`

- [ ] **Step 1: Run impact analysis**

Run:

```text
gitnexus_impact({target: "runAnalysisWithReportAndSnapshotOptions", direction: "upstream", repo: "BinlogVisualizer"})
```

Expected: command-level flow. Report HIGH/CRITICAL first if returned.

- [ ] **Step 2: Add JSON equivalence test**

Add a corpus test that runs the same fixture twice:

```go
func TestAnalyzeDetailStoreNoneAndDuckDBProduceEquivalentReports(t *testing.T) {
	noneJSON := runAnalyzeCorpusJSON(t, "incident-mixed", "--detail-store", "none")
	duckDBJSON := runAnalyzeCorpusJSON(t, "incident-mixed", "--detail-store", "duckdb")

	assertJSONFieldEqual(t, noneJSON, duckDBJSON, "summary")
	assertJSONFieldEqual(t, noneJSON, duckDBJSON, "tables")
	assertJSONFieldEqual(t, noneJSON, duckDBJSON, "transactions")
	assertJSONFieldEqual(t, noneJSON, duckDBJSON, "patterns")
	assertJSONFieldEqual(t, noneJSON, duckDBJSON, "minutes")
	assertJSONFieldEqual(t, noneJSON, duckDBJSON, "diagnostics")
	assertJSONFieldEqual(t, noneJSON, duckDBJSON, "alerts")
	assertJSONFieldEqual(t, noneJSON, duckDBJSON, "warnings")
}
```

If helper names differ in the existing test file, implement local helpers in the test file. The assertion must compare decoded JSON values, not raw strings.

- [ ] **Step 3: Run test**

Run:

```bash
go test ./cmd/binlogviz -run TestAnalyzeDetailStoreNoneAndDuckDBProduceEquivalentReports -count=1
```

Expected: PASS only after Tasks 6 and 7 are complete.

- [ ] **Step 4: Add benchmark variants**

Modify `cmd/binlogviz/benchmark_test.go` so `BenchmarkAnalyzeExternalRealBinlog` runs sub-benchmarks:

```go
b.Run("detail_none", func(b *testing.B) {
	benchmarkAnalyzeExternalRealBinlogWithArgs(b, "--detail-store", "none")
})
b.Run("detail_duckdb", func(b *testing.B) {
	benchmarkAnalyzeExternalRealBinlogWithArgs(b, "--detail-store", "duckdb")
})
```

Keep the benchmark skipped when `BINLOGVIZ_REAL_BINLOG` is not set.

- [ ] **Step 5: Run benchmark without env**

Run:

```bash
go test ./cmd/binlogviz -run '^$' -bench BenchmarkAnalyzeExternalRealBinlog -count=1
```

Expected: PASS with skip when no external file is configured.

- [ ] **Step 6: Commit**

Run:

```bash
git add cmd/binlogviz/analyze_product_test.go cmd/binlogviz/benchmark_test.go
git commit -m "test: compare analyze detail store modes"
```

Expected: commit succeeds.

## Task 9: Full Verification And Real-Binlog Measurement

**Files:**
- No production edits expected
- Update docs only if measured values are recorded

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

Expected: OK or only documented non-blocking L1 reminder.

- [ ] **Step 4: Run diff check**

Run:

```bash
git diff --check
```

Expected: no output.

- [ ] **Step 5: Run GitNexus detect changes**

Run:

```text
gitnexus_detect_changes({repo: "BinlogVisualizer", scope: "all"})
```

Expected: changed symbols only in analyzer/cmd/tests/docs/i18n scope. Investigate unexpected report/compare/trend changes.

- [ ] **Step 6: Run real-binlog benchmark if sample exists**

Run:

```bash
BINLOGVIZ_REAL_BINLOG=/opt/homebrew/var/mysql/mysql-bin.000009 \
go test ./cmd/binlogviz -run '^$' -bench BenchmarkAnalyzeExternalRealBinlog -benchmem -count=3
```

Expected: record separate `detail_none` and `detail_duckdb` wall time, B/op, and allocs/op.

- [ ] **Step 7: Run real smoke commands if sample exists**

Run:

```bash
go run ./cmd/binlogviz analyze /opt/homebrew/var/mysql/mysql-bin.000009 --format json --detail-store none --snapshot-name real_detail_none > /tmp/binlogviz-detail-none.json
go run ./cmd/binlogviz analyze /opt/homebrew/var/mysql/mysql-bin.000009 --format html --detail-store none --lang zh-CN > /tmp/binlogviz-detail-none.zh.html
go run ./cmd/binlogviz analyze /opt/homebrew/var/mysql/mysql-bin.000009 --format text --detail-store none --lang zh-CN > /tmp/binlogviz-detail-none.txt
```

Expected: all commands succeed. JSON has non-empty `summary`, `tables`, `transactions`, `patterns`, `minutes`, and `diagnostics`.

- [ ] **Step 8: Decide parser phase**

Use this decision rule:

```text
If detail_none reaches the accepted performance envelope and profile no longer shows controllable analyzer/store costs, do not start parser work.
If detail_none still misses the target and parser decode/allocation dominates, write a separate parser optimization design.
```

- [ ] **Step 9: Commit measurement docs if updated**

If docs are updated with measurements:

```bash
git add docs/superpowers/plans/2026-04-19-streaming-report-aggregation-detail-store.md
git commit -m "docs: record detail store benchmark results"
```

Expected: commit only if measurement values are added.

## Final Completion Checklist

- [ ] `Analyzer.assembleResult()` does not call `QueryAllTransactions()` for default reports.
- [ ] `--detail-store=none` avoids DuckDB creation for default analyze.
- [ ] `--detail-store=duckdb` remains tested.
- [ ] JSON/HTML/Text output remains compatible.
- [ ] Compare/trend can consume snapshots from default mode.
- [ ] Real 988 MB benchmark results are recorded.
- [ ] GitNexus detect changes matches expected scope.
- [ ] GitNexus index is refreshed after the final commit with `npx gitnexus analyze`.

## Execution Handoff

Recommended execution mode: **Subagent-Driven**.

Dispatch one worker per task, but require review after each commit because Tasks 3, 4, 6, and 7 touch central analyzer or command flow. Do not batch those tasks into one unreviewed change.
