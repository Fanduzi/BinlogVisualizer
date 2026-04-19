// Package analyzer incrementally builds report-ready projections without retaining all transactions.
// input: completed transactions, minute buckets, DDL events, normalized events, and file coverage.
// output: bounded ReportSnapshot values used to assemble model.AnalysisResult.
// pos: streaming report aggregation layer that replaces QueryAllTransactions-dependent finalization.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"sort"
	"strings"
	"time"

	"binlogviz/internal/model"
)

// ReportSnapshot holds a point-in-time view of aggregated report state.
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

// ReportAggregator maintains bounded streaming state for report assembly.
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
	txnSize         txnSizeTracker
}

type txnSizeTracker struct {
	ranges  [4]struct{ min, max int }
	buckets [4]model.TxnSizeBucket
}

func newTxnSizeTracker() txnSizeTracker {
	return txnSizeTracker{
		ranges: [4]struct{ min, max int }{
			{1, 9}, {10, 99}, {100, 999}, {1000, 0},
		},
		buckets: [4]model.TxnSizeBucket{
			{Label: "1-9"}, {Label: "10-99"}, {Label: "100-999"}, {Label: "1000+"},
		},
	}
}

func (t *txnSizeTracker) add(txn model.Transaction) {
	for i, r := range t.ranges {
		if txn.TotalRows < r.min {
			continue
		}
		if r.max > 0 && txn.TotalRows > r.max {
			continue
		}
		t.buckets[i].TxnCount++
		t.buckets[i].Rows += txn.TotalRows
		t.buckets[i].BinlogBytes += txn.BinlogBytes
		break
	}
}

func (t *txnSizeTracker) snapshot() model.TxnSizeSeriesSummary {
	nonEmpty := make([]model.TxnSizeBucket, 0, 4)
	for _, b := range t.buckets {
		if b.TxnCount > 0 {
			nonEmpty = append(nonEmpty, b)
		}
	}
	return model.TxnSizeSeriesSummary{Buckets: nonEmpty}
}

// NewReportAggregator creates a streaming report aggregator with the given options.
func NewReportAggregator(opts Options) *ReportAggregator {
	return &ReportAggregator{
		opts:     opts,
		patterns: make(map[string]*model.PatternStats),
		txnSize:  newTxnSizeTracker(),
	}
}

// ConsumeEvent updates aggregate counters from a normalized event.
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

// ConsumeTransaction ingests a completed transaction into bounded report state.
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
	a.txnSize.add(txn)
	a.alerts = append(a.alerts, DetectLargeTransactionAlerts([]model.Transaction{txn}, a.opts)...)
}

// ConsumeMinuteBucket appends a minute bucket to the aggregation state.
func (a *ReportAggregator) ConsumeMinuteBucket(bucket model.MinuteBucket) {
	if a == nil {
		return
	}
	a.minutes = append(a.minutes, bucket)
}

// ConsumeDDLEvents appends DDL events to the aggregation state.
func (a *ReportAggregator) ConsumeDDLEvents(events []model.DDLEvent) {
	if a == nil || len(events) == 0 {
		return
	}
	a.ddlEvents = append(a.ddlEvents, events...)
}

// SetFileCoverage stores file coverage information.
func (a *ReportAggregator) SetFileCoverage(coverage model.FileCoverage) {
	if a == nil {
		return
	}
	a.fileCoverage = coverage
}

// Snapshot returns a point-in-time view of all aggregated report state.
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

	largestCopy := append([]model.Transaction(nil), a.largest...)
	diagnostics := model.Diagnostics{
		FileCoverage:        a.fileCoverage,
		DDLEvents:           append([]model.DDLEvent(nil), a.ddlEvents...),
		LargestTransactions: largestCopy,
		LongestTransactions: append([]model.Transaction(nil), a.longest...),
		WidestTransactions:  append([]model.Transaction(nil), a.widest...),
		FileSegments:        BuildFileSegments(minutes, 5),
		HotIntervals:        SelectHotIntervals(minutes, 5),
		Findings:            BuildFindingsFromAlerts(alerts, minutes, largestCopy, a.ddlEvents),
	}

	series := BuildTimeseries(TimeseriesBuildInput{Minutes: minutes})
	series.TxnSizeSeriesSummary = a.txnSize.snapshot()

	return ReportSnapshot{
		Summary:           summary,
		Transactions:      append([]model.Transaction(nil), a.topTransactions...),
		Patterns:          patterns,
		Minutes:           minutes,
		Timeseries:        series,
		Diagnostics:       diagnostics,
		Alerts:            alerts,
		Warnings:          a.warnings,
		PatternDrilldowns: BuildPatternDrilldowns(patterns, minutes, largestCopy, alerts),
	}
}

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
	if p.SampleQuerySummary == "" && strings.TrimSpace(txn.QuerySummary) != "" {
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

