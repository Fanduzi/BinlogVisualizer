// Package analyzer orchestrates incremental binlog analysis over normalized events.
// input: analyzer.Options plus ordered model.NormalizedEvent values from the binlog normalization pipeline.
// output: streaming Consume/Finalize analysis state and model.AnalysisResult snapshots for command/report layers.
// pos: module entrypoint that coordinates transaction reconstruction, table/minute aggregation, and alert assembly.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	"binlogviz/internal/model"
)

// Analyzer orchestrates the complete binlog analysis pipeline.
// It consumes normalized events and produces a complete analysis result.
type Analyzer struct {
	opts  Options
	store analysisStore

	// Sub-aggregators
	txnBuilder *TransactionBuilder
	tableAgg   *TableAggregator
	minuteAgg  *MinuteAggregator

	// Event tracking
	eventCount int
	startTime  time.Time
	endTime    time.Time

	// Lifecycle state
	finalized bool
	result    *model.AnalysisResult
	err       error

	ownedTempDir string
}

// New creates a new Analyzer with the given options.
func New(opts Options) *Analyzer {
	store, tempDir, err := newOwnedDuckDBStore()
	a := &Analyzer{
		opts:         opts,
		store:        store,
		ownedTempDir: tempDir,
		err:          err,
	}
	a.reset()
	return a
}

// NewWithStore creates a new Analyzer backed by a caller-managed store.
func NewWithStore(opts Options, store *DuckDBStore) *Analyzer {
	a := &Analyzer{
		opts:  opts,
		store: store,
	}
	a.reset()
	return a
}

// Analyze processes a slice of normalized events and returns the complete analysis result.
// It is a thin wrapper around the streaming Consume/Finalize API.
func (a *Analyzer) Analyze(events []model.NormalizedEvent) (*model.AnalysisResult, error) {
	a.reset()

	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			return nil, err
		}
	}

	return a.Finalize()
}

// Consume processes a single normalized event through the analyzer's streaming pipeline.
// Events outside the configured time window are ignored.
// Once an error is returned, the analyzer remains failed and future Consume/Finalize calls return that error.
func (a *Analyzer) Consume(ev model.NormalizedEvent) error {
	if a.err != nil {
		return a.err
	}
	if a.finalized {
		return errors.New("analyzer already finalized")
	}
	if !a.isInWindow(ev.Timestamp) {
		return nil
	}
	if err := a.consume(ev); err != nil {
		a.err = err
		return err
	}
	return nil
}

// Finalize flushes any in-flight state and assembles the final analysis result.
// It is idempotent after a successful finalize.
func (a *Analyzer) Finalize() (*model.AnalysisResult, error) {
	if a.err != nil {
		return nil, a.err
	}
	if a.finalized {
		return a.result, nil
	}

	a.txnBuilder.Flush()
	if err := a.persistCompletedTransactions(); err != nil {
		a.err = err
		return nil, err
	}
	if err := a.persistMinuteBuckets(a.minuteAgg.DrainAll()); err != nil {
		a.err = err
		return nil, err
	}
	if err := a.store.Flush(); err != nil {
		a.err = err
		return nil, err
	}

	a.result, a.err = a.assembleResult()
	if a.err != nil {
		return nil, a.err
	}
	a.finalized = true
	return a.result, nil
}

// isInWindow checks if a timestamp falls within the configured time window.
// Both Start and End boundaries are inclusive. If Start or End is nil,
// that boundary is not enforced.
func (a *Analyzer) isInWindow(ts time.Time) bool {
	// Check start boundary (inclusive)
	if a.opts.Start != nil && ts.Before(*a.opts.Start) {
		return false
	}
	// Check end boundary (inclusive)
	if a.opts.End != nil && ts.After(*a.opts.End) {
		return false
	}
	return true
}

// consume passes a single event to all sub-aggregators.
// If TransactionBuilder returns an error (e.g., boundary violation),
// fan-out to other aggregators is stopped to prevent inconsistent state.
func (a *Analyzer) consume(ev model.NormalizedEvent) error {
	// TransactionBuilder is the source of truth for transaction boundaries.
	// If it returns an error, stop processing to avoid inconsistent state.
	if err := a.txnBuilder.Consume(ev); err != nil {
		return err
	}

	// Track event count and time bounds only after transaction state accepted the event.
	a.eventCount++
	if a.startTime.IsZero() || ev.Timestamp.Before(a.startTime) {
		a.startTime = ev.Timestamp
	}
	if a.endTime.IsZero() || ev.Timestamp.After(a.endTime) {
		a.endTime = ev.Timestamp
	}

	// Only fan out to other aggregators if transaction processing succeeded.
	a.tableAgg.Consume(ev)
	a.minuteAgg.Consume(ev)

	if err := a.persistCompletedTransactions(); err != nil {
		return err
	}
	if err := a.persistMinuteBuckets(a.minuteAgg.DrainBefore(truncateToMinute(ev.Timestamp))); err != nil {
		return err
	}
	return nil
}

// reset clears all internal state for a fresh analysis run.
func (a *Analyzer) reset() {
	a.txnBuilder = NewTransactionBuilder()
	a.tableAgg = NewTableAggregator()
	a.minuteAgg = NewMinuteAggregator()
	a.eventCount = 0
	a.startTime = time.Time{}
	a.endTime = time.Time{}
	a.finalized = false
	a.result = nil
	if a.store != nil && a.err == nil {
		a.err = a.store.Reset()
	}
	if a.err == nil {
		a.err = nil
	}
}

// assembleResult builds the final AnalysisResult from all sub-aggregator snapshots.
func (a *Analyzer) assembleResult() (*model.AnalysisResult, error) {
	allTransactions, err := a.store.QueryAllTransactions()
	if err != nil {
		return nil, err
	}
	topTransactions, err := a.store.QueryTopTransactions(a.opts.TopTransactions)
	if err != nil {
		return nil, err
	}
	minutes, err := a.store.QueryMinuteBuckets()
	if err != nil {
		return nil, err
	}
	alerts := append(DetectLargeTransactionAlerts(allTransactions, a.opts), DetectSpikeAlerts(minutes, a.opts)...)
	if err := a.store.RecordAlerts(alerts); err != nil {
		return nil, err
	}
	if err := a.store.Flush(); err != nil {
		return nil, err
	}
	persistedAlerts, err := a.store.QueryAlerts()
	if err != nil {
		return nil, err
	}

	// Calculate workload summary
	summary := a.buildSummary(allTransactions)

	return &model.AnalysisResult{
		Summary:      summary,
		Tables:       limitTables(a.tableAgg.Snapshot(), a.opts.TopTables),
		Transactions: topTransactions,
		Minutes:      minutes,
		Alerts:       persistedAlerts,
		Warnings:     0, // No warnings in MVP
	}, nil
}

// buildSummary creates the WorkloadSummary from transaction data.
func (a *Analyzer) buildSummary(transactions []model.Transaction) model.WorkloadSummary {
	totalRows := 0
	for _, txn := range transactions {
		totalRows += txn.TotalRows
	}

	var duration time.Duration
	if !a.startTime.IsZero() && !a.endTime.IsZero() {
		duration = a.endTime.Sub(a.startTime)
	}

	return model.WorkloadSummary{
		TotalTransactions: len(transactions),
		TotalRows:         totalRows,
		TotalEvents:       a.eventCount,
		StartTime:         a.startTime,
		EndTime:           a.endTime,
		Duration:          duration,
	}
}

func (a *Analyzer) persistCompletedTransactions() error {
	drained := a.txnBuilder.DrainCompleted()
	if len(drained) == 0 {
		return nil
	}
	return a.store.RecordTransactions(toPersistedTransactions(drained))
}

func (a *Analyzer) persistMinuteBuckets(buckets []model.MinuteBucket) error {
	if len(buckets) == 0 {
		return nil
	}
	return a.store.RecordMinuteBuckets(buckets)
}

func newOwnedDuckDBStore() (analysisStore, string, error) {
	tempDir, err := os.MkdirTemp("", "binlogviz-analyzer-*")
	if err != nil {
		return nil, "", err
	}
	store, err := NewDuckDBStore(filepath.Join(tempDir, "analysis.duckdb"), DefaultBatchFlushRows)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, "", err
	}
	return store, tempDir, nil
}
