// Package analyzer orchestrates incremental binlog analysis over normalized events.
// input: analyzer.Options plus ordered model.NormalizedEvent values from the binlog normalization pipeline.
// output: streaming Consume/Finalize analysis state and model.AnalysisResult snapshots for command/report layers.
// pos: module entrypoint that coordinates transaction reconstruction, table/minute aggregation, and alert assembly.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"errors"
	"time"

	"binlogviz/internal/model"
)

// Analyzer orchestrates the complete binlog analysis pipeline.
// It consumes normalized events and produces a complete analysis result.
type Analyzer struct {
	opts   Options
	store  detailStore
	filter *EventFilter

	// Sub-aggregators
	txnBuilder    *TransactionBuilder
	tableAgg      *TableAggregator
	minuteAgg     *MinuteAggregator
	ddlAgg        *DDLAggregator
	timeseriesAgg *TimeseriesAggregator
	reportAgg     *ReportAggregator

	// Event tracking
	eventCount int
	startTime  time.Time
	endTime    time.Time

	// Lifecycle state
	finalized bool
	result    *model.AnalysisResult
	err       error
}

// New creates a new Analyzer with the given options.
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

// NewWithStore creates a new Analyzer backed by a caller-managed store.
func NewWithStore(opts Options, store *DuckDBStore) *Analyzer {
	opts.DetailStoreMode = DetailStoreDuckDB
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

	a.reportAgg.ConsumeDDLEvents(a.ddlAgg.Snapshot())

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
	ev = a.withCurrentTxnKey(ev)
	ev = enrichDDLEvent(ev)

	// Track event count and time bounds only after transaction state accepted the event.
	a.eventCount++
	if a.startTime.IsZero() || ev.Timestamp.Before(a.startTime) {
		a.startTime = ev.Timestamp
	}
	if a.endTime.IsZero() || ev.Timestamp.After(a.endTime) {
		a.endTime = ev.Timestamp
	}

	// Only fan out to other aggregators if transaction processing succeeded.
	if a.filter.Allow(ev.Schema, ev.Table) {
		a.reportAgg.ConsumeOperationEvent(ev)
		a.tableAgg.Consume(ev)
		a.minuteAgg.Consume(ev)
		a.ddlAgg.ConsumeEvent(ev)
		a.timeseriesAgg.Consume(ev)
	}
	a.reportAgg.ConsumeEvent(ev)

	if err := a.persistCompletedTransactions(); err != nil {
		return err
	}
	if err := a.persistMinuteBuckets(a.minuteAgg.DrainBefore(truncateToMinute(ev.Timestamp))); err != nil {
		return err
	}
	return nil
}

func (a *Analyzer) withCurrentTxnKey(ev model.NormalizedEvent) model.NormalizedEvent {
	if ev.TxnKey != "" {
		return ev
	}
	if txnKey := a.txnBuilder.CurrentTxnKey(); txnKey != "" {
		ev.TxnKey = txnKey
	}
	return ev
}

func enrichDDLEvent(ev model.NormalizedEvent) model.NormalizedEvent {
	if ev.EventType != "DDL" && ev.EventType != "QUERY" {
		return ev
	}
	ddl, ok := DDLEventFromNormalizedEvent(ev)
	if !ok {
		return ev
	}
	ev.EventType = "DDL"
	if ev.Schema == "" {
		ev.Schema = ddl.Schema
	}
	if ev.Table == "" {
		ev.Table = ddl.Table
	}
	return ev
}

// reset clears all internal state for a fresh analysis run.
func (a *Analyzer) reset() {
	a.err = nil
	a.txnBuilder = NewTransactionBuilder()
	a.tableAgg = NewTableAggregator()
	a.minuteAgg = NewMinuteAggregator()
	a.ddlAgg = NewDDLAggregator()
	a.timeseriesAgg = NewTimeseriesAggregator()
	a.reportAgg = NewReportAggregator(a.opts)
	a.filter = newEventFilter(a.opts)
	a.eventCount = 0
	a.startTime = time.Time{}
	a.endTime = time.Time{}
	a.finalized = false
	a.result = nil
	if a.store != nil {
		a.err = a.store.Reset()
	}
}

// assembleResult builds the final AnalysisResult from the streaming report aggregator snapshot.
func (a *Analyzer) assembleResult() (*model.AnalysisResult, error) {
	snap := a.reportAgg.Snapshot()

	topTransactions := snap.Transactions
	if err := a.attachTopTransactionSQL(topTransactions); err != nil {
		return nil, err
	}

	return &model.AnalysisResult{
		Summary:           snap.Summary,
		Timeseries:        snap.Timeseries,
		Tables:            limitTables(a.tableAgg.Snapshot(), a.opts.TopTables),
		Transactions:      topTransactions,
		Patterns:          snap.Patterns,
		Minutes:           snap.Minutes,
		Diagnostics:       snap.Diagnostics,
		Alerts:            snap.Alerts,
		Warnings:          snap.Warnings,
		PatternDrilldowns: snap.PatternDrilldowns,
	}, nil
}

func (a *Analyzer) attachTopTransactionSQL(transactions []model.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}
	if a.opts.DetailStoreMode == DetailStoreNone {
		return nil
	}
	keys := make([]string, 0, len(transactions))
	for _, txn := range transactions {
		if txn.QueryContext != nil && txn.QueryContext.SQL == "" {
			keys = append(keys, txn.TxnKey)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sqlByTxn, err := a.store.ResolveTransactionQuerySQL(keys)
	if err != nil {
		return err
	}
	for i := range transactions {
		if transactions[i].QueryContext == nil {
			continue
		}
		transactions[i].QueryContext.SQL = sqlByTxn[transactions[i].TxnKey]
	}
	return nil
}

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

func (a *Analyzer) persistMinuteBuckets(buckets []model.MinuteBucket) error {
	if len(buckets) == 0 {
		return nil
	}
	for _, bucket := range buckets {
		a.reportAgg.ConsumeMinuteBucket(bucket)
	}
	return a.store.RecordMinuteBuckets(buckets)
}
