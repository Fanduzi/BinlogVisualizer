package analyzer

import (
	"time"

	"binlogviz/internal/model"
)

// Analyzer orchestrates the complete binlog analysis pipeline.
// It consumes normalized events and produces a complete analysis result.
type Analyzer struct {
	opts Options

	// Sub-aggregators
	txnBuilder   *TransactionBuilder
	tableAgg     *TableAggregator
	minuteAgg    *MinuteAggregator

	// Event tracking
	eventCount   int
	startTime    time.Time
	endTime      time.Time
}

// New creates a new Analyzer with the given options.
func New(opts Options) *Analyzer {
	return &Analyzer{
		opts:         opts,
		txnBuilder:   NewTransactionBuilder(),
		tableAgg:     NewTableAggregator(),
		minuteAgg:    NewMinuteAggregator(),
	}
}

// Analyze processes a slice of normalized events and returns the complete analysis result.
// Events are processed in order, passing each to all sub-aggregators.
// If a boundary error occurs (e.g., malformed transaction sequence), an error is returned.
// After all events are consumed, Flush is called to finalize in-flight transactions.
func (a *Analyzer) Analyze(events []model.NormalizedEvent) (*model.AnalysisResult, error) {
	// Reset state for fresh analysis
	a.reset()

	// Process all events
	for _, ev := range events {
		if err := a.consume(ev); err != nil {
			return nil, err
		}
	}

	// Finalize in-flight transactions
	a.txnBuilder.Flush()

	// Assemble final result
	return a.assembleResult(), nil
}

// consume passes a single event to all sub-aggregators.
// If TransactionBuilder returns an error (e.g., boundary violation),
// fan-out to other aggregators is stopped to prevent inconsistent state.
func (a *Analyzer) consume(ev model.NormalizedEvent) error {
	// Track event count and time bounds
	a.eventCount++
	if a.startTime.IsZero() || ev.Timestamp.Before(a.startTime) {
		a.startTime = ev.Timestamp
	}
	if a.endTime.IsZero() || ev.Timestamp.After(a.endTime) {
		a.endTime = ev.Timestamp
	}

	// TransactionBuilder is the source of truth for transaction boundaries.
	// If it returns an error, stop processing to avoid inconsistent state.
	if err := a.txnBuilder.Consume(ev); err != nil {
		return err
	}

	// Only fan out to other aggregators if transaction processing succeeded.
	a.tableAgg.Consume(ev)
	a.minuteAgg.Consume(ev)
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
}

// assembleResult builds the final AnalysisResult from all sub-aggregator snapshots.
func (a *Analyzer) assembleResult() *model.AnalysisResult {
	transactions := a.txnBuilder.Completed()
	tables := a.tableAgg.Snapshot()
	minutes := a.minuteAgg.Snapshot()

	// Calculate workload summary
	summary := a.buildSummary(transactions)

	return &model.AnalysisResult{
		Summary:      summary,
		Tables:       tables,
		Transactions: transactions,
		Minutes:      minutes,
		Alerts:       nil, // No alerts in MVP
		Warnings:     0,  // No warnings in MVP
	}
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
