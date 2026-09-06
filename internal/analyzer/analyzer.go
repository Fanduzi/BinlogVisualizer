// Package analyzer orchestrates incremental binlog analysis over normalized events.
// input: analyzer.Options plus ordered model.NormalizedEvent values with optional workload identity, provenance, time/position/GTID selectors, and object filters.
// output: identity-, scope-, and provenance-aware intersected event-window aggregates, selector evidence, and retained row/XA transactions with filter-safe DDL boundaries and explicit completeness.
// pos: module entrypoint that coordinates transaction reconstruction, table/minute aggregation, and alert assembly.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"errors"
	"fmt"
	"sort"
	"strings"
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
	selection model.AnalysisSelection

	pendingGroupEvents map[string][]model.NormalizedEvent
	inputGTIDFlavor    string
	matchedGTIDs       map[string]struct{}
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
// Events outside the configured time window are observed only for transaction boundaries.
// Once an error is returned, the analyzer remains failed and future Consume/Finalize calls return that error.
func (a *Analyzer) Consume(ev model.NormalizedEvent) error {
	if a.err != nil {
		return a.err
	}
	if a.finalized {
		return errors.New("analyzer already finalized")
	}
	relation, err := a.eventWindowRelation(ev)
	if err != nil {
		a.err = err
		return err
	}
	if err := a.consume(ev, relation); err != nil {
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
	if a.opts.HasGTIDSelectors() && a.inputGTIDFlavor == "" {
		a.err = fmt.Errorf("cannot resolve GTID flavor from input")
		return nil, a.err
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
	return a.windowRelation(ts) == insideWindow
}

func (a *Analyzer) windowRelation(ts time.Time) windowRelation {
	// Check start boundary (inclusive)
	if a.opts.Start != nil && ts.Before(*a.opts.Start) {
		return beforeWindow
	}
	// Check end boundary (inclusive)
	if a.opts.End != nil && ts.After(*a.opts.End) {
		return afterWindow
	}
	return insideWindow
}

func (a *Analyzer) eventWindowRelation(ev model.NormalizedEvent) (windowRelation, error) {
	relation := a.windowRelation(ev.Timestamp)
	before := relation == beforeWindow
	after := relation == afterWindow
	if a.opts.HasPositionSelectors() {
		if a.opts.StartPosition != nil && *a.opts.StartPosition < 4 {
			return insideWindow, fmt.Errorf("start position %d is before the first binlog event boundary", *a.opts.StartPosition)
		}
		if a.opts.StopPosition != nil && *a.opts.StopPosition < 4 {
			return insideWindow, fmt.Errorf("stop position %d is before the first binlog event boundary", *a.opts.StopPosition)
		}
		if a.opts.StartPosition != nil && a.opts.StopPosition != nil && *a.opts.StopPosition <= *a.opts.StartPosition {
			return insideWindow, fmt.Errorf("stop position %d must be greater than start position %d", *a.opts.StopPosition, *a.opts.StartPosition)
		}
		if ev.PositionStart <= 0 || ev.PositionEnd <= ev.PositionStart {
			return insideWindow, fmt.Errorf("position selection requires exact event offsets, got [%d,%d)", ev.PositionStart, ev.PositionEnd)
		}
		if a.opts.StartPosition != nil && ev.PositionStart < *a.opts.StartPosition {
			before = true
		}
		if a.opts.StopPosition != nil && ev.PositionStart >= *a.opts.StopPosition {
			after = true
		}
	}
	switch {
	case before && after:
		return outsideBoth, nil
	case before:
		return beforeWindow, nil
	case after:
		return afterWindow, nil
	default:
		return insideWindow, nil
	}
}

func (a *Analyzer) observeGTIDFlavor(ev model.NormalizedEvent) error {
	if !a.opts.HasGTIDSelectors() {
		return nil
	}
	flavor := strings.ToLower(strings.TrimSpace(ev.ServerFlavor))
	if flavor != "" && flavor != "mysql" && flavor != "mariadb" {
		return fmt.Errorf("unsupported input GTID flavor %q", ev.ServerFlavor)
	}
	if ev.GTID != "" {
		gtidFlavor, err := transactionGTIDFlavor(ev.GTID)
		if err != nil {
			return err
		}
		if flavor != "" && flavor != gtidFlavor {
			return fmt.Errorf("input flavor %s conflicts with GTID %q", flavor, ev.GTID)
		}
		flavor = gtidFlavor
	}
	if flavor == "" {
		return nil
	}
	if flavor != a.opts.GTIDSelector.Flavor() {
		return fmt.Errorf("GTID selector flavor %s conflicts with input flavor %s", a.opts.GTIDSelector.Flavor(), flavor)
	}
	if a.inputGTIDFlavor != "" && a.inputGTIDFlavor != flavor {
		return fmt.Errorf("mixed input GTID flavors: %s and %s", a.inputGTIDFlavor, flavor)
	}
	a.inputGTIDFlavor = flavor
	return nil
}

// consume passes a single event to all sub-aggregators.
// If TransactionBuilder returns an error (e.g., boundary violation),
// fan-out to other aggregators is stopped to prevent inconsistent state.
func (a *Analyzer) consume(ev model.NormalizedEvent, relation windowRelation) error {
	ev = enrichDDLEvent(ev)
	if err := a.observeGTIDFlavor(ev); err != nil {
		return err
	}
	workloadEv, isWorkload := filteredWorkloadEvent(ev)
	if relation == insideWindow && a.opts.HasObjectFilters() && isWorkload && !a.filter.Allow(workloadEv.Schema, workloadEv.Table) {
		a.txnBuilder.clearCurrentQueryContext()
		if ev.EventType == "DDL" {
			if err := a.txnBuilder.consumeWindowed(ev, relation); err != nil {
				return err
			}
			return a.persistCompletedTransactions()
		}
		return nil
	}

	// TransactionBuilder is the source of truth for transaction boundaries.
	// If it returns an error, stop processing to avoid inconsistent state.
	if err := a.txnBuilder.consumeWindowed(ev, relation); err != nil {
		return err
	}
	ev = a.withCurrentTxnKey(ev)
	if a.opts.HasGTIDSelectors() {
		if relation == insideWindow {
			if ev.TxnKey != "" {
				// ponytail: buffer one in-flight group; spool if real transaction sizes make this a memory ceiling.
				a.pendingGroupEvents[ev.TxnKey] = append(a.pendingGroupEvents[ev.TxnKey], ev)
			}
		}
		return a.persistCompletedTransactions()
	}
	if relation != insideWindow {
		return a.persistCompletedTransactions()
	}
	if err := a.aggregateRetainedEvent(ev); err != nil {
		return err
	}
	return a.persistCompletedTransactions()
}

func (a *Analyzer) aggregateRetainedEvent(ev model.NormalizedEvent) error {
	a.recordEffectivePosition(ev)
	workloadEv, isWorkload := filteredWorkloadEvent(ev)
	if isWorkload {
		workloadEv.TxnKey = ev.TxnKey
	}
	aggregationEv := ev
	if a.opts.HasObjectFilters() && isWorkload {
		aggregationEv = workloadEv
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
	if a.filter.Allow(aggregationEv.Schema, aggregationEv.Table) {
		a.reportAgg.ConsumeOperationEvent(aggregationEv)
		a.tableAgg.Consume(aggregationEv)
		a.minuteAgg.Consume(aggregationEv)
		a.ddlAgg.ConsumeEvent(aggregationEv)
		a.timeseriesAgg.Consume(aggregationEv)
	}
	if !a.opts.HasObjectFilters() || isWorkload {
		a.reportAgg.ConsumeEvent(aggregationEv)
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
	if txnKey := a.txnBuilder.LastEventTxnKey(); txnKey != "" {
		ev.TxnKey = txnKey
	} else if txnKey := a.txnBuilder.CurrentTxnKey(); txnKey != "" {
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
	a.selection = model.AnalysisSelection{
		RequestedStartPosition: cloneInt64(a.opts.StartPosition),
		RequestedStopPosition:  cloneInt64(a.opts.StopPosition),
	}
	if a.opts.HasGTIDSelectors() {
		a.selection.IncludeGTIDs = a.opts.GTIDSelector.Include()
		a.selection.ExcludeGTIDs = a.opts.GTIDSelector.Exclude()
	}
	a.pendingGroupEvents = make(map[string][]model.NormalizedEvent)
	a.inputGTIDFlavor = ""
	a.matchedGTIDs = make(map[string]struct{})
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

	scope := &model.SnapshotFilters{
		IncludeSchemas: append([]string(nil), a.opts.IncludeSchemas...),
		ExcludeSchemas: append([]string(nil), a.opts.ExcludeSchemas...),
		IncludeTables:  append([]string(nil), a.opts.IncludeTables...),
		ExcludeTables:  append([]string(nil), a.opts.ExcludeTables...),
	}
	result := &model.AnalysisResult{
		WorkloadID:          a.opts.WorkloadID,
		Scope:               scope,
		Summary:             snap.Summary,
		Provenance:          snap.Provenance,
		SQLContextAvailable: snap.SQLContextAvailable,
		Timeseries:          snap.Timeseries,
		Tables:              a.tableAgg.Snapshot(),
		Transactions:        topTransactions,
		Patterns:            snap.Patterns,
		Minutes:             snap.Minutes,
		Diagnostics:         snap.Diagnostics,
		Alerts:              snap.Alerts,
		Warnings:            snap.Warnings,
		PatternDrilldowns:   snap.PatternDrilldowns,
	}
	if a.opts.HasPositionSelectors() || a.opts.HasGTIDSelectors() {
		a.selection.ResolvedGTIDFlavor = a.inputGTIDFlavor
		a.selection.MatchedGTIDs = sortedStringSet(a.matchedGTIDs)
		selection := a.selection
		result.Selection = &selection
	}
	return result, nil
}

func sortedStringSet(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func (a *Analyzer) recordEffectivePosition(ev model.NormalizedEvent) {
	if !a.opts.HasPositionSelectors() || ev.PositionStart <= 0 || ev.PositionEnd <= ev.PositionStart {
		return
	}
	if a.selection.EffectiveStartPosition == nil || ev.PositionStart < *a.selection.EffectiveStartPosition {
		a.selection.EffectiveStartPosition = cloneInt64(&ev.PositionStart)
	}
	if a.selection.EffectiveStopPosition == nil || ev.PositionEnd > *a.selection.EffectiveStopPosition {
		a.selection.EffectiveStopPosition = cloneInt64(&ev.PositionEnd)
	}
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
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
	persisted := make([]model.Transaction, 0, len(drained))
	for _, txn := range drained {
		if a.opts.HasGTIDSelectors() {
			events := a.pendingGroupEvents[txn.TxnKey]
			delete(a.pendingGroupEvents, txn.TxnKey)
			if txn.EffectiveCompleteness() == model.TransactionUnknown {
				return fmt.Errorf("cannot apply GTID selectors: transaction %s has unresolved boundaries", txn.TxnKey)
			}
			matched, err := a.opts.GTIDSelector.Match(txn.GTID)
			if err != nil {
				return err
			}
			if !matched {
				continue
			}
			for _, ev := range events {
				if err := a.aggregateRetainedEvent(ev); err != nil {
					return err
				}
			}
			if txn.GTID != "" && len(events) > 0 {
				identity, err := a.opts.GTIDSelector.canonicalIdentity(txn.GTID)
				if err != nil {
					return err
				}
				a.matchedGTIDs[identity] = struct{}{}
			}
		}
		if retainCompletedTransaction(txn) {
			persisted = append(persisted, txn)
		}
	}
	if len(persisted) == 0 {
		return nil
	}
	for _, txn := range persisted {
		a.reportAgg.ConsumeTransaction(txn)
	}
	return a.store.RecordTransactions(toPersistedTransactions(persisted))
}

func filteredWorkloadEvent(ev model.NormalizedEvent) (model.NormalizedEvent, bool) {
	if ev.EventType == "ROWS" || ev.EventType == "DDL" {
		return ev, true
	}
	if ev.EventType != "QUERY" && ev.EventType != "ROWS_QUERY" {
		return ev, false
	}
	ddl, ok := DDLEventFromNormalizedEvent(ev)
	if !ok {
		return ev, false
	}
	ev.EventType = "DDL"
	if ev.Schema == "" {
		ev.Schema = ddl.Schema
	}
	if ev.Table == "" {
		ev.Table = ddl.Table
	}
	return ev, true
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
