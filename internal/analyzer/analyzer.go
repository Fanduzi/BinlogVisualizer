// Package analyzer orchestrates incremental binlog analysis over normalized events.
// input: analyzer.Options plus ordered model.NormalizedEvent values with optional workload identity, provenance, time/position/GTID selectors, and object filters.
// output: identity-, scope-, and provenance-aware intersected event-window aggregates, selector evidence, retained row/XA transactions with filter-safe DDL boundaries and explicit completeness, optional flashback SQL when Options.Flashback is set (generated columns learned from Options.SchemaSQL and from every parsed CREATE/ALTER, including excluded GTIDs; a refusal when a schema-file generated value contradicts the expression; an unverifiable expression is omitted and the script guard checks the target; a warning when a selected table has no definition; and a split-transaction warning when a table or DML filter keeps only part of a transaction), DDL identity taken from a qualified statement when the session schema differs, Unclassified QUERY failures when a GTID-started non-explicit group's only in-window work is Unclassified QUERY, open-BEGIN and Ignored-only next-GTID failures, an open-explicit-group count for BEGIN groups flushed without a close, and open DML groups when those BEGIN groups wrote row images.
// pos: module entrypoint that coordinates transaction reconstruction, table/minute aggregation, and alert assembly.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"binlogviz/internal/flashback"
	"binlogviz/internal/model"
)

// Analyzer orchestrates the complete binlog analysis pipeline.
// It consumes normalized events and produces a complete analysis result.
type Analyzer struct {
	opts   Options
	store  detailStore
	filter *EventFilter

	// Sub-aggregators
	txnBuilder *TransactionBuilder
	tableAgg   *TableAggregator
	hotRows    *hotRowAggregator
	minuteAgg  *MinuteAggregator
	ddlAgg     *DDLAggregator
	reportAgg  *ReportAggregator

	// Event tracking
	eventCount int
	startTime  time.Time
	endTime    time.Time

	// Lifecycle state
	finalized bool
	result    *model.AnalysisResult
	err       error
	selection model.AnalysisSelection

	// gtidGroups is the GTID selector decision for the open group and for a
	// group the current event closed, by transaction key. A group's GTID is
	// fixed by its GTID event, which opens it, so the decision is known before
	// any of its retained events arrive and they stream straight through
	// (#376, docs/gtid-filter-streaming.md). Entries leave at drain.
	gtidGroups      map[string]*gtidGroup
	inputGTIDFlavor string
	matchedGTIDs    *matchedGTIDSet

	flash *flashback.Planner
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

// NoteIgnoredQuery records a dropped session-prefix QUERY on the open group.
// It does not close the group and does not count the event.
func (a *Analyzer) NoteIgnoredQuery() {
	if a == nil || a.err != nil || a.finalized || a.txnBuilder == nil {
		return
	}
	a.txnBuilder.NoteIgnoredQuery()
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

	if err := a.txnBuilder.inFlightUnclassifiedError(); err != nil {
		a.err = err
		return nil, err
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
	a.reportAgg.Close()
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
	if a.flash != nil {
		gtid, _, _ := a.txnBuilder.touchLocation()
		a.flash.Observe(ev, a.txnBuilder.CurrentTxnKey(), gtid)
	}
	if err := a.observeGTIDFlavor(ev); err != nil {
		return err
	}
	workloadEv, isWorkload := filteredWorkloadEvent(ev)
	if relation == insideWindow && a.opts.HasObjectFilters() && isWorkload && !a.filter.Allow(workloadEv.Schema, workloadEv.Table) {
		if a.flash != nil && ev.EventType == "ROWS" {
			gtid, _, _ := a.txnBuilder.touchLocation()
			a.flash.NoteSkipped(a.txnBuilder.CurrentTxnKey(), gtid)
		}
		a.txnBuilder.clearCurrentQueryContext()
		if ev.EventType == "DDL" {
			if err := a.txnBuilder.consumeWindowed(ev, relation); err != nil {
				return err
			}
			a.noteFilteredDDL(ev)
			return a.persistCompletedTransactions()
		}
		return nil
	}
	if relation == insideWindow && ev.EventType == "ROWS" && !a.filter.AllowOperation(ev.Operation) {
		if a.flash != nil {
			gtid, _, _ := a.txnBuilder.touchLocation()
			a.flash.NoteSkipped(a.txnBuilder.CurrentTxnKey(), gtid)
		}
		a.txnBuilder.clearCurrentQueryContext()
		return nil
	}

	// TransactionBuilder is the source of truth for transaction boundaries.
	// If it returns an error, stop processing to avoid inconsistent state.
	if err := a.txnBuilder.consumeWindowed(ev, relation); err != nil {
		return err
	}
	ev = a.withDDLHold(ev)
	// The transaction builder already copied the bounded images it keeps.
	// Drop them here so GTID buffering and later aggregators do not retain cell values.
	ev.RowImages = nil
	ev = a.withCurrentTxnKey(ev)
	ev = a.withTxnLocation(ev)
	if a.opts.HasGTIDSelectors() {
		if relation != insideWindow || ev.TxnKey == "" {
			return a.persistCompletedTransactions()
		}
		// An event of a group it just closed (XID, COMMIT, a GTID-started
		// DDL) is aggregated before that group drains; an event of the open
		// group after any group this event closed. That is the order the
		// old per-group buffer replayed them in.
		if ev.TxnKey != a.txnBuilder.CurrentTxnKey() {
			if err := a.retainSelectedGroupEvent(ev); err != nil {
				return err
			}
			return a.persistCompletedTransactions()
		}
		if err := a.persistCompletedTransactions(); err != nil {
			return err
		}
		return a.retainSelectedGroupEvent(ev)
	}
	if relation != insideWindow {
		return a.persistCompletedTransactions()
	}
	if err := a.aggregateRetainedEvent(ev); err != nil {
		return err
	}
	return a.persistCompletedTransactions()
}

// noteFilteredDDL hands the planner an in-range DDL that the table or schema
// filter left out of the report. The filter judges a DDL by its first name
// only; the planner reads every name (ADR 0015). Under GTID selectors the DDL
// waits for its group's match, like a retained event.
func (a *Analyzer) noteFilteredDDL(ev model.NormalizedEvent) {
	if a.flash == nil {
		return
	}
	ev = a.withDDLHold(ev)
	ev.RowImages = nil
	ev = a.withCurrentTxnKey(ev)
	if !a.opts.HasGTIDSelectors() {
		a.flash.Note(ev)
		return
	}
	if group := a.gtidGroupFor(ev.TxnKey); group != nil && group.matched {
		a.flash.Note(ev)
	}
}

// gtidGroup is the GTID selector decision for one transaction group.
type gtidGroup struct {
	gtid     string
	matched  bool
	retained bool // at least one in-window event was aggregated
}

// gtidGroupFor returns the decision for the open group or a group closed by
// the current event, or nil for any other key (the old buffer dropped such
// events too). A Match error counts as not matched here; drain reports it,
// after the unresolved-boundary check, as before.
func (a *Analyzer) gtidGroupFor(key string) *gtidGroup {
	if key == "" {
		return nil
	}
	if group, ok := a.gtidGroups[key]; ok {
		return group
	}
	gtid, ok := a.txnBuilder.groupGTID(key)
	if !ok {
		return nil
	}
	matched, err := a.opts.GTIDSelector.Match(gtid)
	group := &gtidGroup{gtid: gtid, matched: err == nil && matched}
	a.gtidGroups[key] = group
	return group
}

// retainSelectedGroupEvent aggregates one in-window event of a group the
// GTID selectors keep, and drops it otherwise. Nothing is held per event.
func (a *Analyzer) retainSelectedGroupEvent(ev model.NormalizedEvent) error {
	group := a.gtidGroupFor(ev.TxnKey)
	if group == nil || !group.matched {
		return nil
	}
	group.retained = true
	return a.aggregateRetainedEvent(ev)
}

func (a *Analyzer) aggregateRetainedEvent(ev model.NormalizedEvent) error {
	if a.flash != nil {
		a.flash.Note(ev)
	}
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
	// A DML kind filter counts workload events only, matching an object filter:
	// boundaries of a dropped INSERT group are not summary events or bytes.
	if a.filter.Allow(aggregationEv.Schema, aggregationEv.Table) && (!a.opts.HasDMLFilter() || isWorkload) {
		a.reportAgg.ConsumeOperationEvent(aggregationEv)
		a.tableAgg.Consume(aggregationEv)
		a.hotRows.Consume(aggregationEv)
		a.minuteAgg.Consume(aggregationEv)
		a.ddlAgg.ConsumeEvent(aggregationEv)
	}
	if (!a.opts.HasObjectFilters() && !a.opts.HasDMLFilter()) || isWorkload {
		a.reportAgg.ConsumeEvent(aggregationEv)
	}

	if err := a.persistMinuteBuckets(a.minuteAgg.DrainBefore(truncateToMinute(ev.Timestamp))); err != nil {
		return err
	}
	return nil
}

func (a *Analyzer) withDDLHold(ev model.NormalizedEvent) model.NormalizedEvent {
	gtid, path, pos, ok := a.txnBuilder.takeDDLHold()
	if !ok {
		return ev
	}
	ev.HoldingGTID = gtid
	ev.HoldingStartPath = path
	ev.HoldingStartPos = pos
	return ev
}

func (a *Analyzer) withTxnLocation(ev model.NormalizedEvent) model.NormalizedEvent {
	gtid, path, pos := a.txnBuilder.touchLocation()
	ev.TxnGTID = gtid
	ev.TxnStartPath = path
	ev.TxnStartPos = pos
	return ev
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
	next, _ := withDDLIdentity(ev)
	return next
}

// withDDLIdentity copies a parsed statement's schema and table onto the event.
// A qualified name wins over the session schema carried on the query event.
func withDDLIdentity(ev model.NormalizedEvent) (model.NormalizedEvent, bool) {
	ddl, ok := DDLEventFromNormalizedEvent(ev)
	if !ok {
		return ev, false
	}
	ev.EventType = "DDL"
	if ev.QuerySchema == "" {
		ev.QuerySchema = ev.Schema
	}
	if ddl.Schema != "" {
		ev.Schema = ddl.Schema
	}
	if ddl.Table != "" {
		ev.Table = ddl.Table
	}
	return ev, true
}

// reset clears all internal state for a fresh analysis run.
func (a *Analyzer) reset() {
	a.err = nil
	a.txnBuilder = NewTransactionBuilder()
	a.tableAgg = NewTableAggregator()
	a.hotRows = newHotRowAggregator(HotRowTrackLimit)
	a.minuteAgg = NewMinuteAggregator()
	a.ddlAgg = NewDDLAggregator()
	a.reportAgg.Close()
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
	a.gtidGroups = make(map[string]*gtidGroup)
	a.inputGTIDFlavor = ""
	a.matchedGTIDs = newMatchedGTIDSet()
	if a.flash != nil {
		a.flash.Close()
		a.flash = nil
	}
	if a.opts.Flashback {
		limit := a.opts.FlashbackConflictLimit
		a.flash = flashback.New(flashback.Options{
			SchemaSQL:     a.opts.SchemaSQL,
			SchemaFileDB:  a.opts.SchemaFileDB,
			Conflict:      a.opts.FlashbackConflict,
			Plan:          a.opts.FlashbackPlan,
			ConflictLimit: limit,
			Selection:     a.flashSelection(),
		})
	}
	if a.store != nil {
		a.err = a.store.Reset()
	}
}

// assembleResult builds the final AnalysisResult from the streaming report aggregator snapshot.
func (a *Analyzer) assembleResult() (*model.AnalysisResult, error) {
	snap := a.reportAgg.Snapshot()
	if err := a.reportAgg.Err(); err != nil {
		return nil, err
	}

	topTransactions := snap.Transactions
	if err := a.attachTopTransactionSQL(topTransactions); err != nil {
		return nil, err
	}

	scope := &model.SnapshotFilters{
		IncludeSchemas: append([]string(nil), a.opts.IncludeSchemas...),
		ExcludeSchemas: append([]string(nil), a.opts.ExcludeSchemas...),
		IncludeTables:  append([]string(nil), a.opts.IncludeTables...),
		ExcludeTables:  append([]string(nil), a.opts.ExcludeTables...),
		IncludeDML:     append([]string(nil), a.opts.IncludeDML...),
	}
	result := &model.AnalysisResult{
		WorkloadID:          a.opts.WorkloadID,
		Scope:               scope,
		Summary:             snap.Summary,
		Provenance:          snap.Provenance,
		SQLContextAvailable: snap.SQLContextAvailable,
		Timeseries:          snap.Timeseries,
		Tables:              a.tableAgg.Snapshot(),
		Threads:             snap.Threads,
		ThreadsRankedBy:     snap.ThreadsRankedBy,
		HotRows:             a.hotRows.Snapshot(),
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
		a.selection.MatchedGTIDSet = a.matchedGTIDs.strings()
		if a.opts.HasGTIDSelectors() {
			count := a.matchedGTIDs.count
			a.selection.MatchedTransactions = &count
		}
		selection := a.selection
		result.Selection = &selection
	}
	result.Diagnostics.OpenExplicitGroups = a.txnBuilder.OpenExplicitGroups()
	if groups := a.txnBuilder.OpenDMLGroups(); len(groups) > 0 {
		result.Diagnostics.OpenDMLGroups = groups
		alerts := OpenDMLAlerts(groups, a.opts.LargeTxnDuration)
		result.Alerts = append(alerts, result.Alerts...)
		findings := BuildFindingsFromAlerts(alerts, nil, nil, nil)
		result.Diagnostics.Findings = append(findings, result.Diagnostics.Findings...)
	}
	if pkAlerts := NoPrimaryKeyAlerts(result.Tables); len(pkAlerts) > 0 {
		result.Alerts = append(pkAlerts, result.Alerts...)
		findings := BuildFindingsFromAlerts(pkAlerts, nil, nil, nil)
		result.Diagnostics.Findings = append(findings, result.Diagnostics.Findings...)
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

// Planner is the flashback planner for this run, or nil when flashback is off.
func (a *Analyzer) Planner() *flashback.Planner {
	if a == nil {
		return nil
	}
	return a.flash
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
			group := a.gtidGroups[txn.TxnKey]
			delete(a.gtidGroups, txn.TxnKey)
			if txn.EffectiveCompleteness() == model.TransactionUnknown {
				return fmt.Errorf("cannot apply GTID selectors: transaction %s has unresolved boundaries", txn.TxnKey)
			}
			matched, err := a.opts.GTIDSelector.Match(txn.GTID)
			if err != nil {
				return err
			}
			if group != nil && (group.gtid != txn.GTID || group.matched != matched) {
				// The builder only takes a GTID from the GTID event that opens
				// a group, so this cannot happen; fail rather than misreport.
				return fmt.Errorf("cannot apply GTID selectors: transaction %s was filtered as GTID %q but closed as %q", txn.TxnKey, group.gtid, txn.GTID)
			}
			if txn.GTID != "" && a.opts.GTIDSelector.Flavor() == "mariadb" {
				// Every MariaDB GTID read, matched or not, tells the matched
				// set whether a domain's sequence numbers repeat (#381 QA F1).
				identity, err := a.opts.GTIDSelector.canonicalIdentity(txn.GTID)
				if err != nil {
					return err
				}
				if err := a.matchedGTIDs.observe(identity); err != nil {
					return err
				}
			}
			if !matched {
				continue
			}
			if txn.GTID != "" && group != nil && group.retained {
				identity, err := a.opts.GTIDSelector.canonicalIdentity(txn.GTID)
				if err != nil {
					return err
				}
				if err := a.matchedGTIDs.add(identity); err != nil {
					return err
				}
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
	return withDDLIdentity(ev)
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
