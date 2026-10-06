// Package analyzer reconstructs transaction boundaries and completed transaction snapshots.
// input: ordered normalized events with provenance, intersected window relation, MySQL/MariaDB XA, DDL, independent ADMIN, and Unclassified QUERY, and ROWS/ROWS_QUERY semantics.
// output: closed transaction groups (COMMIT/XID/plain ROLLBACK/XA PREPARE/COMMIT/ROLLBACK, GTID-started DDL, GTID-started ADMIN with no BEGIN, a different GTID after XA END, and out-of-window Unclassified QUERY on a GTID-started non-explicit group), UnclassifiedQueryError when a GTID-started non-explicit group's only in-window work is Unclassified QUERY (named or anonymous empty identity, on the next GTID or at finalize; after-window Unclassified QUERY that never intersected does not fail), OpenBeginError when the next GTID meets an unclosed BEGIN (ROLLBACK TO SAVEPOINT does not close it; row-image groups name duration, tables, rows, and span), IgnoredOnlyGroupError when the next GTID meets only Ignored QUERY, a count of explicit BEGIN groups flushed at end of input without a close, open DML groups for those BEGIN groups that wrote row images, retainCompletedTransaction for report membership (ROW image rows, or XA identity with a file location), a shared file span when expanded payload inners all carry the wrapper range, and group duration as the earliest-to-latest non-zero in-window timestamp (MySQL stamps the leading GTID and the XID at commit; BEGIN keeps the statement start).
// pos: live transaction state machine used by Analyzer before completed transactions are flushed to the result store.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"binlogviz/internal/model"
)

// TransactionBuilder reconstructs transactions from normalized events.
type TransactionBuilder struct {
	current            *inFlightTxn
	completed          []model.Transaction
	txnCounter         uint64
	lastEventTxnKey    string
	openExplicitGroups int
	openDML            []model.OpenDMLGroup
}

type inFlightTxn struct {
	txnKey                     string
	xaXID                      string
	serverID                   uint32
	serverVersion              string
	serverFlavor               string
	gtid                       string
	threadID                   uint32
	xid                        string
	actorUser                  string
	actorHost                  string
	startedByGTID              bool
	isExplicit                 bool // true if started with BEGIN, false if implicit
	hasStartBoundary           bool
	hasEndBoundary             bool
	suspendedAtXAEnd           bool
	hadBeforeWindow            bool
	hadAfterWindow             bool
	startTime                  time.Time
	endTime                    time.Time
	fullBinlogBytes            int64
	fullBinlogPathStart        string
	fullBinlogPathEnd          string
	fullPositionStart          int64
	fullPositionEnd            int64
	totalRows                  int
	eventCount                 int
	binlogBytes                int64
	binlogPathStart            string
	binlogPathEnd              string
	positionStart              int64
	positionEnd                int64
	windowFileSpan             repeatedFileSpan
	fullFileSpan               repeatedFileSpan
	tables                     map[tableIdentity]int
	operations                 map[string]int
	rowOperation               string
	querySQL                   string // Bounded SQL from ROWS_QUERY event
	queryTruncated             bool
	queryOriginalBytes         int // Original SQL byte count before truncation
	retainedQuerySQL           string
	retainedQueryTruncated     bool
	retainedQueryOriginalBytes int
	unclassifiedPrefix         string
	sawIgnoredQuery            bool
	sawSavepointRollback       bool
}

type windowRelation uint8

const (
	insideWindow windowRelation = iota
	beforeWindow
	afterWindow
	outsideBoth
)

// NewTransactionBuilder creates a new TransactionBuilder.
func NewTransactionBuilder() *TransactionBuilder {
	return &TransactionBuilder{
		completed: make([]model.Transaction, 0),
	}
}

// Consume processes a normalized event and updates transaction state.
func (b *TransactionBuilder) Consume(ev model.NormalizedEvent) error {
	return b.consumeWindowed(ev, insideWindow)
}

func (b *TransactionBuilder) consumeWindowed(ev model.NormalizedEvent, relation windowRelation) error {
	b.lastEventTxnKey = ""
	switch ev.EventType {
	case "GTID":
		return b.handleGTID(ev, relation)
	case "BEGIN":
		if err := b.handleBegin(ev, relation); err != nil {
			return err
		}
		return b.mergeProvenance(ev)
	case "XA_START":
		if err := b.handleBegin(ev, relation); err != nil {
			return err
		}
		b.current.xaXID = ev.XAXID
		return b.mergeProvenance(ev)
	case "XID", "COMMIT", "ROLLBACK", "XA_PREPARE", "XA_COMMIT", "XA_ROLLBACK":
		if err := b.mergeProvenance(ev); err != nil {
			return err
		}
		b.handleCommit(ev, relation)
	case "XA_END":
		b.accumulateInTxnEvent(ev, relation)
		if err := b.mergeProvenance(ev); err != nil {
			return err
		}
		if b.current != nil && b.current.xaXID != "" {
			b.current.suspendedAtXAEnd = true
			b.current.hasEndBoundary = true
		}
		return nil
	case "ROWS_QUERY":
		// Capture SQL context for next ROWS events in this transaction
		b.handleRowsQuery(ev, relation)
		return b.mergeProvenance(ev)
	case "ROWS":
		b.accumulateRowEvent(ev, relation)
		return b.mergeProvenance(ev)
	case "TABLE_MAP":
		b.accumulateInTxnEvent(ev, relation)
		return b.mergeProvenance(ev)
	case "DDL", "ADMIN":
		if err := b.mergeProvenance(ev); err != nil {
			return err
		}
		b.accumulateInTxnEvent(ev, relation)
		if b.current != nil && b.current.startedByGTID && !b.current.isExplicit {
			b.current.hasEndBoundary = true
			b.finalizeTransaction()
		}
	case "UNCLASSIFIED_QUERY":
		if isRollbackToSavepoint(ev.QuerySQL) && b.current != nil {
			b.current.sawSavepointRollback = true
		}
		if relation == insideWindow {
			if b.current != nil && b.current.unclassifiedPrefix == "" {
				b.current.unclassifiedPrefix = ev.QuerySQL
			}
			b.accumulateInTxnEvent(ev, relation)
			return b.mergeProvenance(ev)
		}
		if err := b.mergeProvenance(ev); err != nil {
			return err
		}
		b.accumulateInTxnEvent(ev, relation)
		if b.current != nil && b.current.startedByGTID && !b.current.isExplicit {
			b.current.hasEndBoundary = true
			b.finalizeTransaction()
		}
	default:
		// Still record file coverage for in-flight events (Annotate, GTID leftovers, etc.)
		b.accumulateInTxnEvent(ev, relation)
		return b.mergeProvenance(ev)
	}
	return nil
}

// Flush completes any in-flight transaction using its current end time.
func (b *TransactionBuilder) Flush() {
	if b.current != nil {
		b.finalizeTransaction()
	}
}

// Completed returns all completed transactions.
func (b *TransactionBuilder) Completed() []model.Transaction {
	return b.completed
}

// DrainCompleted returns completed transactions accumulated so far and clears the internal buffer.
func (b *TransactionBuilder) DrainCompleted() []model.Transaction {
	if len(b.completed) == 0 {
		return nil
	}
	drained := b.completed
	b.completed = nil
	return drained
}

// UnclassifiedQueryError is returned when a GTID-started non-explicit
// transaction group's only in-window work is an Unclassified QUERY.
type UnclassifiedQueryError struct {
	Prefix string
}

func (e *UnclassifiedQueryError) Error() string {
	if e == nil || e.Prefix == "" {
		return "Unclassified QUERY"
	}
	return "Unclassified QUERY: " + e.Prefix
}

func unclassifiedQueryError(prefix string) error {
	return &UnclassifiedQueryError{Prefix: prefix}
}

const (
	openBeginWithoutCloseMessage = "open BEGIN without close"
	openBeginSavepointMessage    = "open BEGIN without close: ROLLBACK TO SAVEPOINT is not a group close"
	ignoredOnlyGroupMessage      = "Ignored QUERY does not close the transaction group; this is not a missing COMMIT"
)

// OpenBeginError is an intentional exit-1 failure: the next GTID arrived
// while a BEGIN group had no COMMIT and no plain ROLLBACK. The group stays open.
// When that group wrote row images, Rows, Tables, Duration, and Location name the open DML span.
type OpenBeginError struct {
	SavepointRollback bool
	Rows              int
	Tables            string
	Duration          time.Duration
	Location          string
}

func (e *OpenBeginError) Error() string {
	msg := openBeginWithoutCloseMessage
	if e != nil && e.SavepointRollback {
		msg = openBeginSavepointMessage
	}
	if e == nil || e.Rows <= 0 {
		return msg
	}
	return fmt.Sprintf("%s; open DML group still uncommitted in this file/window (not lock-contention proof): dur=%s rows=%d tables=%s file=%s",
		msg, e.Duration.Truncate(time.Millisecond), e.Rows, e.Tables, e.Location)
}

// IgnoredOnlyGroupError is an intentional exit-1 failure: the next GTID
// arrived after a group that held only Ignored QUERY. That QUERY is not a close
// and this is not a missing COMMIT.
type IgnoredOnlyGroupError struct{}

func (e *IgnoredOnlyGroupError) Error() string {
	return ignoredOnlyGroupMessage
}

// NoteIgnoredQuery records that the current group dropped a session-prefix QUERY.
// The event is not consumed and does not close the group.
func (b *TransactionBuilder) NoteIgnoredQuery() {
	if b == nil || b.current == nil {
		return
	}
	b.current.sawIgnoredQuery = true
}

// OpenExplicitGroups counts BEGIN groups flushed without COMMIT or plain ROLLBACK.
func (b *TransactionBuilder) OpenExplicitGroups() int {
	if b == nil {
		return 0
	}
	return b.openExplicitGroups
}

// OpenDMLGroups returns explicit BEGIN groups that wrote row images and were flushed without a close.
func (b *TransactionBuilder) OpenDMLGroups() []model.OpenDMLGroup {
	if b == nil || len(b.openDML) == 0 {
		return nil
	}
	return append([]model.OpenDMLGroup(nil), b.openDML...)
}

func (b *TransactionBuilder) openBeginFailure(conflictAt time.Time) error {
	e := &OpenBeginError{}
	if b == nil || b.current == nil {
		return e
	}
	e.SavepointRollback = b.current.sawSavepointRollback
	if b.current.totalRows <= 0 {
		return e
	}
	e.Rows = b.current.totalRows
	e.Tables = joinedTxnTables(b.current.tables)
	if e.Tables == "" {
		e.Tables = "-"
	}
	start := b.current.startTime
	end := b.current.endTime
	if !conflictAt.IsZero() && (end.IsZero() || conflictAt.After(end)) {
		end = conflictAt
	}
	if !start.IsZero() && !end.Before(start) {
		e.Duration = end.Sub(start)
	}
	e.Location = openSpanLocation(b.current.binlogPathStart, b.current.binlogPathEnd, b.current.positionStart, b.current.positionEnd)
	return e
}

func openDMLGroupFrom(t *inFlightTxn) model.OpenDMLGroup {
	group := model.OpenDMLGroup{
		TxnKey:          t.txnKey,
		GTID:            t.gtid,
		StartTime:       t.startTime,
		EndTime:         t.endTime,
		TotalRows:       t.totalRows,
		Tables:          exportTxnTables(t.tables),
		BinlogPathStart: t.binlogPathStart,
		BinlogPathEnd:   t.binlogPathEnd,
		PositionStart:   t.positionStart,
		PositionEnd:     t.positionEnd,
	}
	if !t.startTime.IsZero() && !t.endTime.Before(t.startTime) {
		group.Duration = t.endTime.Sub(t.startTime)
	}
	return group
}

func joinedTxnTables(tables map[tableIdentity]int) string {
	if len(tables) == 0 {
		return ""
	}
	names := make([]string, 0, len(tables))
	for key := range tables {
		names = append(names, key.String())
	}
	sort.Strings(names)
	const maxNames = 8
	if len(names) > maxNames {
		return strings.Join(names[:maxNames], ",") + fmt.Sprintf(",+%d", len(names)-maxNames)
	}
	return strings.Join(names, ",")
}

func openSpanLocation(pathStart, pathEnd string, start, end int64) string {
	switch {
	case pathStart != "" && pathEnd != "" && pathStart == pathEnd && start != 0 && end != 0:
		return fmt.Sprintf("%s:%d-%d", pathStart, start, end)
	case pathStart != "" && start != 0 && end != 0:
		return fmt.Sprintf("%s:%d-%d", pathStart, start, end)
	case pathStart != "":
		return pathStart
	case start != 0 || end != 0:
		return fmt.Sprintf("%d-%d", start, end)
	default:
		return "N/A"
	}
}

func (b *TransactionBuilder) inFlightUnclassifiedError() error {
	if b == nil || b.current == nil || !b.current.unclassifiedOnly() {
		return nil
	}
	return unclassifiedQueryError(b.current.unclassifiedPrefix)
}

func (t *inFlightTxn) unclassifiedOnly() bool {
	return t != nil && t.startedByGTID && !t.isExplicit && t.totalRows == 0 && t.unclassifiedPrefix != ""
}

func (t *inFlightTxn) ignoredOnly() bool {
	return t != nil && t.startedByGTID && !t.isExplicit && t.xaXID == "" && t.totalRows == 0 &&
		t.unclassifiedPrefix == "" && t.sawIgnoredQuery && !t.hasEndBoundary
}

func isRollbackToSavepoint(sql string) bool {
	sql = strings.TrimSpace(sql)
	if strings.HasSuffix(sql, ";") {
		sql = strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	}
	const prefix = "rollback to savepoint"
	if len(sql) < len(prefix) || !strings.EqualFold(sql[:len(prefix)], prefix) {
		return false
	}
	if len(sql) == len(prefix) {
		return true
	}
	switch sql[len(prefix)] {
	case ' ', '\t', '\n', '\r':
		return true
	default:
		return false
	}
}

// retainCompletedTransaction reports whether a closed transaction group belongs
// in AnalysisResult.Transactions. DDL-only groups stay on the DDL timeline.
func retainCompletedTransaction(txn model.Transaction) bool {
	if txn.TotalRows > 0 {
		return true
	}
	if txn.XAXID != "" && txn.BinlogPathStart != "" && txn.PositionEnd > txn.PositionStart {
		return true
	}
	return false
}

// CurrentTxnKey returns the in-flight transaction key, if any.
func (b *TransactionBuilder) CurrentTxnKey() string {
	if b.current == nil {
		return ""
	}
	return b.current.txnKey
}

// LastEventTxnKey returns the transaction that consumed the most recent event,
// including a commit event that finalized it.
func (b *TransactionBuilder) LastEventTxnKey() string {
	return b.lastEventTxnKey
}

func (b *TransactionBuilder) clearCurrentQueryContext() {
	if b.current == nil {
		return
	}
	b.current.querySQL = ""
	b.current.rowOperation = ""
	b.current.queryTruncated = false
	b.current.queryOriginalBytes = 0
}

func (b *TransactionBuilder) handleBegin(ev model.NormalizedEvent, relation windowRelation) error {
	if b.current != nil && b.current.startedByGTID && !b.current.isExplicit {
		b.current.isExplicit = true
		b.current.hasStartBoundary = true
		b.observeEvent(ev, relation)
		return nil
	}
	if b.current != nil && b.current.isExplicit {
		// Explicit transaction already in-flight - this is a boundary error
		// Do NOT mutate state - return error and let caller decide what to do
		return fmt.Errorf("BEGIN received while explicit transaction %s is in-flight", b.current.txnKey)
	}
	// If there's an implicit transaction, complete it with its own end time
	if b.current != nil {
		b.finalizeTransaction()
	}
	// Start a new explicit transaction
	b.startTransaction(true)
	b.current.hasStartBoundary = true
	b.observeEvent(ev, relation)
	return nil
}

func (b *TransactionBuilder) handleGTID(ev model.NormalizedEvent, relation windowRelation) error {
	if b.current != nil {
		if b.current.gtid != "" && ev.GTID != "" && ev.GTID != b.current.gtid && b.current.unclassifiedOnly() {
			return unclassifiedQueryError(b.current.unclassifiedPrefix)
		}
		if b.current.gtid == "" {
			if err := b.inFlightUnclassifiedError(); err != nil {
				return err
			}
		}
		if b.releasesOnGTID(ev) {
			b.finalizeTransaction()
		} else if b.current.gtid == "" && b.current.isExplicit {
			if b.current.xaXID == "" {
				return b.openBeginFailure(ev.Timestamp)
			}
			return fmt.Errorf("GTID received while explicit transaction %s is in-flight", b.current.txnKey)
		} else {
			if err := b.nextGTIDGroupError(ev); err != nil {
				return err
			}
			if err := b.mergeProvenance(ev); err != nil {
				return err
			}
			b.observeEvent(ev, relation)
			return nil
		}
	}
	b.startTransaction(false)
	b.current.startedByGTID = true
	b.current.hasStartBoundary = true
	b.observeEvent(ev, relation)
	return b.mergeProvenance(ev)
}

func (b *TransactionBuilder) nextGTIDGroupError(ev model.NormalizedEvent) error {
	if b.current == nil || b.current.gtid == "" || ev.GTID == "" || ev.GTID == b.current.gtid {
		return nil
	}
	if b.current.isExplicit && b.current.xaXID == "" {
		return b.openBeginFailure(ev.Timestamp)
	}
	if b.current.ignoredOnly() {
		return &IgnoredOnlyGroupError{}
	}
	return nil
}

func (b *TransactionBuilder) releasesOnGTID(ev model.NormalizedEvent) bool {
	if b.current == nil {
		return false
	}
	if b.current.suspendedAtXAEnd {
		sameNamed := b.current.gtid != "" && ev.GTID == b.current.gtid
		return !sameNamed
	}
	return b.current.gtid == "" && !b.current.isExplicit
}

func (b *TransactionBuilder) mergeProvenance(ev model.NormalizedEvent) error {
	if b.current == nil {
		return nil
	}
	if b.current.gtid != "" && ev.GTID != "" && b.current.gtid != ev.GTID {
		return fmt.Errorf("conflicting GTID %q for transaction %s with canonical GTID %q", ev.GTID, b.current.txnKey, b.current.gtid)
	}
	if b.current.serverID == 0 {
		b.current.serverID = ev.ServerID
	}
	if b.current.serverVersion == "" {
		b.current.serverVersion = ev.ServerVersion
	}
	if b.current.serverFlavor == "" {
		b.current.serverFlavor = ev.ServerFlavor
	}
	if b.current.gtid == "" {
		b.current.gtid = ev.GTID
	}
	if b.current.threadID == 0 {
		b.current.threadID = ev.ThreadID
	}
	if b.current.xid == "" {
		b.current.xid = ev.XID
	}
	if b.current.xaXID == "" {
		b.current.xaXID = ev.XAXID
	}
	if b.current.actorUser == "" {
		b.current.actorUser = ev.ActorUser
	}
	if b.current.actorHost == "" {
		b.current.actorHost = ev.ActorHost
	}
	return nil
}

func (b *TransactionBuilder) handleCommit(ev model.NormalizedEvent, relation windowRelation) {
	if b.current == nil {
		return
	}
	b.current.hasEndBoundary = true
	b.observeEvent(ev, relation)
	b.finalizeTransaction()
}

// handleRowsQuery captures SQL context from ROWS_QUERY event.
// The SQL has already been bounded by the normalization layer.
func (b *TransactionBuilder) handleRowsQuery(ev model.NormalizedEvent, relation windowRelation) {
	// If no transaction in flight, start an implicit one
	if b.current == nil {
		b.startTransaction(false)
	}
	b.observeEvent(ev, relation)

	// Capture the SQL context (already bounded at normalize layer)
	b.current.querySQL = ev.QuerySQL
	b.current.rowOperation = ev.Operation
	b.current.queryTruncated = ev.QueryTruncated
	b.current.queryOriginalBytes = ev.QueryOriginalBytes
}

func (b *TransactionBuilder) startTransaction(isExplicit bool) {
	b.current = &inFlightTxn{
		txnKey:     b.generateTxnKey(),
		isExplicit: isExplicit,
		tables:     make(map[tableIdentity]int, 1),
		operations: make(map[string]int, 1),
	}
}

func (b *TransactionBuilder) accumulateInTxnEvent(ev model.NormalizedEvent, relation windowRelation) {
	if b.current == nil {
		return
	}
	b.observeEvent(ev, relation)
}

func (b *TransactionBuilder) accumulateRowEvent(ev model.NormalizedEvent, relation windowRelation) {
	// If no transaction in flight, start an implicit one
	if b.current == nil {
		b.startTransaction(false)
	}
	b.observeEvent(ev, relation)
	if relation != insideWindow {
		return
	}

	b.current.totalRows += ev.RowCount
	b.current.eventCount++
	b.current.retainedQuerySQL = b.current.querySQL
	b.current.retainedQueryTruncated = b.current.queryTruncated
	b.current.retainedQueryOriginalBytes = b.current.queryOriginalBytes

	// Track table: "schema.table"
	if ev.Schema != "" && ev.Table != "" {
		b.current.tables[newTableIdentity(ev.Schema, ev.Table)] += ev.RowCount
	}

	// Track operation
	operation := ev.Operation
	if b.current.rowOperation != "" {
		operation = b.current.rowOperation
	}
	if operation != "" {
		b.current.operations[operation] += ev.RowCount
	}
}

func (b *TransactionBuilder) finalizeTransaction() {
	if b.current == nil {
		return
	}
	openExplicit := b.current.isExplicit && !b.current.hasEndBoundary && b.current.xaXID == ""
	if openExplicit {
		b.openExplicitGroups++
	}

	if start, end, ok := b.current.windowFileSpan.shared(); ok {
		b.current.positionStart = start
		b.current.positionEnd = end
	}
	if openExplicit && b.current.totalRows > 0 {
		b.openDML = append(b.openDML, openDMLGroupFrom(b.current))
	}
	if start, end, ok := b.current.fullFileSpan.shared(); ok {
		b.current.fullPositionStart = start
		b.current.fullPositionEnd = end
		b.current.fullBinlogBytes = end - start
	}

	binlogBytes := b.current.binlogBytes
	if b.current.binlogPathStart != "" &&
		b.current.binlogPathStart == b.current.binlogPathEnd &&
		b.current.positionEnd > b.current.positionStart {
		binlogBytes = b.current.positionEnd - b.current.positionStart
	}

	txn := model.Transaction{
		TxnKey:          b.current.txnKey,
		XAXID:           b.current.xaXID,
		ServerID:        b.current.serverID,
		ServerVersion:   b.current.serverVersion,
		ServerFlavor:    b.current.serverFlavor,
		GTID:            b.current.gtid,
		ThreadID:        b.current.threadID,
		XID:             b.current.xid,
		ActorUser:       b.current.actorUser,
		ActorHost:       b.current.actorHost,
		StartTime:       b.current.startTime,
		EndTime:         b.current.endTime,
		Duration:        b.current.endTime.Sub(b.current.startTime),
		TotalRows:       b.current.totalRows,
		EventCount:      b.current.eventCount,
		BinlogBytes:     binlogBytes,
		BinlogPathStart: b.current.binlogPathStart,
		BinlogPathEnd:   b.current.binlogPathEnd,
		PositionStart:   b.current.positionStart,
		PositionEnd:     b.current.positionEnd,
		Completeness:    b.current.completeness(),
		Tables:          exportTxnTables(b.current.tables),
		Operations:      b.current.operations,
		QuerySummary:    model.FormatQuerySummary(b.current.retainedQuerySQL, b.current.retainedQueryOriginalBytes),
		QueryContext: model.NewQueryContextFromNormalized(
			b.current.retainedQuerySQL,
			b.current.retainedQueryTruncated,
			b.current.retainedQueryOriginalBytes,
		),
	}
	if txn.EffectiveCompleteness() != model.TransactionUnknown &&
		b.current.fullPositionStart > 0 && b.current.fullPositionEnd > b.current.fullPositionStart &&
		b.current.fullBinlogPathStart != "" {
		txn.FullReplaySpan = &model.TransactionReplaySpan{
			BinlogPathStart: b.current.fullBinlogPathStart,
			BinlogPathEnd:   b.current.fullBinlogPathEnd,
			PositionStart:   b.current.fullPositionStart,
			PositionEnd:     b.current.fullPositionEnd,
			BinlogBytes:     b.current.fullBinlogBytes,
		}
	}

	b.completed = append(b.completed, txn)
	b.current = nil
}

func exportTxnTables(src map[tableIdentity]int) map[string]int {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]int, len(src))
	for key, rows := range src {
		dst[key.String()] = rows
	}
	return dst
}

func (b *TransactionBuilder) generateTxnKey() string {
	id := atomic.AddUint64(&b.txnCounter, 1)
	return "txn-" + strconv.FormatUint(id, 10)
}

func (b *TransactionBuilder) updateBinlogCoverage(ev model.NormalizedEvent) {
	if b.current == nil {
		return
	}
	b.current.binlogBytes += ev.BinlogBytes

	if b.current.binlogPathStart == "" && ev.BinlogPath != "" {
		b.current.binlogPathStart = ev.BinlogPath
	}
	if b.current.positionStart == 0 && ev.PositionStart != 0 {
		b.current.positionStart = ev.PositionStart
	}
	if ev.BinlogPath != "" {
		b.current.binlogPathEnd = ev.BinlogPath
	}
	if ev.PositionEnd != 0 {
		b.current.positionEnd = ev.PositionEnd
	}
	b.current.windowFileSpan.note(ev.PositionStart, ev.PositionEnd)
}

func (b *TransactionBuilder) observeEvent(ev model.NormalizedEvent, relation windowRelation) {
	if b.current == nil {
		return
	}
	b.lastEventTxnKey = b.current.txnKey
	switch relation {
	case beforeWindow:
		b.current.hadBeforeWindow = true
	case afterWindow:
		b.current.hadAfterWindow = true
	case outsideBoth:
		b.current.hadBeforeWindow = true
		b.current.hadAfterWindow = true
	case insideWindow:
		// MySQL writes the GTID event first and stamps it at commit, same as the XID.
		// BEGIN and row events keep their statement start, so the wall span is earliest to latest.
		if !ev.Timestamp.IsZero() {
			if b.current.startTime.IsZero() || ev.Timestamp.Before(b.current.startTime) {
				b.current.startTime = ev.Timestamp
			}
			if ev.Timestamp.After(b.current.endTime) {
				b.current.endTime = ev.Timestamp
			}
		}
		b.updateBinlogCoverage(ev)
	}
	b.current.fullBinlogBytes += ev.BinlogBytes
	if b.current.fullBinlogPathStart == "" && ev.BinlogPath != "" {
		b.current.fullBinlogPathStart = ev.BinlogPath
	}
	if b.current.fullPositionStart == 0 && ev.PositionStart != 0 {
		b.current.fullPositionStart = ev.PositionStart
	}
	if ev.BinlogPath != "" {
		b.current.fullBinlogPathEnd = ev.BinlogPath
	}
	if ev.PositionEnd != 0 {
		b.current.fullPositionEnd = ev.PositionEnd
	}
	b.current.fullFileSpan.note(ev.PositionStart, ev.PositionEnd)
}

// repeatedFileSpan records a file range that more than one event shared.
// Expanded transaction-payload inners all carry the wrapper's [start, end).
type repeatedFileSpan struct {
	start int64
	end   int64
	hits  int
}

func (s *repeatedFileSpan) note(evStart, evEnd int64) {
	if evStart <= 0 || evEnd <= evStart {
		return
	}
	if s.hits > 0 && s.start == evStart && s.end == evEnd {
		s.hits++
		return
	}
	if s.hits >= 2 {
		return
	}
	s.start, s.end, s.hits = evStart, evEnd, 1
}

func (s repeatedFileSpan) shared() (start, end int64, ok bool) {
	if s.hits < 2 || s.start <= 0 || s.end <= s.start {
		return 0, 0, false
	}
	return s.start, s.end, true
}

func (t *inFlightTxn) completeness() model.TransactionCompleteness {
	if !t.hasStartBoundary || !t.hasEndBoundary {
		return model.TransactionUnknown
	}
	switch {
	case t.hadBeforeWindow && t.hadAfterWindow:
		return model.TransactionPartialBoth
	case t.hadBeforeWindow:
		return model.TransactionPartialStart
	case t.hadAfterWindow:
		return model.TransactionPartialEnd
	default:
		return model.TransactionComplete
	}
}
