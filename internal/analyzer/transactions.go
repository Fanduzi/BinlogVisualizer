// Package analyzer reconstructs transaction boundaries and completed transaction snapshots.
// input: ordered normalized events with provenance, intersected window relation, MySQL/MariaDB XA and DDL boundaries, and ROWS/ROWS_QUERY semantics.
// output: retained transaction evidence with canonical provenance, explicit completeness, trusted replay spans, isolated DDL GTID groups, XA identity, and safe query context.
// pos: live transaction state machine used by Analyzer before completed transactions are flushed to the result store.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"binlogviz/internal/model"
)

// TransactionBuilder reconstructs transactions from normalized events.
type TransactionBuilder struct {
	current         *inFlightTxn
	completed       []model.Transaction
	txnCounter      uint64
	lastEventTxnKey string
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
	tables                     map[tableIdentity]int
	operations                 map[string]int
	rowOperation               string
	querySQL                   string // Bounded SQL from ROWS_QUERY event
	queryTruncated             bool
	queryOriginalBytes         int // Original SQL byte count before truncation
	retainedQuerySQL           string
	retainedQueryTruncated     bool
	retainedQueryOriginalBytes int
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
	case "XID", "COMMIT", "XA_PREPARE", "XA_COMMIT":
		if err := b.mergeProvenance(ev); err != nil {
			return err
		}
		b.handleCommit(ev, relation)
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
	case "DDL":
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
		if b.current.gtid != "" {
			if err := b.mergeProvenance(ev); err != nil {
				return err
			}
			b.observeEvent(ev, relation)
			return nil
		}
		if b.current.isExplicit {
			return fmt.Errorf("GTID received while explicit transaction %s is in-flight", b.current.txnKey)
		}
		b.finalizeTransaction()
	}
	b.startTransaction(false)
	b.current.startedByGTID = true
	b.current.hasStartBoundary = true
	b.observeEvent(ev, relation)
	return b.mergeProvenance(ev)
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
	if ev.EventType == "XA_COMMIT" && b.current.startedByGTID && b.current.totalRows == 0 {
		b.observeEvent(ev, relation)
		b.current = nil
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
		QuerySummary:    model.MakeQuerySummary(b.current.retainedQuerySQL),
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
		if b.current.startTime.IsZero() {
			b.current.startTime = ev.Timestamp
		}
		b.current.endTime = ev.Timestamp
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
