// Package analyzer reconstructs transaction boundaries and completed transaction snapshots.
// input: ordered normalized events that carry producer/transaction provenance, MySQL and MariaDB XA boundaries, and ROWS/ROWS_QUERY semantics.
// output: completed model.Transaction values with canonical provenance, optional XA identity, deterministic txn keys, and filter-safe query context.
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
	current    *inFlightTxn
	completed  []model.Transaction
	txnCounter uint64
}

type inFlightTxn struct {
	txnKey             string
	xaXID              string
	serverID           uint32
	serverVersion      string
	serverFlavor       string
	gtid               string
	threadID           uint32
	xid                string
	actorUser          string
	actorHost          string
	startedByGTID      bool
	isExplicit         bool // true if started with BEGIN, false if implicit
	startTime          time.Time
	endTime            time.Time
	totalRows          int
	eventCount         int
	binlogBytes        int64
	binlogPathStart    string
	binlogPathEnd      string
	positionStart      int64
	positionEnd        int64
	tables             map[tableIdentity]int
	operations         map[string]int
	rowOperation       string
	querySQL           string // Bounded SQL from ROWS_QUERY event
	queryTruncated     bool
	queryOriginalBytes int // Original SQL byte count before truncation
}

// NewTransactionBuilder creates a new TransactionBuilder.
func NewTransactionBuilder() *TransactionBuilder {
	return &TransactionBuilder{
		completed: make([]model.Transaction, 0),
	}
}

// Consume processes a normalized event and updates transaction state.
func (b *TransactionBuilder) Consume(ev model.NormalizedEvent) error {
	switch ev.EventType {
	case "GTID":
		return b.handleGTID(ev)
	case "BEGIN":
		if err := b.handleBegin(ev); err != nil {
			return err
		}
		return b.mergeProvenance(ev)
	case "XA_START":
		if err := b.handleBegin(ev); err != nil {
			return err
		}
		b.current.xaXID = ev.XAXID
		return b.mergeProvenance(ev)
	case "XID", "COMMIT", "XA_PREPARE", "XA_COMMIT":
		if err := b.mergeProvenance(ev); err != nil {
			return err
		}
		b.handleCommit(ev)
	case "ROWS_QUERY":
		// Capture SQL context for next ROWS events in this transaction
		b.handleRowsQuery(ev)
		return b.mergeProvenance(ev)
	case "ROWS":
		b.accumulateRowEvent(ev)
		return b.mergeProvenance(ev)
	case "TABLE_MAP":
		b.accumulateInTxnEvent(ev)
		return b.mergeProvenance(ev)
	default:
		// Still record file coverage for in-flight events (Annotate, GTID leftovers, etc.)
		b.accumulateInTxnEvent(ev)
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

func (b *TransactionBuilder) clearCurrentQueryContext() {
	if b.current == nil {
		return
	}
	b.current.querySQL = ""
	b.current.rowOperation = ""
	b.current.queryTruncated = false
	b.current.queryOriginalBytes = 0
}

func (b *TransactionBuilder) handleBegin(ev model.NormalizedEvent) error {
	if b.current != nil && b.current.startedByGTID && !b.current.isExplicit {
		b.current.isExplicit = true
		b.updateBinlogCoverage(ev)
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
	b.startTransaction(ev.Timestamp, true)
	b.updateBinlogCoverage(ev)
	return nil
}

func (b *TransactionBuilder) handleGTID(ev model.NormalizedEvent) error {
	if b.current != nil {
		if b.current.gtid != "" {
			if err := b.mergeProvenance(ev); err != nil {
				return err
			}
			b.updateBinlogCoverage(ev)
			return nil
		}
		if b.current.isExplicit {
			return fmt.Errorf("GTID received while explicit transaction %s is in-flight", b.current.txnKey)
		}
		b.finalizeTransaction()
	}
	b.startTransaction(ev.Timestamp, false)
	b.current.startedByGTID = true
	b.updateBinlogCoverage(ev)
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

func (b *TransactionBuilder) handleCommit(ev model.NormalizedEvent) {
	if b.current == nil {
		return
	}
	if ev.EventType == "XA_COMMIT" && b.current.startedByGTID && b.current.totalRows == 0 {
		b.updateBinlogCoverage(ev)
		b.current = nil
		return
	}
	// For explicit transactions, use COMMIT/XID timestamp as end time
	// For implicit transactions (shouldn't normally get here), use current end time
	if b.current.isExplicit {
		b.current.endTime = ev.Timestamp
	}
	b.updateBinlogCoverage(ev)
	b.finalizeTransaction()
}

// handleRowsQuery captures SQL context from ROWS_QUERY event.
// The SQL has already been bounded by the normalization layer.
func (b *TransactionBuilder) handleRowsQuery(ev model.NormalizedEvent) {
	// If no transaction in flight, start an implicit one
	if b.current == nil {
		b.startTransaction(ev.Timestamp, false)
	}
	b.updateBinlogCoverage(ev)

	// Capture the SQL context (already bounded at normalize layer)
	b.current.querySQL = ev.QuerySQL
	b.current.rowOperation = ev.Operation
	b.current.queryTruncated = ev.QueryTruncated
	b.current.queryOriginalBytes = ev.QueryOriginalBytes
}

func (b *TransactionBuilder) startTransaction(ts time.Time, isExplicit bool) {
	b.current = &inFlightTxn{
		txnKey:     b.generateTxnKey(),
		isExplicit: isExplicit,
		startTime:  ts,
		endTime:    ts,
		tables:     make(map[tableIdentity]int, 1),
		operations: make(map[string]int, 1),
	}
}

func (b *TransactionBuilder) accumulateInTxnEvent(ev model.NormalizedEvent) {
	if b.current == nil {
		return
	}
	b.updateBinlogCoverage(ev)
}

func (b *TransactionBuilder) accumulateRowEvent(ev model.NormalizedEvent) {
	// If no transaction in flight, start an implicit one
	if b.current == nil {
		b.startTransaction(ev.Timestamp, false)
	}

	b.current.totalRows += ev.RowCount
	b.current.eventCount++
	b.current.endTime = ev.Timestamp
	b.updateBinlogCoverage(ev)

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
		Tables:          exportTxnTables(b.current.tables),
		Operations:      b.current.operations,
		QuerySummary:    model.MakeQuerySummary(b.current.querySQL),
		QueryContext: model.NewQueryContextFromNormalized(
			b.current.querySQL,
			b.current.queryTruncated,
			b.current.queryOriginalBytes,
		),
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
