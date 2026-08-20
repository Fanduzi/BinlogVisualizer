// Package analyzer reconstructs transaction boundaries and completed transaction snapshots.
// input: ordered normalized events that carry BEGIN/COMMIT/XID/ROWS/ROWS_QUERY transaction semantics.
// output: completed model.Transaction values plus deterministic txn keys for downstream aggregation and persistence.
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
	case "BEGIN":
		return b.handleBegin(ev)
	case "XID", "COMMIT":
		b.handleCommit(ev)
	case "ROWS_QUERY":
		// Capture SQL context for next ROWS events in this transaction
		b.handleRowsQuery(ev)
	case "ROWS":
		b.accumulateRowEvent(ev)
	case "TABLE_MAP":
		b.accumulateInTxnEvent(ev)
	default:
		// Still record file coverage for in-flight events (Annotate, GTID leftovers, etc.)
		b.accumulateInTxnEvent(ev)
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

func (b *TransactionBuilder) handleBegin(ev model.NormalizedEvent) error {
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

func (b *TransactionBuilder) handleCommit(ev model.NormalizedEvent) {
	if b.current == nil {
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
	if ev.Operation != "" {
		b.current.operations[ev.Operation] += ev.RowCount
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
