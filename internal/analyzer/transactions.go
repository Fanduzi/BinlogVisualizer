package analyzer

import (
	"fmt"
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
	txnKey     string
	isExplicit bool // true if started with BEGIN, false if implicit
	startTime  time.Time
	endTime    time.Time
	totalRows  int
	eventCount int
	tables     map[string]int
	operations map[string]int
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
		return b.handleBegin(ev.Timestamp)
	case "XID", "COMMIT":
		b.handleCommit(ev.Timestamp)
	case "ROWS":
		b.accumulateRowEvent(ev)
	default:
		// Ignore other event types (TABLE_MAP, etc.)
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

func (b *TransactionBuilder) handleBegin(ts time.Time) error {
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
	b.startTransaction(ts, true)
	return nil
}

func (b *TransactionBuilder) handleCommit(ts time.Time) {
	if b.current == nil {
		return
	}
	// For explicit transactions, use COMMIT/XID timestamp as end time
	// For implicit transactions (shouldn't normally get here), use current end time
	if b.current.isExplicit {
		b.current.endTime = ts
	}
	b.finalizeTransaction()
}

func (b *TransactionBuilder) startTransaction(ts time.Time, isExplicit bool) {
	b.current = &inFlightTxn{
		txnKey:     b.generateTxnKey(),
		isExplicit: isExplicit,
		startTime:  ts,
		endTime:    ts,
		tables:     make(map[string]int),
		operations: make(map[string]int),
	}
}

func (b *TransactionBuilder) accumulateRowEvent(ev model.NormalizedEvent) {
	// If no transaction in flight, start an implicit one
	if b.current == nil {
		b.startTransaction(ev.Timestamp, false)
	}

	b.current.totalRows += ev.RowCount
	b.current.eventCount++
	b.current.endTime = ev.Timestamp

	// Track table: "schema.table"
	if ev.Schema != "" && ev.Table != "" {
		key := fmt.Sprintf("%s.%s", ev.Schema, ev.Table)
		b.current.tables[key] += ev.RowCount
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

	txn := model.Transaction{
		TxnKey:     b.current.txnKey,
		StartTime:  b.current.startTime,
		EndTime:    b.current.endTime,
		Duration:   b.current.endTime.Sub(b.current.startTime),
		TotalRows:  b.current.totalRows,
		EventCount: b.current.eventCount,
		Tables:     b.current.tables,
		Operations: b.current.operations,
	}

	b.completed = append(b.completed, txn)
	b.current = nil
}

func (b *TransactionBuilder) generateTxnKey() string {
	id := atomic.AddUint64(&b.txnCounter, 1)
	return fmt.Sprintf("txn-%d", id)
}
