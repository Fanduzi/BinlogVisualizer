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
		// Start a new explicit transaction
		b.startTransaction(ev.Timestamp)
	case "XID", "COMMIT":
		// Complete the current transaction
		b.completeTransaction(ev.Timestamp)
	case "ROWS":
		// Accumulate row event into current transaction
		b.accumulateRowEvent(ev)
	default:
		// Ignore other event types (TABLE_MAP, etc.)
	}
	return nil
}

// Flush completes any in-flight implicit transaction.
func (b *TransactionBuilder) Flush() {
	if b.current != nil {
		b.completeTransaction(b.current.endTime)
	}
}

// Completed returns all completed transactions.
func (b *TransactionBuilder) Completed() []model.Transaction {
	return b.completed
}

func (b *TransactionBuilder) startTransaction(ts time.Time) {
	// If there's already a transaction in flight, complete it first (implicit)
	if b.current != nil {
		b.completeTransaction(ts)
	}

	b.current = &inFlightTxn{
		txnKey:     b.generateTxnKey(),
		startTime:  ts,
		endTime:    ts,
		tables:     make(map[string]int),
		operations: make(map[string]int),
	}
}

func (b *TransactionBuilder) accumulateRowEvent(ev model.NormalizedEvent) {
	// If no transaction in flight, start an implicit one
	if b.current == nil {
		b.startTransaction(ev.Timestamp)
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

func (b *TransactionBuilder) completeTransaction(endTime time.Time) {
	if b.current == nil {
		return
	}

	b.current.endTime = endTime

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
