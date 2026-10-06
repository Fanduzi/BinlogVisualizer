// Package model defines reconstructed transaction contracts and bounded SQL context.
// input: retained event-window evidence, physical boundaries, producer/transaction provenance, XA identity, and normalized SQL metadata.
// output: provenance-aware Transaction completeness/replay-span and QueryContext contracts reused across analysis and reporting.
// pos: shared transaction model layer between analyzer reconstruction and renderer output.
// note: if this file changes, keep internal/model/README.md synchronized.
package model

import "time"

const maxReplayXIDSpanBytes = 64

// TransactionCompleteness describes whether the retained window contains a
// whole physical transaction or only bounded evidence from one.
type TransactionCompleteness string

const (
	TransactionComplete     TransactionCompleteness = "complete"
	TransactionPartialStart TransactionCompleteness = "partial_start"
	TransactionPartialEnd   TransactionCompleteness = "partial_end"
	TransactionPartialBoth  TransactionCompleteness = "partial_both"
	TransactionUnknown      TransactionCompleteness = "unknown"
)

// TransactionReplaySpan is a physically observed full-transaction span.
type TransactionReplaySpan struct {
	BinlogPathStart string
	BinlogPathEnd   string
	PositionStart   int64
	PositionEnd     int64
	BinlogBytes     int64
}

// Constants for SQL context limits
const (
	// MaxStoredSQLBytes is the maximum bytes of SQL stored in memory.
	// SQL exceeding this limit is truncated.
	MaxStoredSQLBytes = 4096

	// MaxQuerySummaryChars is the maximum characters shown in QuerySummary.
	MaxQuerySummaryChars = 160

	// MaxRowImagesPerTxn is how many logical row changes one transaction keeps
	// when row images are requested. Further rows increment RowImagesOmitted.
	MaxRowImagesPerTxn = 32

	// MaxRowValueBytes is the displayed prefix of one cell value.
	MaxRowValueBytes = 64
)

const (
	// RowNamesFull means column labels came from binlog_row_metadata=FULL.
	RowNamesFull = "full"
	// RowNamesPositional means the binlog had no column names; labels are @1..@N.
	RowNamesPositional = "positional"
)

// RowCell is one decoded column. Null is SQL NULL.
// Text is unquoted: a number, a string, or a bounded hex blob.
type RowCell struct {
	Null bool
	Text string
}

// RowImage is one logical INSERT, UPDATE, or DELETE row kept on a listed transaction.
type RowImage struct {
	Schema  string
	Table   string
	Op      string
	Columns []string
	Names   string // RowNamesFull or RowNamesPositional
	Before  []RowCell
	After   []RowCell
	Changed []string // UPDATE column labels whose before and after differ
}

// QueryContext holds bounded SQL context from Rows_query_log_event.
// SQL is truncated if it exceeds MaxStoredSQLBytes.
type QueryContext struct {
	SQL           string // Truncated SQL (max 4096 bytes)
	Truncated     bool   // True if SQL was truncated
	OriginalBytes int    // Original SQL length in bytes before truncation
}

// Transaction represents a reconstructed database transaction.
type Transaction struct {
	TxnKey           string
	XAXID            string
	ServerID         uint32
	ServerVersion    string
	ServerFlavor     string
	GTID             string
	ThreadID         uint32
	XID              string
	ActorUser        string
	ActorHost        string
	StartTime        time.Time
	EndTime          time.Time
	Duration         time.Duration
	TotalRows        int
	EventCount       int
	BinlogBytes      int64
	BinlogPathStart  string
	BinlogPathEnd    string
	PositionStart    int64
	PositionEnd      int64
	Completeness     TransactionCompleteness
	FullReplaySpan   *TransactionReplaySpan
	Tables           map[string]int
	Operations       map[string]int
	QuerySummary     string        // Bounded summary of triggering SQL (max 160 chars, plus a truncation marker when cut)
	QueryContext     *QueryContext // Full context if available, nil otherwise
	RowImages        []RowImage    // Bounded row images; nil unless capture was requested
	RowImagesOmitted int           // Logical rows not stored after MaxRowImagesPerTxn
	// StdinInput is set when the binlog was read from stdin or another non-seekable
	// stream. The displayed path "stdin" is not a file that mysqlbinlog can open.
	StdinInput bool
}

// EffectiveCompleteness maps missing or invalid metadata to unknown.
func (t Transaction) EffectiveCompleteness() TransactionCompleteness {
	switch t.Completeness {
	case TransactionComplete, TransactionPartialStart, TransactionPartialEnd, TransactionPartialBoth:
		return t.Completeness
	default:
		return TransactionUnknown
	}
}

// IsPartial reports whether a known window boundary clipped the transaction.
func (t Transaction) IsPartial() bool {
	switch t.EffectiveCompleteness() {
	case TransactionPartialStart, TransactionPartialEnd, TransactionPartialBoth:
		return true
	default:
		return false
	}
}

// FullReplayAvailable reports whether the recorded full span can be replayed
// safely with one bounded binlog command.
func (t Transaction) FullReplayAvailable() bool {
	span := t.FullReplaySpan
	if span == nil || span.BinlogPathStart == "" || span.BinlogPathStart != span.BinlogPathEnd {
		return false
	}
	if span.PositionStart <= 0 || span.PositionEnd <= span.PositionStart {
		return false
	}
	spanBytes := span.BinlogBytes
	if spanBytes <= 0 {
		spanBytes = span.PositionEnd - span.PositionStart
	}
	return spanBytes > maxReplayXIDSpanBytes || (t.EventCount <= 1 && t.TotalRows <= 1)
}
