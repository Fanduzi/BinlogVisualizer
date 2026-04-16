// Package model defines reconstructed transaction contracts and bounded SQL context.
// input: transaction-boundary events plus optional normalized SQL metadata from binlog parsing.
// output: Transaction and QueryContext types reused by analyzer, report, and diagnostics code.
// pos: shared transaction model layer between analyzer reconstruction and renderer output.
// note: if this file changes, keep internal/model/README.md synchronized.
package model

import "time"

// Constants for SQL context limits
const (
	// MaxStoredSQLBytes is the maximum bytes of SQL stored in memory.
	// SQL exceeding this limit is truncated.
	MaxStoredSQLBytes = 4096

	// MaxQuerySummaryChars is the maximum characters shown in QuerySummary.
	MaxQuerySummaryChars = 160
)

// QueryContext holds bounded SQL context from Rows_query_log_event.
// SQL is truncated if it exceeds MaxStoredSQLBytes.
type QueryContext struct {
	SQL           string // Truncated SQL (max 4096 bytes)
	Truncated     bool   // True if SQL was truncated
	OriginalBytes int    // Original SQL length in bytes before truncation
}

// Transaction represents a reconstructed database transaction.
type Transaction struct {
	TxnKey          string
	StartTime       time.Time
	EndTime         time.Time
	Duration        time.Duration
	TotalRows       int
	EventCount      int
	BinlogBytes     int64
	BinlogPathStart string
	BinlogPathEnd   string
	PositionStart   int64
	PositionEnd     int64
	Tables          map[string]int
	Operations      map[string]int
	QuerySummary    string        // Bounded summary of triggering SQL (max 160 chars)
	QueryContext    *QueryContext // Full context if available, nil otherwise
}
