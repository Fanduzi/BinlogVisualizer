package model

import "time"

// NormalizedEvent represents a parsed binlog event normalized for analysis.
type NormalizedEvent struct {
	Timestamp         time.Time
	EventType         string
	TxnKey            string
	Schema            string
	Table             string
	Operation         string // INSERT, UPDATE, DELETE
	RowCount          int
	QuerySQL          string // Original SQL from Rows_query_log_event (bounded)
	QueryTruncated    bool   // True if QuerySQL was truncated
	QueryOriginalBytes int    // Original SQL byte count before truncation
}
