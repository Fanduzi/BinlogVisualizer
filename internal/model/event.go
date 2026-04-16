// Package model defines normalized event contracts shared across parsing and analysis.
// input: parsed binlog metadata and bounded SQL context captured from the binlog layer.
// output: NormalizedEvent values reused by analyzer and downstream report builders.
// pos: shared model boundary between internal/binlog and internal/analyzer.
// note: if this file changes, keep internal/model/README.md synchronized.
package model

import "time"

// NormalizedEvent represents a parsed binlog event normalized for analysis.
type NormalizedEvent struct {
	Timestamp          time.Time
	BinlogPath         string
	PositionStart      int64
	PositionEnd        int64
	BinlogBytes        int64
	EventType          string
	TxnKey             string
	Schema             string
	Table              string
	Operation          string // INSERT, UPDATE, DELETE
	RowCount           int
	QuerySQL           string // Original SQL from Rows_query_log_event (bounded)
	QueryTruncated     bool   // True if QuerySQL was truncated
	QueryOriginalBytes int    // Original SQL byte count before truncation
}
