// Package model defines normalized event contracts shared across parsing and analysis.
// input: parsed binlog metadata, producer/transaction provenance, XA identity, and bounded SQL context captured from the binlog layer.
// output: NormalizedEvent values with stable provenance and transaction/operation semantics reused by analyzer and downstream report builders.
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
	XAXID              string
	ServerID           uint32
	ServerVersion      string
	ServerFlavor       string
	GTID               string
	ThreadID           uint32
	XID                string
	ActorUser          string
	ActorHost          string
	Schema             string
	Table              string
	Operation          string // INSERT, UPDATE, DELETE, LOAD_DATA
	RowCount           int
	QuerySQL           string // Original SQL from Rows_query_log_event (bounded)
	QueryTruncated     bool   // True if QuerySQL was truncated
	QueryOriginalBytes int    // Original SQL byte count before truncation
}
