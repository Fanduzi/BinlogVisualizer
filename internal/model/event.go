package model

import "time"

// NormalizedEvent represents a parsed binlog event normalized for analysis.
type NormalizedEvent struct {
	Timestamp  time.Time
	EventType  string
	TxnKey     string
	Schema     string
	Table      string
	Operation  string // INSERT, UPDATE, DELETE
	RowCount   int
}
