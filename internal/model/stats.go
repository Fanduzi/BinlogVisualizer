// Package model defines table and workload statistics shared by analysis outputs.
// input: aggregated row counts, event counts, per-minute activity, and SQL context bounds.
// output: table, minute, and summary structs consumed by renderers and follow-on analysis.
// pos: shared statistics model layer between analyzer aggregation and report rendering.
// note: if this file changes, keep internal/model/README.md synchronized.
package model

import "time"

// TableActivityPoint holds per-minute activity for a specific table.
type TableActivityPoint struct {
	Minute      time.Time
	Rows        int
	InsertRows  int
	UpdateRows  int
	DeleteRows  int
	EventCount  int
	BinlogBytes int64
	DDLCount    int
}

// TableStats holds per-table write statistics.
type TableStats struct {
	Schema        string
	Table         string
	TotalRows     int
	InsertRows    int
	UpdateRows    int
	DeleteRows    int
	TxnCount      int
	EventCount    int
	BinlogBytes   int64
	DDLCount      int
	LastChangedAt time.Time
	Activity      []TableActivityPoint
}

// MinuteBucket holds aggregated activity for a single minute.
type MinuteBucket struct {
	Minute      time.Time
	TotalRows   int
	TxnCount    int
	EventCount  int
	BinlogBytes int64
	DDLCount    int
	TableRows   map[string]int
}
