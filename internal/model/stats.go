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

const (
	// KeyStatusHasPK means binlog_row_metadata=FULL named a primary key.
	KeyStatusHasPK = "has_pk"
	// KeyStatusNoPK means FULL metadata was present and named no primary key.
	KeyStatusNoPK = "no_pk"
	// KeyStatusUnknown means the binlog did not carry FULL row metadata.
	KeyStatusUnknown = "unknown"
)

// TableStats holds per-table write statistics.
type TableStats struct {
	Schema        string
	Table         string
	TotalRows     int
	InsertRows    int
	UpdateRows    int
	UpdateEvents  int
	DeleteRows    int
	TxnCount      int
	EventCount    int
	BinlogBytes   int64
	DDLCount      int
	LastChangedAt time.Time
	Activity      []TableActivityPoint
	// KeyStatus is has_pk, no_pk, or unknown for tables that received row events.
	// Empty means the table had no row events.
	KeyStatus string
	// NoPKInsertRows, NoPKUpdateRows, and NoPKDeleteRows count rows whose
	// TABLE_MAP said the table had no primary key.
	NoPKInsertRows int
	NoPKUpdateRows int
	NoPKDeleteRows int
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
