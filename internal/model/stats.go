package model

import "time"

// TableStats holds per-table write statistics.
type TableStats struct {
	Schema     string
	Table      string
	TotalRows  int
	InsertRows int
	UpdateRows int
	DeleteRows int
	TxnCount   int
}

// MinuteBucket holds aggregated activity for a single minute.
type MinuteBucket struct {
	Minute    time.Time
	TotalRows int
	TxnCount  int
	TableRows map[string]int
}
