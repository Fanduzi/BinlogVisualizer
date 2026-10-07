// Package model defines the hot-row ranking shared by analysis and report output.
// input: primary-key identities taken from MySQL 8 FULL row metadata, not from column position.
// output: bounded HotRow rankings plus tables whose key could not be read.
// pos: shared result-model layer between analyzer aggregation and report rendering.
// note: if this file changes, keep internal/model/README.md synchronized.
package model

import "time"

const (
	// HotRowReasonMetadata means binlog_row_metadata was not FULL, so the
	// primary key columns are not known. Do not guess them from @1.
	HotRowReasonMetadata = "metadata"
	// HotRowReasonValues means FULL metadata named a primary key, but the
	// row image did not contain those columns.
	HotRowReasonValues = "values"
)

// HotRow is one primary key touched by UPDATE or DELETE row images.
// PrimaryKey is schema-qualified by Schema and Table. Touches is the number
// of row images. Transactions is how many distinct transactions touched it.
// First and last identify the transaction, not the row-event byte: GTID when
// the binlog has one, and the file:byte where that transaction starts.
// Approximate is set when this key replaced another key after the tracking
// table filled; Touches may include touches that belonged to the dropped key.
type HotRow struct {
	Schema       string
	Table        string
	PrimaryKey   string
	Touches      int
	Transactions int
	FirstTime    time.Time
	LastTime     time.Time
	FirstGTID    string
	FirstFile    string
	FirstPos     int64
	LastGTID     string
	LastFile     string
	LastPos      int64
	Approximate  bool
}

// HotRowGap is a table that had UPDATE or DELETE images and no usable primary key.
// No-primary-key tables are not listed here; they stay in the no-primary-key section.
type HotRowGap struct {
	Schema string
	Table  string
	Reason string
}

// HotRowReport is the bounded hot-row ranking for one analyze window.
// TrackLimit is the most primary keys kept in memory. Overflow means a new
// key replaced the least-touched key.
type HotRowReport struct {
	Rows        []HotRow
	Unavailable []HotRowGap
	TrackLimit  int
	Overflow    bool
}
