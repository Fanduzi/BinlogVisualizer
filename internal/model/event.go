// Package model defines normalized event contracts shared across parsing and analysis.
// input: parsed binlog metadata, producer/transaction provenance, XA identity, and bounded SQL context captured from the binlog layer.
// output: NormalizedEvent values with stable provenance, optional MySQL 8 commit timestamps, optional flashback rows, and transaction/operation semantics reused by analyzer and downstream report builders.
// pos: shared model boundary between internal/binlog and internal/analyzer.
// note: if this file changes, keep internal/model/README.md synchronized.
package model

import "time"

// NormalizedEvent represents a parsed binlog event normalized for analysis.
type NormalizedEvent struct {
	Timestamp     time.Time
	BinlogPath    string
	PositionStart int64
	PositionEnd   int64
	BinlogBytes   int64
	EventType     string
	TxnKey        string
	XAXID         string
	ServerID      uint32
	ServerVersion string
	ServerFlavor  string
	// SQLMode is the query event's sql_mode (Q_SQL_MODE_CODE, 8 little-endian
	// bytes). Zero means the event did not carry it. ANSI_QUOTES is bit 4.
	SQLMode uint64
	GTID    string
	// OriginalCommitUs and ImmediateCommitUs are MySQL 8 GTID commit timestamps
	// in microseconds since the Unix epoch. Zero means the event did not carry them.
	OriginalCommitUs  uint64
	ImmediateCommitUs uint64
	ThreadID          uint32
	XID               string
	ActorUser         string
	ActorHost         string
	Schema            string
	Table             string
	// QuerySchema is the default database of a QUERY event. The analyzer
	// keeps it when a DDL's first qualified name replaces Schema, so an
	// unqualified name later in the statement still resolves (ADR 0015).
	QuerySchema        string
	Operation          string // INSERT, UPDATE, DELETE, LOAD_DATA
	RowCount           int
	KeyStatus          string // has_pk, no_pk, or unknown; set on ROW events from TABLE_MAP metadata
	QuerySQL           string // Original SQL from Rows_query_log_event (bounded)
	QueryTruncated     bool   // True if QuerySQL was truncated
	QueryOriginalBytes int    // Original SQL byte count before truncation
	RowImages          []RowImage
	RowImagesOmitted   int
	// FlashRows is set only when flashback capture is on. Nil otherwise.
	FlashRows []FlashRow
	// RedoRows is set only when redo capture is on. Nil otherwise.
	RedoRows []RedoRow
	// RowKeys are primary-key identities for UPDATE and DELETE images when
	// FULL metadata named the key. Empty means the key is not known. Never
	// filled from column @1.
	RowKeys []string
	// TxnGTID, TxnStartPath, and TxnStartPos are set by the analyzer from the
	// open transaction group. The parser leaves them empty. TxnStartPos is
	// the group's first event (the GTID event when one exists).
	TxnGTID      string
	TxnStartPath string
	TxnStartPos  int64
	// HoldingGTID, HoldingStartPath, and HoldingStartPos are set by the
	// analyzer on a DDL event from the open transaction group. The parser
	// leaves them empty. HoldingStartPos is the group's first event (the
	// GTID event when one exists).
	HoldingGTID      string
	HoldingStartPath string
	HoldingStartPos  int64
}
