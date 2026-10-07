// Package model defines DBA-facing diagnostics contracts for analyze reports.
// input: file coverage, DDL events, ranked transactions, findings, guessed input format, Ignored QUERY counts, unmapped parser events, open explicit BEGIN groups, and Format Description server version.
// output: Diagnostics and related evidence types reused by report renderers, including filtered event-byte coverage, optional Ignored QUERY counts, an optional open-explicit-group count, open uncommitted DML groups, committed duration buckets, and byte-ranked transactions.
// pos: shared diagnostics model between analyzer Finalize and text/JSON/HTML replay commands.
// note: if this file changes, keep internal/model/README.md synchronized.
package model

import "time"

// Diagnostics groups DBA-oriented evidence and coverage metadata.
type Diagnostics struct {
	FileCoverage FileCoverage
	// CountedEventBytes is the sum of row/DDL event bytes retained after object filtering.
	CountedEventBytes     int64
	DDLEvents             []DDLEvent
	LargestTransactions   []Transaction
	LongestTransactions   []Transaction
	WidestTransactions    []Transaction
	FileSegments          []FileSegment
	HotIntervals          []MinuteBucket
	Findings              []Finding
	InputFormatGuess      string
	IgnoredQueryDMLEvents int
	// IgnoredQueryEvents is how many session-prefix QUERY events analyze dropped on purpose.
	// Distinct from IgnoredQueryDMLEvents (STATEMENT/MIXED Query-DML).
	IgnoredQueryEvents int
	// OpenExplicitGroups counts BEGIN groups flushed at end of input without COMMIT or plain ROLLBACK.
	// A later GTID does not reach this count: that case fails analyze instead of closing the group.
	OpenExplicitGroups int
	// OpenDMLGroups are explicit BEGIN groups that wrote row images and never COMMIT or ROLLBACK in this input.
	// Empty when none. This is an occurrence span, not a lock-wait proof.
	OpenDMLGroups []OpenDMLGroup
	// DurationBuckets counts committed transactions by duration. Empty when none.
	DurationBuckets []DurationBucket
	// LargestByteTransactions ranks committed transactions by binlog bytes. Empty when none.
	LargestByteTransactions []Transaction
	// UnmappedEvents is how many parser events had no canonical kind (ROTATE, etc.).
	UnmappedEvents int
	ServerVersion  string
}

// FileCoverage summarizes which input files were selected or skipped.
type FileCoverage struct {
	Selected []FileCoverageItem
	Skipped  []FileCoverageItem
}

// FileCoverageItem describes one analyzed or skipped binlog file.
type FileCoverageItem struct {
	BinlogPath   string
	Reason       string
	Size         int64
	FirstEventAt time.Time
	LastEventAt  time.Time
}

// DDLEvent captures a single DDL event for diagnostics and timeline rendering.
type DDLEvent struct {
	BinlogPath string
	Timestamp  time.Time
	Schema     string
	Table      string
	Operation  string
	Object     string
	Statement  string
	// StatementTruncated is the 4096-byte store cap, same as query context.
	// StatementOriginalBytes is the whitespace-normalized length before that cap.
	StatementTruncated     bool
	StatementOriginalBytes int
	// PositionStart and PositionEnd are the Query event that holds the DDL.
	PositionStart int64
	PositionEnd   int64
	BinlogBytes   int64
	// GTID is the transaction that holds this DDL. Empty when the binlog has
	// none (GTID_MODE=OFF or an anonymous GTID event).
	GTID string
	// TxnStartPath and TxnStartPos are the file and byte offset where that
	// transaction starts. With a GTID event this is that event's start, not
	// the Query event and not end_log_pos.
	TxnStartPath string
	TxnStartPos  int64
	// ServerID, ThreadID, and the actor are copied from the DDL event when
	// the binlog recorded them. Zero and empty stay omitted.
	ServerID  uint32
	ThreadID  uint32
	ActorUser string
	ActorHost string
}

// OpenDMLNote is the stable JSON wording for an uncommitted row-image group.
const OpenDMLNote = "open DML group still uncommitted in this file/window; not lock-contention proof"

// OpenDMLGroup is an explicit BEGIN group that wrote row images and had no COMMIT or plain ROLLBACK.
type OpenDMLGroup struct {
	TxnKey          string
	GTID            string
	StartTime       time.Time
	EndTime         time.Time
	Duration        time.Duration
	TotalRows       int
	Tables          map[string]int
	BinlogPathStart string
	BinlogPathEnd   string
	PositionStart   int64
	PositionEnd     int64
}

// DurationBucket counts committed transactions in one duration band.
type DurationBucket struct {
	Label    string
	TxnCount int
}

// Finding captures one evidence-backed diagnostic finding.
type Finding struct {
	Kind         string
	Severity     string
	Message      string
	TxnKey       string
	Minute       time.Time
	EvidenceRefs []string
}

// FileSegment describes one contiguous time window of binlog generation activity.
type FileSegment struct {
	StartTime   time.Time
	EndTime     time.Time
	BinlogBytes int64
	Rows        int
	Events      int
}
