// Package model defines DBA-facing diagnostics contracts for analyze reports.
// input: file coverage, DDL events, ranked transactions, findings, guessed input format, and Format Description server version.
// output: Diagnostics and related evidence types reused by report renderers, including filtered event-byte coverage.
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
	ServerVersion         string
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
	BinlogPath    string
	Timestamp     time.Time
	Schema        string
	Table         string
	Operation     string
	Object        string
	Statement     string
	PositionStart int64
	PositionEnd   int64
	BinlogBytes   int64
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
