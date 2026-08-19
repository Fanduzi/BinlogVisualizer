package model

import "time"

// Diagnostics groups DBA-oriented evidence and coverage metadata.
type Diagnostics struct {
	FileCoverage          FileCoverage
	DDLEvents             []DDLEvent
	LargestTransactions   []Transaction
	LongestTransactions   []Transaction
	WidestTransactions    []Transaction
	FileSegments          []FileSegment
	HotIntervals          []MinuteBucket
	Findings              []Finding
	InputFormatGuess      string
	IgnoredQueryDMLEvents int
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
