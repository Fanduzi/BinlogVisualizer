package model

import "time"

// WorkloadSummary provides high-level analysis statistics.
type WorkloadSummary struct {
	TotalTransactions int
	TotalRows         int
	TotalEvents       int
	StartTime         time.Time
	EndTime           time.Time
	Duration          time.Duration
}

// Alert represents a detected anomaly or threshold breach.
type Alert struct {
	Type     string // "large_transaction", "spike"
	Severity string // "info", "warning", "critical"
	Message  string
	TxnKey   string
	Minute   time.Time
	Details  map[string]any // supports int, float64, string, time.Duration, etc.
}

// Snapshot describes the top-level analyze report envelope.
type Snapshot struct {
	Name             string
	Label            string
	CreatedAt        time.Time
	BinlogvizVersion string
	InputMode        string
	Input            SnapshotInput
	Window           SnapshotWindow
	Filters          SnapshotFilters
}

// SnapshotInput captures the command input that produced the analyze result.
type SnapshotInput struct {
	Files   []string
	FromDir string
	Prefix  string
}

// SnapshotWindow captures the analyzed time range.
type SnapshotWindow struct {
	StartTime time.Time
	EndTime   time.Time
}

// SnapshotFilters captures the applied schema/table filters.
type SnapshotFilters struct {
	IncludeSchemas []string
	ExcludeSchemas []string
	IncludeTables  []string
	ExcludeTables  []string
}

// AnalysisResult is the complete output of binlog analysis.
type AnalysisResult struct {
	Summary      WorkloadSummary
	Tables       []TableStats
	Transactions []Transaction
	Patterns     []PatternStats
	Minutes      []MinuteBucket
	Alerts       []Alert
	Warnings     int
	Snapshot     *Snapshot
}
