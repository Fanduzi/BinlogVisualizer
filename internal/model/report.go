// Package model defines top-level analyze report contracts and snapshot metadata.
// input: aggregated analyzer output, producer provenance, completeness summaries, diagnostics, tables, transactions, and filters.
// output: completeness-aware AnalysisResult, ReportProvenance, and snapshot envelopes reused by report and compare modules.
// pos: shared result-model layer between analyzer finalization and renderer or loader pipelines.
// note: if this file changes, keep internal/model/README.md synchronized.
package model

import "time"

// WorkloadSummary provides high-level analysis statistics.
type WorkloadSummary struct {
	TotalTransactions   int
	PartialTransactions int
	UnknownTransactions int
	TotalRows           int
	TotalEvents         int
	StartTime           time.Time
	EndTime             time.Time
	Duration            time.Duration
}

// Alert represents a detected anomaly or threshold breach.
type Alert struct {
	Type     string // "large_transaction", "spike", "input_format", "partial_transaction", "unknown_transaction"
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

// ReportProvenance summarizes the distinct producers observed in a report.
type ReportProvenance struct {
	ServerIDs      []uint32
	ServerVersions []string
	ServerFlavors  []string
	MixedProducers bool
}

// AnalysisResult is the complete output of binlog analysis.
type AnalysisResult struct {
	Summary             WorkloadSummary
	Provenance          ReportProvenance
	SQLContextAvailable bool
	Timeseries          Timeseries
	Tables              []TableStats
	Transactions        []Transaction
	Patterns            []PatternStats
	// Minutes is the source-of-truth aggregated minute series.
	// Timeseries is the chart-ready projection derived from Minutes and may omit raw table context.
	Minutes     []MinuteBucket
	Diagnostics Diagnostics
	Alerts      []Alert
	Warnings    int
	Snapshot    *Snapshot

	// PatternDrilldowns holds optional bounded explanations for high-signal patterns.
	// Empty in low-signal windows.
	PatternDrilldowns []PatternDrilldown
}
