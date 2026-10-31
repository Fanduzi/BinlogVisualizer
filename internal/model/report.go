// Package model defines top-level analyze report contracts and snapshot metadata.
// input: aggregated analyzer output, explicit workload identity, canonical scope, producer provenance, selector evidence, completeness summaries, diagnostics, tables, transactions, and filters.
// output: identity- and completeness-aware AnalysisResult, AnalysisSelection, ReportProvenance, and snapshot envelopes reused by report and compare modules.
// pos: shared result-model layer between analyzer finalization and renderer or loader pipelines.
// note: if this file changes, update this header and module README.md.
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
	Type     string // "large_transaction", "spike", "input_format", "partial_transaction", "unknown_transaction", "open_dml_group", "no_primary_key"
	Severity string // "info", "warning", "critical"
	Message  string
	TxnKey   string
	// GTID is the transaction's GTID when this alert names one and the binlog has one.
	// Empty for anonymous groups and for alerts that are not about one transaction.
	GTID    string
	Minute  time.Time
	Details map[string]any // supports int, float64, string, time.Duration, etc.
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
	IncludeDML     []string // INSERT, UPDATE, DELETE; empty means every DML kind
}

// AnalysisSelection records requested selectors and the retained event evidence they matched.
type AnalysisSelection struct {
	RequestedStartPosition *int64
	RequestedStopPosition  *int64
	EffectiveStartPosition *int64
	EffectiveStopPosition  *int64
	IncludeGTIDs           []string
	ExcludeGTIDs           []string
	ResolvedGTIDFlavor     string
	// MatchedGTIDSet is the matched GTIDs as an interval set (#379): one
	// MySQL GTID set per server UUID, or one MariaDB entry per range.
	MatchedGTIDSet []string
	// MatchedTransactions counts matched transaction groups with at least one
	// event in the window, once per group read (repeats and row-less groups
	// included); nil without GTID selectors.
	MatchedTransactions *int
	// ThreadIDs is the requested --thread-id set (ADR 0027): sorted,
	// deduplicated binlog connection ids. nil without the thread selector.
	ThreadIDs []uint32
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
	WorkloadID          string
	Scope               *SnapshotFilters
	Summary             WorkloadSummary
	Provenance          ReportProvenance
	Selection           *AnalysisSelection
	SQLContextAvailable bool
	Timeseries          Timeseries
	Tables              []TableStats
	Threads             []ThreadStats
	ThreadsRankedBy     string
	HotRows             HotRowReport
	Transactions        []Transaction
	Patterns            []PatternStats
	// StatementFingerprints ranks literal-normalized Rows_query write-statement
	// text across the whole workload (not only listed transactions). Empty when
	// the binlog carried no retained Rows_query text; FingerprintOverflow
	// counts statement occurrences dropped when the group map hit its cap.
	StatementFingerprints []StatementFingerprint
	FingerprintOverflow   int
	// Minutes is the source-of-truth aggregated minute series.
	// Timeseries is the chart-ready projection derived from Minutes and may omit raw table context.
	Minutes []MinuteBucket
	// MinuteTables is set when the per-(table, minute) series was spilled
	// instead of inlined onto Minutes and Tables. Reports read it from
	// here. It is not a JSON field.
	MinuteTables MinuteTableSource
	Diagnostics  Diagnostics
	Alerts       []Alert
	Warnings     int
	Snapshot     *Snapshot

	// PatternDrilldowns holds optional bounded explanations for high-signal patterns.
	// Empty in low-signal windows.
	PatternDrilldowns []PatternDrilldown
}
