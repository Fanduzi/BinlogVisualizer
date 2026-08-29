// Package compare defines compare-input contracts and comparison result models.
// input: report-v0-v3 analyze JSON with optional workload identity/scope, v3 provenance, selector evidence, SQL mode, completeness, replay, and counted-byte evidence.
// output: typed compare inputs/results preserving comparability and selector evidence, known identity, completeness, safe replay, counts, and render-only byte coverage.
// pos: compare pipeline boundary between JSON loading and diff/render stages.
// note: if this file changes, update this header and module README.md.
package compare

type InputReport struct {
	ReportVersion *int                  `json:"report_version,omitempty"`
	WorkloadID    string                `json:"workload_id,omitempty"`
	Scope         *InputSnapshotFilters `json:"scope,omitempty"`
	Provenance    *InputProvenance      `json:"provenance,omitempty"`
	Selection     *InputSelection       `json:"selection,omitempty"`
	SQLContext    *InputSQLContext      `json:"sql_context,omitempty"`
	Summary       InputSummary          `json:"summary"`
	Timeseries    InputTimeseries       `json:"timeseries"`
	Diagnostics   InputDiagnostics      `json:"diagnostics"`
	Tables        []InputTable          `json:"tables"`
	Transactions  []InputTransaction    `json:"transactions"`
	Patterns      []InputPattern        `json:"patterns"`
	Alerts        []InputAlert          `json:"alerts"`
	Warnings      int                   `json:"warnings"`
	Snapshot      *InputSnapshot        `json:"snapshot,omitempty"`
}

type InputSelection struct {
	RequestedStartPosition *int64   `json:"requested_start_position,omitempty"`
	RequestedStopPosition  *int64   `json:"requested_stop_position,omitempty"`
	EffectiveStartPosition *int64   `json:"effective_start_position,omitempty"`
	EffectiveStopPosition  *int64   `json:"effective_stop_position,omitempty"`
	IncludeGTIDs           []string `json:"include_gtids,omitempty"`
	ExcludeGTIDs           []string `json:"exclude_gtids,omitempty"`
	ResolvedGTIDFlavor     string   `json:"resolved_gtid_flavor,omitempty"`
	MatchedGTIDs           []string `json:"matched_gtids,omitempty"`
}

type InputSQLContext struct {
	Mode      string `json:"mode"`
	Available bool   `json:"available"`
}

type InputProvenance struct {
	ServerIDs      []uint32 `json:"server_ids,omitempty"`
	ServerVersions []string `json:"server_versions,omitempty"`
	ServerFlavors  []string `json:"server_flavors,omitempty"`
	MixedProducers bool     `json:"mixed_producers"`
}

type InputSummary struct {
	TotalTransactions   int    `json:"total_transactions"`
	PartialTransactions *int   `json:"partial_transactions,omitempty"`
	UnknownTransactions *int   `json:"unknown_transactions,omitempty"`
	TotalRows           int    `json:"total_rows"`
	TotalEvents         int    `json:"total_events"`
	StartTime           string `json:"start_time"`
	EndTime             string `json:"end_time"`
	Duration            string `json:"duration"`
}

type InputTimeseries struct {
	TPSSeries            []InputTimeseriesPoint    `json:"tps_series"`
	RowsSeries           []InputTimeseriesPoint    `json:"rows_series"`
	EventsSeries         []InputTimeseriesPoint    `json:"events_series"`
	InsertEventSeries    []InputTimeseriesPoint    `json:"insert_event_series"`
	UpdateEventSeries    []InputTimeseriesPoint    `json:"update_event_series"`
	DeleteEventSeries    []InputTimeseriesPoint    `json:"delete_event_series"`
	DDLEventSeries       []InputTimeseriesPoint    `json:"ddl_event_series"`
	BinlogBytesSeries    []InputTimeseriesPoint    `json:"binlog_bytes_series"`
	TxnSizeSeriesSummary InputTxnSizeSeriesSummary `json:"txn_size_series_summary"`
}

type InputTimeseriesPoint struct {
	Minute string  `json:"minute"`
	Value  float64 `json:"value"`
}

type InputTxnSizeSeriesSummary struct {
	Buckets []InputTxnSizeBucket `json:"buckets"`
}

type InputTxnSizeBucket struct {
	Label       string `json:"label"`
	TxnCount    int    `json:"txn_count"`
	Rows        int    `json:"rows"`
	BinlogBytes int64  `json:"binlog_bytes"`
}

type InputDiagnostics struct {
	FileCoverage        InputFileCoverage  `json:"file_coverage"`
	CountedEventBytes   int64              `json:"counted_event_bytes"`
	DDLEvents           []InputDDLEvent    `json:"ddl_events"`
	LargestTransactions []InputTransaction `json:"largest_transactions"`
	LongestTransactions []InputTransaction `json:"longest_transactions"`
	WidestTransactions  []InputTransaction `json:"widest_transactions"`
	HotIntervals        []InputHotInterval `json:"hot_intervals"`
	Findings            []InputFinding     `json:"findings"`
}

type InputFileCoverage struct {
	Selected []InputFileCoverageItem `json:"selected"`
	Skipped  []InputFileCoverageItem `json:"skipped"`
}

type InputFileCoverageItem struct {
	BinlogPath   string `json:"binlog_path"`
	Reason       string `json:"reason,omitempty"`
	Size         int64  `json:"size"`
	FirstEventAt string `json:"first_event_at,omitempty"`
	LastEventAt  string `json:"last_event_at,omitempty"`
}

type InputDDLEvent struct {
	BinlogPath    string `json:"binlog_path,omitempty"`
	Timestamp     string `json:"timestamp"`
	Schema        string `json:"schema,omitempty"`
	Table         string `json:"table,omitempty"`
	Operation     string `json:"operation"`
	Object        string `json:"object,omitempty"`
	Statement     string `json:"statement,omitempty"`
	PositionStart int64  `json:"position_start,omitempty"`
	PositionEnd   int64  `json:"position_end,omitempty"`
	BinlogBytes   int64  `json:"binlog_bytes,omitempty"`
}

type InputTransaction struct {
	TxnKey             string         `json:"txn_key"`
	XAXID              string         `json:"xa_xid,omitempty"`
	ServerID           uint32         `json:"server_id,omitempty"`
	ServerVersion      string         `json:"server_version,omitempty"`
	ServerFlavor       string         `json:"server_flavor,omitempty"`
	GTID               string         `json:"gtid,omitempty"`
	ThreadID           uint32         `json:"thread_id,omitempty"`
	XID                string         `json:"xid,omitempty"`
	Actor              *InputActor    `json:"actor,omitempty"`
	StartTime          string         `json:"start_time"`
	EndTime            string         `json:"end_time"`
	Duration           string         `json:"duration"`
	TotalRows          int            `json:"total_rows"`
	EventCount         int            `json:"event_count"`
	BinlogBytes        int64          `json:"binlog_bytes,omitempty"`
	BinlogFileStart    string         `json:"binlog_file_start,omitempty"`
	BinlogFileEnd      string         `json:"binlog_file_end,omitempty"`
	PosStart           int64          `json:"pos_start,omitempty"`
	PosEnd             int64          `json:"pos_end,omitempty"`
	Completeness       string         `json:"completeness"`
	ReplayAvailable    *bool          `json:"replay_available,omitempty"`
	ReplayScope        string         `json:"replay_scope,omitempty"`
	MysqlbinlogCmd     string         `json:"mysqlbinlog_cmd,omitempty"`
	Tables             map[string]int `json:"tables,omitempty"`
	Operations         map[string]int `json:"operations,omitempty"`
	QuerySummary       string         `json:"query_summary,omitempty"`
	QuerySQL           string         `json:"query_sql,omitempty"`
	QueryTruncated     *bool          `json:"query_truncated,omitempty"`
	QueryOriginalBytes *int           `json:"query_original_bytes,omitempty"`
}

type InputActor struct {
	User string `json:"user,omitempty"`
	Host string `json:"host,omitempty"`
}

type InputHotInterval struct {
	Minute      string         `json:"minute"`
	TotalRows   int            `json:"total_rows"`
	TxnCount    int            `json:"txn_count"`
	EventCount  int            `json:"event_count"`
	BinlogBytes int64          `json:"binlog_bytes"`
	DDLCount    int            `json:"ddl_count"`
	TableRows   map[string]int `json:"table_rows,omitempty"`
}

type InputFinding struct {
	Kind         string   `json:"kind"`
	Severity     string   `json:"severity"`
	Message      string   `json:"message"`
	TxnKey       string   `json:"txn_key,omitempty"`
	Minute       string   `json:"minute,omitempty"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
}

type InputTable struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	TotalRows  int    `json:"total_rows"`
	InsertRows int    `json:"insert_rows"`
	UpdateRows int    `json:"update_rows"`
	DeleteRows int    `json:"delete_rows"`
	TxnCount   int    `json:"txn_count"`
}

type InputPattern struct {
	PatternKey         string         `json:"pattern_key"`
	Label              string         `json:"label"`
	TotalRows          int            `json:"total_rows"`
	TxnCount           int            `json:"txn_count"`
	EventCount         int            `json:"event_count"`
	ShareOfRows        float64        `json:"share_of_rows"`
	ShareOfTxns        float64        `json:"share_of_txns"`
	Tables             map[string]int `json:"tables"`
	Operations         map[string]int `json:"operations"`
	AvgRowsPerTxn      float64        `json:"avg_rows_per_txn"`
	SampleQuerySummary string         `json:"sample_query_summary,omitempty"`
}

type InputAlert struct {
	Type     string         `json:"type"`
	Severity string         `json:"severity"`
	Message  string         `json:"message"`
	TxnKey   string         `json:"txn_key,omitempty"`
	Minute   string         `json:"minute,omitempty"`
	Details  map[string]any `json:"details,omitempty"`
}

type InputSnapshot struct {
	Name             string               `json:"name"`
	Label            string               `json:"label"`
	CreatedAt        string               `json:"created_at"`
	BinlogvizVersion string               `json:"binlogviz_version"`
	InputMode        string               `json:"input_mode"`
	Input            InputSnapshotInput   `json:"input"`
	Window           InputSnapshotWindow  `json:"window"`
	Filters          InputSnapshotFilters `json:"filters"`
}

type InputSnapshotInput struct {
	Files   []string `json:"files"`
	FromDir string   `json:"from_dir"`
	Prefix  string   `json:"prefix"`
}

type InputSnapshotWindow struct {
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
}

type InputSnapshotFilters struct {
	IncludeSchemas []string `json:"include_schema"`
	ExcludeSchemas []string `json:"exclude_schema"`
	IncludeTables  []string `json:"include_table"`
	ExcludeTables  []string `json:"exclude_table"`
}

type CompareResult struct {
	Comparability     Comparability      `json:"comparability"`
	Summary           SummaryDelta       `json:"summary"`
	KeyFindings       []CompareFinding   `json:"key_findings"`
	Recommendations   []Recommendation   `json:"recommendations"`
	TableChanges      []TableChange      `json:"table_changes"`
	PatternChanges    []PatternChange    `json:"pattern_changes"`
	OperationMix      []OperationDelta   `json:"operation_mix"`
	AlertChanges      AlertDelta         `json:"alert_changes"`
	PatternDrilldowns []PatternDrilldown `json:"pattern_drilldowns"`
	DiagnosticsDelta  DiagnosticsDelta   `json:"diagnostics_delta"`
	CurrentLabel      string             `json:"current_label"`
	BaselineLabel     string             `json:"baseline_label"`
	CurrentSnapshot   *InputSnapshot     `json:"current_snapshot,omitempty"`
	BaselineSnapshot  *InputSnapshot     `json:"baseline_snapshot,omitempty"`
}

type ComparabilityVerdict string

const (
	VerdictComparable    ComparabilityVerdict = "comparable"
	VerdictNotComparable ComparabilityVerdict = "not_comparable"
	VerdictUnknown       ComparabilityVerdict = "unknown"
)

const (
	ReasonWorkloadIdentityMismatch     = "workload_identity_mismatch"
	ReasonMixedProducers               = "mixed_producers"
	ReasonProducerFlavorConflict       = "producer_flavor_conflict"
	ReasonIncompatibleScope            = "incompatible_scope"
	ReasonMissingWorkloadIdentity      = "missing_workload_identity"
	ReasonMissingProducerProvenance    = "missing_producer_provenance"
	ReasonLegacyReportMetadata         = "legacy_report_metadata"
	ReasonMissingScope                 = "missing_scope"
	ReasonInsufficientCompleteness     = "insufficient_completeness_evidence"
	ReasonPartialOrUnknownTransactions = "partial_or_unknown_transactions"
)

// Comparability is the structured safety verdict emitted by compare and trend.
type Comparability struct {
	Verdict     ComparabilityVerdict    `json:"verdict"`
	ReasonCodes []string                `json:"reason_codes"`
	Evidence    []ComparabilityEvidence `json:"evidence"`
}

// ComparabilityEvidence keeps operator-visible facts without treating them as workload identity.
type ComparabilityEvidence struct {
	Role                string                `json:"role"`
	Name                string                `json:"name,omitempty"`
	ReportVersion       *int                  `json:"report_version,omitempty"`
	WorkloadID          string                `json:"workload_id,omitempty"`
	ServerIDs           []uint32              `json:"server_ids,omitempty"`
	ServerVersions      []string              `json:"server_versions,omitempty"`
	ServerFlavors       []string              `json:"server_flavors,omitempty"`
	MixedProducers      bool                  `json:"mixed_producers"`
	Schemas             []string              `json:"schemas,omitempty"`
	Scope               *InputSnapshotFilters `json:"scope,omitempty"`
	TotalTransactions   int                   `json:"total_transactions"`
	PartialTransactions *int                  `json:"partial_transactions,omitempty"`
	UnknownTransactions *int                  `json:"unknown_transactions,omitempty"`
}

// ComparabilityInput names one report participating in a shared verdict.
type ComparabilityInput struct {
	Role   string
	Report InputReport
}

// DiagnosticsDelta holds DBA-oriented diagnostic comparison results.
type DiagnosticsDelta struct {
	DDLChanges       DDLChangeDelta     `json:"ddl_changes"`
	TxnDiagnostics   TxnDiagnosticDelta `json:"txn_diagnostics"`
	HotIntervalDelta HotIntervalDelta   `json:"hot_interval_delta"`
	EventMixDelta    EventMixDelta      `json:"event_mix_delta"`
	// Byte coverage is consumed by the HTML renderer and omitted from compare JSON for compatibility.
	BaselineInputFileBytes    *int64 `json:"-"`
	CurrentInputFileBytes     *int64 `json:"-"`
	BaselineCountedEventBytes int64  `json:"-"`
	CurrentCountedEventBytes  int64  `json:"-"`
}

// DDLChangeDelta compares DDL event counts and details between windows.
type DDLChangeDelta struct {
	BaselineCount int            `json:"baseline_count"`
	CurrentCount  int            `json:"current_count"`
	Delta         int            `json:"delta"`
	Added         []DDLEventItem `json:"added"`
	Removed       []DDLEventItem `json:"removed"`
}

// DDLEventItem is a flat DDL event summary for compare output.
type DDLEventItem struct {
	Timestamp string `json:"timestamp"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	Operation string `json:"operation"`
	Statement string `json:"statement,omitempty"`
}

// TxnDiagnosticDelta compares largest/longest transaction stats between windows.
type TxnDiagnosticDelta struct {
	LargestTxnDelta TxnSizeCompare     `json:"largest_txn_delta"`
	LongestTxnDelta TxnDurationCompare `json:"longest_txn_delta"`
}

// TransactionEvidence preserves the current transaction's retained location and
// explicit report-v3 completeness/replay contract. Commands are never inferred.
type TransactionEvidence struct {
	TxnKey          string `json:"txn_key,omitempty"`
	BinlogFileStart string `json:"binlog_file_start,omitempty"`
	BinlogFileEnd   string `json:"binlog_file_end,omitempty"`
	PosStart        int64  `json:"pos_start,omitempty"`
	PosEnd          int64  `json:"pos_end,omitempty"`
	BinlogSpan      string `json:"binlog_span,omitempty"`
	Completeness    string `json:"completeness,omitempty"`
	ReplayAvailable *bool  `json:"replay_available,omitempty"`
	ReplayScope     string `json:"replay_scope,omitempty"`
	MysqlbinlogCmd  string `json:"mysqlbinlog_cmd,omitempty"`
}

// TxnSizeCompare holds baseline/current/delta for a single transaction metric (rows).
type TxnSizeCompare struct {
	BaselineRows    int                  `json:"baseline_rows"`
	CurrentRows     int                  `json:"current_rows"`
	DeltaRows       int                  `json:"delta_rows"`
	BaselineKey     string               `json:"baseline_key,omitempty"`
	CurrentKey      string               `json:"current_key,omitempty"`
	BaselineTable   string               `json:"baseline_table,omitempty"`
	CurrentTable    string               `json:"current_table,omitempty"`
	BaselineOp      string               `json:"baseline_op,omitempty"`
	CurrentOp       string               `json:"current_op,omitempty"`
	IdentityNew     bool                 `json:"identity_new,omitempty"`
	CurrentEvidence *TransactionEvidence `json:"current_evidence,omitempty"`
}

// TxnDurationCompare holds baseline/current/delta for transaction duration.
type TxnDurationCompare struct {
	BaselineDuration string               `json:"baseline_duration"`
	CurrentDuration  string               `json:"current_duration"`
	BaselineKey      string               `json:"baseline_key,omitempty"`
	CurrentKey       string               `json:"current_key,omitempty"`
	CurrentEvidence  *TransactionEvidence `json:"current_evidence,omitempty"`
}

// HotIntervalDelta compares hot interval summaries between windows.
type HotIntervalDelta struct {
	BaselineTopRows int               `json:"baseline_top_rows"`
	CurrentTopRows  int               `json:"current_top_rows"`
	DeltaTopRows    int               `json:"delta_top_rows"`
	BaselineCount   int               `json:"baseline_count"`
	CurrentCount    int               `json:"current_count"`
	TopItems        []HotIntervalItem `json:"top_items"`
}

// HotIntervalItem is a flat hot interval summary for compare output.
type HotIntervalItem struct {
	Minute      string `json:"minute"`
	Source      string `json:"source"` // "baseline" or "current"
	TotalRows   int    `json:"total_rows"`
	TxnCount    int    `json:"txn_count"`
	BinlogBytes int64  `json:"binlog_bytes"`
}

// EventMixDelta compares the event type distribution between windows.
type EventMixDelta struct {
	InsertDelta int `json:"insert_delta"`
	UpdateDelta int `json:"update_delta"`
	DeleteDelta int `json:"delete_delta"`
	DDLDelta    int `json:"ddl_delta"`
}

type Recommendation struct {
	Kind                string        `json:"kind"`
	Priority            string        `json:"priority"`
	Title               string        `json:"title"`
	Summary             string        `json:"summary"`
	Rationale           string        `json:"rationale"`
	RelatedFindingKinds []string      `json:"related_finding_kinds"`
	EvidenceRefs        []EvidenceRef `json:"evidence_refs,omitempty"`
}

type CompareFinding struct {
	Kind         string         `json:"kind"`
	Title        string         `json:"title"`
	Summary      string         `json:"summary"`
	Evidence     map[string]any `json:"evidence"`
	EvidenceRefs []EvidenceRef  `json:"evidence_refs,omitempty"`
}

// EvidenceRef points from a finding to an existing section or item in the report.
// Anchors must be stable and predictable so HTML rendering can link directly.
type EvidenceRef struct {
	Section string `json:"section"`
	Key     string `json:"key,omitempty"`
	Label   string `json:"label"`
	Anchor  string `json:"anchor"`
}

type SummaryDelta struct {
	CurrentTotalRows            int  `json:"current_total_rows"`
	BaselineTotalRows           int  `json:"baseline_total_rows"`
	TotalRowsDelta              int  `json:"total_rows_delta"`
	CurrentTotalTransactions    int  `json:"current_total_transactions"`
	BaselineTotalTransactions   int  `json:"baseline_total_transactions"`
	TotalTransactionsDelta      int  `json:"total_transactions_delta"`
	CurrentPartialTransactions  *int `json:"current_partial_transactions,omitempty"`
	BaselinePartialTransactions *int `json:"baseline_partial_transactions,omitempty"`
	CurrentUnknownTransactions  *int `json:"current_unknown_transactions,omitempty"`
	BaselineUnknownTransactions *int `json:"baseline_unknown_transactions,omitempty"`
	CurrentWarnings             int  `json:"current_warnings"`
	BaselineWarnings            int  `json:"baseline_warnings"`
}

type TableChange struct {
	Schema       string   `json:"schema"`
	Table        string   `json:"table"`
	CurrentRows  int      `json:"current_rows"`
	BaselineRows int      `json:"baseline_rows"`
	DeltaRows    int      `json:"delta_rows"`
	DeltaPercent *float64 `json:"delta_percent"`
}

type PatternChange struct {
	PatternKey         string         `json:"pattern_key"`
	Label              string         `json:"label"`
	CurrentRows        int            `json:"current_rows"`
	BaselineRows       int            `json:"baseline_rows"`
	DeltaRows          int            `json:"delta_rows"`
	DeltaPercent       *float64       `json:"delta_percent"`
	CurrentTxnCount    int            `json:"current_txn_count"`
	BaselineTxnCount   int            `json:"baseline_txn_count"`
	DeltaTxnCount      int            `json:"delta_txn_count"`
	Tables             map[string]int `json:"tables"`
	Operations         map[string]int `json:"operations"`
	SampleQuerySummary string         `json:"sample_query_summary,omitempty"`
}

type OperationDelta struct {
	Operation string `json:"operation"`
	Current   int    `json:"current"`
	Baseline  int    `json:"baseline"`
	Delta     int    `json:"delta"`
}

type AlertDelta struct {
	Added   []InputAlert `json:"added"`
	Removed []InputAlert `json:"removed"`
}

type PatternDrilldown struct {
	PatternKey   string                  `json:"pattern_key"`
	Label        string                  `json:"label"`
	WhySelected  string                  `json:"why_selected"`
	BaselineRows int                     `json:"baseline_rows"`
	CurrentRows  int                     `json:"current_rows"`
	DeltaRows    int                     `json:"delta_rows"`
	BaselineTxns int                     `json:"baseline_txns"`
	CurrentTxns  int                     `json:"current_txns"`
	DeltaTxns    int                     `json:"delta_txns"`
	SignalFlags  CompareDrilldownSignals `json:"signal_flags"`
	KeyPoints    []CompareKeyPoint       `json:"key_points"`
}

type CompareDrilldownSignals struct {
	DominantDelta   bool `json:"dominant_delta"`
	NewPattern      bool `json:"new_pattern"`
	Disappeared     bool `json:"disappeared"`
	TxnRowsDiverged bool `json:"txn_rows_diverged"`
}

type CompareKeyPoint struct {
	Label   string `json:"label"`
	Summary string `json:"summary"`
}
