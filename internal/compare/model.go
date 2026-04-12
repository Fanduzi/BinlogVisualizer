// Package compare defines compare-input contracts and comparison result models.
// input: JSON reports emitted by `binlogviz analyze --format json`.
// output: typed compare input structures and, later, compare result structures.
// pos: compare pipeline boundary between JSON loading and diff/render stages.
package compare

type InputReport struct {
	Summary  InputSummary   `json:"summary"`
	Tables   []InputTable   `json:"tables"`
	Patterns []InputPattern `json:"patterns"`
	Alerts   []InputAlert   `json:"alerts"`
	Warnings int            `json:"warnings"`
	Snapshot *InputSnapshot `json:"snapshot,omitempty"`
}

type InputSummary struct {
	TotalTransactions int    `json:"total_transactions"`
	TotalRows         int    `json:"total_rows"`
	TotalEvents       int    `json:"total_events"`
	StartTime         string `json:"start_time"`
	EndTime           string `json:"end_time"`
	Duration          string `json:"duration"`
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
	Summary           SummaryDelta      `json:"summary"`
	KeyFindings       []CompareFinding  `json:"key_findings"`
	Recommendations   []Recommendation  `json:"recommendations"`
	TableChanges      []TableChange     `json:"table_changes"`
	PatternChanges    []PatternChange   `json:"pattern_changes"`
	OperationMix      []OperationDelta  `json:"operation_mix"`
	AlertChanges      AlertDelta        `json:"alert_changes"`
	PatternDrilldowns []PatternDrilldown `json:"pattern_drilldowns"`
	CurrentLabel      string            `json:"current_label"`
	BaselineLabel     string            `json:"baseline_label"`
	CurrentSnapshot   *InputSnapshot    `json:"current_snapshot,omitempty"`
	BaselineSnapshot  *InputSnapshot    `json:"baseline_snapshot,omitempty"`
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
	CurrentTotalRows          int `json:"current_total_rows"`
	BaselineTotalRows         int `json:"baseline_total_rows"`
	TotalRowsDelta            int `json:"total_rows_delta"`
	CurrentTotalTransactions  int `json:"current_total_transactions"`
	BaselineTotalTransactions int `json:"baseline_total_transactions"`
	TotalTransactionsDelta    int `json:"total_transactions_delta"`
	CurrentWarnings           int `json:"current_warnings"`
	BaselineWarnings          int `json:"baseline_warnings"`
}

type TableChange struct {
	Schema       string  `json:"schema"`
	Table        string  `json:"table"`
	CurrentRows  int     `json:"current_rows"`
	BaselineRows int     `json:"baseline_rows"`
	DeltaRows    int     `json:"delta_rows"`
	DeltaPercent float64 `json:"delta_percent"`
}

type PatternChange struct {
	PatternKey         string         `json:"pattern_key"`
	Label              string         `json:"label"`
	CurrentRows        int            `json:"current_rows"`
	BaselineRows       int            `json:"baseline_rows"`
	DeltaRows          int            `json:"delta_rows"`
	DeltaPercent       float64        `json:"delta_percent"`
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
