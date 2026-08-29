// Package trend defines trend-input contracts and multi-snapshot result models.
// input: ordered snapshot-backed analyze JSON reports with workload comparability metadata plus optional baseline.
// output: deterministic trend results with a series-wide comparability verdict, completeness-aware point summaries, baseline deltas, and trusted replay evidence.
// pos: trend pipeline boundary between snapshot loading and renderer-specific output.
// note: if this file changes, keep internal/trend/README.md synchronized.
package trend

import comparepkg "binlogviz/internal/compare"

type BuildInput struct {
	Path   string
	Report InputReport
}

type InputReport = comparepkg.InputReport
type InputSummary = comparepkg.InputSummary
type InputTable = comparepkg.InputTable
type InputAlert = comparepkg.InputAlert
type InputSnapshot = comparepkg.InputSnapshot
type InputSnapshotInput = comparepkg.InputSnapshotInput
type InputSnapshotWindow = comparepkg.InputSnapshotWindow
type InputSnapshotFilters = comparepkg.InputSnapshotFilters
type InputTimeseries = comparepkg.InputTimeseries
type InputTimeseriesPoint = comparepkg.InputTimeseriesPoint
type InputDiagnostics = comparepkg.InputDiagnostics
type InputDDLEvent = comparepkg.InputDDLEvent
type InputHotInterval = comparepkg.InputHotInterval
type InputTransaction = comparepkg.InputTransaction
type TransactionEvidence = comparepkg.TransactionEvidence
type Comparability = comparepkg.Comparability

type BuildOptions struct {
	InputMode   string
	SnapshotDir string
	Points      []BuildInput
	Baseline    *BuildInput
	TopTables   int
	// Order is "cli" (keep input order) or "time" (sort by window start_time).
	// Empty defaults to "cli".
	Order string
}

type Result struct {
	Comparability     Comparability      `json:"comparability"`
	InputMode         string             `json:"input_mode"`
	SnapshotDir       string             `json:"snapshot_dir"`
	Order             string             `json:"order"`
	Reordered         bool               `json:"reordered,omitempty"`
	BaselineSnapshot  *SnapshotMeta      `json:"baseline_snapshot,omitempty"`
	Points            []Point            `json:"points"`
	TableTrends       []TableTrend       `json:"table_trends"`
	PatternTrends     []PatternTrend     `json:"pattern_trends"`
	Insights          Insights           `json:"insights"`
	TrendSummary      []TrendFinding     `json:"trend_summary"`
	Recommendations   []Recommendation   `json:"recommendations"`
	PatternDrilldowns []PatternDrilldown `json:"pattern_drilldowns"`
	DiagnosticsTrends DiagnosticsTrends  `json:"diagnostics_trends"`
}

// DiagnosticsTrends holds trend-series data for DBA diagnostics.
type DiagnosticsTrends struct {
	TPSTrends          []MetricTrendSeries     `json:"tps_trends"`
	DDLTrends          []MetricTrendSeries     `json:"ddl_trends"`
	TxnSizeTrends      []MetricTrendSeries     `json:"txn_size_trends"`
	TxnDurationTrends  []MetricTrendSeries     `json:"txn_duration_trends"`
	EventMixTrends     EventMixTrendSeries     `json:"event_mix_trends"`
	HotIntervalSummary HotIntervalTrendSummary `json:"hot_interval_summary"`
}

// MetricTrendSeries is a per-snapshot metric series for a single diagnostic dimension.
type MetricTrendSeries struct {
	SnapshotName string                          `json:"snapshot_name"`
	Value        float64                         `json:"value"`
	Evidence     *comparepkg.TransactionEvidence `json:"evidence,omitempty"`
}

// EventMixTrendSeries holds per-snapshot event type breakdowns.
type EventMixTrendSeries struct {
	Snapshots []EventMixSnapshot `json:"snapshots"`
}

// EventMixSnapshot is the event mix at one snapshot point.
type EventMixSnapshot struct {
	SnapshotName string  `json:"snapshot_name"`
	Inserts      float64 `json:"inserts"`
	Updates      float64 `json:"updates"`
	Deletes      float64 `json:"deletes"`
	DDL          float64 `json:"ddl"`
}

// HotIntervalTrendSummary summarizes hot interval changes across snapshots.
type HotIntervalTrendSummary struct {
	MaxHotRows     []MetricTrendSeries `json:"max_hot_rows"`
	HotCountSeries []MetricTrendSeries `json:"hot_count_series"`
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

type TrendFinding struct {
	Kind         string         `json:"kind"`
	Title        string         `json:"title"`
	Summary      string         `json:"summary"`
	Evidence     map[string]any `json:"evidence"`
	EvidenceRefs []EvidenceRef  `json:"evidence_refs,omitempty"`
}

// EvidenceRef points from a finding to an existing section or item in the trend report.
type EvidenceRef struct {
	Section string `json:"section"`
	Key     string `json:"key,omitempty"`
	Label   string `json:"label"`
	Anchor  string `json:"anchor"`
}

type SnapshotMeta struct {
	Name      string               `json:"name"`
	Label     string               `json:"label"`
	Path      string               `json:"path"`
	CreatedAt string               `json:"created_at"`
	InputMode string               `json:"input_mode"`
	Input     InputSnapshotInput   `json:"input"`
	Window    InputSnapshotWindow  `json:"window"`
	Filters   InputSnapshotFilters `json:"filters"`
}

type Point struct {
	Snapshot      SnapshotMeta        `json:"snapshot"`
	Window        InputSnapshotWindow `json:"window"`
	Summary       PointSummary        `json:"summary"`
	Operations    OperationBreakdown  `json:"operations"`
	AlertCount    int                 `json:"alert_count"`
	BaselineDelta *BaselineDelta      `json:"baseline_delta,omitempty"`
}

type PointSummary struct {
	TotalRows           int  `json:"total_rows"`
	TotalTransactions   int  `json:"total_transactions"`
	PartialTransactions *int `json:"partial_transactions,omitempty"`
	UnknownTransactions *int `json:"unknown_transactions,omitempty"`
	TotalEvents         int  `json:"total_events"`
	Warnings            int  `json:"warnings"`
}

type OperationBreakdown struct {
	Inserts     int     `json:"inserts"`
	Updates     int     `json:"updates"`
	Deletes     int     `json:"deletes"`
	InsertShare float64 `json:"insert_share"`
	UpdateShare float64 `json:"update_share"`
	DeleteShare float64 `json:"delete_share"`
}

type BaselineDelta struct {
	RowsDelta         int     `json:"rows_delta"`
	TransactionsDelta int     `json:"transactions_delta"`
	EventsDelta       int     `json:"events_delta"`
	AlertDelta        int     `json:"alert_delta"`
	RowsPercent       float64 `json:"rows_percent"`
	TransactionsPct   float64 `json:"transactions_percent"`
	EventsPercent     float64 `json:"events_percent"`
	AlertsPercent     float64 `json:"alerts_percent"`
}

type TableTrend struct {
	Schema    string           `json:"schema"`
	Table     string           `json:"table"`
	FirstRows int              `json:"first_rows"`
	LastRows  int              `json:"last_rows"`
	DeltaRows int              `json:"delta_rows"`
	Series    []TableTrendItem `json:"series"`
}

type TableTrendItem struct {
	SnapshotName string `json:"snapshot_name"`
	StartTime    string `json:"start_time"`
	Rows         int    `json:"rows"`
}

type PatternTrend struct {
	PatternKey        string                   `json:"pattern_key"`
	Label             string                   `json:"label"`
	FirstRows         int                      `json:"first_rows"`
	LastRows          int                      `json:"last_rows"`
	DeltaRows         int                      `json:"delta_rows"`
	FirstShareOfRows  float64                  `json:"first_share_of_rows"`
	LastShareOfRows   float64                  `json:"last_share_of_rows"`
	DeltaShareOfRows  float64                  `json:"delta_share_of_rows"`
	RowsSeries        []PatternTrendRowsPoint  `json:"rows_series"`
	ShareOfRowsSeries []PatternTrendSharePoint `json:"share_of_rows_series"`
	// SampleQuerySummary carries one representative query summary for display.
	// The builder chooses it deterministically in a later task.
	SampleQuerySummary string `json:"sample_query_summary,omitempty"`
}

type PatternTrendRowsPoint struct {
	SnapshotName string `json:"snapshot_name"`
	StartTime    string `json:"start_time"`
	Rows         int    `json:"rows"`
}

type PatternTrendSharePoint struct {
	SnapshotName string  `json:"snapshot_name"`
	StartTime    string  `json:"start_time"`
	ShareOfRows  float64 `json:"share_of_rows"`
}

type Insights struct {
	FirstSnapshot   string `json:"first_snapshot"`
	LastSnapshot    string `json:"last_snapshot"`
	RowsDelta       int    `json:"rows_delta"`
	TxnsDelta       int    `json:"transactions_delta"`
	EventsDelta     int    `json:"events_delta"`
	AlertCountDelta int    `json:"alert_count_delta"`
}

type PatternDrilldown struct {
	PatternKey  string                `json:"pattern_key"`
	Label       string                `json:"label"`
	WhySelected string                `json:"why_selected"`
	StartShare  float64               `json:"start_share"`
	EndShare    float64               `json:"end_share"`
	ShareDelta  float64               `json:"share_delta"`
	StartRows   int                   `json:"start_rows"`
	EndRows     int                   `json:"end_rows"`
	RowsDelta   int                   `json:"rows_delta"`
	SignalFlags TrendDrilldownSignals `json:"signal_flags"`
	KeyPoints   []TrendKeyPoint       `json:"key_points"`
}

type TrendDrilldownSignals struct {
	DominantShareShift bool `json:"dominant_share_shift"`
	SteadyRise         bool `json:"steady_rise"`
	SteadyFall         bool `json:"steady_fall"`
	ConcentratedJump   bool `json:"concentrated_jump"`
}

type TrendKeyPoint struct {
	Label   string `json:"label"`
	Summary string `json:"summary"`
}
