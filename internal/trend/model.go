// Package trend defines trend-input contracts and multi-snapshot result models.
// input: ordered snapshot-backed analyze JSON reports plus optional baseline metadata.
// output: deterministic trend results for text, JSON, and HTML renderers.
// pos: trend pipeline boundary between snapshot loading and renderer-specific output.
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

type BuildOptions struct {
	InputMode   string
	SnapshotDir string
	Points      []BuildInput
	Baseline    *BuildInput
	TopTables   int
}

type Result struct {
	InputMode         string            `json:"input_mode"`
	SnapshotDir       string            `json:"snapshot_dir"`
	BaselineSnapshot  *SnapshotMeta     `json:"baseline_snapshot,omitempty"`
	Points            []Point           `json:"points"`
	TableTrends       []TableTrend      `json:"table_trends"`
	PatternTrends     []PatternTrend    `json:"pattern_trends"`
	Insights          Insights          `json:"insights"`
	TrendSummary      []TrendFinding    `json:"trend_summary"`
	Recommendations   []Recommendation  `json:"recommendations"`
	PatternDrilldowns []PatternDrilldown `json:"pattern_drilldowns"`
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
	TotalRows         int `json:"total_rows"`
	TotalTransactions int `json:"total_transactions"`
	TotalEvents       int `json:"total_events"`
	Warnings          int `json:"warnings"`
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
