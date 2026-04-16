// Package binlogviz verifies trend command registration and output behavior.
// input: Cobra root and trend commands plus fixture-backed analyze reports built by tests.
// output: assertions for trend CLI wiring, snapshot handling, and user-facing output.
// pos: command-level regression coverage for the trend workflow.
// note: if this file changes, keep cmd/binlogviz/README.md synchronized.
package binlogviz

import (
	"bytes"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootCommandRegistersTrendCommand(t *testing.T) {
	cmd := NewRootCommand()

	found, _, err := cmd.Find([]string{"trend"})
	if err != nil {
		t.Fatalf("find trend: %v", err)
	}
	if found == nil || found.Name() != "trend" {
		t.Fatalf("expected trend command to be registered, got %#v", found)
	}
}

func TestTrendCommandRequiresAtLeastTwoSnapshots(t *testing.T) {
	dir := t.TempDir()
	writeSnapshotFixture(t, dir, "only_one", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "only_one",
		Label:     "Only One",
		StartTime: "2026-03-20T10:00:00Z",
		EndTime:   "2026-03-20T10:30:00Z",
		Rows:      1200,
		Txns:      60,
		Events:    1400,
		Inserts:   700,
		Updates:   300,
		Deletes:   200,
		Alerts:    1,
	}))

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"trend", "only_one", "--snapshot-dir", dir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "at least 2 snapshots") {
		t.Fatalf("expected minimum snapshot error, got %v", err)
	}
}

func TestTrendCommandJSONOutputOrdersSnapshotsByWindowStartTime(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	dir := t.TempDir()
	writeSnapshotFixture(t, dir, "later", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "later",
		Label:     "Later",
		StartTime: "2026-03-21T10:00:00Z",
		EndTime:   "2026-03-21T10:30:00Z",
		Rows:      3000,
		Txns:      150,
		Events:    3600,
		Inserts:   1600,
		Updates:   900,
		Deletes:   500,
		Alerts:    3,
	}))
	writeSnapshotFixture(t, dir, "earlier", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "earlier",
		Label:     "Earlier",
		StartTime: "2026-03-19T10:00:00Z",
		EndTime:   "2026-03-19T10:30:00Z",
		Rows:      1000,
		Txns:      50,
		Events:    1200,
		Inserts:   500,
		Updates:   350,
		Deletes:   150,
		Alerts:    0,
	}))
	writeSnapshotFixture(t, dir, "middle", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "middle",
		Label:     "Middle",
		StartTime: "2026-03-20T10:00:00Z",
		EndTime:   "2026-03-20T10:30:00Z",
		Rows:      1800,
		Txns:      90,
		Events:    2200,
		Inserts:   900,
		Updates:   600,
		Deletes:   300,
		Alerts:    1,
	}))

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"trend", "later", "earlier", "middle", "--snapshot-dir", dir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	var decoded struct {
		Points []struct {
			Snapshot struct {
				Name string `json:"name"`
			} `json:"snapshot"`
			Window struct {
				StartTime string `json:"start_time"`
			} `json:"window"`
		} `json:"points"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode trend json: %v\n%s", err, stdout)
	}
	if len(decoded.Points) != 3 {
		t.Fatalf("expected 3 points, got %d", len(decoded.Points))
	}
	gotOrder := []string{
		decoded.Points[0].Snapshot.Name,
		decoded.Points[1].Snapshot.Name,
		decoded.Points[2].Snapshot.Name,
	}
	wantOrder := []string{"earlier", "middle", "later"}
	for i := range wantOrder {
		if gotOrder[i] != wantOrder[i] {
			t.Fatalf("unexpected point order: got %v want %v", gotOrder, wantOrder)
		}
	}
}

func TestTrendCommandJSONOutputIncludesPatternTrends(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	dir := t.TempDir()
	writeSnapshotFixture(t, dir, "later", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "later",
		Label:     "Later",
		StartTime: "2026-03-21T10:00:00Z",
		EndTime:   "2026-03-21T10:30:00Z",
		Rows:      3000,
		Txns:      150,
		Events:    3600,
		Inserts:   1600,
		Updates:   900,
		Deletes:   500,
		Alerts:    3,
		Patterns: []trendPatternFixture{
			{
				PatternKey:         "orders.payments|UPDATE|medium",
				Label:              "payments.update_status",
				TotalRows:          1300,
				TxnCount:           18,
				EventCount:         90,
				ShareOfRows:        0.4333333333,
				ShareOfTxns:        0.12,
				AvgRowsPerTxn:      72.2,
				Tables:             map[string]int{"orders.payments": 1300},
				Operations:         map[string]int{"UPDATE": 1300},
				SampleQuerySummary: "update payments set status = ?",
			},
		},
	}))
	writeSnapshotFixture(t, dir, "earlier", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "earlier",
		Label:     "Earlier",
		StartTime: "2026-03-19T10:00:00Z",
		EndTime:   "2026-03-19T10:30:00Z",
		Rows:      1000,
		Txns:      50,
		Events:    1200,
		Inserts:   500,
		Updates:   350,
		Deletes:   150,
		Alerts:    0,
		Patterns: []trendPatternFixture{
			{
				PatternKey:         "orders.payments|UPDATE|medium",
				Label:              "payments.update_status",
				TotalRows:          600,
				TxnCount:           9,
				EventCount:         45,
				ShareOfRows:        0.6,
				ShareOfTxns:        0.18,
				AvgRowsPerTxn:      66.7,
				Tables:             map[string]int{"orders.payments": 600},
				Operations:         map[string]int{"UPDATE": 600},
				SampleQuerySummary: "update payments set status = ?",
			},
		},
	}))
	writeSnapshotFixture(t, dir, "middle", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "middle",
		Label:     "Middle",
		StartTime: "2026-03-20T10:00:00Z",
		EndTime:   "2026-03-20T10:30:00Z",
		Rows:      1800,
		Txns:      90,
		Events:    2200,
		Inserts:   900,
		Updates:   600,
		Deletes:   300,
		Alerts:    1,
		Patterns: []trendPatternFixture{
			{
				PatternKey:         "orders.payments|UPDATE|medium",
				Label:              "payments.update_status",
				TotalRows:          1000,
				TxnCount:           13,
				EventCount:         65,
				ShareOfRows:        0.5555555556,
				ShareOfTxns:        0.1444444444,
				AvgRowsPerTxn:      76.9,
				Tables:             map[string]int{"orders.payments": 1000},
				Operations:         map[string]int{"UPDATE": 1000},
				SampleQuerySummary: "update payments set status = ?",
			},
		},
	}))

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"trend", "later", "earlier", "middle", "--snapshot-dir", dir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	var decoded struct {
		PatternTrends []struct {
			PatternKey        string `json:"pattern_key"`
			RowsSeries        []any  `json:"rows_series"`
			ShareOfRowsSeries []any  `json:"share_of_rows_series"`
		} `json:"pattern_trends"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode trend json: %v\n%s", err, stdout)
	}
	if len(decoded.PatternTrends) != 1 {
		t.Fatalf("expected 1 pattern trend, got %d", len(decoded.PatternTrends))
	}
	if decoded.PatternTrends[0].PatternKey != "orders.payments|UPDATE|medium" {
		t.Fatalf("expected pattern key to round-trip, got %+v", decoded.PatternTrends[0])
	}
	if len(decoded.PatternTrends[0].RowsSeries) != 3 || len(decoded.PatternTrends[0].ShareOfRowsSeries) != 3 {
		t.Fatalf("expected rows/share series for each snapshot, got %+v", decoded.PatternTrends[0])
	}
}

func TestTrendCommandPatternModeIncludesBaselineContext(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	dir := t.TempDir()
	writeSnapshotFixture(t, dir, "incident_alpha", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "incident_alpha",
		Label:     "Incident Alpha",
		StartTime: "2026-03-20T10:00:00Z",
		EndTime:   "2026-03-20T10:30:00Z",
		Rows:      2400,
		Txns:      120,
		Events:    3000,
		Inserts:   1200,
		Updates:   800,
		Deletes:   400,
		Alerts:    2,
	}))
	writeSnapshotFixture(t, dir, "incident_beta", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "incident_beta",
		Label:     "Incident Beta",
		StartTime: "2026-03-21T10:00:00Z",
		EndTime:   "2026-03-21T10:30:00Z",
		Rows:      3200,
		Txns:      160,
		Events:    3800,
		Inserts:   1800,
		Updates:   1000,
		Deletes:   400,
		Alerts:    3,
	}))
	writeSnapshotFixture(t, dir, "baseline_weekly", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "baseline_weekly",
		Label:     "Baseline Weekly",
		StartTime: "2026-03-10T10:00:00Z",
		EndTime:   "2026-03-10T10:30:00Z",
		Rows:      1800,
		Txns:      100,
		Events:    2100,
		Inserts:   900,
		Updates:   700,
		Deletes:   200,
		Alerts:    1,
	}))
	writeSnapshotFixture(t, dir, "candidate_ignore", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "candidate_ignore",
		Label:     "Candidate Ignore",
		StartTime: "2026-03-22T10:00:00Z",
		EndTime:   "2026-03-22T10:30:00Z",
		Rows:      999,
		Txns:      30,
		Events:    1100,
		Inserts:   600,
		Updates:   200,
		Deletes:   199,
		Alerts:    0,
	}))

	cmd := NewRootCommand()
	cmd.SetArgs([]string{
		"trend",
		"--from-snapshots", "incident_*",
		"--baseline-snapshot", "baseline_weekly",
		"--snapshot-dir", dir,
		"--format", "text",
	})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	for _, token := range []string{
		"Trend Summary",
		"Key Findings",
		"Baseline Weekly (baseline_weekly)",
		"incident_alpha",
		"incident_beta",
	} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("expected text output to contain %q, got %q", token, stdout)
		}
	}
	if strings.Contains(stdout, "candidate_ignore") {
		t.Fatalf("expected pattern mode to exclude non-matching snapshot, got %q", stdout)
	}
}

func TestTrendCommandHTMLOutputContainsTrendCharts(t *testing.T) {
	dir := t.TempDir()
	writeSnapshotFixture(t, dir, "alpha", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "alpha",
		Label:     "Alpha",
		StartTime: "2026-03-20T10:00:00Z",
		EndTime:   "2026-03-20T10:30:00Z",
		Rows:      2400,
		Txns:      120,
		Events:    3000,
		Inserts:   1200,
		Updates:   800,
		Deletes:   400,
		Alerts:    2,
		Patterns: []trendPatternFixture{
			{
				PatternKey:         "orders.payments|UPDATE|medium",
				Label:              "payments.update_status",
				TotalRows:          600,
				TxnCount:           9,
				EventCount:         45,
				ShareOfRows:        0.6,
				ShareOfTxns:        0.18,
				AvgRowsPerTxn:      66.7,
				Tables:             map[string]int{"orders.payments": 600},
				Operations:         map[string]int{"UPDATE": 600},
				SampleQuerySummary: "update payments set status = ?",
			},
		},
	}))
	writeSnapshotFixture(t, dir, "beta", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "beta",
		Label:     "Beta",
		StartTime: "2026-03-21T10:00:00Z",
		EndTime:   "2026-03-21T10:30:00Z",
		Rows:      3200,
		Txns:      160,
		Events:    3800,
		Inserts:   1800,
		Updates:   1000,
		Deletes:   400,
		Alerts:    3,
		Patterns: []trendPatternFixture{
			{
				PatternKey:         "orders.payments|UPDATE|medium",
				Label:              "payments.update_status",
				TotalRows:          1300,
				TxnCount:           18,
				EventCount:         90,
				ShareOfRows:        0.40625,
				ShareOfTxns:        0.1125,
				AvgRowsPerTxn:      72.2,
				Tables:             map[string]int{"orders.payments": 1300},
				Operations:         map[string]int{"UPDATE": 1300},
				SampleQuerySummary: "update payments set status = ?",
			},
		},
	}))

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"--lang", "en", "trend", "alpha", "beta", "--snapshot-dir", dir, "--format", "html"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	err := cmd.Execute()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{"<html", `id="trend-overall-chart"`, `id="trend-ops-chart"`, `id="trend-tables-chart"`, `Pattern Trends`, `id="trend-pattern-chart"`, `data-pattern-view="share"`, `data-pattern-view="rows"`} {
		if !strings.Contains(output.String(), token) {
			t.Fatalf("expected html output to contain %q, got %q", token, output.String())
		}
	}
}

func TestTrendCommandRejectsMissingWindowStartTime(t *testing.T) {
	dir := t.TempDir()
	writeSnapshotFixture(t, dir, "good", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "good",
		Label:     "Good",
		StartTime: "2026-03-20T10:00:00Z",
		EndTime:   "2026-03-20T10:30:00Z",
		Rows:      2400,
		Txns:      120,
		Events:    3000,
		Inserts:   1200,
		Updates:   800,
		Deletes:   400,
		Alerts:    2,
	}))
	writeSnapshotFixture(t, dir, "bad", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "bad",
		Label:     "Bad",
		StartTime: "",
		EndTime:   "2026-03-21T10:30:00Z",
		Rows:      3200,
		Txns:      160,
		Events:    3800,
		Inserts:   1800,
		Updates:   1000,
		Deletes:   400,
		Alerts:    3,
	}))

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"trend", "good", "bad", "--snapshot-dir", dir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "window.start_time") {
		t.Fatalf("expected missing start time error, got %v", err)
	}
}

func TestTrendCommandFallsBackToSummaryStartTimeWhenSnapshotWindowMissing(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	dir := t.TempDir()
	writeSnapshotFixture(t, dir, "good", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "good",
		Label:     "Good",
		StartTime: "2026-03-20T10:00:00Z",
		EndTime:   "2026-03-20T10:30:00Z",
		Rows:      2400,
		Txns:      120,
		Events:    3000,
		Inserts:   1200,
		Updates:   800,
		Deletes:   400,
		Alerts:    2,
	}))
	writeSnapshotFixture(t, dir, "legacy", trendSnapshotFixtureJSONWithWindowOverride(trendSnapshotFixture{
		Name:      "legacy",
		Label:     "Legacy",
		StartTime: "2026-03-21T10:00:00Z",
		EndTime:   "2026-03-21T10:30:00Z",
		Rows:      3200,
		Txns:      160,
		Events:    3800,
		Inserts:   1800,
		Updates:   1000,
		Deletes:   400,
		Alerts:    3,
	}, "", "2026-03-21T10:30:00Z"))

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"trend", "good", "legacy", "--snapshot-dir", dir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("expected summary.start_time fallback, got %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	var decoded struct {
		Points []struct {
			Snapshot struct {
				Name string `json:"name"`
			} `json:"snapshot"`
			Window struct {
				StartTime string `json:"start_time"`
			} `json:"window"`
		} `json:"points"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode trend json: %v\n%s", err, stdout)
	}
	if len(decoded.Points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(decoded.Points))
	}
	if decoded.Points[1].Snapshot.Name != "legacy" {
		t.Fatalf("expected fallback snapshot to participate in trend, got %+v", decoded.Points)
	}
}

type trendSnapshotFixture struct {
	Name      string
	Label     string
	StartTime string
	EndTime   string
	Rows      int
	Txns      int
	Events    int
	Inserts   int
	Updates   int
	Deletes   int
	Alerts    int
	Patterns  []trendPatternFixture
}

type trendPatternFixture struct {
	PatternKey         string
	Label              string
	TotalRows          int
	TxnCount           int
	EventCount         int
	ShareOfRows        float64
	ShareOfTxns        float64
	AvgRowsPerTxn      float64
	Tables             map[string]int
	Operations         map[string]int
	SampleQuerySummary string
}

func trendSnapshotFixtureJSON(f trendSnapshotFixture) string {
	return trendSnapshotFixtureJSONWithWindowOverride(f, f.StartTime, f.EndTime)
}

func trendSnapshotFixtureJSONWithWindowOverride(f trendSnapshotFixture, windowStart, windowEnd string) string {
	return `{
  "summary": {
    "total_transactions": ` + intString(f.Txns) + `,
    "total_rows": ` + intString(f.Rows) + `,
    "total_events": ` + intString(f.Events) + `,
    "start_time": "` + f.StartTime + `",
    "end_time": "` + f.EndTime + `",
    "duration": "30m0s"
  },
  "tables": [
    {
      "schema": "shop",
      "table": "orders",
      "total_rows": ` + intString(f.Rows/2) + `,
      "insert_rows": ` + intString(f.Inserts/2) + `,
      "update_rows": ` + intString(f.Updates/2) + `,
      "delete_rows": ` + intString(f.Deletes/2) + `,
      "txn_count": ` + intString(f.Txns/2) + `
    },
    {
      "schema": "shop",
      "table": "payments",
      "total_rows": ` + intString(f.Rows-(f.Rows/2)) + `,
      "insert_rows": ` + intString(f.Inserts-(f.Inserts/2)) + `,
      "update_rows": ` + intString(f.Updates-(f.Updates/2)) + `,
      "delete_rows": ` + intString(f.Deletes-(f.Deletes/2)) + `,
      "txn_count": ` + intString(f.Txns-(f.Txns/2)) + `
    }
  ],
  "patterns": ` + trendPatternsJSON(f.Patterns) + `,
  "transactions": [],
  "minutes": [],
  "alerts": ` + trendAlertsJSON(f.Alerts, f.StartTime) + `,
  "warnings": 0,
  "snapshot": {
    "name": "` + f.Name + `",
    "label": "` + f.Label + `",
    "created_at": "` + f.EndTime + `",
    "binlogviz_version": "dev",
    "input_mode": "files",
    "input": {
      "files": ["` + filepath.Base(f.Name) + `.binlog"],
      "from_dir": "",
      "prefix": ""
    },
    "window": {
      "start_time": "` + windowStart + `",
      "end_time": "` + windowEnd + `"
    },
    "filters": {
      "include_schema": [],
      "exclude_schema": [],
      "include_table": [],
      "exclude_table": []
    }
  }
}`
}

func trendPatternsJSON(patterns []trendPatternFixture) string {
	if len(patterns) == 0 {
		return "[]"
	}

	type trendPatternFixtureJSON struct {
		PatternKey         string         `json:"pattern_key"`
		Label              string         `json:"label"`
		TotalRows          int            `json:"total_rows"`
		TxnCount           int            `json:"txn_count"`
		EventCount         int            `json:"event_count"`
		ShareOfRows        float64        `json:"share_of_rows"`
		ShareOfTxns        float64        `json:"share_of_txns"`
		AvgRowsPerTxn      float64        `json:"avg_rows_per_txn"`
		Tables             map[string]int `json:"tables"`
		Operations         map[string]int `json:"operations"`
		SampleQuerySummary string         `json:"sample_query_summary,omitempty"`
	}

	encoded := make([]trendPatternFixtureJSON, 0, len(patterns))
	for _, pattern := range patterns {
		encoded = append(encoded, trendPatternFixtureJSON{
			PatternKey:         pattern.PatternKey,
			Label:              pattern.Label,
			TotalRows:          pattern.TotalRows,
			TxnCount:           pattern.TxnCount,
			EventCount:         pattern.EventCount,
			ShareOfRows:        pattern.ShareOfRows,
			ShareOfTxns:        pattern.ShareOfTxns,
			AvgRowsPerTxn:      pattern.AvgRowsPerTxn,
			Tables:             pattern.Tables,
			Operations:         pattern.Operations,
			SampleQuerySummary: pattern.SampleQuerySummary,
		})
	}

	data, err := json.Marshal(encoded)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func trendAlertsJSON(count int, minute string) string {
	if count <= 0 {
		return "[]"
	}
	parts := make([]string, 0, count)
	for i := 0; i < count; i++ {
		parts = append(parts, `{
      "type": "spike",
      "severity": "warning",
      "message": "alert `+intString(i+1)+`",
      "minute": "`+minute+`"
    }`)
	}
	return "[" + strings.Join(parts, ",") + "]"
}
