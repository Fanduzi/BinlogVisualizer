package trend

import (
	"math"
	"testing"

	comparepkg "binlogviz/internal/compare"
)

func TestBuildResultOrdersPointsAndComputesBaselineDelta(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		TopTables:   5,
		Points: []BuildInput{
			{
				Path:   "/tmp/snapshots/later.json",
				Report: testInputReport("later", "Later", "2026-03-21T10:00:00Z", 3000, 150, 3600, 1600, 900, 500, 3),
			},
			{
				Path:   "/tmp/snapshots/earlier.json",
				Report: testInputReport("earlier", "Earlier", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0),
			},
		},
		Baseline: &BuildInput{
			Path:   "/tmp/snapshots/baseline.json",
			Report: testInputReport("baseline", "Baseline", "2026-03-18T10:00:00Z", 800, 40, 1000, 400, 300, 100, 1),
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	if len(result.Points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(result.Points))
	}
	if result.Points[0].Snapshot.Name != "earlier" || result.Points[1].Snapshot.Name != "later" {
		t.Fatalf("unexpected ordering: %+v", []string{result.Points[0].Snapshot.Name, result.Points[1].Snapshot.Name})
	}
	if result.Points[1].BaselineDelta == nil {
		t.Fatal("expected baseline delta on ordered points")
	}
	if result.Points[1].BaselineDelta.RowsDelta != 2200 {
		t.Fatalf("unexpected rows delta: %+v", result.Points[1].BaselineDelta)
	}
	if len(result.TableTrends) == 0 {
		t.Fatal("expected table trends to be populated")
	}
	if result.Insights.RowsDelta != 2000 {
		t.Fatalf("unexpected insights rows delta: %+v", result.Insights)
	}
}

func TestBuildResultBuildsPatternTrendSeriesAndZeroFillsMissingSnapshots(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		TopTables:   5,
		Points: []BuildInput{
			{
				Path: "/tmp/snapshots/earlier.json",
				Report: testInputReportWithPatterns(
					"earlier",
					"Earlier",
					"2026-03-19T10:00:00Z",
					1000,
					50,
					1200,
					500,
					350,
					150,
					0,
					[]comparepkg.InputPattern{{
						PatternKey:  "orders.payments|UPDATE|medium",
						Label:       "payments.update_status",
						TotalRows:   600,
						TxnCount:    9,
						EventCount:  45,
						ShareOfRows: 0.999,
						ShareOfTxns: 0.1,
						Tables: map[string]int{
							"orders.payments": 600,
						},
						Operations: map[string]int{
							"UPDATE": 600,
						},
						AvgRowsPerTxn:      66.7,
						SampleQuerySummary: "update payments set status = ?",
					}},
				),
			},
			{
				Path:   "/tmp/snapshots/middle.json",
				Report: testInputReport("middle", "Middle", "2026-03-20T10:00:00Z", 1600, 80, 1800, 700, 500, 200, 1),
			},
			{
				Path: "/tmp/snapshots/later.json",
				Report: testInputReportWithPatterns(
					"later",
					"Later",
					"2026-03-21T10:00:00Z",
					3000,
					150,
					3600,
					1600,
					900,
					500,
					3,
					[]comparepkg.InputPattern{{
						PatternKey:  "orders.payments|UPDATE|medium",
						Label:       "payments.update_status",
						TotalRows:   1300,
						TxnCount:    18,
						EventCount:  90,
						ShareOfRows: 0.4333333333,
						ShareOfTxns: 0.15,
						Tables: map[string]int{
							"orders.payments": 1200,
							"orders.refunds":  100,
						},
						Operations: map[string]int{
							"UPDATE": 1200,
							"INSERT": 100,
						},
						AvgRowsPerTxn:      72.2,
						SampleQuerySummary: "update payments set status = ?",
					}},
				),
			},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	if len(result.PatternTrends) != 1 {
		t.Fatalf("expected 1 pattern trend, got %+v", result.PatternTrends)
	}

	got := result.PatternTrends[0]
	if got.PatternKey != "orders.payments|UPDATE|medium" {
		t.Fatalf("unexpected pattern key: %+v", got)
	}
	if got.Label != "payments.update_status" {
		t.Fatalf("unexpected label: %+v", got)
	}
	if got.FirstRows != 600 || got.LastRows != 1300 || got.DeltaRows != 700 {
		t.Fatalf("unexpected row trend values: %+v", got)
	}
	if !floatEquals(got.FirstShareOfRows, 0.6) || !floatEquals(got.LastShareOfRows, 0.4333333333) || !floatEquals(got.DeltaShareOfRows, -0.1666666667) {
		t.Fatalf("unexpected share trend values: %+v", got)
	}
	if len(got.RowsSeries) != 3 {
		t.Fatalf("expected 3 rows series points, got %+v", got.RowsSeries)
	}
	if got.RowsSeries[0].SnapshotName != "earlier" || got.RowsSeries[0].StartTime != "2026-03-19T10:00:00Z" || got.RowsSeries[0].Rows != 600 {
		t.Fatalf("unexpected first rows series point: %+v", got.RowsSeries[0])
	}
	if got.RowsSeries[1].SnapshotName != "middle" || got.RowsSeries[1].StartTime != "2026-03-20T10:00:00Z" || got.RowsSeries[1].Rows != 0 {
		t.Fatalf("unexpected middle rows series point: %+v", got.RowsSeries[1])
	}
	if got.RowsSeries[2].SnapshotName != "later" || got.RowsSeries[2].StartTime != "2026-03-21T10:00:00Z" || got.RowsSeries[2].Rows != 1300 {
		t.Fatalf("unexpected rows series: %+v", got.RowsSeries)
	}
	if len(got.ShareOfRowsSeries) != 3 {
		t.Fatalf("expected 3 share series points, got %+v", got.ShareOfRowsSeries)
	}
	if got.ShareOfRowsSeries[0].SnapshotName != "earlier" || got.ShareOfRowsSeries[0].StartTime != "2026-03-19T10:00:00Z" || !floatEquals(got.ShareOfRowsSeries[0].ShareOfRows, 0.6) {
		t.Fatalf("unexpected first share series point: %+v", got.ShareOfRowsSeries[0])
	}
	if got.ShareOfRowsSeries[1].SnapshotName != "middle" || got.ShareOfRowsSeries[1].StartTime != "2026-03-20T10:00:00Z" || !floatEquals(got.ShareOfRowsSeries[1].ShareOfRows, 0) {
		t.Fatalf("unexpected middle share series point: %+v", got.ShareOfRowsSeries[1])
	}
	if got.ShareOfRowsSeries[2].SnapshotName != "later" || got.ShareOfRowsSeries[2].StartTime != "2026-03-21T10:00:00Z" || !floatEquals(got.ShareOfRowsSeries[2].ShareOfRows, 0.4333333333) {
		t.Fatalf("unexpected share series: %+v", got.ShareOfRowsSeries)
	}
}

func TestBuildResultOrdersPatternTrendsByShareRowsAndKey(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{
				Path: "/tmp/snapshots/earlier.json",
				Report: testInputReportWithPatterns(
					"earlier",
					"Earlier",
					"2026-03-19T10:00:00Z",
					1000,
					50,
					1200,
					500,
					350,
					150,
					0,
					[]comparepkg.InputPattern{
						{
							PatternKey:  "pattern.z",
							Label:       "z",
							TotalRows:   250,
							TxnCount:    5,
							EventCount:  20,
							ShareOfRows: 0.25,
							ShareOfTxns: 0.1,
							Tables: map[string]int{
								"shop.orders": 250,
							},
							Operations: map[string]int{
								"UPDATE": 250,
							},
							AvgRowsPerTxn:      20,
							SampleQuerySummary: "query z",
						},
						{
							PatternKey:  "pattern.a",
							Label:       "a",
							TotalRows:   0,
							TxnCount:    1,
							EventCount:  4,
							ShareOfRows: 0.0,
							ShareOfTxns: 0.02,
							Tables: map[string]int{
								"shop.orders": 0,
							},
							Operations: map[string]int{
								"UPDATE": 0,
							},
							AvgRowsPerTxn:      0,
							SampleQuerySummary: "query a",
						},
						{
							PatternKey:  "pattern.b",
							Label:       "b",
							TotalRows:   0,
							TxnCount:    1,
							EventCount:  4,
							ShareOfRows: 0.0,
							ShareOfTxns: 0.02,
							Tables: map[string]int{
								"shop.orders": 0,
							},
							Operations: map[string]int{
								"UPDATE": 0,
							},
							AvgRowsPerTxn:      0,
							SampleQuerySummary: "query b",
						},
					},
				),
			},
			{
				Path: "/tmp/snapshots/later.json",
				Report: testInputReportWithPatterns(
					"later",
					"Later",
					"2026-03-20T10:00:00Z",
					2000,
					80,
					1800,
					700,
					500,
					200,
					1,
					[]comparepkg.InputPattern{
						{
							PatternKey:  "pattern.b",
							Label:       "b",
							TotalRows:   250,
							TxnCount:    6,
							EventCount:  24,
							ShareOfRows: 0.125,
							ShareOfTxns: 0.075,
							Tables: map[string]int{
								"shop.orders": 250,
							},
							Operations: map[string]int{
								"UPDATE": 250,
							},
							AvgRowsPerTxn:      29.2,
							SampleQuerySummary: "query b",
						},
						{
							PatternKey:  "pattern.z",
							Label:       "z",
							TotalRows:   750,
							TxnCount:    9,
							EventCount:  30,
							ShareOfRows: 0.375,
							ShareOfTxns: 0.1125,
							Tables: map[string]int{
								"shop.orders": 750,
							},
							Operations: map[string]int{
								"UPDATE": 750,
							},
							AvgRowsPerTxn:      33.3,
							SampleQuerySummary: "query z",
						},
						{
							PatternKey:  "pattern.a",
							Label:       "a",
							TotalRows:   250,
							TxnCount:    7,
							EventCount:  28,
							ShareOfRows: 0.125,
							ShareOfTxns: 0.0875,
							Tables: map[string]int{
								"shop.orders": 250,
							},
							Operations: map[string]int{
								"UPDATE": 250,
							},
							AvgRowsPerTxn:      28.6,
							SampleQuerySummary: "query a",
						},
					},
				),
			},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	if len(result.PatternTrends) != 3 {
		t.Fatalf("expected 3 pattern trends, got %+v", result.PatternTrends)
	}

	if result.PatternTrends[0].PatternKey != "pattern.z" {
		t.Fatalf("expected larger abs rows delta to win first tie, got %+v", result.PatternTrends)
	}
	if result.PatternTrends[0].DeltaRows != 500 || !floatEquals(result.PatternTrends[0].DeltaShareOfRows, 0.125) {
		t.Fatalf("unexpected leading trend values: %+v", result.PatternTrends[0])
	}
	if result.PatternTrends[1].PatternKey != "pattern.a" || result.PatternTrends[2].PatternKey != "pattern.b" {
		t.Fatalf("expected tertiary key tie-break after equal share and row deltas, got %+v", result.PatternTrends)
	}
	if result.PatternTrends[1].DeltaRows != 250 || result.PatternTrends[2].DeltaRows != 250 || !floatEquals(result.PatternTrends[1].DeltaShareOfRows, 0.125) || !floatEquals(result.PatternTrends[2].DeltaShareOfRows, 0.125) {
		t.Fatalf("unexpected tied trend values: %+v", result.PatternTrends[1:])
	}
}

func TestBuildResultLeavesPatternTrendsEmptyForLegacySnapshots(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{
				Path:   "/tmp/snapshots/legacy-a.json",
				Report: testInputReport("legacy-a", "Legacy A", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0),
			},
			{
				Path:   "/tmp/snapshots/legacy-b.json",
				Report: testInputReport("legacy-b", "Legacy B", "2026-03-20T10:00:00Z", 1600, 80, 1800, 700, 500, 200, 1),
			},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}
	if len(result.PatternTrends) != 0 {
		t.Fatalf("expected empty pattern trends for legacy snapshots, got %+v", result.PatternTrends)
	}
}

func TestBuildResultRejectsMissingWindowStartTime(t *testing.T) {
	_, err := BuildResult(BuildOptions{
		Points: []BuildInput{
			{Path: "/tmp/a.json", Report: testInputReport("a", "A", "", 1000, 50, 1200, 500, 350, 150, 0)},
			{Path: "/tmp/b.json", Report: testInputReport("b", "B", "2026-03-20T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0)},
		},
	})
	if err == nil {
		t.Fatal("expected missing start time error")
	}
}

func TestBuildResultFallsBackToSummaryStartTimeWhenSnapshotWindowMissing(t *testing.T) {
	legacy := testInputReport("legacy", "Legacy", "2026-03-21T10:00:00Z", 3000, 150, 3600, 1600, 900, 500, 3)
	legacy.Snapshot.Window.StartTime = ""

	result, err := BuildResult(BuildOptions{
		Points: []BuildInput{
			{
				Path:   "/tmp/good.json",
				Report: testInputReport("good", "Good", "2026-03-20T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0),
			},
			{
				Path:   "/tmp/legacy.json",
				Report: legacy,
			},
		},
	})
	if err != nil {
		t.Fatalf("expected summary.start_time fallback, got %v", err)
	}
	if len(result.Points) != 2 {
		t.Fatalf("expected 2 points, got %d", len(result.Points))
	}
	if result.Points[1].Snapshot.Name != "legacy" {
		t.Fatalf("expected legacy snapshot to be included, got %+v", result.Points)
	}
	if result.Points[1].Window.StartTime != "2026-03-21T10:00:00Z" {
		t.Fatalf("expected fallback window start time, got %+v", result.Points[1].Window)
	}
}

func testInputReport(name, label, start string, rows, txns, events, inserts, updates, deletes, alerts int) InputReport {
	return testInputReportWithPatterns(name, label, start, rows, txns, events, inserts, updates, deletes, alerts, nil)
}

func testInputReportWithPatterns(name, label, start string, rows, txns, events, inserts, updates, deletes, alerts int, patterns []comparepkg.InputPattern) InputReport {
	report := InputReport{
		Summary: InputSummary{
			TotalTransactions: txns,
			TotalRows:         rows,
			TotalEvents:       events,
			StartTime:         start,
			EndTime:           "2026-03-20T10:30:00Z",
			Duration:          "30m0s",
		},
		Tables: []InputTable{
			{
				Schema:     "shop",
				Table:      "orders",
				TotalRows:  rows,
				InsertRows: inserts,
				UpdateRows: updates,
				DeleteRows: deletes,
				TxnCount:   txns,
			},
		},
		Alerts:   make([]InputAlert, alerts),
		Patterns: patterns,
		Snapshot: &InputSnapshot{
			Name:      name,
			Label:     label,
			InputMode: "files",
			Input: InputSnapshotInput{
				Files: []string{name + ".binlog"},
			},
			Window: InputSnapshotWindow{
				StartTime: start,
				EndTime:   "2026-03-20T10:30:00Z",
			},
		},
	}
	return report
}

func floatEquals(got, want float64) bool {
	return math.Abs(got-want) < 0.0001
}
