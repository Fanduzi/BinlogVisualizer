package trend

import "testing"

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

func testInputReport(name, label, start string, rows, txns, events, inserts, updates, deletes, alerts int) InputReport {
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
		Alerts: make([]InputAlert, alerts),
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
