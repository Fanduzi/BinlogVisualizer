package compare

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildCompareResultCalculatesSummaryDeltas(t *testing.T) {
	current, err := LoadReport(filepath.Join("testdata", "current.json"))
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	baseline, err := LoadReport(filepath.Join("testdata", "baseline.json"))
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}

	result := BuildCompareResult(current, baseline)
	if result.Summary.TotalRowsDelta != 900 {
		t.Fatalf("expected rows delta 900, got %d", result.Summary.TotalRowsDelta)
	}
	if result.Summary.TotalTransactionsDelta != 30 {
		t.Fatalf("expected txn delta 30, got %d", result.Summary.TotalTransactionsDelta)
	}
}

func TestBuildCompareResultBuildsTopTableUnionSortedByAbsoluteDelta(t *testing.T) {
	current, _ := LoadReport(filepath.Join("testdata", "current.json"))
	baseline, _ := LoadReport(filepath.Join("testdata", "baseline.json"))

	result := BuildCompareResult(current, baseline)
	if len(result.TableChanges) != 3 {
		t.Fatalf("expected 3 table changes, got %d", len(result.TableChanges))
	}

	expected := []struct {
		schema string
		table  string
		delta  int
	}{
		{schema: "orders", table: "refunds", delta: 900},
		{schema: "orders", table: "chargebacks", delta: -400},
		{schema: "orders", table: "payments", delta: 400},
	}

	for i, want := range expected {
		got := result.TableChanges[i]
		if got.Schema != want.schema || got.Table != want.table || got.DeltaRows != want.delta {
			t.Fatalf("expected position %d to be %s.%s delta %d, got %s.%s delta %d", i, want.schema, want.table, want.delta, got.Schema, got.Table, got.DeltaRows)
		}
	}
}

func TestBuildCompareResultClassifiesAlertChanges(t *testing.T) {
	current, _ := LoadReport(filepath.Join("testdata", "current.json"))
	baseline, _ := LoadReport(filepath.Join("testdata", "baseline.json"))

	result := BuildCompareResult(current, baseline)
	if len(result.AlertChanges.Added) != 2 {
		t.Fatalf("expected 2 added alerts, got %d", len(result.AlertChanges.Added))
	}
	if len(result.AlertChanges.Removed) != 1 {
		t.Fatalf("expected 1 removed alert, got %d", len(result.AlertChanges.Removed))
	}
}

func TestBuildCompareResultUsesSnapshotIdentityWhenPresent(t *testing.T) {
	dir := t.TempDir()
	currentPath := filepath.Join(dir, "current.json")
	baselinePath := filepath.Join(dir, "baseline.json")

	if err := os.WriteFile(currentPath, []byte(`{
  "summary": {
    "total_transactions": 120,
    "total_rows": 2400,
    "total_events": 3000,
    "start_time": "2026-03-20T10:00:00Z",
    "end_time": "2026-03-20T10:30:00Z",
    "duration": "30m0s"
  },
  "tables": [],
  "alerts": [],
  "warnings": 0,
  "snapshot": {
    "name": "current-snap",
    "label": "Current Snapshot",
    "created_at": "2026-03-20T10:31:00Z",
    "binlogviz_version": "1.2.3",
    "input_mode": "files",
    "input": {"files": [], "from_dir": "", "prefix": ""},
    "window": {"start_time": "2026-03-20T10:00:00Z", "end_time": "2026-03-20T10:30:00Z"},
    "filters": {"include_schema": [], "exclude_schema": [], "include_table": [], "exclude_table": []}
  }
}`), 0o644); err != nil {
		t.Fatalf("write current fixture: %v", err)
	}
	if err := os.WriteFile(baselinePath, []byte(`{
  "summary": {
    "total_transactions": 90,
    "total_rows": 1500,
    "total_events": 1800,
    "start_time": "2026-03-13T10:00:00Z",
    "end_time": "2026-03-13T10:30:00Z",
    "duration": "30m0s"
  },
  "tables": [],
  "alerts": [],
  "warnings": 0,
  "snapshot": {
    "name": "baseline-snap",
    "label": "Baseline Snapshot",
    "created_at": "2026-03-13T10:31:00Z",
    "binlogviz_version": "1.2.3",
    "input_mode": "files",
    "input": {"files": [], "from_dir": "", "prefix": ""},
    "window": {"start_time": "2026-03-13T10:00:00Z", "end_time": "2026-03-13T10:30:00Z"},
    "filters": {"include_schema": [], "exclude_schema": [], "include_table": [], "exclude_table": []}
  }
}`), 0o644); err != nil {
		t.Fatalf("write baseline fixture: %v", err)
	}

	current, err := LoadReport(currentPath)
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	baseline, err := LoadReport(baselinePath)
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}

	result := BuildCompareResult(current, baseline)
	if result.CurrentLabel != "Current Snapshot (current-snap)" {
		t.Fatalf("expected snapshot-aware current label, got %q", result.CurrentLabel)
	}
	if result.BaselineLabel != "Baseline Snapshot (baseline-snap)" {
		t.Fatalf("expected snapshot-aware baseline label, got %q", result.BaselineLabel)
	}
}

func TestDecodeReportJSONCarriesSnapshotMetadataWhenPresent(t *testing.T) {
	report, err := DecodeReportJSON([]byte(`{
  "summary": {
    "total_transactions": 120,
    "total_rows": 2400,
    "total_events": 3000,
    "start_time": "2026-03-20T10:00:00Z",
    "end_time": "2026-03-20T10:30:00Z",
    "duration": "30m0s"
  },
  "tables": [],
  "alerts": [],
  "warnings": 0,
  "snapshot": {
    "name": "current-snap",
    "label": "Current Snapshot",
    "created_at": "2026-03-20T10:31:00Z",
    "binlogviz_version": "1.2.3",
    "input_mode": "files",
    "input": {
      "files": ["mysql-bin.000001"],
      "from_dir": "",
      "prefix": ""
    },
    "window": {
      "start_time": "2026-03-20T10:00:00Z",
      "end_time": "2026-03-20T10:30:00Z"
    },
    "filters": {
      "include_schema": [],
      "exclude_schema": [],
      "include_table": [],
      "exclude_table": []
    }
  }
}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Snapshot == nil {
		t.Fatal("expected snapshot metadata to be decoded")
	}
	if report.Snapshot.Name != "current-snap" {
		t.Fatalf("expected snapshot name to round-trip, got %q", report.Snapshot.Name)
	}
	if report.Snapshot.Label != "Current Snapshot" {
		t.Fatalf("expected snapshot label to round-trip, got %q", report.Snapshot.Label)
	}
}

func TestBuildCompareResultBuildsPatternUnionSortedByAbsoluteRowsDelta(t *testing.T) {
	current, err := LoadReport(filepath.Join("testdata", "current_patterns.json"))
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	baseline, err := LoadReport(filepath.Join("testdata", "baseline_patterns.json"))
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}

	result := BuildCompareResult(current, baseline)
	if len(result.PatternChanges) != 3 {
		t.Fatalf("expected 3 pattern changes, got %d", len(result.PatternChanges))
	}

	expected := []struct {
		key   string
		delta int
	}{
		{key: "orders.refunds|INSERT|small", delta: 700},
		{key: "orders.chargebacks|INSERT|small", delta: -500},
		{key: "orders.payments|UPDATE|medium", delta: 700},
	}

	for _, want := range expected {
		found := false
		for _, got := range result.PatternChanges {
			if got.PatternKey == want.key && got.DeltaRows == want.delta {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected pattern %q with delta %d in %+v", want.key, want.delta, result.PatternChanges)
		}
	}
}

func TestBuildCompareResultTreatsLegacyPatternsAsEmptySet(t *testing.T) {
	current, err := LoadReport(filepath.Join("testdata", "current_patterns.json"))
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	baseline, err := LoadReport(filepath.Join("testdata", "legacy_no_patterns.json"))
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}

	result := BuildCompareResult(current, baseline)
	if len(result.PatternChanges) != 2 {
		t.Fatalf("expected 2 pattern changes, got %d", len(result.PatternChanges))
	}
	for _, change := range result.PatternChanges {
		if change.BaselineRows != 0 {
			t.Fatalf("expected legacy baseline rows to stay at 0, got %+v", change)
		}
	}
}

func TestDecodeReportJSONTreatsMissingPatternsAsEmpty(t *testing.T) {
	report, err := LoadReport(filepath.Join("testdata", "legacy_no_patterns.json"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.Patterns) != 0 {
		t.Fatalf("expected empty pattern slice for legacy report, got %+v", report.Patterns)
	}
}
