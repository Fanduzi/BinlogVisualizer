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
