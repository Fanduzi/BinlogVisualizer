package compare

import (
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
