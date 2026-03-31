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
	if result.TableChanges[0].Schema != "orders" || result.TableChanges[0].Table != "payments" {
		t.Fatalf("expected first table change to be orders.payments, got %s.%s", result.TableChanges[0].Schema, result.TableChanges[0].Table)
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
