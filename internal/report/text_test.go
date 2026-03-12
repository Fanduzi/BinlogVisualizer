package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestRenderTextIncludesWorkloadSummary(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 10,
			TotalRows:         1000,
			TotalEvents:       50,
			StartTime:         time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC),
			EndTime:           time.Date(2026, 3, 9, 10, 30, 0, 0, time.UTC),
			Duration:          30 * time.Minute,
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Workload Summary") {
		t.Fatal("expected 'Workload Summary' section")
	}
	if !strings.Contains(out, "10") {
		t.Fatal("expected transaction count in output")
	}
}

func TestRenderTextIncludesTopTables(t *testing.T) {
	result := model.AnalysisResult{
		Tables: []model.TableStats{
			{Schema: "shop", Table: "orders", TotalRows: 500},
			{Schema: "shop", Table: "users", TotalRows: 300},
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Top Tables") {
		t.Fatal("expected 'Top Tables' section")
	}
	if !strings.Contains(out, "shop.orders") {
		t.Fatal("expected table name in output")
	}
}

func TestRenderTextIncludesTopTransactions(t *testing.T) {
	result := model.AnalysisResult{
		Transactions: []model.Transaction{
			{TxnKey: "txn-1", TotalRows: 100, Duration: 5 * time.Second},
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Top Transactions") {
		t.Fatal("expected 'Top Transactions' section")
	}
}

func TestRenderTextIncludesMinuteActivity(t *testing.T) {
	result := model.AnalysisResult{
		Minutes: []model.MinuteBucket{
			{Minute: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC), TotalRows: 100},
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Minute Activity") {
		t.Fatal("expected 'Minute Activity' section")
	}
}

func TestRenderTextIncludesAlerts(t *testing.T) {
	result := model.AnalysisResult{
		Alerts: []model.Alert{
			{Type: "large_transaction", Severity: "warning", Message: "Transaction too large"},
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Alerts") {
		t.Fatal("expected 'Alerts' section")
	}
}

func TestRenderTextHandlesEmptySections(t *testing.T) {
	// All sections empty - should still render all section headers
	result := model.AnalysisResult{}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All sections should be present even if empty
	expectedSections := []string{
		"Workload Summary",
		"Top Tables",
		"Top Transactions",
		"Minute Activity",
		"Alerts",
	}
	for _, section := range expectedSections {
		if !strings.Contains(out, section) {
			t.Fatalf("expected section %q in output", section)
		}
	}
}

func TestRenderTextSectionOrder(t *testing.T) {
	// Verify sections appear in the correct order
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 1},
		Tables:  []model.TableStats{{Schema: "s", Table: "t", TotalRows: 1}},
		Transactions: []model.Transaction{{TxnKey: "t1"}},
		Minutes: []model.MinuteBucket{{Minute: time.Now()}},
		Alerts:  []model.Alert{{Type: "test"}},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify order
	summaryIdx := strings.Index(out, "Workload Summary")
	tablesIdx := strings.Index(out, "Top Tables")
	txnIdx := strings.Index(out, "Top Transactions")
	minuteIdx := strings.Index(out, "Minute Activity")
	alertsIdx := strings.Index(out, "Alerts")

	if !(summaryIdx < tablesIdx && tablesIdx < txnIdx && txnIdx < minuteIdx && minuteIdx < alertsIdx) {
		t.Fatal("sections not in correct order: Workload Summary < Top Tables < Top Transactions < Minute Activity < Alerts")
	}
}
