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

func TestRenderTopTransactionsDeterministicOrder(t *testing.T) {
	// Create transactions with same TotalRows but different TxnKey
	// Order should be deterministic: TotalRows DESC, TxnKey ASC
	result := model.AnalysisResult{
		Transactions: []model.Transaction{
			{TxnKey: "txn-z", TotalRows: 100, Duration: 1 * time.Second},
			{TxnKey: "txn-a", TotalRows: 100, Duration: 1 * time.Second},
			{TxnKey: "txn-m", TotalRows: 100, Duration: 1 * time.Second},
			{TxnKey: "txn-b", TotalRows: 50, Duration: 500 * time.Millisecond},
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify transactions with same TotalRows appear in TxnKey order (a, m, z)
	// All three with 100 rows should come before the one with 50 rows
	idxA := strings.Index(out, "txn-a")
	idxM := strings.Index(out, "txn-m")
	idxZ := strings.Index(out, "txn-z")
	idxB := strings.Index(out, "txn-b")

	if idxA == -1 || idxM == -1 || idxZ == -1 || idxB == -1 {
		t.Fatalf("missing transaction in output: a=%d, m=%d, z=%d, b=%d", idxA, idxM, idxZ, idxB)
	}

	// Verify order: txn-a < txn-m < txn-z (all with 100 rows)
	// and all 100-row txns come before txn-b (50 rows)
	if !(idxA < idxM && idxM < idxZ) {
		t.Errorf("transactions with same TotalRows should be ordered by TxnKey ASC: got a=%d, m=%d, z=%d", idxA, idxM, idxZ)
	}
	if !(idxZ < idxB) {
		t.Errorf("transactions with higher TotalRows should come first: got z(100)=%d, b(50)=%d", idxZ, idxB)
	}
}

func TestRenderTopTransactionsMixedRowsDeterministic(t *testing.T) {
	// Test with mixed TotalRows where tie-breaker matters for middle items
	result := model.AnalysisResult{
		Transactions: []model.Transaction{
			{TxnKey: "z-large", TotalRows: 1000, Duration: 1 * time.Second},
			{TxnKey: "c-medium", TotalRows: 500, Duration: 1 * time.Second},
			{TxnKey: "a-medium", TotalRows: 500, Duration: 1 * time.Second},
			{TxnKey: "b-medium", TotalRows: 500, Duration: 1 * time.Second},
			{TxnKey: "x-small", TotalRows: 100, Duration: 1 * time.Second},
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Find positions
	idxLarge := strings.Index(out, "z-large")
	idxA := strings.Index(out, "a-medium")
	idxB := strings.Index(out, "b-medium")
	idxC := strings.Index(out, "c-medium")
	idxSmall := strings.Index(out, "x-small")

	// Expected order: z-large (1000), a-medium (500), b-medium (500), c-medium (500), x-small (100)
	if !(idxLarge < idxA && idxA < idxB && idxB < idxC && idxC < idxSmall) {
		t.Errorf("unexpected order: large=%d, a=%d, b=%d, c=%d, small=%d",
			idxLarge, idxA, idxB, idxC, idxSmall)
	}
}
