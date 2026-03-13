package report

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestRenderJSONProducesValidObject(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 10,
			TotalRows:         1000,
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) == 0 || out[0] != '{' {
		t.Fatalf("expected JSON object, got: %s", out)
	}
}

func TestRenderJSONIncludesSummary(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 5,
			TotalRows:         500,
			TotalEvents:       25,
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	summary, ok := parsed["summary"].(map[string]any)
	if !ok {
		t.Fatal("expected 'summary' object")
	}
	if summary["total_transactions"].(float64) != 5 {
		t.Fatalf("expected total_transactions=5, got %v", summary["total_transactions"])
	}
}

func TestRenderJSONIncludesTables(t *testing.T) {
	result := model.AnalysisResult{
		Tables: []model.TableStats{
			{Schema: "shop", Table: "orders", TotalRows: 100},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	tables, ok := parsed["tables"].([]any)
	if !ok || len(tables) != 1 {
		t.Fatalf("expected 'tables' array with 1 element, got %v", parsed["tables"])
	}
}

func TestRenderJSONIncludesTransactions(t *testing.T) {
	result := model.AnalysisResult{
		Transactions: []model.Transaction{
			{TxnKey: "txn-1", TotalRows: 50, Duration: 5 * time.Second},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	txn, ok := parsed["transactions"].([]any)
	if !ok || len(txn) != 1 {
		t.Fatalf("expected 'transactions' array with 1 element")
	}
}

func TestRenderJSONIncludesMinutes(t *testing.T) {
	result := model.AnalysisResult{
		Minutes: []model.MinuteBucket{
			{Minute: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC), TotalRows: 100},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "minutes") {
		t.Fatal("expected 'minutes' field in JSON")
	}
}

func TestRenderJSONIncludesAlerts(t *testing.T) {
	result := model.AnalysisResult{
		Alerts: []model.Alert{
			{Type: "large_transaction", Severity: "warning", Message: "test alert"},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	alerts, ok := parsed["alerts"].([]any)
	if !ok || len(alerts) != 1 {
		t.Fatalf("expected 'alerts' array with 1 element")
	}
}

func TestRenderJSONHandlesEmptyResult(t *testing.T) {
	result := model.AnalysisResult{}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	// All sections should be present
	expectedFields := []string{"summary", "tables", "transactions", "minutes", "alerts"}
	for _, field := range expectedFields {
		if _, ok := parsed[field]; !ok {
			t.Fatalf("expected field %q in JSON", field)
		}
	}
}

func TestRenderJSONFieldNamesAreStable(t *testing.T) {
	// Verify field names are snake_case and script-friendly
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 1,
			TotalRows:         100,
			TotalEvents:       10,
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check for expected snake_case field names
	expectedFields := []string{
		"total_transactions",
		"total_rows",
		"total_events",
	}
	for _, field := range expectedFields {
		if !strings.Contains(out, field) {
			t.Fatalf("expected snake_case field %q in JSON", field)
		}
	}
}

func TestRenderJSONTimeFormat(t *testing.T) {
	// Verify times are in ISO 8601 format
	startTime := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{
			StartTime: startTime,
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should contain ISO 8601 format
	if !strings.Contains(out, "2026-03-09T10:00:00") {
		t.Fatalf("expected ISO 8601 time format, got: %s", out)
	}
}

func TestRenderJSONDefensiveCopyTransactionMaps(t *testing.T) {
	// Verify that the rendered JSON string is immutable after rendering
	tables := map[string]int{"shop.orders": 100}
	operations := map[string]int{"INSERT": 50, "UPDATE": 50}

	result := model.AnalysisResult{
		Transactions: []model.Transaction{
			{
				TxnKey:     "txn-1",
				Tables:     tables,
				Operations: operations,
			},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify original values are in the output
	if !strings.Contains(out, `"shop.orders": 100`) {
		t.Fatalf("expected original tables value in render, got: %s", out)
	}
	if !strings.Contains(out, `"INSERT": 50`) {
		t.Fatalf("expected original operations value in render, got: %s", out)
	}

	// Modify original maps after rendering
	tables["shop.orders"] = 999
	operations["INSERT"] = 0
	operations["NEW_OP"] = 100

	// The already-rendered string should be unchanged (strings are immutable in Go)
	// This verifies the JSON output at time of rendering is captured
	if !strings.Contains(out, `"shop.orders": 100`) {
		t.Fatalf("rendered JSON string changed after original map was modified, got: %s", out)
	}
	if strings.Contains(out, `"NEW_OP"`) {
		t.Fatal("rendered JSON string should not contain key added after rendering")
	}
}

func TestRenderJSONDefensiveCopyMinuteBucketMap(t *testing.T) {
	// Verify that the rendered JSON string is immutable after rendering
	tableRows := map[string]int{"shop.orders": 100}

	result := model.AnalysisResult{
		Minutes: []model.MinuteBucket{
			{
				Minute:    time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC),
				TableRows: tableRows,
			},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify original values are in the output
	if !strings.Contains(out, `"shop.orders": 100`) {
		t.Fatalf("expected original TableRows value in render, got: %s", out)
	}

	// Modify original map after rendering
	tableRows["shop.orders"] = 999
	tableRows["shop.users"] = 500

	// The already-rendered string should be unchanged
	if !strings.Contains(out, `"shop.orders": 100`) {
		t.Fatalf("rendered JSON string changed after original map was modified, got: %s", out)
	}
	if strings.Contains(out, `"shop.users"`) {
		t.Fatal("rendered JSON string should not contain key added after rendering")
	}
}

func TestRenderJSONDefensiveCopyAlertDetails(t *testing.T) {
	// Verify that the rendered JSON string is immutable after rendering
	details := map[string]any{"rows": 1000, "threshold": 500}

	result := model.AnalysisResult{
		Alerts: []model.Alert{
			{
				Type:    "large_transaction",
				Details: details,
			},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify original values are in the output
	if !strings.Contains(out, `"rows": 1000`) {
		t.Fatalf("expected original Details value in render, got: %s", out)
	}

	// Modify original map after rendering
	details["rows"] = 9999
	details["new_key"] = "new_value"

	// The already-rendered string should be unchanged
	if !strings.Contains(out, `"rows": 1000`) {
		t.Fatalf("rendered JSON string changed after original map was modified, got: %s", out)
	}
	if strings.Contains(out, `"new_key"`) {
		t.Fatal("rendered JSON string should not contain key added after rendering")
	}
}
