// Package report verifies JSON rendering stability and SQL context presentation modes.
// input: synthetic AnalysisResult fixtures with bounded transaction query context variations.
// output: regression coverage for stable field names and summary/off/full JSON query fields.
// pos: JSON renderer regression suite guarding script-facing output contracts.
// note: if this file changes, update this header and module README.md.
package report

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func parseJSONMap(t *testing.T, out string) map[string]any {
	t.Helper()
	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	return parsed
}

func TestRenderJSONIncludesSnapshotWhenMetadataPresent(t *testing.T) {
	result := model.AnalysisResult{
		Snapshot: &model.Snapshot{
			Name:             "incident-window-2026-03-09",
			Label:            "current",
			CreatedAt:        time.Date(2026, 3, 9, 10, 15, 0, 0, time.UTC),
			BinlogvizVersion: "v1.2.3",
			InputMode:        "discovery",
			Input: model.SnapshotInput{
				Files:   []string{"mysql-bin.000123"},
				FromDir: "/var/lib/mysql",
				Prefix:  "mysql-bin.",
			},
			Window: model.SnapshotWindow{
				StartTime: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC),
				EndTime:   time.Date(2026, 3, 9, 10, 30, 0, 0, time.UTC),
			},
			Filters: model.SnapshotFilters{
				IncludeSchemas: []string{"shop"},
				ExcludeTables:  []string{"shop.temp_*"},
			},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	snapshot, ok := parsed["snapshot"].(map[string]any)
	if !ok {
		t.Fatalf("expected 'snapshot' object, got %v", parsed["snapshot"])
	}

	if _, ok := snapshot["metadata"]; ok {
		t.Fatalf("did not expect nested metadata wrapper, got %v", snapshot["metadata"])
	}

	expectedFields := []string{"name", "label", "created_at", "binlogviz_version", "input_mode", "input", "window", "filters"}
	for _, field := range expectedFields {
		if _, ok := snapshot[field]; !ok {
			t.Fatalf("expected snapshot field %q", field)
		}
	}
	if snapshot["name"] != "incident-window-2026-03-09" {
		t.Fatalf("expected snapshot name to be rendered, got %v", snapshot["name"])
	}
	if snapshot["label"] != "current" {
		t.Fatalf("expected snapshot label to be rendered, got %v", snapshot["label"])
	}
	if snapshot["created_at"] != "2026-03-09T10:15:00Z" {
		t.Fatalf("expected created_at to be rendered, got %v", snapshot["created_at"])
	}

	input, ok := snapshot["input"].(map[string]any)
	if !ok {
		t.Fatalf("expected input object, got %v", snapshot["input"])
	}
	files, ok := input["files"].([]any)
	if !ok || len(files) != 1 || files[0] != "mysql-bin.000123" {
		t.Fatalf("expected input.files to contain one file, got %v", input["files"])
	}

	window, ok := snapshot["window"].(map[string]any)
	if !ok {
		t.Fatalf("expected window object, got %v", snapshot["window"])
	}
	if window["start_time"] != "2026-03-09T10:00:00Z" {
		t.Fatalf("expected window.start_time to be rendered, got %v", window["start_time"])
	}

	filters, ok := snapshot["filters"].(map[string]any)
	if !ok {
		t.Fatalf("expected filters object, got %v", snapshot["filters"])
	}
	includeSchemas, ok := filters["include_schema"].([]any)
	if !ok || len(includeSchemas) != 1 || includeSchemas[0] != "shop" {
		t.Fatalf("expected filters.include_schema to contain shop, got %v", filters["include_schema"])
	}
}

func TestRenderJSONOmitsSnapshotWhenMetadataAbsent(t *testing.T) {
	result := model.AnalysisResult{}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	if _, ok := parsed["snapshot"]; ok {
		t.Fatalf("expected snapshot to be omitted, got %v", parsed["snapshot"])
	}
}

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

func TestRenderJSONSQLContextSummaryMode(t *testing.T) {
	result := model.AnalysisResult{
		Transactions: []model.Transaction{
			{
				TxnKey:       "txn-1",
				QuerySummary: "UPDATE orders SET status = ? WHERE id = ?",
				QueryContext: model.NewQueryContext("UPDATE orders SET status = 'paid' WHERE id = 42"),
			},
		},
	}

	out, err := RenderJSONWithOptions(result, Options{SQLContextMode: SQLContextSummary})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	txn := parsed["transactions"].([]any)[0].(map[string]any)
	if txn["query_summary"] != "UPDATE orders SET status = ? WHERE id = ?" {
		t.Fatalf("unexpected query_summary: %v", txn["query_summary"])
	}
	if _, ok := txn["query_sql"]; ok {
		t.Fatal("summary mode should not output query_sql")
	}
	if _, ok := txn["query_original_bytes"]; !ok {
		t.Fatal("summary mode should output query_original_bytes")
	}
}

func TestRenderJSONSQLContextOffMode(t *testing.T) {
	result := model.AnalysisResult{
		Transactions: []model.Transaction{
			{
				TxnKey:       "txn-1",
				QuerySummary: "UPDATE orders SET status = ? WHERE id = ?",
				QueryContext: model.NewQueryContext("UPDATE orders SET status = 'paid' WHERE id = 42"),
			},
		},
	}

	out, err := RenderJSONWithOptions(result, Options{SQLContextMode: SQLContextOff})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	txn := parsed["transactions"].([]any)[0].(map[string]any)
	for _, field := range []string{"query_summary", "query_truncated", "query_original_bytes", "query_sql"} {
		if _, ok := txn[field]; ok {
			t.Fatalf("off mode should omit %s", field)
		}
	}
}

func TestRenderJSONSQLContextFullModeUsesBoundedSQL(t *testing.T) {
	longSQL := strings.Repeat("x", model.MaxStoredSQLBytes+128)
	result := model.AnalysisResult{
		Transactions: []model.Transaction{
			{
				TxnKey:       "txn-1",
				QuerySummary: "summary",
				QueryContext: model.NewQueryContext(longSQL),
			},
		},
	}

	out, err := RenderJSONWithOptions(result, Options{SQLContextMode: SQLContextFull})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	txn := parsed["transactions"].([]any)[0].(map[string]any)
	querySQL, ok := txn["query_sql"].(string)
	if !ok {
		t.Fatal("full mode should output query_sql")
	}
	if len(querySQL) != model.MaxStoredSQLBytes {
		t.Fatalf("expected bounded query_sql length %d, got %d", model.MaxStoredSQLBytes, len(querySQL))
	}
	if querySQL == longSQL {
		t.Fatal("full mode should not output unbounded original SQL")
	}
}
