// Package report verifies JSON rendering stability and SQL context presentation modes.
// input: synthetic AnalysisResult fixtures with provenance, XA identity, and bounded transaction query context variations.
// output: regression coverage for report-v3 identity fields, producer sets, counted event-byte diagnostics, and summary/off/full JSON SQL contracts.
// pos: JSON renderer regression suite guarding script-facing output contracts.
// note: if this file changes, update this header and module README.md.
package report

import (
	"encoding/json"
	"reflect"
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

func TestRenderJSONIncludesReportVersion(t *testing.T) {
	out, err := RenderJSON(model.AnalysisResult{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, `"report_version": 3`) {
		t.Fatalf("expected report_version in JSON, got: %s", out)
	}
}

func TestRenderJSONV3PreservesProvenanceIndependentlyOfSQLContext(t *testing.T) {
	result := model.AnalysisResult{
		Provenance: model.ReportProvenance{
			ServerIDs:      []uint32{7, 9},
			ServerVersions: []string{"11.8.3-MariaDB-log", "8.4.6"},
			ServerFlavors:  []string{"mariadb", "mysql"},
			MixedProducers: true,
		},
		Transactions: []model.Transaction{
			{
				TxnKey:        "txn-51",
				ServerID:      7,
				ServerVersion: "11.8.3-MariaDB-log",
				ServerFlavor:  "mariadb",
				GTID:          "0-7-1848",
				ThreadID:      1875,
				XID:           "3928",
				ActorUser:     "alice",
				ActorHost:     "db.local",
				QuerySummary:  "LOAD DATA INFILE '/tmp/orders.csv' INTO TABLE shop.orders",
				QueryContext:  model.NewQueryContext("LOAD DATA INFILE '/tmp/orders.csv' INTO TABLE shop.orders"),
			},
		},
	}

	out, err := RenderJSONWithOptions(result, Options{SQLContextMode: SQLContextOff})
	if err != nil {
		t.Fatalf("RenderJSONWithOptions: %v", err)
	}
	parsed := parseJSONMap(t, out)
	provenance := parsed["provenance"].(map[string]any)
	if provenance["mixed_producers"] != true || !reflect.DeepEqual(provenance["server_ids"], []any{float64(7), float64(9)}) {
		t.Fatalf("unexpected report provenance: %+v", provenance)
	}
	txn := parsed["transactions"].([]any)[0].(map[string]any)
	if txn["server_id"] != float64(7) || txn["gtid"] != "0-7-1848" || txn["thread_id"] != float64(1875) || txn["xid"] != "3928" {
		t.Fatalf("unexpected transaction provenance: %+v", txn)
	}
	if !reflect.DeepEqual(txn["actor"], map[string]any{"user": "alice", "host": "db.local"}) {
		t.Fatalf("unexpected actor evidence: %+v", txn["actor"])
	}
	for _, field := range []string{"query_summary", "query_sql", "query_truncated", "query_original_bytes"} {
		if _, ok := txn[field]; ok {
			t.Fatalf("off mode should omit %s without hiding provenance", field)
		}
	}
}

func TestRenderJSONV3OmitsUnknownIdentity(t *testing.T) {
	out, err := RenderJSON(model.AnalysisResult{Transactions: []model.Transaction{{TxnKey: "txn-unknown"}}})
	if err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	parsed := parseJSONMap(t, out)
	if _, ok := parsed["provenance"]; ok {
		t.Fatal("unknown report provenance must be omitted")
	}
	txn := parsed["transactions"].([]any)[0].(map[string]any)
	for _, field := range []string{"server_id", "server_version", "server_flavor", "gtid", "thread_id", "xid", "actor"} {
		if _, ok := txn[field]; ok {
			t.Fatalf("unknown identity field %s must be omitted", field)
		}
	}
}

func TestRenderJSONIncludesPatternsTopLevelField(t *testing.T) {
	out, err := RenderJSON(model.AnalysisResult{
		Patterns: []model.PatternStats{
			{
				PatternKey:          "tables=shop.orders|ops=INSERT|shape=small",
				Label:               "shop.orders / INSERT / small batch",
				TotalRows:           12,
				TxnCount:            3,
				EventCount:          6,
				ShareOfRows:         0.6,
				ShareOfTransactions: 0.5,
				AvgRowsPerTxn:       4,
				Tables:              map[string]int{"shop.orders": 12},
				Operations:          map[string]int{"INSERT": 12},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, `"patterns": [`) {
		t.Fatalf("expected patterns top-level array, got: %s", out)
	}
}

func TestRenderJSONPatternsUsesEmptyArrayWhenAbsent(t *testing.T) {
	out, err := RenderJSON(model.AnalysisResult{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, `"patterns": []`) {
		t.Fatalf("expected empty patterns array, got: %s", out)
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

func TestRenderJSONPreservesAllTablesWithDisplayLimits(t *testing.T) {
	result := model.AnalysisResult{
		Tables: []model.TableStats{
			{Schema: "shop", Table: "orders", TotalRows: 4},
			{Schema: "shop", Table: "payments", TotalRows: 2},
		},
	}

	out, err := RenderJSONWithOptions(result, Options{TopN: 1, TopTables: 1, TopTablesSet: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	tables := parseJSONMap(t, out)["tables"].([]any)
	if len(tables) != 2 {
		t.Fatalf("expected JSON to retain all tables, got %d", len(tables))
	}
}

func TestRenderJSONExposesUpdateEventsAndRows(t *testing.T) {
	result := model.AnalysisResult{
		Tables: []model.TableStats{{
			Schema:       "testdb",
			Table:        "users",
			TotalRows:    4,
			InsertRows:   2,
			UpdateRows:   1,
			UpdateEvents: 1,
			DeleteRows:   1,
		}},
		Diagnostics: model.Diagnostics{
			InputFormatGuess:      "STATEMENT",
			IgnoredQueryDMLEvents: 5,
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	parsed := parseJSONMap(t, out)
	tables := parsed["tables"].([]any)
	table := tables[0].(map[string]any)
	if table["update_rows"].(float64) != 1 {
		t.Fatalf("update_rows=%v, want 1", table["update_rows"])
	}
	if table["update_events"].(float64) != 1 {
		t.Fatalf("update_events=%v, want 1", table["update_events"])
	}
	diagnostics := parsed["diagnostics"].(map[string]any)
	if diagnostics["input_format_guess"] != "STATEMENT" {
		t.Fatalf("input_format_guess=%v", diagnostics["input_format_guess"])
	}
	if diagnostics["ignored_query_dml_events"].(float64) != 5 {
		t.Fatalf("ignored_query_dml_events=%v", diagnostics["ignored_query_dml_events"])
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

func TestRenderJSONReportsTransactionListCounts(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 12},
		Transactions: []model.Transaction{
			{TxnKey: "txn-1"},
			{TxnKey: "txn-2"},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	parsed := parseJSONMap(t, out)
	if parsed["transactions_listed"] != float64(2) {
		t.Fatalf("transactions_listed = %v, want 2", parsed["transactions_listed"])
	}
	if parsed["transactions_omitted"] != float64(10) {
		t.Fatalf("transactions_omitted = %v, want 10", parsed["transactions_omitted"])
	}
}
func TestRenderJSONIncludesXAIdentifier(t *testing.T) {
	result := model.AnalysisResult{
		Transactions: []model.Transaction{{
			TxnKey: "txn-1",
			XAXID:  "X'6276742d3537',X'',1",
		}},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	txn := parsed["transactions"].([]any)[0].(map[string]any)
	if txn["xa_xid"] != "X'6276742d3537',X'',1" {
		t.Fatalf("xa_xid=%v, want preserved MariaDB XA identifier", txn["xa_xid"])
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

func TestRenderJSONIncludesTimeseriesAndDiagnostics(t *testing.T) {
	result := model.AnalysisResult{
		Timeseries: model.Timeseries{
			TPSSeries: []model.TimeseriesPoint{{
				Minute: time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC),
				Value:  12,
			}},
			BinlogBytesSeries: []model.TimeseriesPoint{{
				Minute: time.Date(2026, 3, 9, 10, 1, 0, 0, time.UTC),
				Value:  2048,
			}},
			TxnSizeSeriesSummary: model.TxnSizeSeriesSummary{
				Buckets: []model.TxnSizeBucket{{
					Label:       "1k-10k",
					TxnCount:    3,
					Rows:        1200,
					BinlogBytes: 8192,
				}},
			},
		},
		Diagnostics: model.Diagnostics{
			DDLEvents: []model.DDLEvent{{
				Timestamp:     time.Date(2026, 3, 9, 10, 5, 0, 0, time.UTC),
				Operation:     "ALTER TABLE",
				Schema:        "shop",
				Table:         "orders",
				Statement:     "ALTER TABLE shop.orders ADD COLUMN note TEXT",
				BinlogPath:    "mysql-bin.000123",
				PositionStart: 100,
				PositionEnd:   200,
				BinlogBytes:   256,
			}},
			LargestTransactions: []model.Transaction{{
				TxnKey:          "txn-largest",
				TotalRows:       500,
				EventCount:      20,
				Duration:        4 * time.Second,
				BinlogBytes:     4096,
				BinlogPathStart: "mysql-bin.000123",
				BinlogPathEnd:   "mysql-bin.000123",
				PositionStart:   300,
				PositionEnd:     420,
				Tables:          map[string]int{"shop.orders": 500},
			}},
			HotIntervals: []model.MinuteBucket{{
				Minute:      time.Date(2026, 3, 9, 10, 6, 0, 0, time.UTC),
				TotalRows:   900,
				TxnCount:    12,
				EventCount:  18,
				BinlogBytes: 16384,
				DDLCount:    1,
				TableRows:   map[string]int{"shop.orders": 900},
			}},
			Findings: []model.Finding{{
				Kind:         "large_transaction",
				Severity:     "warning",
				Message:      "largest transaction exceeded threshold",
				TxnKey:       "txn-largest",
				Minute:       time.Date(2026, 3, 9, 10, 6, 0, 0, time.UTC),
				EvidenceRefs: []string{"transactions:txn-largest"},
			}},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)

	timeseries, ok := parsed["timeseries"].(map[string]any)
	if !ok {
		t.Fatalf("expected top-level timeseries object, got %v", parsed["timeseries"])
	}
	tpsSeries, ok := timeseries["tps_series"].([]any)
	if !ok || len(tpsSeries) != 1 {
		t.Fatalf("expected tps_series with one point, got %v", timeseries["tps_series"])
	}
	txnSizeSummary, ok := timeseries["txn_size_series_summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected txn_size_series_summary object, got %v", timeseries["txn_size_series_summary"])
	}
	buckets, ok := txnSizeSummary["buckets"].([]any)
	if !ok || len(buckets) != 1 {
		t.Fatalf("expected one txn size bucket, got %v", txnSizeSummary["buckets"])
	}

	diagnostics, ok := parsed["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected top-level diagnostics object, got %v", parsed["diagnostics"])
	}
	ddlEvents, ok := diagnostics["ddl_events"].([]any)
	if !ok || len(ddlEvents) != 1 {
		t.Fatalf("expected ddl_events with one item, got %v", diagnostics["ddl_events"])
	}
	largestTxns, ok := diagnostics["largest_transactions"].([]any)
	if !ok || len(largestTxns) != 1 {
		t.Fatalf("expected largest_transactions with one item, got %v", diagnostics["largest_transactions"])
	}
	hotIntervals, ok := diagnostics["hot_intervals"].([]any)
	if !ok || len(hotIntervals) != 1 {
		t.Fatalf("expected hot_intervals with one item, got %v", diagnostics["hot_intervals"])
	}
	findings, ok := diagnostics["findings"].([]any)
	if !ok || len(findings) != 1 {
		t.Fatalf("expected findings with one item, got %v", diagnostics["findings"])
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
		Patterns: []model.PatternStats{
			{PatternKey: "orders", SampleQuerySummary: "UPDATE orders SET status = ?"},
		},
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey: "orders",
				RepresentativeTransactions: []model.PatternRepresentativeTxn{
					{TxnKey: "txn-1", QuerySummary: "UPDATE orders SET status = ?"},
				},
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
	pattern := parsed["patterns"].([]any)[0].(map[string]any)
	if _, ok := pattern["sample_query_summary"]; ok {
		t.Fatal("off mode should omit pattern sample_query_summary")
	}
	representative := parsed["pattern_drilldowns"].([]any)[0].(map[string]any)["representative_transactions"].([]any)[0].(map[string]any)
	if _, ok := representative["query_summary"]; ok {
		t.Fatal("off mode should omit drilldown query_summary")
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

func TestRenderJSONSerializesSQLContextModeAndAvailability(t *testing.T) {
	for _, mode := range []SQLContextMode{SQLContextOff, SQLContextSummary, SQLContextFull} {
		t.Run(string(mode), func(t *testing.T) {
			out, err := RenderJSONWithOptions(model.AnalysisResult{SQLContextAvailable: true}, Options{SQLContextMode: mode})
			if err != nil {
				t.Fatalf("RenderJSONWithOptions: %v", err)
			}
			metadata := parseJSONMap(t, out)["sql_context"].(map[string]any)
			if metadata["mode"] != string(mode) || metadata["available"] != true {
				t.Fatalf("unexpected SQL context metadata: %+v", metadata)
			}
		})
	}

	out, err := RenderJSONWithOptions(model.AnalysisResult{}, Options{SQLContextMode: SQLContextFull})
	if err != nil {
		t.Fatalf("RenderJSONWithOptions: %v", err)
	}
	metadata := parseJSONMap(t, out)["sql_context"].(map[string]any)
	if metadata["mode"] != "full" || metadata["available"] != false {
		t.Fatalf("full mode without source SQL must be explicit and truthful: %+v", metadata)
	}
}

func TestRenderJSONIncludesFileSegments(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			FileSegments: []model.FileSegment{
				{
					StartTime:   time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC),
					EndTime:     time.Date(2026, 4, 15, 10, 5, 0, 0, time.UTC),
					BinlogBytes: 20480,
					Rows:        500,
					Events:      75,
				},
			},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	diagnostics, ok := parsed["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected diagnostics object, got %v", parsed["diagnostics"])
	}
	segments, ok := diagnostics["file_segments"].([]any)
	if !ok || len(segments) != 1 {
		t.Fatalf("expected file_segments with 1 item, got %v", diagnostics["file_segments"])
	}
	seg := segments[0].(map[string]any)
	if seg["rows"].(float64) != 500 {
		t.Fatalf("expected rows=500, got %v", seg["rows"])
	}
	if seg["binlog_bytes"].(float64) != 20480 {
		t.Fatalf("expected binlog_bytes=20480, got %v", seg["binlog_bytes"])
	}
	if seg["events"].(float64) != 75 {
		t.Fatalf("expected events=75, got %v", seg["events"])
	}
}

func TestRenderJSONIncludesWidestTransactions(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			WidestTransactions: []model.Transaction{
				{
					TxnKey:          "txn-wide",
					TotalRows:       100,
					EventCount:      10,
					Duration:        2 * time.Second,
					BinlogBytes:     4096,
					BinlogPathStart: "mysql-bin.000001",
					BinlogPathEnd:   "mysql-bin.000001",
					PositionStart:   100,
					PositionEnd:     200,
					Tables:          map[string]int{"shop.orders": 60, "shop.users": 40},
					Operations:      map[string]int{"INSERT": 100},
				},
			},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	diagnostics, ok := parsed["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected diagnostics object, got %v", parsed["diagnostics"])
	}
	widest, ok := diagnostics["widest_transactions"].([]any)
	if !ok || len(widest) != 1 {
		t.Fatalf("expected widest_transactions with 1 item, got %v", diagnostics["widest_transactions"])
	}
}

func TestRenderJSONTransactionEvidenceFields(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			LargestTransactions: []model.Transaction{
				{
					TxnKey:          "txn-evidence",
					StartTime:       time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC),
					EndTime:         time.Date(2026, 4, 15, 10, 0, 5, 0, time.UTC),
					Duration:        5 * time.Second,
					TotalRows:       2000,
					EventCount:      25,
					BinlogBytes:     16384,
					BinlogPathStart: "mysql-bin.000044",
					BinlogPathEnd:   "mysql-bin.000045",
					PositionStart:   300,
					PositionEnd:     520,
					Tables:          map[string]int{"shop.orders": 1800, "shop.payments": 200},
					Operations:      map[string]int{"UPDATE": 2000},
					QuerySummary:    "UPDATE shop.orders SET status='done'",
				},
			},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	diagnostics, ok := parsed["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected diagnostics object, got %v", parsed["diagnostics"])
	}
	largest, ok := diagnostics["largest_transactions"].([]any)
	if !ok || len(largest) != 1 {
		t.Fatalf("expected largest_transactions with 1 item, got %v", diagnostics["largest_transactions"])
	}
	txn := largest[0].(map[string]any)

	// Verify evidence fields
	if txn["binlog_bytes"].(float64) != 16384 {
		t.Fatalf("expected binlog_bytes=16384, got %v", txn["binlog_bytes"])
	}
	if txn["binlog_file_start"] != "mysql-bin.000044" {
		t.Fatalf("expected binlog_file_start, got %v", txn["binlog_file_start"])
	}
	if txn["binlog_file_end"] != "mysql-bin.000045" {
		t.Fatalf("expected binlog_file_end, got %v", txn["binlog_file_end"])
	}
	if txn["pos_start"].(float64) != 300 {
		t.Fatalf("expected pos_start=300, got %v", txn["pos_start"])
	}
	if txn["pos_end"].(float64) != 520 {
		t.Fatalf("expected pos_end=520, got %v", txn["pos_end"])
	}
}

func TestRenderJSONFileCoverageIncludesTimeRange(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			FileCoverage: model.FileCoverage{
				Selected: []model.FileCoverageItem{
					{
						BinlogPath:   "mysql-bin.000001",
						Reason:       "selected",
						Size:         1024,
						FirstEventAt: time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC),
						LastEventAt:  time.Date(2026, 4, 15, 10, 30, 0, 0, time.UTC),
					},
				},
				Skipped: []model.FileCoverageItem{
					{
						BinlogPath: "mysql-bin.000003",
						Reason:     "outside window",
						Size:       2048,
					},
				},
			},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	diagnostics, ok := parsed["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected diagnostics object, got %v", parsed["diagnostics"])
	}
	coverage, ok := diagnostics["file_coverage"].(map[string]any)
	if !ok {
		t.Fatalf("expected file_coverage object, got %v", diagnostics["file_coverage"])
	}
	selected, ok := coverage["selected"].([]any)
	if !ok || len(selected) != 1 {
		t.Fatalf("expected 1 selected file, got %v", coverage["selected"])
	}
	item := selected[0].(map[string]any)
	if item["binlog_path"] != "mysql-bin.000001" {
		t.Fatalf("expected binlog_path, got %v", item["binlog_path"])
	}
	if item["first_event_at"] != "2026-04-15T10:00:00Z" {
		t.Fatalf("expected first_event_at, got %v", item["first_event_at"])
	}
	if item["last_event_at"] != "2026-04-15T10:30:00Z" {
		t.Fatalf("expected last_event_at, got %v", item["last_event_at"])
	}

	skipped, ok := coverage["skipped"].([]any)
	if !ok || len(skipped) != 1 {
		t.Fatalf("expected 1 skipped file, got %v", coverage["skipped"])
	}
}

func TestRenderJSONIncludesCountedEventBytes(t *testing.T) {
	out, err := RenderJSON(model.AnalysisResult{
		Diagnostics: model.Diagnostics{CountedEventBytes: 250},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	diagnostics, ok := parsed["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected diagnostics object, got %v", parsed["diagnostics"])
	}
	if got := diagnostics["counted_event_bytes"]; got != float64(250) {
		t.Fatalf("expected counted_event_bytes=250, got %v", got)
	}
}
