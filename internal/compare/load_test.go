// Package compare verifies compare report loading and input validation behavior.
// input: report-v0-v3 fixture JSON with optional workload identity/scope/provenance plus invalid payloads.
// output: assertions for identity/scope persistence, compatible decoding without fabricated legacy metadata, and explicit shape/version errors.
// pos: regression coverage for the compare input loading boundary.
// note: if this file changes, keep internal/compare/README.md synchronized.
package compare

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadReportLoadsValidBinlogVizJSON(t *testing.T) {
	report, err := LoadReport(filepath.Join("testdata", "current.json"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Summary.TotalRows != 2400 {
		t.Fatalf("expected total rows 2400, got %d", report.Summary.TotalRows)
	}
	if len(report.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(report.Tables))
	}
	if report.WorkloadID != "orders-production" || report.Scope == nil || len(report.Scope.IncludeSchemas) != 1 || report.Scope.IncludeSchemas[0] != "orders" {
		t.Fatalf("report-v3 workload identity/scope were not preserved: %+v", report)
	}
}

func TestLoadReportAcceptsEmptySummaryTimestamps(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	content := `{
  "summary": {
    "total_transactions": 0,
    "total_rows": 0,
    "total_events": 0,
    "start_time": "",
    "end_time": "",
    "duration": "0s"
  },
  "tables": [],
  "alerts": [],
  "warnings": 0
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp report: %v", err)
	}

	report, err := LoadReport(path)
	if err != nil {
		t.Fatalf("expected valid report, got %v", err)
	}
	if report.Summary.StartTime != "" || report.Summary.EndTime != "" {
		t.Fatalf("expected empty timestamps, got start=%q end=%q", report.Summary.StartTime, report.Summary.EndTime)
	}
}

func TestLoadReportAcceptsValidEmptyReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	content := `{
  "summary": {
    "total_transactions": 0,
    "total_rows": 0,
    "total_events": 0,
    "start_time": "",
    "end_time": "",
    "duration": "0s"
  },
  "tables": [],
  "alerts": [],
  "warnings": 0
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp report: %v", err)
	}

	report, err := LoadReport(path)
	if err != nil {
		t.Fatalf("expected valid empty report, got %v", err)
	}
	if report.Summary.TotalRows != 0 || len(report.Tables) != 0 || len(report.Alerts) != 0 || report.Warnings != 0 {
		t.Fatalf("unexpected empty report contents: %+v", report)
	}
}

func TestLoadReportRejectsInvalidJSON(t *testing.T) {
	_, err := LoadReport(filepath.Join("testdata", "invalid.json"))
	if err == nil || !strings.Contains(err.Error(), "decode compare input") {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestLoadReportRejectsForeignJSON(t *testing.T) {
	_, err := LoadReport(filepath.Join("testdata", "foreign.json"))
	if err == nil || !strings.Contains(err.Error(), "unsupported BinlogViz report shape") {
		t.Fatalf("expected shape error, got %v", err)
	}
}

func TestLoadReportRejectsReportsMissingRequiredCompareFields(t *testing.T) {
	tests := []struct {
		name    string
		content string
	}{
		{
			name: "missing summary duration field",
			content: `{
  "summary": {
    "total_transactions": 120,
    "total_rows": 2400,
    "total_events": 3000,
    "start_time": "",
    "end_time": ""
  },
  "tables": [],
  "alerts": [],
  "warnings": 0
}`,
		},
		{
			name: "missing table identifier fields",
			content: `{
  "summary": {
    "total_transactions": 120,
    "total_rows": 2400,
    "total_events": 3000,
    "start_time": "2026-03-20T10:00:00Z",
    "end_time": "2026-03-20T10:30:00Z",
    "duration": "30m0s"
  },
  "tables": [
    {
      "total_rows": 1200,
      "insert_rows": 700,
      "update_rows": 400,
      "delete_rows": 100,
      "txn_count": 80
    }
  ],
  "alerts": [],
  "warnings": 0
}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "report.json")
			if err := os.WriteFile(path, []byte(tt.content), 0o644); err != nil {
				t.Fatalf("write temp report: %v", err)
			}

			_, err := LoadReport(path)
			if err == nil || !strings.Contains(err.Error(), "unsupported BinlogViz report shape") {
				t.Fatalf("expected shape error, got %v", err)
			}
		})
	}
}

func TestDecodeReportJSONLoadsValidBinlogVizJSON(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "current.json"))
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}

	report, err := DecodeReportJSON(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Summary.TotalRows != 2400 {
		t.Fatalf("expected total rows 2400, got %d", report.Summary.TotalRows)
	}
}

func TestDecodeReportJSONPreservesTransactionReplayCommand(t *testing.T) {
	data := []byte(`{
  "summary": {
    "total_transactions": 1,
    "total_rows": 2,
    "total_events": 3,
    "start_time": "2026-04-01T10:00:00Z",
    "end_time": "2026-04-01T10:01:00Z",
    "duration": "1m0s"
  },
  "tables": [],
  "transactions": [{
    "txn_key": "txn-current",
    "total_rows": 2,
    "event_count": 3,
    "binlog_file_start": "minimal.binlog",
    "binlog_file_end": "minimal.binlog",
    "pos_start": 962,
    "pos_end": 1186,
    "mysqlbinlog_cmd": "mysqlbinlog --start-position=962 --stop-position=1186 /tmp/minimal.binlog"
  }],
  "alerts": [],
  "warnings": 0
}`)

	report, err := DecodeReportJSON(data)
	if err != nil {
		t.Fatalf("DecodeReportJSON returned error: %v", err)
	}
	if len(report.Transactions) != 1 || report.Transactions[0].MysqlbinlogCmd == "" {
		t.Fatalf("expected transaction replay command to survive decoding, got %+v", report.Transactions)
	}
}

func TestDecodeReportJSONRejectsForeignJSON(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "foreign.json"))
	if err != nil {
		t.Fatalf("read foreign testdata: %v", err)
	}

	_, err = DecodeReportJSON(data)
	if err == nil || !strings.Contains(err.Error(), "unsupported BinlogViz report shape") {
		t.Fatalf("expected shape error, got %v", err)
	}
}

func TestDecodeReportJSONAcceptsLegacyReportWithoutVersion(t *testing.T) {
	data := []byte(`{
  "summary": {
    "total_transactions": 1,
    "total_rows": 2,
    "total_events": 3,
    "start_time": "2026-04-01T10:00:00Z",
    "end_time": "2026-04-01T10:01:00Z",
    "duration": "1m0s"
  },
  "tables": [
    {
      "schema": "shop",
      "table": "orders",
      "total_rows": 2,
      "insert_rows": 2,
      "update_rows": 0,
      "delete_rows": 0,
      "txn_count": 1
    }
  ],
  "transactions": [],
  "minutes": [],
  "alerts": [],
  "warnings": 0
}`)

	report, err := DecodeReportJSON(data)
	if err != nil {
		t.Fatalf("DecodeReportJSON returned error for legacy payload: %v", err)
	}
	if report.Summary.TotalRows != 2 {
		t.Fatalf("expected total rows 2, got %d", report.Summary.TotalRows)
	}
}

func TestDecodeReportJSONRejectsUnsupportedReportVersion(t *testing.T) {
	data := []byte(`{
  "report_version": 99,
  "summary": {
    "total_transactions": 1,
    "total_rows": 2,
    "total_events": 3,
    "start_time": "2026-04-01T10:00:00Z",
    "end_time": "2026-04-01T10:01:00Z",
    "duration": "1m0s"
  },
  "tables": [
    {
      "schema": "shop",
      "table": "orders",
      "total_rows": 2,
      "insert_rows": 2,
      "update_rows": 0,
      "delete_rows": 0,
      "txn_count": 1
    }
  ],
  "transactions": [],
  "minutes": [],
  "alerts": [],
  "warnings": 0
}`)

	_, err := DecodeReportJSON(data)
	if err == nil || !strings.Contains(err.Error(), "unsupported report_version") {
		t.Fatalf("expected report_version compatibility error, got %v", err)
	}
}

func TestDecodeReportJSONAcceptsCurrentReportVersionTwo(t *testing.T) {
	data := []byte(`{
  "report_version": 2,
  "summary": {
    "total_transactions": 1,
    "total_rows": 2,
    "total_events": 3,
    "start_time": "2026-04-01T10:00:00Z",
    "end_time": "2026-04-01T10:01:00Z",
    "duration": "1m0s"
  },
  "tables": [
    {
      "schema": "shop",
      "table": "orders",
      "total_rows": 2,
      "insert_rows": 2,
      "update_rows": 0,
      "delete_rows": 0,
      "txn_count": 1
    }
  ],
  "transactions": [],
  "patterns": [],
  "minutes": [],
  "alerts": [],
  "warnings": 0
}`)

	report, err := DecodeReportJSON(data)
	if err != nil {
		t.Fatalf("DecodeReportJSON returned error for report_version=2 payload: %v", err)
	}
	if report.Summary.TotalRows != 2 {
		t.Fatalf("expected total rows 2, got %d", report.Summary.TotalRows)
	}
}

func TestDecodeReportJSONAcceptsReportVersionOneWithoutFabricatingIdentity(t *testing.T) {
	data := []byte(`{
  "report_version": 1,
  "summary": {"total_transactions":1,"total_rows":2,"total_events":3,"start_time":"2026-04-01T10:00:00Z","end_time":"2026-04-01T10:01:00Z","duration":"1m0s"},
  "tables": [{"schema":"shop","table":"orders"}],
  "transactions": [{"txn_key":"txn-1","total_rows":2,"event_count":1,"duration":"1s"}],
  "alerts": [],
  "warnings": 0
}`)
	report, err := DecodeReportJSON(data)
	if err != nil {
		t.Fatalf("DecodeReportJSON returned error for report_version=1 payload: %v", err)
	}
	if report.Provenance != nil || len(report.Transactions) != 1 || report.Transactions[0].GTID != "" || report.Transactions[0].Actor != nil {
		t.Fatalf("legacy report identity must remain unknown, got %+v", report)
	}
}

func TestDecodeReportJSONAcceptsReportVersionThreeProvenance(t *testing.T) {
	data := []byte(`{
  "report_version": 3,
  "provenance": {"server_ids":[7],"server_versions":["11.8.3-MariaDB-log"],"server_flavors":["mariadb"],"mixed_producers":false},
  "sql_context": {"mode":"full","available":true},
  "summary": {"total_transactions":1,"partial_transactions":0,"unknown_transactions":0,"total_rows":2,"total_events":3,"start_time":"2026-04-01T10:00:00Z","end_time":"2026-04-01T10:01:00Z","duration":"1m0s"},
  "tables": [{"schema":"shop","table":"orders"}],
  "transactions": [{"txn_key":"txn-1","server_id":7,"server_version":"11.8.3-MariaDB-log","server_flavor":"mariadb","gtid":"0-7-1848","thread_id":1875,"xid":"3928","actor":{"user":"alice","host":"db.local"},"total_rows":2,"event_count":1,"duration":"1s","completeness":"complete","replay_available":false}],
  "alerts": [],
  "warnings": 0
}`)

	report, err := DecodeReportJSON(data)
	if err != nil {
		t.Fatalf("DecodeReportJSON returned error for report_version=3 payload: %v", err)
	}
	if report.ReportVersion == nil || *report.ReportVersion != 3 || report.Summary.PartialTransactions == nil || *report.Summary.PartialTransactions != 0 || report.Summary.UnknownTransactions == nil || *report.Summary.UnknownTransactions != 0 {
		t.Fatalf("unexpected report-v3 completeness summary: %+v", report)
	}
	if report.Provenance == nil || report.Provenance.MixedProducers || report.SQLContext == nil || report.SQLContext.Mode != "full" || !report.SQLContext.Available || len(report.Transactions) != 1 {
		t.Fatalf("unexpected report-v3 provenance: %+v", report)
	}
	txn := report.Transactions[0]
	if txn.GTID != "0-7-1848" || txn.XID != "3928" || txn.Actor == nil || txn.Actor.User != "alice" || txn.Completeness != "complete" || txn.ReplayAvailable == nil || *txn.ReplayAvailable {
		t.Fatalf("unexpected report-v3 transaction provenance: %+v", txn)
	}
}

func TestDecodeReportJSONPreservesV3TransactionCompleteness(t *testing.T) {
	data := []byte(`{
  "report_version": 3,
  "summary": {
    "total_transactions": 1,
    "partial_transactions": 1,
    "unknown_transactions": 0,
    "total_rows": 1,
    "total_events": 2,
    "start_time": "2026-08-29T11:59:13Z",
    "end_time": "2026-08-29T11:59:14Z",
    "duration": "1s"
  },
  "tables": [{"schema":"shop","table":"orders"}],
  "transactions": [{
    "txn_key":"txn-5",
    "completeness":"partial_end",
    "replay_available":true,
    "replay_scope":"full_transaction",
    "pos_start":3183,
    "pos_end":3234
  }],
  "alerts": [],
  "warnings": 0
}`)
	report, err := DecodeReportJSON(data)
	if err != nil {
		t.Fatalf("DecodeReportJSON returned error for report_version=3 payload: %v", err)
	}
	if report.ReportVersion == nil || *report.ReportVersion != 3 || report.Summary.PartialTransactions == nil || *report.Summary.PartialTransactions != 1 {
		t.Fatalf("v3 summary metadata was not preserved: %+v", report)
	}
	if len(report.Transactions) != 1 || report.Transactions[0].Completeness != "partial_end" || report.Transactions[0].ReplayAvailable == nil || !*report.Transactions[0].ReplayAvailable {
		t.Fatalf("v3 transaction metadata was not preserved: %+v", report.Transactions)
	}
}

func TestDecodeReportJSONMapsLegacyTransactionCompletenessToUnknown(t *testing.T) {
	data := []byte(`{
  "report_version": 2,
  "summary": {"total_transactions":1,"total_rows":2,"total_events":1,"start_time":"","end_time":"","duration":"0s"},
  "tables": [{"schema":"shop","table":"orders"}],
  "transactions": [{"txn_key":"txn-1","total_rows":2}],
  "alerts": [],
  "warnings": 0
}`)

	report, err := DecodeReportJSON(data)
	if err != nil {
		t.Fatalf("DecodeReportJSON returned error for legacy payload: %v", err)
	}
	if len(report.Transactions) != 1 || report.Transactions[0].Completeness != "unknown" {
		t.Fatalf("legacy completeness=%+v, want unknown", report.Transactions)
	}
}

func TestDecodeReportJSONPreservesTimeseriesAndDiagnostics(t *testing.T) {
	data := []byte(`{
  "report_version": 2,
  "summary": {
    "total_transactions": 1,
    "total_rows": 2,
    "total_events": 3,
    "start_time": "2026-04-01T10:00:00Z",
    "end_time": "2026-04-01T10:01:00Z",
    "duration": "1m0s"
  },
  "timeseries": {
    "tps_series": [{"minute":"2026-04-01T10:00:00Z","value":1}],
    "rows_series": [],
    "events_series": [],
    "insert_event_series": [],
    "update_event_series": [],
    "delete_event_series": [],
    "ddl_event_series": [],
    "binlog_bytes_series": [],
    "txn_size_series_summary": {"buckets":[{"label":"1k-10k","txn_count":1,"rows":2,"binlog_bytes":256}]}
  },
  "diagnostics": {
    "file_coverage": {"selected":[],"skipped":[]},
    "ddl_events": [{"timestamp":"2026-04-01T10:00:30Z","operation":"ALTER TABLE","schema":"shop","table":"orders"}],
    "largest_transactions": [{"txn_key":"txn-1","total_rows":2,"event_count":1,"duration":"1s"}],
    "longest_transactions": [],
    "hot_intervals": [{"minute":"2026-04-01T10:00:00Z","total_rows":2,"txn_count":1,"event_count":1,"binlog_bytes":256,"ddl_count":0}],
    "findings": [{"kind":"large_transaction","severity":"warning","message":"test"}]
  },
  "tables": [
    {
      "schema": "shop",
      "table": "orders",
      "total_rows": 2,
      "insert_rows": 2,
      "update_rows": 0,
      "delete_rows": 0,
      "txn_count": 1
    }
  ],
  "transactions": [],
  "patterns": [],
  "minutes": [],
  "alerts": [],
  "warnings": 0
}`)

	report, err := DecodeReportJSON(data)
	if err != nil {
		t.Fatalf("DecodeReportJSON returned error for expanded payload: %v", err)
	}
	if len(report.Timeseries.TPSSeries) != 1 {
		t.Fatalf("expected one tps point, got %+v", report.Timeseries.TPSSeries)
	}
	if len(report.Diagnostics.DDLEvents) != 1 {
		t.Fatalf("expected one ddl event, got %+v", report.Diagnostics.DDLEvents)
	}
	if len(report.Diagnostics.LargestTransactions) != 1 {
		t.Fatalf("expected one largest transaction, got %+v", report.Diagnostics.LargestTransactions)
	}
	if len(report.Diagnostics.HotIntervals) != 1 {
		t.Fatalf("expected one hot interval, got %+v", report.Diagnostics.HotIntervals)
	}
	if len(report.Diagnostics.Findings) != 1 {
		t.Fatalf("expected one finding, got %+v", report.Diagnostics.Findings)
	}
}
