// Package trend verifies trend diagnostic series construction and replay evidence.
// input: synthetic ordered snapshot reports with diagnostic metrics and transaction spans.
// output: assertions for trend metric values and preserved replay payloads.
// pos: regression coverage for trend diagnostic build paths.
// note: if this file changes, update this header and internal/trend/README.md.
package trend

import (
	"testing"

	comparepkg "binlogviz/internal/compare"
)

func TestBuildResultProducesTPSTrends(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{
				Path:   "/tmp/a.json",
				Report: testInputReportWithTimeseries("a", "Alpha", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0, 33.5),
			},
			{
				Path:   "/tmp/b.json",
				Report: testInputReportWithTimeseries("b", "Beta", "2026-03-20T10:00:00Z", 2000, 90, 2400, 800, 600, 200, 1, 55.0),
			},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	got := result.DiagnosticsTrends.TPSTrends
	if len(got) != 2 {
		t.Fatalf("expected 2 TPS trend points, got %d", len(got))
	}
	if got[0].SnapshotName != "a" {
		t.Fatalf("expected first TPS snapshot 'a', got %q", got[0].SnapshotName)
	}
	if got[0].Value != 33.5 {
		t.Fatalf("expected first TPS value 33.5, got %f", got[0].Value)
	}
	if got[1].SnapshotName != "b" {
		t.Fatalf("expected second TPS snapshot 'b', got %q", got[1].SnapshotName)
	}
	if got[1].Value != 55.0 {
		t.Fatalf("expected second TPS value 55.0, got %f", got[1].Value)
	}
}

func TestBuildResultProducesDDLTrends(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{
				Path:   "/tmp/a.json",
				Report: testInputReportWithDDLCount("a", "Alpha", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0, 2),
			},
			{
				Path:   "/tmp/b.json",
				Report: testInputReportWithDDLCount("b", "Beta", "2026-03-20T10:00:00Z", 2000, 90, 2400, 800, 600, 200, 1, 5),
			},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	got := result.DiagnosticsTrends.DDLTrends
	if len(got) != 2 {
		t.Fatalf("expected 2 DDL trend points, got %d", len(got))
	}
	if got[0].Value != 2 {
		t.Fatalf("expected first DDL count 2, got %f", got[0].Value)
	}
	if got[1].Value != 5 {
		t.Fatalf("expected second DDL count 5, got %f", got[1].Value)
	}
}

func TestBuildResultProducesTxnSizeTrends(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{
				Path:   "/tmp/a.json",
				Report: testInputReportWithLargestTxn("a", "Alpha", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0, 300),
			},
			{
				Path:   "/tmp/b.json",
				Report: testInputReportWithLargestTxn("b", "Beta", "2026-03-20T10:00:00Z", 2000, 90, 2400, 800, 600, 200, 1, 800),
			},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	got := result.DiagnosticsTrends.TxnSizeTrends
	if len(got) != 2 {
		t.Fatalf("expected 2 txn size trend points, got %d", len(got))
	}
	if got[0].Value != 300 {
		t.Fatalf("expected first txn size 300, got %f", got[0].Value)
	}
	if got[1].Value != 800 {
		t.Fatalf("expected second txn size 800, got %f", got[1].Value)
	}
}

func TestBuildResultPreservesTxnReplayEvidencePerSnapshot(t *testing.T) {
	makeReport := func(name, start, key, file string, startPos, endPos int64, cmd string) InputReport {
		r := testInputReport(name, name, start, 1000, 50, 1200, 500, 350, 150, 0)
		r.Diagnostics.LargestTransactions = []comparepkg.InputTransaction{{
			TxnKey:          key,
			TotalRows:       2000,
			EventCount:      8,
			BinlogFileStart: file,
			BinlogFileEnd:   file,
			PosStart:        startPos,
			PosEnd:          endPos,
			MysqlbinlogCmd:  cmd,
		}}
		return r
	}

	result, err := BuildResult(BuildOptions{
		InputMode: "explicit",
		Points: []BuildInput{
			{Path: "/tmp/a.json", Report: makeReport("a", "2026-03-19T10:00:00Z", "txn-a", "minimal.binlog", 962, 1186, "mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=962 --stop-position=1186 /tmp/minimal.binlog")},
			{Path: "/tmp/b.json", Report: makeReport("b", "2026-03-20T10:00:00Z", "txn-b", "mysql-bin.000008", 300, 520, "mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=300 --stop-position=520 /tmp/mysql-bin.000008")},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	for i, want := range []string{"minimal.binlog:962-1186", "mysql-bin.000008:300-520"} {
		evidence := result.DiagnosticsTrends.TxnSizeTrends[i].Evidence
		if evidence == nil || evidence.BinlogSpan != want {
			t.Fatalf("snapshot %d evidence span = %+v, want %q", i, evidence, want)
		}
		if evidence.MysqlbinlogCmd == "" {
			t.Fatalf("snapshot %d missing replay command", i)
		}
	}
}

func TestBuildResultProducesEventMixTrends(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{
				Path:   "/tmp/a.json",
				Report: testInputReportWithEventSeries("a", "Alpha", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0, 100, 200, 50, 2),
			},
			{
				Path:   "/tmp/b.json",
				Report: testInputReportWithEventSeries("b", "Beta", "2026-03-20T10:00:00Z", 2000, 90, 2400, 800, 600, 200, 1, 300, 400, 100, 5),
			},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	got := result.DiagnosticsTrends.EventMixTrends.Snapshots
	if len(got) != 2 {
		t.Fatalf("expected 2 event mix snapshots, got %d", len(got))
	}
	if got[0].Inserts != 100 {
		t.Fatalf("expected first snapshot inserts 100, got %f", got[0].Inserts)
	}
	if got[1].DDL != 5 {
		t.Fatalf("expected second snapshot DDL 5, got %f", got[1].DDL)
	}
}

func TestBuildResultProducesHotIntervalSummary(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{
				Path:   "/tmp/a.json",
				Report: testInputReportWithHotIntervals("a", "Alpha", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0, 300),
			},
			{
				Path:   "/tmp/b.json",
				Report: testInputReportWithHotIntervals("b", "Beta", "2026-03-20T10:00:00Z", 2000, 90, 2400, 800, 600, 200, 1, 700),
			},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	got := result.DiagnosticsTrends.HotIntervalSummary.MaxHotRows
	if len(got) != 2 {
		t.Fatalf("expected 2 hot interval trend points, got %d", len(got))
	}
	if got[0].Value != 300 {
		t.Fatalf("expected first max hot rows 300, got %f", got[0].Value)
	}
	if got[1].Value != 700 {
		t.Fatalf("expected second max hot rows 700, got %f", got[1].Value)
	}
}

func TestBuildResultHandlesEmptyDiagnosticsTrends(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{Path: "/tmp/a.json", Report: testInputReport("a", "Alpha", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0)},
			{Path: "/tmp/b.json", Report: testInputReport("b", "Beta", "2026-03-20T10:00:00Z", 2000, 90, 2400, 800, 600, 200, 1)},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	// Should not panic; trend fields should be zero-valued but present (one entry per snapshot)
	if len(result.DiagnosticsTrends.TPSTrends) != 2 {
		t.Fatalf("expected 2 TPS trend points for legacy snapshots, got %d", len(result.DiagnosticsTrends.TPSTrends))
	}
	if result.DiagnosticsTrends.TPSTrends[0].Value != 0 {
		t.Fatalf("expected zero TPS value for legacy snapshot, got %f", result.DiagnosticsTrends.TPSTrends[0].Value)
	}
	if len(result.DiagnosticsTrends.DDLTrends) != 2 {
		t.Fatalf("expected 2 DDL trend points for legacy snapshots, got %d", len(result.DiagnosticsTrends.DDLTrends))
	}
	if result.DiagnosticsTrends.DDLTrends[0].Value != 0 {
		t.Fatalf("expected zero DDL value for legacy snapshot, got %f", result.DiagnosticsTrends.DDLTrends[0].Value)
	}
}

// testInputReportWithTimeseries creates a report with TPS timeseries data.
func testInputReportWithTimeseries(name, label, start string, rows, txns, events, inserts, updates, deletes, alerts int, tps float64) InputReport {
	r := testInputReport(name, label, start, rows, txns, events, inserts, updates, deletes, alerts)
	r.Timeseries = comparepkg.InputTimeseries{
		TPSSeries: []comparepkg.InputTimeseriesPoint{
			{Minute: start, Value: tps},
		},
	}
	return r
}

// testInputReportWithDDLCount creates a report with DDL event diagnostics.
func testInputReportWithDDLCount(name, label, start string, rows, txns, events, inserts, updates, deletes, alerts, ddlCount int) InputReport {
	r := testInputReport(name, label, start, rows, txns, events, inserts, updates, deletes, alerts)
	events_list := make([]comparepkg.InputDDLEvent, ddlCount)
	for i := range events_list {
		events_list[i] = comparepkg.InputDDLEvent{
			Timestamp: start,
			Schema:    "shop",
			Table:     "orders",
			Operation: "ALTER TABLE",
		}
	}
	r.Diagnostics.DDLEvents = events_list
	return r
}

// testInputReportWithLargestTxn creates a report with largest transaction diagnostics.
func testInputReportWithLargestTxn(name, label, start string, rows, txns, events, inserts, updates, deletes, alerts, largestRows int) InputReport {
	r := testInputReport(name, label, start, rows, txns, events, inserts, updates, deletes, alerts)
	r.Diagnostics.LargestTransactions = []comparepkg.InputTransaction{
		{TxnKey: "txn-largest", TotalRows: largestRows, Duration: "5s"},
	}
	return r
}

// testInputReportWithEventSeries creates a report with event type timeseries.
func testInputReportWithEventSeries(name, label, start string, rows, txns, events, inserts, updates, deletes, alerts int, insertEvt, updateEvt, deleteEvt, ddlEvt float64) InputReport {
	r := testInputReport(name, label, start, rows, txns, events, inserts, updates, deletes, alerts)
	r.Timeseries = comparepkg.InputTimeseries{
		InsertEventSeries: []comparepkg.InputTimeseriesPoint{{Minute: start, Value: insertEvt}},
		UpdateEventSeries: []comparepkg.InputTimeseriesPoint{{Minute: start, Value: updateEvt}},
		DeleteEventSeries: []comparepkg.InputTimeseriesPoint{{Minute: start, Value: deleteEvt}},
		DDLEventSeries:    []comparepkg.InputTimeseriesPoint{{Minute: start, Value: ddlEvt}},
	}
	return r
}

// testInputReportWithHotIntervals creates a report with hot interval diagnostics.
func testInputReportWithHotIntervals(name, label, start string, rows, txns, events, inserts, updates, deletes, alerts, topHotRows int) InputReport {
	r := testInputReport(name, label, start, rows, txns, events, inserts, updates, deletes, alerts)
	r.Diagnostics.HotIntervals = []comparepkg.InputHotInterval{
		{Minute: start, TotalRows: topHotRows, TxnCount: 10, BinlogBytes: 10240},
	}
	return r
}
