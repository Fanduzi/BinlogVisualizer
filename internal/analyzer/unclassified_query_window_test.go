// Package analyzer verifies after-window Unclassified QUERY at the window-plus-Analyzer seam.
// input: Query-normalized events with an in-window business transaction, then a trailing GTID-started Unclassified QUERY or ADMIN clipped by --end / --stop-position.
// output: in-window business transaction retained when Unclassified QUERY never intersected the window, including when another GTID follows that QUERY; UnclassifiedQueryError when that group intersects; after-window ADMIN omitted from transactions and DDL.
// pos: #79 window relation for ADR-0003 Unclassified QUERY; does not admit ADMIN verbs.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
)

func TestAnalyzerAfterEndUnclassifiedQueryKeepsInWindowReport(t *testing.T) {
	ts := time.Date(2026, 9, 16, 16, 0, 0, 0, time.UTC)
	end := ts.Add(3 * time.Second)
	events := normalizeRawEvents(t, mysqlBusinessThenQuery(ts, "CHECK TABLE app.orders"))

	result, err := consumeAnalyzerWithOptions(events, Options{End: &end})
	if err != nil {
		t.Fatalf("after --end Unclassified QUERY must not fail analyze, got %v", err)
	}
	assertSingleBusinessTransaction(t, result, mysqlIssue72SID+":40", 3)
}

func TestAnalyzerAfterStopPositionUnclassifiedQueryKeepsInWindowReport(t *testing.T) {
	ts := time.Date(2026, 9, 16, 16, 10, 0, 0, time.UTC)
	stop := int64(360)
	events := normalizeRawEvents(t, mysqlBusinessThenQuery(ts, "CHECK TABLE app.orders"))

	result, err := consumeAnalyzerWithOptions(events, Options{StopPosition: &stop})
	if err != nil {
		t.Fatalf("after --stop-position Unclassified QUERY must not fail analyze, got %v", err)
	}
	assertSingleBusinessTransaction(t, result, mysqlIssue72SID+":40", 3)
}

func TestAnalyzerAfterWindowUnclassifiedQueryThenNextGTIDKeepsInWindowReport(t *testing.T) {
	ts := time.Date(2026, 9, 16, 16, 15, 0, 0, time.UTC)
	end := ts.Add(3 * time.Second)
	stop := int64(360)
	raws := mysqlBusinessThenQueryThenBusiness(ts, "CHECK TABLE app.orders")

	for _, tt := range []struct {
		name string
		opts Options
	}{
		{name: "after_end", opts: Options{End: &end}},
		{name: "after_stop_position", opts: Options{StopPosition: &stop}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := consumeAnalyzerWithOptions(normalizeRawEvents(t, raws), tt.opts)
			if err != nil {
				t.Fatalf("after-window Unclassified QUERY then next GTID must not fail analyze, got %v", err)
			}
			assertSingleBusinessTransaction(t, result, mysqlIssue72SID+":40", 3)
		})
	}
}

func TestAnalyzerInWindowUnclassifiedQueryStillFails(t *testing.T) {
	ts := time.Date(2026, 9, 16, 16, 20, 0, 0, time.UTC)
	end := ts.Add(5 * time.Second)
	stop := int64(520)
	events := normalizeRawEvents(t, mysqlBusinessThenQuery(ts, "CHECK TABLE app.orders"))

	for _, tt := range []struct {
		name string
		opts Options
	}{
		{name: "no_window", opts: Options{}},
		{name: "end_includes_query", opts: Options{End: &end}},
		{name: "stop_position_includes_query", opts: Options{StopPosition: &stop}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := consumeAnalyzerWithOptions(events, tt.opts)
			if err == nil {
				t.Fatal("in-window Unclassified QUERY must still fail analyze")
			}
			if strings.Contains(err.Error(), "conflicting GTID") {
				t.Fatalf("must not be conflicting GTID, got %v", err)
			}
			if !strings.Contains(err.Error(), "Unclassified QUERY") || !strings.Contains(err.Error(), "CHECK TABLE app.orders") {
				t.Fatalf("must be Unclassified QUERY with the statement prefix, got %v", err)
			}
		})
	}
}

func TestAnalyzerAfterWindowAdminOmitsFromTransactionsAndDDL(t *testing.T) {
	ts := time.Date(2026, 9, 16, 16, 30, 0, 0, time.UTC)
	end := ts.Add(3 * time.Second)
	stop := int64(360)
	raws := mysqlBusinessThenQuery(ts, "ANALYZE TABLE app.orders")

	for _, tt := range []struct {
		name string
		opts Options
	}{
		{name: "after_end", opts: Options{End: &end}},
		{name: "after_stop_position", opts: Options{StopPosition: &stop}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			result, err := consumeAnalyzerWithOptions(normalizeRawEvents(t, raws), tt.opts)
			if err != nil {
				t.Fatalf("after-window ADMIN must not fail analyze, got %v", err)
			}
			assertSingleBusinessTransaction(t, result, mysqlIssue72SID+":40", 3)
			if len(result.Diagnostics.DDLEvents) != 0 {
				t.Fatalf("after-window ADMIN leaked onto the DDL timeline: %+v", result.Diagnostics.DDLEvents)
			}
			for _, table := range result.Tables {
				if table.DDLCount != 0 {
					t.Fatalf("after-window ADMIN incremented table DDLCount: %+v", table)
				}
			}
		})
	}
}

func mysqlBusinessThenQuery(ts time.Time, query string) []binlog.RawEvent {
	return []binlog.RawEvent{
		mysqlGTID(ts, 40, 100, 180),
		mysqlQuery(ts.Add(time.Second), "BEGIN", 180, 220),
		mysqlRows(ts.Add(2*time.Second), 3, 220, 340),
		mysqlXID(ts.Add(3*time.Second), 340, 360),
		mysqlGTID(ts.Add(4*time.Second), 41, 360, 440),
		mysqlQuery(ts.Add(5*time.Second), query, 440, 520),
	}
}

func mysqlBusinessThenQueryThenBusiness(ts time.Time, query string) []binlog.RawEvent {
	return append(mysqlBusinessThenQuery(ts, query),
		mysqlGTID(ts.Add(6*time.Second), 42, 520, 600),
		mysqlQuery(ts.Add(7*time.Second), "BEGIN", 600, 640),
		mysqlRows(ts.Add(8*time.Second), 2, 640, 760),
		mysqlXID(ts.Add(9*time.Second), 760, 780),
	)
}

func consumeAnalyzerWithOptions(events []model.NormalizedEvent, opts Options) (*model.AnalysisResult, error) {
	a := New(opts)
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			return nil, err
		}
	}
	return a.Finalize()
}
