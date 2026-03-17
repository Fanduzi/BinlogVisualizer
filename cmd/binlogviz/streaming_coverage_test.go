// Package binlogviz adds streaming-path regression coverage for fixtures, alerts, SQL context, and parser backpressure.
// input: real binlog fixtures from internal/binlog/testdata plus synthetic parser workloads that exercise the command streaming path.
// output: evidence that parser->normalize->analyzer.Consume->Finalize stays streaming and covers large transaction, spike, and Rows_query cases.
// pos: Stage 5 command-layer regression suite focused on true streaming + DuckDB execution rather than slice-based wrappers.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

type failAfterNAnalyzer struct {
	failAfter    int
	consumeCalls int
}

func (a *failAfterNAnalyzer) Consume(model.NormalizedEvent) error {
	a.consumeCalls++
	if a.consumeCalls >= a.failAfter {
		return errors.New("stop now")
	}
	return nil
}

func (a *failAfterNAnalyzer) Finalize() (*model.AnalysisResult, error) {
	return &model.AnalysisResult{}, nil
}

func captureStdoutRun(t *testing.T, fn func() error) (string, error) {
	t.Helper()

	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe error: %v", err)
	}
	os.Stdout = w

	runErr := fn()

	_ = w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String(), runErr
}

func mustFixturePath(t *testing.T, name string) string {
	t.Helper()
	path := filepath.Join("..", "..", "internal", "binlog", "testdata", name)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("fixture not found: %s: %v", path, err)
	}
	return path
}

func TestRunAnalysisRealFixtureMultiFileOrderedInput(t *testing.T) {
	fixture := mustFixturePath(t, "minimal.binlog")
	out, err := captureStdoutRun(t, func() error {
		return runAnalysisWithReportOptions([]string{fixture, fixture}, analyzer.DefaultOptions(), report.DefaultOptions(), true)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, `"total_transactions": 8`) {
		t.Fatalf("expected eight transactions from two ordered files, got: %s", out)
	}
	if !strings.Contains(out, `"total_rows": 10`) {
		t.Fatalf("expected doubled row count from two ordered files, got: %s", out)
	}
}

func TestRunAnalysisRowsQueryPresentWithFullContext(t *testing.T) {
	parser := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 1, 0, time.UTC), EventType: "ROWS_QUERY_EVENT", QuerySQL: "UPDATE users SET name = 'alice' WHERE id = 7"},
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 2, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "testdb", Table: "users", RowCount: 1},
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 3, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	out, err := captureStdoutRun(t, func() error {
		return runAnalysisWithParserAndTempDirAndReportOptions([]string{"dummy.binlog"}, analyzer.DefaultOptions(), report.Options{SQLContextMode: report.SQLContextFull}, true, parser, "", nil)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, `"query_sql": "UPDATE users SET name = 'alice' WHERE id = 7"`) {
		t.Fatalf("expected bounded query_sql in full mode, got: %s", out)
	}
}

func TestRunAnalysisRowsQueryAbsentOmitsFullContext(t *testing.T) {
	parser := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 2, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "testdb", Table: "users", RowCount: 1},
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 3, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	out, err := captureStdoutRun(t, func() error {
		return runAnalysisWithParserAndTempDirAndReportOptions([]string{"dummy.binlog"}, analyzer.DefaultOptions(), report.Options{SQLContextMode: report.SQLContextFull}, true, parser, "", nil)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out, `"query_sql"`) {
		t.Fatalf("expected no query_sql without Rows_query_log_event, got: %s", out)
	}
}

func TestRunAnalysisLargeTransactionAlertStreamingPath(t *testing.T) {
	opts := analyzer.DefaultOptions()
	opts.LargeTxnRows = 5
	opts.LargeTxnDuration = 30 * time.Second

	parser := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 17, 11, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 17, 11, 0, 1, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "shop", Table: "orders", RowCount: 8},
			{Timestamp: time.Date(2026, 3, 17, 11, 0, 2, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	out, err := captureStdoutRun(t, func() error {
		return runAnalysisWithParser([]string{"dummy.binlog"}, opts, true, parser)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, `"type": "large_transaction"`) {
		t.Fatalf("expected large transaction alert, got: %s", out)
	}
}

func TestRunAnalysisStopsParsingImmediatelyOnConsumeFailure(t *testing.T) {
	parserCalls := 0
	stopAfter := 3
	streamAnalyzer := &failAfterNAnalyzer{failAfter: stopAfter}

	parser := &mockParser{
		parseFiles: func(paths []string, handler func(binlog.RawEvent) error) error {
			for i := 0; i < 1000; i++ {
				parserCalls++
				err := handler(binlog.RawEvent{
					Timestamp: time.Date(2026, 3, 17, 12, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Millisecond),
					EventType: "WRITE_ROWS_EVENT",
					Schema:    "shop",
					Table:     "orders",
					RowCount:  1,
				})
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	_, err := captureStdoutRun(t, func() error {
		return runAnalysisStreamingWithDeps([]string{"dummy.binlog"}, analyzer.DefaultOptions(), report.DefaultOptions(), false, parser, binlog.NormalizeRawEvent, func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer {
			return streamAnalyzer
		}, createDuckDBTempStore, "")
	})
	if err == nil {
		t.Fatal("expected consume failure")
	}
	if parserCalls != stopAfter {
		t.Fatalf("expected parser to stop after %d events, got %d", stopAfter, parserCalls)
	}
	if streamAnalyzer.consumeCalls != stopAfter {
		t.Fatalf("expected %d consume calls, got %d", stopAfter, streamAnalyzer.consumeCalls)
	}
}

func TestRunAnalysisRowsQueryPresentAndAbsentRegressionJSONShape(t *testing.T) {
	tests := []struct {
		name        string
		events      []binlog.RawEvent
		expectQuery bool
	}{
		{
			name: "present",
			events: []binlog.RawEvent{
				{Timestamp: time.Date(2026, 3, 17, 13, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
				{Timestamp: time.Date(2026, 3, 17, 13, 0, 1, 0, time.UTC), EventType: "ROWS_QUERY_EVENT", QuerySQL: "INSERT INTO users VALUES (1, 'alice')"},
				{Timestamp: time.Date(2026, 3, 17, 13, 0, 2, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "testdb", Table: "users", RowCount: 1},
				{Timestamp: time.Date(2026, 3, 17, 13, 0, 3, 0, time.UTC), EventType: "XID_EVENT"},
			},
			expectQuery: true,
		},
		{
			name: "absent",
			events: []binlog.RawEvent{
				{Timestamp: time.Date(2026, 3, 17, 13, 1, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
				{Timestamp: time.Date(2026, 3, 17, 13, 1, 2, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "testdb", Table: "users", RowCount: 1},
				{Timestamp: time.Date(2026, 3, 17, 13, 1, 3, 0, time.UTC), EventType: "XID_EVENT"},
			},
			expectQuery: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := captureStdoutRun(t, func() error {
				return runAnalysisWithParserAndTempDirAndReportOptions([]string{"dummy.binlog"}, analyzer.DefaultOptions(), report.Options{SQLContextMode: report.SQLContextSummary}, true, &mockParser{events: tt.events}, "", nil)
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var parsed map[string]any
			if err := json.Unmarshal([]byte(out), &parsed); err != nil {
				t.Fatalf("invalid json: %v", err)
			}
			txn := parsed["transactions"].([]any)[0].(map[string]any)
			_, hasSummary := txn["query_summary"]
			if hasSummary != tt.expectQuery {
				t.Fatalf("query_summary presence = %v, want %v, out=%s", hasSummary, tt.expectQuery, out)
			}
		})
	}
}
