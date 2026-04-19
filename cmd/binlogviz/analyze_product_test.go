// Package binlogviz validates DBA-oriented analyze report behavior using deterministic SQL corpus scenarios.
// input: synthetic raw-event corpus files, analyzer defaults, and report renderers under test.
// output: regression coverage for scenario presence, diagnostics rankings, and text/HTML product contracts.
// pos: command-layer product test suite for analyze report semantics beyond single-fixture smoke tests.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

type corpusRawEvent struct {
	Timestamp     string `json:"timestamp"`
	BinlogPath    string `json:"binlog_path"`
	PositionStart int64  `json:"position_start"`
	PositionEnd   int64  `json:"position_end"`
	BinlogBytes   int64  `json:"binlog_bytes"`
	EventType     string `json:"event_type"`
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	Query         string `json:"query"`
	QuerySQL      string `json:"query_sql"`
	RowCount      int    `json:"row_count"`
}

func loadCorpusEvents(t *testing.T, scenario string) []model.NormalizedEvent {
	t.Helper()

	path := filepath.Join("testdata", "sql-corpus", scenario, "events.json")
	rawBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read corpus %s: %v", scenario, err)
	}

	var raw []corpusRawEvent
	if err := json.Unmarshal(rawBytes, &raw); err != nil {
		t.Fatalf("decode corpus %s: %v", scenario, err)
	}

	events := make([]model.NormalizedEvent, 0, len(raw))
	for _, item := range raw {
		ts, err := time.Parse(time.RFC3339, item.Timestamp)
		if err != nil {
			t.Fatalf("parse corpus %s timestamp %q: %v", scenario, item.Timestamp, err)
		}
		normalized, err := binlog.NormalizeRawEvent(binlog.RawEvent{
			Timestamp:     ts,
			BinlogPath:    item.BinlogPath,
			PositionStart: item.PositionStart,
			PositionEnd:   item.PositionEnd,
			BinlogBytes:   item.BinlogBytes,
			EventType:     item.EventType,
			Schema:        item.Schema,
			Table:         item.Table,
			Query:         item.Query,
			QuerySQL:      item.QuerySQL,
			RowCount:      item.RowCount,
		})
		if err != nil {
			t.Fatalf("normalize corpus %s: %v", scenario, err)
		}
		if normalized != nil {
			events = append(events, *normalized)
		}
	}
	return events
}

func analyzeCorpus(t *testing.T, scenario string) model.AnalysisResult {
	t.Helper()

	return analyzeCorpusWithOptions(t, scenario, analyzer.DefaultOptions())
}

func analyzeCorpusWithOptions(t *testing.T, scenario string, opts analyzer.Options) model.AnalysisResult {
	t.Helper()

	a := analyzer.New(opts)
	result, err := a.Analyze(loadCorpusEvents(t, scenario))
	if err != nil {
		t.Fatalf("analyze corpus %s: %v", scenario, err)
	}
	return *result
}

func TestAnalyzeCorpusCoversRequiredScenarios(t *testing.T) {
	for _, scenario := range []string{
		"baseline-small",
		"tps-spike",
		"rows-spike",
		"ddl-incident",
		"large-transaction",
		"long-transaction",
		"wide-transaction",
		"multi-file-window",
	} {
		t.Run(scenario, func(t *testing.T) {
			if len(loadCorpusEvents(t, scenario)) == 0 {
				t.Fatalf("scenario %s has no normalized events", scenario)
			}
		})
	}
}

func TestAnalyzeCorpusDetailStoreNoneAndDuckDBProduceEquivalentReports(t *testing.T) {
	none := analyzeCorpusWithOptions(t, "incident-mixed", analyzer.DefaultOptions())
	duckDBOpts := analyzer.DefaultOptions()
	duckDBOpts.DetailStoreMode = analyzer.DetailStoreDuckDB
	duckDB := analyzeCorpusWithOptions(t, "incident-mixed", duckDBOpts)

	assertEquivalentAnalyzeField(t, "summary", none.Summary, duckDB.Summary)
	assertEquivalentAnalyzeField(t, "timeseries", none.Timeseries, duckDB.Timeseries)
	assertEquivalentAnalyzeField(t, "tables", none.Tables, duckDB.Tables)
	assertEquivalentAnalyzeField(t, "transactions", none.Transactions, duckDB.Transactions)
	assertEquivalentAnalyzeField(t, "patterns", none.Patterns, duckDB.Patterns)
	assertEquivalentAnalyzeField(t, "minutes", none.Minutes, duckDB.Minutes)
	assertEquivalentAnalyzeField(t, "diagnostics", none.Diagnostics, duckDB.Diagnostics)
	assertEquivalentAnalyzeField(t, "alerts", none.Alerts, duckDB.Alerts)
	assertEquivalentAnalyzeField(t, "warnings", none.Warnings, duckDB.Warnings)
	assertEquivalentAnalyzeField(t, "pattern_drilldowns", none.PatternDrilldowns, duckDB.PatternDrilldowns)
}

func assertEquivalentAnalyzeField(t *testing.T, name string, left, right any) {
	t.Helper()
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("%s differs between detail-store modes\nnone: %#v\nduckdb: %#v", name, left, right)
	}
}

func TestAnalyzeCorpusTPSAndRowsSpikes(t *testing.T) {
	tps := analyzeCorpus(t, "tps-spike")
	if len(tps.Timeseries.TPSSeries) == 0 {
		t.Fatal("expected TPS series")
	}
	if len(tps.Diagnostics.HotIntervals) == 0 {
		t.Fatal("expected hot intervals for TPS spike")
	}

	rows := analyzeCorpus(t, "rows-spike")
	if len(rows.Diagnostics.LargestTransactions) == 0 {
		t.Fatal("expected largest transaction evidence for rows spike")
	}
}

func TestAnalyzeCorpusDDLAndTransactionRankings(t *testing.T) {
	ddl := analyzeCorpus(t, "ddl-incident")
	if len(ddl.Diagnostics.DDLEvents) == 0 {
		t.Fatal("expected DDL timeline events")
	}

	large := analyzeCorpus(t, "large-transaction")
	if len(large.Diagnostics.LargestTransactions) == 0 || large.Diagnostics.LargestTransactions[0].TxnKey == "" {
		t.Fatal("expected largest transaction rank 1")
	}

	long := analyzeCorpus(t, "long-transaction")
	if len(long.Diagnostics.LongestTransactions) == 0 || long.Diagnostics.LongestTransactions[0].Duration < 30*time.Second {
		t.Fatal("expected long transaction rank 1 by duration")
	}

	wide := analyzeCorpus(t, "wide-transaction")
	if len(wide.Diagnostics.WidestTransactions) == 0 || len(wide.Diagnostics.WidestTransactions[0].Tables) < 3 {
		t.Fatal("expected wide transaction touching at least three tables")
	}
}

func TestAnalyzeCorpusTextAndHTMLContracts(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	result := analyzeCorpus(t, "tps-spike")
	textOut, err := report.RenderText(result)
	if err != nil {
		t.Fatalf("render text: %v", err)
	}
	if strings.Contains(textOut, "Minute Details") || strings.Contains(textOut, "Write Shape Patterns") {
		t.Fatalf("default text output leaked detail sections\n%s", textOut)
	}

	htmlOut, err := report.RenderHTML(result)
	if err != nil {
		t.Fatalf("render html: %v", err)
	}
	for _, token := range []string{`id="chart-tps"`, `id="section-activity"`, `id="section-objects"`} {
		if !strings.Contains(htmlOut, token) {
			t.Fatalf("expected HTML token %q", token)
		}
	}
}

func TestAnalyzeCorpusTextAndHTMLShareTopTableLimit(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	result := analyzeCorpus(t, "event-mix-burst")
	textOut, err := report.RenderTextWithOptions(result, report.Options{TopN: 1})
	if err != nil {
		t.Fatalf("render text: %v", err)
	}
	htmlOut, err := report.RenderHTMLWithOptions(result, report.Options{TopN: 1})
	if err != nil {
		t.Fatalf("render html: %v", err)
	}

	if strings.Count(textOut, "shop.") < 1 {
		t.Fatalf("expected at least one rendered top table in text\n%s", textOut)
	}
	if strings.Contains(textOut, "shop.orders") && strings.Contains(textOut, "shop.audit_logs") {
		t.Fatalf("expected text output top tables to respect TopN=1\n%s", textOut)
	}
	if strings.Count(htmlOut, `data-table-row="`) != 1 {
		t.Fatalf("expected html top tables to respect TopN=1")
	}
}
