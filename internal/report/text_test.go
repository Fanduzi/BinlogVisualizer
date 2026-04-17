// Package report verifies concise diagnostic text rendering and opt-in detail sections.
// input: synthetic AnalysisResult fixtures with summary, table, minute, pattern, and diagnostic evidence.
// output: regression coverage for default diagnostic sections, table limits, and detail flags.
// pos: text renderer regression suite guarding user-facing CLI report formatting.
// note: if this file changes, update this header and module README.md.
package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func sampleTransactionWithQueryContext() model.Transaction {
	return model.Transaction{
		TxnKey:       "txn-1",
		TotalRows:    100,
		EventCount:   2,
		Duration:     5 * time.Second,
		QuerySummary: "UPDATE orders SET status = ? WHERE id = ?",
		QueryContext: model.NewQueryContext("UPDATE orders SET status = 'paid' WHERE id = 42"),
	}
}

func TestRenderTextDefaultIsConciseDiagnosticSummary(t *testing.T) {
	result := productTextFixture()

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{"Summary", "Top Findings", "Top Tables", "Next Actions", "Peak avg TPS/min"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected default text report to contain %q\n%s", token, out)
		}
	}
	for _, hidden := range []string{"Minute Activity", "Top Patterns", "2026-04-17 09:01: 9000 rows"} {
		if strings.Contains(out, hidden) {
			t.Fatalf("default text report should hide %q\n%s", hidden, out)
		}
	}
}

func TestRenderTextTopTablesUsesAlignedTableAndTopLimit(t *testing.T) {
	result := productTextFixture()
	out, err := RenderTextWithOptions(result, Options{TopN: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "#  Table") || !strings.Contains(out, "Rows") || !strings.Contains(out, "Share") {
		t.Fatalf("expected aligned top tables header\n%s", out)
	}
	if !strings.Contains(out, "shop.orders") {
		t.Fatalf("expected top table\n%s", out)
	}
	if strings.Contains(out, "shop.users") {
		t.Fatalf("expected top limit to hide second table\n%s", out)
	}
}

func TestRenderTextDetailsCanShowMinuteAndPatternSections(t *testing.T) {
	result := productTextFixture()
	result.Patterns = []model.PatternStats{{
		PatternKey:    "shop.orders|UPDATE|medium",
		Label:         "shop.orders / UPDATE / medium batch",
		TotalRows:     9000,
		TxnCount:      100,
		AvgRowsPerTxn: 90,
	}}

	out, err := RenderTextWithOptions(result, Options{Details: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{"Minute Details", "Write Shape Patterns", "shop.orders / UPDATE / medium batch"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected detail output to contain %q\n%s", token, out)
		}
	}
}

func TestRenderTextAndHTMLShareTopNDefault(t *testing.T) {
	if DefaultOptions().TopN != DefaultTopN {
		t.Fatalf("default options top N drifted from product default")
	}
}

func productTextFixture() model.AnalysisResult {
	start := time.Date(2026, 4, 17, 9, 0, 0, 0, time.UTC)
	return model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 120,
			TotalRows:         10000,
			TotalEvents:       500,
			StartTime:         start,
			EndTime:           start.Add(10 * time.Minute),
			Duration:          10 * time.Minute,
		},
		Timeseries: model.Timeseries{
			TPSSeries:  []model.TimeseriesPoint{{Minute: start.Add(time.Minute), Value: 22.5}},
			RowsSeries: []model.TimeseriesPoint{{Minute: start.Add(time.Minute), Value: 9000}},
		},
		Tables: []model.TableStats{
			{Schema: "shop", Table: "orders", TotalRows: 9000, TxnCount: 100, EventCount: 300},
			{Schema: "shop", Table: "users", TotalRows: 1000, TxnCount: 20, EventCount: 80},
		},
		Minutes: []model.MinuteBucket{
			{Minute: start, TotalRows: 1000, TxnCount: 10},
			{Minute: start.Add(time.Minute), TotalRows: 9000, TxnCount: 100},
		},
		Diagnostics: model.Diagnostics{
			HotIntervals: []model.MinuteBucket{{Minute: start.Add(time.Minute), TotalRows: 9000, TxnCount: 100}},
			DDLEvents: []model.DDLEvent{{
				Timestamp: start.Add(2 * time.Minute),
				Operation: "ALTER",
				Schema:    "shop",
				Table:     "orders",
				Statement: "ALTER TABLE shop.orders ADD COLUMN marker INT",
			}},
			LongestTransactions: []model.Transaction{{
				TxnKey:          "txn-long",
				TotalRows:       500,
				Duration:        45 * time.Second,
				BinlogPathStart: "mysql-bin.000044",
				PositionStart:   100,
				PositionEnd:     200,
			}},
		},
	}
}
