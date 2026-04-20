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

	for _, token := range []string{"Summary", "Top Findings", "Activity", "Top Tables", "Top Transactions", "Next Actions"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected default text report to contain %q\n%s", token, out)
		}
	}
	for _, hidden := range []string{"Minute Activity", "Top Patterns", "2026-04-17 09:01: 9000 rows", "Write Shape Patterns"} {
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

func TestRenderTextTopTransactionsUsesTopLimit(t *testing.T) {
	result := productTextFixture()
	result.Diagnostics.LargestTransactions = []model.Transaction{
		{TxnKey: "txn-largest-a", TotalRows: 5000, Duration: 5 * time.Second, Tables: map[string]int{"shop.orders": 5000}},
		{TxnKey: "txn-largest-b", TotalRows: 4000, Duration: 4 * time.Second, Tables: map[string]int{"shop.users": 4000}},
	}
	result.Diagnostics.LongestTransactions = []model.Transaction{
		{TxnKey: "txn-long-a", TotalRows: 20, Duration: 45 * time.Second, Tables: map[string]int{"shop.accounts": 20}},
		{TxnKey: "txn-long-b", TotalRows: 18, Duration: 30 * time.Second, Tables: map[string]int{"shop.settlements": 18}},
	}
	result.Diagnostics.WidestTransactions = []model.Transaction{
		{TxnKey: "txn-wide-a", TotalRows: 120, Duration: 8 * time.Second, Tables: map[string]int{"shop.orders": 1, "shop.users": 1, "shop.payments": 1, "shop.audit_logs": 1}},
		{TxnKey: "txn-wide-b", TotalRows: 100, Duration: 7 * time.Second, Tables: map[string]int{"shop.orders": 1, "shop.users": 1, "shop.shipments": 1}},
	}

	out, err := RenderTextWithOptions(result, Options{TopN: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{"txn-largest-a", "txn-long-a", "txn-wide-a"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected top transaction token %q\n%s", token, out)
		}
	}
	for _, token := range []string{"txn-largest-b", "txn-long-b", "txn-wide-b"} {
		if strings.Contains(out, token) {
			t.Fatalf("expected top limit to hide token %q\n%s", token, out)
		}
	}
}

func TestRenderTextActivitySectionIncludesMiniSeries(t *testing.T) {
	result := productTextFixture()

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{"TPS", "Rows/min", "▁"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected activity token %q\n%s", token, out)
		}
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

func TestRenderTextDefaultDoesNotRenderPatternDrilldowns(t *testing.T) {
	result := productTextFixture()
	result.Patterns = []model.PatternStats{{
		PatternKey:    "shop.orders|UPDATE|medium",
		Label:         "shop.orders / UPDATE / medium batch",
		TotalRows:     9000,
		TxnCount:      100,
		AvgRowsPerTxn: 90,
	}}
	result.PatternDrilldowns = []model.PatternDrilldown{{
		PatternKey:  "shop.orders|UPDATE|medium",
		Label:       "shop.orders / UPDATE / medium batch",
		WhySelected: "high row share",
	}}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out, "high row share") || strings.Contains(out, "drilldown:") {
		t.Fatalf("default text path rendered pattern drilldown\n%s", out)
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
			LargestTransactions: []model.Transaction{{
				TxnKey:          "txn-largest",
				TotalRows:       5000,
				Duration:        6 * time.Second,
				BinlogPathStart: "mysql-bin.000044",
				PositionStart:   210,
				PositionEnd:     420,
				Tables:          map[string]int{"shop.orders": 5000},
			}},
			WidestTransactions: []model.Transaction{{
				TxnKey:          "txn-wide",
				TotalRows:       600,
				Duration:        8 * time.Second,
				Tables:          map[string]int{"shop.orders": 200, "shop.users": 200, "shop.payments": 200},
				BinlogPathStart: "mysql-bin.000045",
				PositionStart:   500,
				PositionEnd:     700,
			}},
		},
	}
}

func TestDownsampleSeriesReturnsOriginalWhenUnderMax(t *testing.T) {
	points := []model.TimeseriesPoint{
		{Minute: time.Now(), Value: 1},
		{Minute: time.Now().Add(time.Minute), Value: 2},
		{Minute: time.Now().Add(2 * time.Minute), Value: 3},
	}
	result := downsampleSeries(points, 10)
	if len(result) != 3 {
		t.Fatalf("expected 3 points, got %d", len(result))
	}
}

func TestDownsampleSeriesAveragesBuckets(t *testing.T) {
	points := make([]model.TimeseriesPoint, 100)
	base := time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC)
	for i := range points {
		points[i] = model.TimeseriesPoint{
			Minute: base.Add(time.Duration(i) * time.Minute),
			Value:  float64(i),
		}
	}
	result := downsampleSeries(points, 5)
	if len(result) != 5 {
		t.Fatalf("expected 5 points, got %d", len(result))
	}
	// First bucket averages indices 0-19: avg = 9.5
	if result[0].Value < 9.0 || result[0].Value > 10.0 {
		t.Fatalf("expected first bucket avg ~9.5, got %.1f", result[0].Value)
	}
}

func TestDownsampleSeriesPreservesFirstBucketMinute(t *testing.T) {
	points := make([]model.TimeseriesPoint, 100)
	base := time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC)
	for i := range points {
		points[i] = model.TimeseriesPoint{
			Minute: base.Add(time.Duration(i) * time.Minute),
			Value:  float64(i),
		}
	}
	result := downsampleSeries(points, 5)
	if !result[0].Minute.Equal(base) {
		t.Fatalf("expected first bucket minute %s, got %s", base, result[0].Minute)
	}
}
