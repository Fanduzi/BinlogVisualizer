// Package report verifies analyze HTML pattern drilldown rendering behavior.
// input: synthetic AnalysisResult values with selected and unselected pattern drilldowns.
// output: assertions for drilldown cards, badges, and bounded detail content in HTML.
// pos: regression coverage for analyze HTML drilldown presentation.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func productHTMLFixture() model.AnalysisResult {
	result := productTextFixture()
	start := time.Date(2026, 4, 17, 9, 0, 0, 0, time.UTC)

	result.PatternDrilldowns = []model.PatternDrilldown{{
		PatternKey:    "shop.orders|UPDATE|medium",
		Label:         "shop.orders / UPDATE / medium batch",
		WhySelected:   "dominant updater during the incident window",
		ShareOfRows:   0.90,
		ShareOfTxns:   0.80,
		AvgRowsPerTxn: 90,
		SignalFlags:   model.PatternSignalFlags{Dominance: true},
		BusiestMinutes: []model.PatternPeakMinute{
			{Minute: start.Add(time.Minute), TotalRows: 9000, TxnCount: 100},
		},
	}}
	result.Diagnostics.FileCoverage = model.FileCoverage{
		Selected: []model.FileCoverageItem{{
			BinlogPath:   "mysql-bin.000044",
			Reason:       "selected: overlaps requested time window",
			Size:         1073746389,
			FirstEventAt: start,
			LastEventAt:  start.Add(10 * time.Minute),
		}},
		Skipped: []model.FileCoverageItem{{
			BinlogPath: "mysql-bin.000045",
			Reason:     "skipped: mtime coarse filter",
			Size:       1073747711,
		}},
	}
	result.Diagnostics.FileSegments = []model.FileSegment{{
		StartTime:   start,
		EndTime:     start.Add(5 * time.Minute),
		BinlogBytes: 20480,
		Rows:        500,
		Events:      75,
	}}
	result.Diagnostics.LargestTransactions = []model.Transaction{{
		TxnKey:          "txn-largest",
		TotalRows:       12000,
		Duration:        20 * time.Second,
		BinlogBytes:     20480,
		EventCount:      80,
		Tables:          map[string]int{"shop.orders": 11000, "shop.order_items": 1000},
		QuerySummary:    "UPDATE shop.orders SET status = 'paid' WHERE id BETWEEN ? AND ?",
		BinlogPathStart: "mysql-bin.000044",
		PositionStart:   12345,
		PositionEnd:     23456,
	}}
	result.Diagnostics.LongestTransactions = []model.Transaction{{
		TxnKey:          "txn-longest",
		TotalRows:       8000,
		Duration:        48 * time.Second,
		BinlogBytes:     18432,
		EventCount:      60,
		Tables:          map[string]int{"shop.orders": 5000, "shop.users": 3000},
		QuerySummary:    "UPDATE shop.users SET state = 'locked' WHERE tenant_id = ?",
		BinlogPathStart: "mysql-bin.000044",
		PositionStart:   34567,
		PositionEnd:     45678,
	}}
	result.Diagnostics.WidestTransactions = []model.Transaction{{
		TxnKey:          "txn-widest",
		TotalRows:       6000,
		Duration:        12 * time.Second,
		BinlogBytes:     12288,
		EventCount:      45,
		Tables:          map[string]int{"shop.orders": 2000, "shop.order_items": 1500, "shop.users": 1200, "shop.payments": 900, "shop.audit_logs": 400},
		QuerySummary:    "UPDATE multiple shop tables in one logical settlement txn",
		BinlogPathStart: "mysql-bin.000044",
		PositionStart:   56789,
		PositionEnd:     67890,
	}}
	return result
}

func TestHTMLPatternDrilldown_SelectedPatternsGetCard(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 1, TotalRows: 100},
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey:    "p1",
				Label:         "big insert batch",
				WhySelected:   "high signal: dominant",
				ShareOfRows:   0.80,
				ShareOfTxns:   0.70,
				AvgRowsPerTxn: 500,
				SignalFlags:   model.PatternSignalFlags{Dominance: true},
				BusiestMinutes: []model.PatternPeakMinute{
					{Minute: time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC), TotalRows: 30000, TxnCount: 60},
				},
			},
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "drilldown") {
		t.Fatal("expected drilldown card in HTML output")
	}
	if !strings.Contains(out, "high signal: dominant") {
		t.Fatal("expected why_selected text in HTML output")
	}
}

func TestHTMLPatternDrilldown_IncludesMetricHelp(t *testing.T) {
	result := model.AnalysisResult{
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey:  "p1",
				WhySelected: "dominant",
				ShareOfRows: 0.80,
				ShareOfTxns: 0.70,
			},
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should include inline help for share metrics
	if !strings.Contains(out, "share of rows") {
		t.Fatal("expected share of rows help text in HTML")
	}
}

func TestHTMLPatternDrilldown_NoUnboundedList(t *testing.T) {
	result := model.AnalysisResult{
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey:  "p1",
				WhySelected: "dominant",
				BusiestMinutes: []model.PatternPeakMinute{
					{Minute: time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC), TotalRows: 30000},
					{Minute: time.Date(2026, 4, 12, 10, 1, 0, 0, time.UTC), TotalRows: 15000},
					{Minute: time.Date(2026, 4, 12, 10, 2, 0, 0, time.UTC), TotalRows: 5000},
				},
			},
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Count rendered minute divs — 1 from CSS definition + at most 2 rendered = max 3
	// Subtract 1 for the CSS class definition
	renderedCount := strings.Count(out, "drilldown-minute") - 1 // -1 for CSS definition
	if renderedCount > 2 {
		t.Fatalf("expected at most 2 rendered drilldown minutes in HTML, got %d", renderedCount)
	}
}

func TestHTMLPatternDrilldown_NotRenderedWhenEmpty(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 1},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The section header "Pattern Drilldowns" should not appear when empty
	if strings.Contains(out, "Pattern Drilldowns") {
		t.Fatal("expected no Pattern Drilldowns section when PatternDrilldowns is empty")
	}
}

func TestRenderHTMLUsesExpandedResponsiveChartLayout(t *testing.T) {
	out, err := RenderHTML(model.AnalysisResult{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"chart-panel chart-panel-wide",
		"grid-template-columns: repeat(2, minmax(0, 1fr));",
		".chart-box { width: 100%; height: 320px; }",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected responsive chart layout token %q in HTML output", token)
		}
	}
}

func TestRenderHTMLTopTablesChartHidesLegend(t *testing.T) {
	out, err := RenderHTML(model.AnalysisResult{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "legend: { show: false }") {
		t.Fatal("expected top tables chart legend to be disabled")
	}
}

func TestRenderHTMLTopTablesExposeInlineActivityPanel(t *testing.T) {
	result := model.AnalysisResult{
		Tables: []model.TableStats{{
			Schema: "shop",
			Table:  "orders",
			Activity: []model.TableActivityPoint{
				{
					Minute:      time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC),
					Rows:        5,
					InsertRows:  3,
					UpdateRows:  2,
					EventCount:  2,
					BinlogBytes: 200,
				},
				{
					Minute:      time.Date(2026, time.April, 15, 10, 1, 0, 0, time.UTC),
					Rows:        1,
					DeleteRows:  1,
					EventCount:  1,
					BinlogBytes: 50,
				},
			},
		}},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		`data-table-key="shop.orders"`,
		`table-detail-panel`,
		`chart-table-activity-shop-orders`,
		`chart-table-ops-shop-orders`,
		`window.tableActivitySeries =`,
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected inline top-table drilldown token %q in HTML output", token)
		}
	}
}

func TestRenderHTMLFileCoverageSection(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			FileCoverage: model.FileCoverage{
				Selected: []model.FileCoverageItem{
					{
						BinlogPath:   "mysql-bin.000001",
						Reason:       "selected",
						Size:         1024000,
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

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		`id="file-coverage"`,
		"mysql-bin.000001",
		"mysql-bin.000003",
		"outside window",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected file coverage token %q in HTML output", token)
		}
	}
}

func TestRenderHTMLBinlogThroughputSection(t *testing.T) {
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
				{
					StartTime:   time.Date(2026, 4, 15, 10, 5, 0, 0, time.UTC),
					EndTime:     time.Date(2026, 4, 15, 10, 10, 0, 0, time.UTC),
					BinlogBytes: 40960,
					Rows:        1000,
					Events:      150,
				},
			},
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		`id="binlog-throughput"`,
		"chart-throughput",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected binlog throughput token %q in HTML output", token)
		}
	}
}

func TestRenderHTMLTransactionEvidenceCards(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			LargestTransactions: []model.Transaction{{
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
			}},
			LongestTransactions: []model.Transaction{{
				TxnKey:          "txn-long",
				Duration:        35 * time.Second,
				TotalRows:       50,
				EventCount:      5,
				BinlogBytes:     512,
				BinlogPathStart: "mysql-bin.000010",
				BinlogPathEnd:   "mysql-bin.000010",
				PositionStart:   100,
				PositionEnd:     200,
				Tables:          map[string]int{"shop.accounts": 50},
			}},
			WidestTransactions: []model.Transaction{{
				TxnKey:    "txn-wide",
				TotalRows: 100,
				Duration:  2 * time.Second,
				Tables:    map[string]int{"shop.orders": 60, "shop.users": 40},
			}},
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		`id="transaction-evidence"`,
		"mysql-bin.000044:300-mysql-bin.000045:520",
		"txn-evidence", // largest
		"txn-long",     // longest
		"txn-wide",     // widest
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected transaction evidence token %q in HTML output", token)
		}
	}
}

func TestAnalyzeHTMLUsesDBAReadingPath(t *testing.T) {
	out, err := RenderHTML(productHTMLFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedOrder := []string{
		`id="executive-summary"`,
		`id="timeline"`,
		`id="hotspots"`,
		`id="ddl-timeline"`,
		`id="transaction-evidence"`,
		`id="analyzed-files"`,
		`id="write-shape-patterns"`,
	}
	last := -1
	for _, token := range expectedOrder {
		idx := strings.Index(out, token)
		if idx < 0 {
			t.Fatalf("expected HTML to contain %s", token)
		}
		if idx < last {
			t.Fatalf("expected %s after previous section", token)
		}
		last = idx
	}
}

func TestAnalyzeHTMLIncludesReadableTPSChart(t *testing.T) {
	out, err := RenderHTML(productHTMLFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{`id="chart-tps"`, `avg TPS/min`, `min-height: 420px`} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected TPS chart token %q\n%s", token, out)
		}
	}
}

func TestAnalyzeHTMLTransactionEvidenceLabelsRankingMetric(t *testing.T) {
	out, err := RenderHTMLWithOptions(productHTMLFixture(), Options{TopN: 5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"Top 5 Largest Transactions by Rows",
		"Top 5 Longest Transactions by Duration",
		"Top 5 Widest Transactions by Touched Tables",
		"touched tables",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected transaction evidence label %q\n%s", token, out)
		}
	}
}

func TestAnalyzeHTMLExplainsAnalyzedFilesAndPatternMetrics(t *testing.T) {
	out, err := RenderHTML(productHTMLFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"Analyzed Files",
		"selected: overlaps requested time window",
		"filesystem mtime",
		"Row share",
		"Transaction share",
		"Avg rows per transaction",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected explanatory token %q\n%s", token, out)
		}
	}
}

func TestRenderHTMLNewSectionsEmptyState(t *testing.T) {
	result := model.AnalysisResult{}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// File coverage and throughput sections should be present even when empty
	if !strings.Contains(out, `id="file-coverage"`) {
		t.Fatal("expected file-coverage section even when empty")
	}
}

func TestRenderHTMLIncludesDDLTimelineAndTransactionDiagnostics(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			DDLEvents: []model.DDLEvent{{
				Timestamp:     time.Date(2026, time.April, 15, 10, 5, 0, 0, time.UTC),
				Operation:     "ALTER TABLE",
				Schema:        "shop",
				Table:         "orders",
				Statement:     "ALTER TABLE shop.orders ADD COLUMN note TEXT",
				BinlogPath:    "mysql-bin.000044",
				PositionStart: 120,
				PositionEnd:   240,
			}},
			LargestTransactions: []model.Transaction{{
				TxnKey:          "txn-rows",
				TotalRows:       2000,
				EventCount:      20,
				Duration:        2 * time.Second,
				BinlogBytes:     7000,
				BinlogPathStart: "mysql-bin.000044",
				BinlogPathEnd:   "mysql-bin.000044",
				PositionStart:   300,
				PositionEnd:     520,
				Tables:          map[string]int{"shop.orders": 1800, "shop.payments": 200},
				QuerySummary:    "UPDATE shop.orders SET status='done' WHERE id IN (...)",
			}},
			LongestTransactions: []model.Transaction{{
				TxnKey:          "txn-long",
				TotalRows:       50,
				EventCount:      5,
				Duration:        35 * time.Second,
				BinlogBytes:     400,
				BinlogPathStart: "mysql-bin.000045",
				BinlogPathEnd:   "mysql-bin.000045",
				PositionStart:   220,
				PositionEnd:     260,
				Tables:          map[string]int{"shop.accounts": 50},
			}},
			HotIntervals: []model.MinuteBucket{{
				Minute:      time.Date(2026, time.April, 15, 10, 7, 0, 0, time.UTC),
				TotalRows:   3200,
				TxnCount:    18,
				EventCount:  42,
				BinlogBytes: 8192,
				DDLCount:    1,
			}},
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"DDL Timeline",
		"ALTER TABLE shop.orders",
		"mysql-bin.000044:120-240",
		"Largest Transactions",
		"Longest Transactions",
		"Hot Intervals",
		"txn-rows",
		"txn-long",
		"shop.orders",
		"shop.accounts",
		"shop.orders · 1,800",
		"shop.payments · 200",
		"UPDATE shop.orders SET status",
		"2026-04-15 10:07:00",
		"Rows=3,200",
		"Txns=18",
		"Events=42",
		"Binlog Bytes: 8,192",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected diagnostics token %q in HTML output", token)
		}
	}
}
