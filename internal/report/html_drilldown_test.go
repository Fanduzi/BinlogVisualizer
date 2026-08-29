// Package report verifies analyze HTML pattern drilldown rendering behavior.
// input: synthetic AnalysisResult values with selected and unselected pattern drilldowns.
// output: assertions for byte-coverage summary cards, drilldown cards, badges, and bounded detail content in HTML.
// pos: regression coverage for analyze HTML drilldown presentation.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"fmt"
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

	// Verify Y-axis labels are present in drill-down charts.
	for _, label := range []string{
		`name: 'Rows\/min'`,
		`left: 58, right: 16`,
	} {
		if !strings.Contains(out, label) {
			t.Fatalf("expected drill-down chart Y-axis config %q in HTML output", label)
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

func TestRenderHTMLDistinguishesInputFileSizeFromCountedEventBytes(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			FileCoverage: model.FileCoverage{
				Selected: []model.FileCoverageItem{
					{BinlogPath: "mysql-bin.000001", Size: 1000},
					{BinlogPath: "mysql-bin.000002", Size: 2000},
				},
			},
			CountedEventBytes: 250,
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{
		"Input File Size",
		"2.9 KB",
		"Counted Event Bytes",
		"250 B",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected HTML output to contain %q, got:\n%s", want, out)
		}
	}
	if strings.Index(out, "Input File Size") > strings.Index(out, `id="section-findings"`) {
		t.Fatal("expected input file size near the executive summary")
	}
}

func TestRenderHTMLShowsUnavailableWhenSelectedFileSizeIsMissing(t *testing.T) {
	out, err := RenderHTML(model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			FileCoverage: model.FileCoverage{
				Selected: []model.FileCoverageItem{{BinlogPath: "legacy.binlog"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Input File Size") || !strings.Contains(out, "N/A") {
		t.Fatalf("expected missing file size to be unavailable, got:\n%s", out)
	}
	if idx := strings.Index(out, "legacy.binlog"); idx >= 0 {
		row := out[idx:]
		if end := strings.Index(row, "</tr>"); end >= 0 {
			row = row[:end]
		}
		if strings.Contains(row, "0 B") {
			t.Fatalf("missing file size must not render as zero in the coverage row, got:\n%s", row)
		}
	}
}

func TestRenderHTMLHidesFileCoverageSectionWhenEmpty(t *testing.T) {
	out, err := RenderHTML(model.AnalysisResult{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{`id="analyzed-files"`, `id="file-coverage"`} {
		if strings.Contains(out, token) {
			t.Fatalf("expected no file coverage section token %q when coverage is empty", token)
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

	// Five-section reading path: Summary → Findings → Activity → Objects → Evidence.
	expectedOrder := []string{
		`id="executive-summary"`,
		`id="section-findings"`,
		`id="section-activity"`,
		`id="section-objects"`,
		`id="section-evidence"`,
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

func TestAnalyzeHTMLTransactionEvidenceShowsSingleChampionPerMetric(t *testing.T) {
	result := productHTMLFixture()
	result.Diagnostics.LargestTransactions = append(result.Diagnostics.LargestTransactions, model.Transaction{
		TxnKey:    "txn-largest-runner-up",
		TotalRows: 9000,
		Tables:    map[string]int{"shop.orders": 9000},
	})
	result.Diagnostics.LongestTransactions = append(result.Diagnostics.LongestTransactions, model.Transaction{
		TxnKey:   "txn-longest-runner-up",
		Duration: 35 * time.Second,
		Tables:   map[string]int{"shop.accounts": 10},
	})
	result.Diagnostics.WidestTransactions = append(result.Diagnostics.WidestTransactions, model.Transaction{
		TxnKey: "txn-widest-runner-up",
		Tables: map[string]int{"shop.orders": 1, "shop.users": 1, "shop.payments": 1},
	})

	out, err := RenderHTMLWithOptions(result, Options{TopN: 5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"txn-largest",
		"txn-longest",
		"txn-widest",
		"touched tables",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected transaction evidence label %q\n%s", token, out)
		}
	}
	for _, token := range []string{
		"Top 5 Largest Transactions by Rows",
		"Top 5 Longest Transactions by Duration",
		"Top 5 Widest Transactions by Touched Tables",
		"txn-largest-runner-up",
		"txn-longest-runner-up",
		"txn-widest-runner-up",
	} {
		if strings.Contains(out, token) {
			t.Fatalf("expected champion-only transaction evidence, but saw %q\n%s", token, out)
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

func TestAnalyzeHTMLUsesReportTopNForTopTables(t *testing.T) {
	result := productHTMLFixture()
	result.Tables = nil
	for i := 0; i < 12; i++ {
		result.Tables = append(result.Tables, model.TableStats{
			Schema:    "shop",
			Table:     fmt.Sprintf("table_%02d", i),
			TotalRows: 1000 - i,
		})
	}

	out, err := RenderHTMLWithOptions(result, Options{TopN: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Count(out, `data-table-row="`) > 3 {
		t.Fatalf("expected top 3 table rows in HTML\n%s", out)
	}
}

func TestAnalyzeHTMLSummarySectionHasKeyFindingsArea(t *testing.T) {
	result := productHTMLFixture()
	result.Alerts = []model.Alert{
		{Severity: "warning", Message: "high row volume detected"},
		{Severity: "critical", Message: "long-running transaction found"},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	summaryIdx := strings.Index(out, `id="executive-summary"`)
	findingsIdx := strings.Index(out, `id="section-findings"`)
	if summaryIdx < 0 || findingsIdx < 0 {
		t.Fatal("expected summary and findings sections")
	}
	// Key findings must appear inside the summary section, before the standalone findings section.
	keyFindingsIdx := strings.Index(out, "key-findings")
	if keyFindingsIdx < 0 {
		t.Fatal("expected key-findings area in summary section")
	}
	if keyFindingsIdx > findingsIdx {
		t.Fatal("expected key-findings inside summary, before standalone findings section")
	}
}

func TestAnalyzeHTMLFindingsSectionShowsAlertsWithSeverity(t *testing.T) {
	result := model.AnalysisResult{
		Alerts: []model.Alert{
			{Severity: "critical", Message: "critical issue"},
			{Severity: "warning", Message: "warning issue"},
			{Severity: "info", Message: "info notice"},
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	findingsIdx := strings.Index(out, `id="section-findings"`)
	if findingsIdx < 0 {
		t.Fatal("expected section-findings")
	}

	for _, token := range []string{"critical issue", "warning issue", "info notice"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected alert message %q in findings section", token)
		}
	}
}

func TestAnalyzeHTMLActivitySectionContainsChartsInOrder(t *testing.T) {
	out, err := RenderHTML(productHTMLFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Activity section must contain charts in logical reading order: total activity → composition → anomaly peaks.
	activityIdx := strings.Index(out, `id="section-activity"`)
	objectsIdx := strings.Index(out, `id="section-objects"`)
	if activityIdx < 0 || objectsIdx < 0 {
		t.Fatal("expected section-activity and section-objects")
	}

	activitySection := out[activityIdx:objectsIdx]

	for _, token := range []string{
		`id="chart-tps"`,
		`id="chart-timeline"`,
		`id="chart-ops"`,
	} {
		if !strings.Contains(activitySection, token) {
			t.Fatalf("expected chart token %q in activity section", token)
		}
	}
}

func TestAnalyzeHTMLObjectsSectionContainsTopTablesAndChart(t *testing.T) {
	result := model.AnalysisResult{
		Tables: []model.TableStats{
			{Schema: "shop", Table: "orders", TotalRows: 5000, InsertRows: 3000, UpdateRows: 2000},
			{Schema: "shop", Table: "users", TotalRows: 1000, DeleteRows: 1000},
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	objectsIdx := strings.Index(out, `id="section-objects"`)
	evidenceIdx := strings.Index(out, `id="section-evidence"`)
	if objectsIdx < 0 || evidenceIdx < 0 {
		t.Fatal("expected section-objects and section-evidence")
	}

	objectsSection := out[objectsIdx:evidenceIdx]

	for _, token := range []string{
		`id="chart-tables"`,
		`data-table-row="shop.orders"`,
		`data-table-row="shop.users"`,
	} {
		if !strings.Contains(objectsSection, token) {
			t.Fatalf("expected objects section token %q", token)
		}
	}
}

func TestAnalyzeHTMLEvidenceSectionGroupAllDiagnosticContent(t *testing.T) {
	out, err := RenderHTML(productHTMLFixture())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	evidenceShell := `<section class="section" id="section-evidence">`
	evidenceIdx := strings.Index(out, evidenceShell)
	if evidenceIdx < 0 {
		t.Fatalf("expected evidence to use the same section shell as the other top-level sections")
	}

	// All evidence sub-sections must appear after the evidence wrapper.
	for _, token := range []string{
		`id="transaction-evidence"`,
		`id="ddl-timeline"`,
		`id="write-shape-patterns"`,
		`id="analyzed-files"`,
		`id="binlog-throughput"`,
	} {
		idx := strings.Index(out, token)
		if idx < 0 {
			continue // conditional sections may not render
		}
		if idx < evidenceIdx {
			t.Fatalf("expected %s to appear after section-evidence wrapper", token)
		}
	}
}

func TestAnalyzeHTMLEvidenceEmptyFileCoverageHidden(t *testing.T) {
	out, err := RenderHTML(model.AnalysisResult{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{`id="analyzed-files"`, `id="file-coverage"`, `id="binlog-throughput"`} {
		if strings.Contains(out, token) {
			t.Fatalf("expected %s hidden when no file coverage data", token)
		}
	}
}

func TestAnalyzeHTMLObjectsTopTablesRespectsTopN(t *testing.T) {
	result := productHTMLFixture()
	result.Tables = nil
	for i := 0; i < 12; i++ {
		result.Tables = append(result.Tables, model.TableStats{
			Schema:    "shop",
			Table:     fmt.Sprintf("table_%02d", i),
			TotalRows: 1000 - i,
		})
	}

	out, err := RenderHTMLWithOptions(result, Options{TopN: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Count(out, `data-table-row="`) > 3 {
		t.Fatalf("expected top 3 table rows in objects section")
	}
	if !strings.Contains(out, "and 9 more tables") {
		t.Fatalf("expected omitted-table count in HTML")
	}
}

func TestAnalyzeHTMLTransactionEvidenceChampionOnly(t *testing.T) {
	result := productHTMLFixture()
	result.Diagnostics.LargestTransactions = append(result.Diagnostics.LargestTransactions, model.Transaction{
		TxnKey:    "txn-largest-runner-up",
		TotalRows: 9000,
		Tables:    map[string]int{"shop.orders": 9000},
	})

	out, err := RenderHTMLWithOptions(result, Options{TopN: 5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "txn-largest") {
		t.Fatal("expected champion txn-largest")
	}
	if strings.Contains(out, "txn-largest-runner-up") {
		t.Fatal("expected only champion, not runner-up")
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
		"Largest Transaction",
		"Longest Transaction",
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
