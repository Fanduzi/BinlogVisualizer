package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestHTMLPatternDrilldown_SelectedPatternsGetCard(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 1, TotalRows: 100},
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey:   "p1",
				Label:        "big insert batch",
				WhySelected:  "high signal: dominant",
				ShareOfRows:  0.80,
				ShareOfTxns:  0.70,
				AvgRowsPerTxn: 500,
				SignalFlags: model.PatternSignalFlags{Dominance: true},
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
				PatternKey:   "p1",
				WhySelected:  "dominant",
				ShareOfRows:  0.80,
				ShareOfTxns:  0.70,
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
