package report

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

// Markdown intentionally omits drilldown rendering — Top Patterns section
// is absent from markdown, so drilldowns have no anchor point.
// This test locks that decision explicitly.

func TestMarkdownPatternDrilldown_IntentionallyOmitted(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{TotalTransactions: 1, TotalRows: 100},
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey:  "p1",
				WhySelected: "high signal: dominant",
				ShareOfRows: 0.80,
			},
		},
	}

	out, err := RenderMarkdown(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out, "drilldown:") {
		t.Fatal("markdown should not render drilldown blocks — this is an intentional omission since Top Patterns section is not in markdown output")
	}
	if strings.Contains(out, "high signal: dominant") {
		t.Fatal("markdown should not contain drilldown why_selected text")
	}
}
