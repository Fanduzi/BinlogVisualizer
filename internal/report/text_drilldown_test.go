package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestTextPatternDrilldown_NoneWhenEmpty(t *testing.T) {
	result := model.AnalysisResult{
		Patterns: []model.PatternStats{
			{PatternKey: "p1", Label: "small", TotalRows: 10, TxnCount: 5, AvgRowsPerTxn: 2},
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out, "drilldown:") {
		t.Fatalf("expected no drilldown block for low-signal pattern, got: %s", out)
	}
}

func TestTextPatternDrilldown_SelectedPatternGetsBlock(t *testing.T) {
	result := model.AnalysisResult{
		Patterns: []model.PatternStats{
			{
				PatternKey:     "p1",
				Label:          "big insert batch",
				TotalRows:      50000,
				TxnCount:       100,
				AvgRowsPerTxn:  500,
				ShareOfRows:    0.80,
				ShareOfTransactions: 0.70,
			},
		},
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey:   "p1",
				Label:        "big insert batch",
				WhySelected:  "high signal: dominates workload (80% rows, 70% txns)",
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

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "drilldown:") {
		t.Fatalf("expected drilldown block, got: %s", out)
	}
	if !strings.Contains(out, "why:") {
		t.Fatalf("expected 'why:' line in drilldown, got: %s", out)
	}
}

func TestTextPatternDrilldown_PeakMinutesCappedAtTwo(t *testing.T) {
	result := model.AnalysisResult{
		Patterns: []model.PatternStats{
			{PatternKey: "p1", Label: "big", TotalRows: 50000, TxnCount: 100, AvgRowsPerTxn: 500},
		},
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey:  "p1",
				Label:       "big",
				WhySelected: "dominant",
				BusiestMinutes: []model.PatternPeakMinute{
					{Minute: time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC), TotalRows: 30000},
					{Minute: time.Date(2026, 4, 12, 10, 1, 0, 0, time.UTC), TotalRows: 15000},
					{Minute: time.Date(2026, 4, 12, 10, 2, 0, 0, time.UTC), TotalRows: 5000},
				},
			},
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should only render at most 2 peak minute lines
	count := strings.Count(out, "peak minute")
	if count > 2 {
		t.Fatalf("expected at most 2 peak minute lines, got %d", count)
	}
}

func TestTextPatternDrilldown_RepresentativeTxnsCappedAtTwo(t *testing.T) {
	result := model.AnalysisResult{
		Patterns: []model.PatternStats{
			{PatternKey: "p1", Label: "big", TotalRows: 50000, TxnCount: 100, AvgRowsPerTxn: 500},
		},
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey:  "p1",
				Label:       "big",
				WhySelected: "dominant",
				RepresentativeTransactions: []model.PatternRepresentativeTxn{
					{TxnKey: "t1", TotalRows: 500},
					{TxnKey: "t2", TotalRows: 480},
					{TxnKey: "t3", TotalRows: 460},
				},
			},
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	count := strings.Count(out, "rep txn")
	if count > 2 {
		t.Fatalf("expected at most 2 rep txn lines, got %d", count)
	}
}
