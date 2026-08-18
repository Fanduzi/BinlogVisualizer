// Package report verifies analyze HTML activity charts stay visible on short series.
// input: a one-minute Duration-0s AnalysisResult like the v0.21.0 fixture first screen.
// output: TPS and rows-per-minute series use a visible mark when a polyline cannot be drawn.
// pos: regression coverage for the empty single-point ECharts line (symbol: none).
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestRenderHTMLSingleMinuteChartsStayVisible(t *testing.T) {
	start := time.Date(2026, 3, 15, 14, 10, 26, 0, time.UTC)
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 4,
			TotalRows:         5,
			TotalEvents:       16,
			StartTime:         start,
			EndTime:           start,
			Duration:          0,
		},
		Minutes: []model.MinuteBucket{{
			Minute:    start,
			TotalRows: 5,
			TxnCount:  4,
		}},
		Timeseries: model.Timeseries{
			TPSSeries:  []model.TimeseriesPoint{{Minute: start, Value: 0.0666666667}},
			RowsSeries: []model.TimeseriesPoint{{Minute: start, Value: 5}},
		},
	}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}

	if !strings.Contains(out, `"14:10"`) {
		t.Fatal("expected the single minute to remain on the x-axis")
	}
	if !strings.Contains(out, "function visibleLineMark") {
		t.Fatal("expected a short-series visible-mark helper")
	}
	if !strings.Contains(out, "visibleLineMark(tpsValues)") {
		t.Fatal("expected TPS/min to use the visible-mark helper")
	}
	if !strings.Contains(out, "visibleLineMark(minuteRows)") {
		t.Fatal("expected rows-per-minute to use the visible-mark helper")
	}
	if !strings.Contains(out, `symbol: sparse ? 'circle' : 'none'`) {
		t.Fatal("expected a line+circle mark when a series has fewer than 2 points")
	}
	if !strings.Contains(out, "symbolSize: sparse ? 10 : 0") {
		t.Fatal("expected a symbol size of at least 8 for a single-point series")
	}

	// The 1-minute series must not be hardcoded to an invisible polyline.
	tpsBlock := out[strings.Index(out, "visibleLineMark(tpsValues)"):]
	if i := strings.Index(tpsBlock, "visibleLineMark(minuteRows)"); i > 0 {
		tpsBlock = tpsBlock[:i]
	}
	if strings.Contains(tpsBlock, "symbol: 'none'") {
		t.Fatal("TPS series must not force symbol: none for a single-minute report")
	}
}
