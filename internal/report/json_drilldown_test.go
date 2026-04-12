package report

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func makeBoundedMinutes(n int) []model.PatternPeakMinute {
	result := make([]model.PatternPeakMinute, n)
	for i := 0; i < n; i++ {
		result[i] = model.PatternPeakMinute{
			Minute:    time.Date(2026, 4, 12, 10, i, 0, 0, time.UTC),
			TotalRows: 1000 * (n - i),
			TxnCount:  10 * (n - i),
		}
	}
	return result
}

func makeBoundedTxns(n int) []model.PatternRepresentativeTxn {
	result := make([]model.PatternRepresentativeTxn, n)
	for i := 0; i < n; i++ {
		result[i] = model.PatternRepresentativeTxn{
			TxnKey:    "rep-txn-" + string(rune('a'+i)),
			TotalRows: 500 - i*10,
		}
	}
	return result
}

func TestJSONPatternDrilldown_AlwaysPresent(t *testing.T) {
	out, err := RenderJSON(model.AnalysisResult{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, `"pattern_drilldowns": []`) {
		t.Fatalf("expected empty pattern_drilldowns array, got: %s", out)
	}
}

func TestJSONPatternDrilldown_BoundedFields(t *testing.T) {
	result := model.AnalysisResult{
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey:    "p1",
				Label:         "big batch",
				WhySelected:   "high signal: dominates workload (80% rows, 70% txns)",
				ShareOfRows:   0.80,
				ShareOfTxns:   0.70,
				AvgRowsPerTxn: 500,
				SignalFlags: model.PatternSignalFlags{
					Dominance: true,
					Anomaly:   true,
				},
			},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	drilldowns, ok := parsed["pattern_drilldowns"].([]any)
	if !ok || len(drilldowns) != 1 {
		t.Fatalf("expected 1 drilldown, got %v", parsed["pattern_drilldowns"])
	}

	d := drilldowns[0].(map[string]any)
	if d["pattern_key"] != "p1" {
		t.Fatalf("expected pattern_key p1, got %v", d["pattern_key"])
	}
	if d["why_selected"] != "high signal: dominates workload (80% rows, 70% txns)" {
		t.Fatalf("expected why_selected, got %v", d["why_selected"])
	}

	flags, ok := d["signal_flags"].(map[string]any)
	if !ok {
		t.Fatalf("expected signal_flags object, got %v", d["signal_flags"])
	}
	if flags["dominance"] != true {
		t.Fatalf("expected dominance=true, got %v", flags["dominance"])
	}
	if flags["anomaly"] != true {
		t.Fatalf("expected anomaly=true, got %v", flags["anomaly"])
	}
}

func TestJSONPatternDrilldown_NestedListsBounded(t *testing.T) {
	result := model.AnalysisResult{
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey:  "p1",
				WhySelected: "dominant",
				BusiestMinutes: makeBoundedMinutes(3),
				RepresentativeTransactions: makeBoundedTxns(3),
			},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	drilldowns := parsed["pattern_drilldowns"].([]any)
	d := drilldowns[0].(map[string]any)

	minutes, ok := d["busiest_minutes"].([]any)
	if !ok {
		t.Fatalf("expected busiest_minutes array, got %v", d["busiest_minutes"])
	}
	if len(minutes) > 2 {
		t.Fatalf("expected at most 2 busiest_minutes, got %d", len(minutes))
	}

	txns, ok := d["representative_transactions"].([]any)
	if !ok {
		t.Fatalf("expected representative_transactions array, got %v", d["representative_transactions"])
	}
	if len(txns) > 2 {
		t.Fatalf("expected at most 2 representative_transactions, got %d", len(txns))
	}
}

func TestJSONPatternDrilldown_DeterministicSerialization(t *testing.T) {
	result := model.AnalysisResult{
		PatternDrilldowns: []model.PatternDrilldown{
			{PatternKey: "p1", WhySelected: "dominant", ShareOfRows: 0.80, ShareOfTxns: 0.70, AvgRowsPerTxn: 500},
		},
	}

	out1, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out2, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out1 != out2 {
		t.Fatal("JSON output should be deterministic")
	}
}

func TestJSONPatternDrilldown_SharesAndWhySerialize(t *testing.T) {
	result := model.AnalysisResult{
		PatternDrilldowns: []model.PatternDrilldown{
			{
				PatternKey:    "p1",
				WhySelected:   "high signal: dominant",
				ShareOfRows:   0.75,
				ShareOfTxns:   0.60,
				AvgRowsPerTxn: 400,
			},
		},
	}

	out, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseJSONMap(t, out)
	drilldowns := parsed["pattern_drilldowns"].([]any)
	d := drilldowns[0].(map[string]any)

	if d["share_of_rows"] == nil {
		t.Fatal("expected share_of_rows to be present")
	}
	if d["share_of_txns"] == nil {
		t.Fatal("expected share_of_txns to be present")
	}
	if d["avg_rows_per_txn"] == nil {
		t.Fatal("expected avg_rows_per_txn to be present")
	}
	if d["why_selected"] == nil {
		t.Fatal("expected why_selected to be present")
	}
}

// Verify contract stability: pattern_drilldowns is always a top-level array.
func TestJSONPatternDrilldown_ContractStability(t *testing.T) {
	var parsed map[string]any

	// Empty result
	out, _ := RenderJSON(model.AnalysisResult{})
	json.Unmarshal([]byte(out), &parsed)
	if _, ok := parsed["pattern_drilldowns"]; !ok {
		t.Fatal("pattern_drilldowns field must always be present")
	}

	// With data
	out, _ = RenderJSON(model.AnalysisResult{
		PatternDrilldowns: []model.PatternDrilldown{
			{PatternKey: "p1", WhySelected: "test"},
		},
	})
	json.Unmarshal([]byte(out), &parsed)
	drilldowns := parsed["pattern_drilldowns"].([]any)
	if len(drilldowns) != 1 {
		t.Fatalf("expected 1 drilldown, got %d", len(drilldowns))
	}
}
