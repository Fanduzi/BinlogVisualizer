package compare

import (
	"math"
	"testing"
)

func TestBuildKeyFindings_PatternDriverGrowth(t *testing.T) {
	current := InputReport{
		Summary: InputSummary{TotalRows: 10000, TotalTransactions: 500},
		Tables: []InputTable{
			{Schema: "shop", Table: "orders", TotalRows: 6000},
			{Schema: "shop", Table: "payments", TotalRows: 4000},
		},
		Patterns: []InputPattern{
			{PatternKey: "orders.insert_batch", Label: "orders.insert_batch", TotalRows: 6000, ShareOfRows: 0.6, Operations: map[string]int{"INSERT": 6000}},
			{PatternKey: "payments.update_status", Label: "payments.update_status", TotalRows: 4000, ShareOfRows: 0.4, Operations: map[string]int{"UPDATE": 4000}},
		},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 5000, TotalTransactions: 250},
		Tables: []InputTable{
			{Schema: "shop", Table: "orders", TotalRows: 3000},
			{Schema: "shop", Table: "payments", TotalRows: 2000},
		},
		Patterns: []InputPattern{
			{PatternKey: "orders.insert_batch", Label: "orders.insert_batch", TotalRows: 3000, ShareOfRows: 0.6, Operations: map[string]int{"INSERT": 3000}},
			{PatternKey: "payments.update_status", Label: "payments.update_status", TotalRows: 2000, ShareOfRows: 0.4, Operations: map[string]int{"UPDATE": 2000}},
		},
	}

	result := BuildCompareResult(current, baseline)
	findings := result.KeyFindings

	if len(findings) == 0 {
		t.Fatal("expected at least one finding for significant growth")
	}

	// First finding should be about overall volume change (severity)
	if findings[0].Kind != "volume_change" {
		t.Fatalf("expected first finding kind volume_change, got %q", findings[0].Kind)
	}

	// Should contain a pattern_driver finding
	hasPatternDriver := false
	for _, f := range findings {
		if f.Kind == "pattern_driver" {
			hasPatternDriver = true
			break
		}
	}
	if !hasPatternDriver {
		t.Fatalf("expected pattern_driver finding, got %+v", findings)
	}
}

func TestBuildKeyFindings_NewPattern(t *testing.T) {
	current := InputReport{
		Summary: InputSummary{TotalRows: 8000, TotalTransactions: 400},
		Patterns: []InputPattern{
			{PatternKey: "orders.insert_batch", Label: "orders.insert_batch", TotalRows: 6000, ShareOfRows: 0.75},
			{PatternKey: "payments.new_workflow", Label: "payments.new_workflow", TotalRows: 2000, ShareOfRows: 0.25},
		},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 6000, TotalTransactions: 300},
		Patterns: []InputPattern{
			{PatternKey: "orders.insert_batch", Label: "orders.insert_batch", TotalRows: 6000, ShareOfRows: 1.0},
		},
	}

	result := BuildCompareResult(current, baseline)
	findings := result.KeyFindings

	hasNewPattern := false
	for _, f := range findings {
		if f.Kind == "new_pattern" {
			hasNewPattern = true
			if f.Evidence["pattern_key"] != "payments.new_workflow" {
				t.Fatalf("expected new pattern key, got %v", f.Evidence["pattern_key"])
			}
		}
	}
	if !hasNewPattern {
		t.Fatalf("expected new_pattern finding, got %+v", findings)
	}
}

func TestBuildKeyFindings_LowSignalFindsFew(t *testing.T) {
	// Nearly identical reports should produce zero or very few findings
	current := InputReport{
		Summary: InputSummary{TotalRows: 1002, TotalTransactions: 51},
		Patterns: []InputPattern{
			{PatternKey: "misc", Label: "misc", TotalRows: 1002, ShareOfRows: 1.0},
		},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 1000, TotalTransactions: 50},
		Patterns: []InputPattern{
			{PatternKey: "misc", Label: "misc", TotalRows: 1000, ShareOfRows: 1.0},
		},
	}

	result := BuildCompareResult(current, baseline)
	findings := result.KeyFindings

	// Delta is only 2 rows on 1000 — too small to be significant
	for _, f := range findings {
		if f.Kind == "volume_change" {
			delta, _ := f.Evidence["delta_rows"].(int)
			if math.Abs(float64(delta)) < 100 {
				t.Fatalf("should not emit volume_change for tiny delta, got %+v", f)
			}
		}
	}
}

func TestBuildKeyFindings_CappedAtFive(t *testing.T) {
	current := InputReport{
		Summary: InputSummary{TotalRows: 50000, TotalTransactions: 2500},
		Tables: []InputTable{
			{Schema: "s", Table: "t1", TotalRows: 20000, InsertRows: 12000, UpdateRows: 6000, DeleteRows: 2000},
			{Schema: "s", Table: "t2", TotalRows: 15000, InsertRows: 8000, UpdateRows: 5000, DeleteRows: 2000},
			{Schema: "s", Table: "t3", TotalRows: 10000, InsertRows: 5000, UpdateRows: 3000, DeleteRows: 2000},
			{Schema: "s", Table: "t4", TotalRows: 3000, InsertRows: 1000, UpdateRows: 1000, DeleteRows: 1000},
			{Schema: "s", Table: "t5", TotalRows: 2000, InsertRows: 500, UpdateRows: 500, DeleteRows: 1000},
		},
		Patterns: []InputPattern{
			{PatternKey: "p1", Label: "p1", TotalRows: 20000, ShareOfRows: 0.4},
			{PatternKey: "p2", Label: "p2", TotalRows: 15000, ShareOfRows: 0.3},
			{PatternKey: "p3", Label: "p3", TotalRows: 10000, ShareOfRows: 0.2},
			{PatternKey: "p4", Label: "p4", TotalRows: 3000, ShareOfRows: 0.06},
			{PatternKey: "p5", Label: "p5", TotalRows: 2000, ShareOfRows: 0.04},
		},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 5000, TotalTransactions: 250},
		Tables: []InputTable{
			{Schema: "s", Table: "t1", TotalRows: 1000, InsertRows: 500, UpdateRows: 300, DeleteRows: 200},
			{Schema: "s", Table: "t2", TotalRows: 1000, InsertRows: 500, UpdateRows: 300, DeleteRows: 200},
			{Schema: "s", Table: "t3", TotalRows: 1000, InsertRows: 500, UpdateRows: 300, DeleteRows: 200},
			{Schema: "s", Table: "t4", TotalRows: 1000, InsertRows: 500, UpdateRows: 300, DeleteRows: 200},
			{Schema: "s", Table: "t5", TotalRows: 1000, InsertRows: 500, UpdateRows: 300, DeleteRows: 200},
		},
		Patterns: []InputPattern{
			{PatternKey: "p1", Label: "p1", TotalRows: 1000, ShareOfRows: 0.2},
			{PatternKey: "p2", Label: "p2", TotalRows: 1000, ShareOfRows: 0.2},
			{PatternKey: "p3", Label: "p3", TotalRows: 1000, ShareOfRows: 0.2},
		},
	}

	result := BuildCompareResult(current, baseline)
	if len(result.KeyFindings) > 5 {
		t.Fatalf("findings capped at 5, got %d", len(result.KeyFindings))
	}
}

func TestBuildKeyFindings_DeterministicOrdering(t *testing.T) {
	current := InputReport{
		Summary: InputSummary{TotalRows: 10000, TotalTransactions: 500},
		Tables: []InputTable{
			{Schema: "shop", Table: "orders", TotalRows: 7000},
			{Schema: "shop", Table: "payments", TotalRows: 3000},
		},
		Patterns: []InputPattern{
			{PatternKey: "orders.bulk", Label: "orders.bulk", TotalRows: 7000, ShareOfRows: 0.7},
			{PatternKey: "payments.status", Label: "payments.status", TotalRows: 3000, ShareOfRows: 0.3},
		},
	}
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 5000, TotalTransactions: 250},
		Tables: []InputTable{
			{Schema: "shop", Table: "orders", TotalRows: 3500},
			{Schema: "shop", Table: "payments", TotalRows: 1500},
		},
		Patterns: []InputPattern{
			{PatternKey: "orders.bulk", Label: "orders.bulk", TotalRows: 3500, ShareOfRows: 0.7},
			{PatternKey: "payments.status", Label: "payments.status", TotalRows: 1500, ShareOfRows: 0.3},
		},
	}

	result1 := BuildCompareResult(current, baseline)
	result2 := BuildCompareResult(current, baseline)

	if len(result1.KeyFindings) != len(result2.KeyFindings) {
		t.Fatalf("non-deterministic length: %d vs %d", len(result1.KeyFindings), len(result2.KeyFindings))
	}
	for i := range result1.KeyFindings {
		if result1.KeyFindings[i].Kind != result2.KeyFindings[i].Kind {
			t.Fatalf("non-deterministic order at %d: %q vs %q", i, result1.KeyFindings[i].Kind, result2.KeyFindings[i].Kind)
		}
		if result1.KeyFindings[i].Title != result2.KeyFindings[i].Title {
			t.Fatalf("non-deterministic title at %d: %q vs %q", i, result1.KeyFindings[i].Title, result2.KeyFindings[i].Title)
		}
	}
}
