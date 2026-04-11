package compare

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRenderJSONProducesStableCompareContract(t *testing.T) {
	current, err := LoadReport(filepath.Join("testdata", "current.json"))
	if err != nil {
		t.Fatalf("load current report: %v", err)
	}
	baseline, err := LoadReport(filepath.Join("testdata", "baseline.json"))
	if err != nil {
		t.Fatalf("load baseline report: %v", err)
	}

	result := BuildCompareResult(current, baseline)
	output, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	outputAgain, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error on second render: %v", err)
	}
	if output != outputAgain {
		t.Fatal("expected deterministic JSON output")
	}

	var decoded CompareResult
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatalf("expected valid json: %v", err)
	}

	expected := CompareResult{
		Summary: SummaryDelta{
			CurrentTotalRows:          2400,
			BaselineTotalRows:         1500,
			TotalRowsDelta:            900,
			CurrentTotalTransactions:  120,
			BaselineTotalTransactions: 90,
			TotalTransactionsDelta:    30,
			CurrentWarnings:           0,
			BaselineWarnings:          0,
		},
		KeyFindings: []CompareFinding{
			{
				Kind:    "volume_change",
				Title:   "Total volume change",
				Summary: "rows grew sharply",
				Evidence: map[string]any{
					"delta_rows":    float64(900),
					"baseline_rows": float64(1500),
					"current_rows":  float64(2400),
				},
				EvidenceRefs: nil,
			},
			{
				Kind:    "table_driver",
				Title:   "Top table driver",
				Summary: "orders.refunds had the largest row change",
				Evidence: map[string]any{
					"table":                "orders.refunds",
					"delta_rows":           float64(900),
					"share_of_total_delta": float64(1),
				},
				EvidenceRefs: []EvidenceRef{
					{Section: "table_changes", Key: "orders.refunds", Label: "orders.refunds", Anchor: "section-table-changes"},
				},
			},
		},
		Recommendations: []Recommendation{
			{
				Kind:                "check_table_hotspot",
				Priority:            "high",
				Title:               "Check table hotspot",
				Summary:             "orders.refunds had the largest row change; review the table owner, recent jobs, and whether this table-specific write movement is expected.",
				Rationale:           "A table-level delta is large enough to be an operator follow-up target.",
				RelatedFindingKinds: []string{"table_driver"},
				EvidenceRefs: []EvidenceRef{
					{Section: "table_changes", Key: "orders.refunds", Label: "orders.refunds", Anchor: "section-table-changes"},
				},
			},
		},
		TableChanges: []TableChange{
			{Schema: "orders", Table: "refunds", CurrentRows: 900, BaselineRows: 0, DeltaRows: 900, DeltaPercent: 0},
			{Schema: "orders", Table: "chargebacks", CurrentRows: 0, BaselineRows: 400, DeltaRows: -400, DeltaPercent: -100},
			{Schema: "orders", Table: "payments", CurrentRows: 1200, BaselineRows: 800, DeltaRows: 400, DeltaPercent: 50},
		},
		PatternChanges: []PatternChange{},
		OperationMix: []OperationDelta{
			{Operation: "INSERT", Current: 1000, Baseline: 600, Delta: 400},
			{Operation: "UPDATE", Current: 900, Baseline: 500, Delta: 400},
			{Operation: "DELETE", Current: 200, Baseline: 100, Delta: 100},
		},
		AlertChanges: AlertDelta{
			Added: []InputAlert{
				{
					Type:     "large_transaction",
					Severity: "warning",
					Message:  "large transaction detected",
					TxnKey:   "trx-current-1",
					Details: map[string]any{
						"rows": float64(1500),
					},
				},
				{
					Type:     "spike",
					Severity: "warning",
					Message:  "rows spiked at 10:12",
					Minute:   "2026-03-20T10:12:00Z",
					Details: map[string]any{
						"rows": float64(900),
					},
				},
			},
			Removed: []InputAlert{
				{
					Type:     "spike",
					Severity: "warning",
					Message:  "rows spiked at 10:05",
					Minute:   "2026-03-13T10:05:00Z",
					Details: map[string]any{
						"rows": float64(600),
					},
				},
			},
		},
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
	}

	if !reflect.DeepEqual(decoded, expected) {
		t.Fatalf("unexpected compare contract:\n got: %#v\nwant: %#v", decoded, expected)
	}
}

func TestRenderJSONIncludesSnapshotIdentityWhenPresent(t *testing.T) {
	output, err := RenderJSON(CompareResult{
		CurrentLabel:  "Current Snapshot (current-snap)",
		BaselineLabel: "Baseline Snapshot (baseline-snap)",
		CurrentSnapshot: &InputSnapshot{
			Name:  "current-snap",
			Label: "Current Snapshot",
		},
		BaselineSnapshot: &InputSnapshot{
			Name:  "baseline-snap",
			Label: "Baseline Snapshot",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		`"current_label": "Current Snapshot (current-snap)"`,
		`"baseline_label": "Baseline Snapshot (baseline-snap)"`,
		`"current_snapshot": {`,
		`"baseline_snapshot": {`,
		`"name": "current-snap"`,
		`"name": "baseline-snap"`,
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected json output to contain %q, got %s", token, output)
		}
	}
}

func TestRenderJSONKeyFindingsShape(t *testing.T) {
	// key_findings is always present, evidence_refs omitted when empty
	result := CompareResult{
		Summary: SummaryDelta{
			CurrentTotalRows: 10, BaselineTotalRows: 5, TotalRowsDelta: 5,
			CurrentTotalTransactions: 4, BaselineTotalTransactions: 2, TotalTransactionsDelta: 2,
		},
		KeyFindings: []CompareFinding{
			{Kind: "volume_change", Title: "Volume", Summary: "grew", Evidence: map[string]any{"delta_rows": 5}},
			{
				Kind: "table_driver", Title: "Table", Summary: "drove",
				Evidence: map[string]any{"table": "s.t"},
				EvidenceRefs: []EvidenceRef{{Section: "table_changes", Key: "s.t", Label: "s.t", Anchor: "section-table-changes"}},
			},
		},
	}

	output, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Top-level key_findings must be present
	if !strings.Contains(output, `"key_findings"`) {
		t.Fatalf("expected key_findings in JSON output")
	}

	// Parse to verify evidence_refs behavior
	var decoded CompareResult
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(decoded.KeyFindings) != 2 {
		t.Fatalf("expected 2 findings, got %d", len(decoded.KeyFindings))
	}

	// volume_change: no evidence_refs → omitempty → nil in decoded struct
	if decoded.KeyFindings[0].EvidenceRefs != nil {
		t.Fatalf("expected nil evidence_refs for volume_change, got %v", decoded.KeyFindings[0].EvidenceRefs)
	}
	// Raw JSON should not contain "evidence_refs" for the first finding
	var raw []map[string]any
	if err := json.Unmarshal([]byte(output), &struct {
		KF *[]map[string]any `json:"key_findings"`
	}{KF: &raw}); err != nil {
		t.Fatalf("unmarshal raw: %v", err)
	}
	if _, ok := raw[0]["evidence_refs"]; ok {
		t.Fatalf("expected evidence_refs omitted for finding without refs, got %v", raw[0]["evidence_refs"])
	}

	// table_driver: has evidence_refs → must be present
	if _, ok := raw[1]["evidence_refs"]; !ok {
		t.Fatalf("expected evidence_refs present for finding with refs")
	}
	if len(decoded.KeyFindings[1].EvidenceRefs) != 1 {
		t.Fatalf("expected 1 evidence_ref, got %d", len(decoded.KeyFindings[1].EvidenceRefs))
	}
}

func TestRenderJSONIncludesPatternChanges(t *testing.T) {
	current, err := LoadReport(filepath.Join("testdata", "current_patterns.json"))
	if err != nil {
		t.Fatalf("load current: %v", err)
	}
	baseline, err := LoadReport(filepath.Join("testdata", "baseline_patterns.json"))
	if err != nil {
		t.Fatalf("load baseline: %v", err)
	}

	output, err := RenderJSON(BuildCompareResult(current, baseline))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	raw, ok := decoded["pattern_changes"].([]any)
	if !ok {
		t.Fatalf("expected pattern_changes array, got %#v", decoded["pattern_changes"])
	}
	if len(raw) != 3 {
		t.Fatalf("expected 3 pattern changes, got %d", len(raw))
	}
}
