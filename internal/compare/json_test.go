package compare

import (
	"encoding/json"
	"path/filepath"
	"reflect"
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
		TableChanges: []TableChange{
			{Schema: "orders", Table: "refunds", CurrentRows: 900, BaselineRows: 0, DeltaRows: 900, DeltaPercent: 0},
			{Schema: "orders", Table: "chargebacks", CurrentRows: 0, BaselineRows: 400, DeltaRows: -400, DeltaPercent: -100},
			{Schema: "orders", Table: "payments", CurrentRows: 1200, BaselineRows: 800, DeltaRows: 400, DeltaPercent: 50},
		},
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
