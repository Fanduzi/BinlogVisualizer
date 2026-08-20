package compare

import (
	"strings"
	"testing"
)

func TestBuildCompareResultAlignsLargeTxnAlertsByContentNotTxnKey(t *testing.T) {
	baseline := InputReport{
		Summary: InputSummary{TotalRows: 252561, TotalTransactions: 40, TotalEvents: 1000},
		Diagnostics: InputDiagnostics{
			LargestTransactions: []InputTransaction{{
				TxnKey:          "txn-1",
				TotalRows:       180000,
				Tables:          map[string]int{"dogfood_week.orders": 180000},
				Operations:      map[string]int{"INSERT": 180000},
				BinlogFileStart: "mysql-bin.000010",
				PosStart:        4,
				PosEnd:          88000,
			}},
		},
		Alerts: []InputAlert{{
			Type:     "large_transaction",
			Severity: "warning",
			Message:  "txn-1 exceeds row threshold",
			TxnKey:   "txn-1",
			Details: map[string]any{
				"rows":   float64(180000),
				"tables": []any{"dogfood_week.orders"},
			},
		}},
	}
	current := InputReport{
		Summary: InputSummary{TotalRows: 2740885, TotalTransactions: 80, TotalEvents: 3000},
		Diagnostics: InputDiagnostics{
			LargestTransactions: []InputTransaction{{
				TxnKey:          "txn-1",
				TotalRows:       400000,
				Tables:          map[string]int{"dogfood_big.t": 400000},
				Operations:      map[string]int{"INSERT": 400000},
				BinlogFileStart: "mysql-bin.000044",
				PosStart:        256,
				PosEnd:          512000,
			}},
		},
		Alerts: []InputAlert{
			{
				Type:     "large_transaction",
				Severity: "warning",
				Message:  "txn-1 exceeds row threshold",
				TxnKey:   "txn-1",
				Details: map[string]any{
					"rows":   float64(400000),
					"tables": []any{"dogfood_big.t"},
				},
			},
			{
				Type:     "large_transaction",
				Severity: "warning",
				Message:  "txn-2 exceeds row threshold",
				TxnKey:   "txn-2",
				Details: map[string]any{
					"rows":   float64(50000),
					"tables": []any{"dogfood_big.t"},
				},
			},
		},
	}

	result := BuildCompareResult(current, baseline)

	if !result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.IdentityNew {
		t.Fatal("expected colliding txn-1 keys with different content to be marked NEW")
	}
	if result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.BaselineTable != "dogfood_week.orders" {
		t.Fatalf("baseline table = %q", result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.BaselineTable)
	}
	if result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.CurrentTable != "dogfood_big.t" {
		t.Fatalf("current table = %q", result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.CurrentTable)
	}
	if result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.BaselineRows != 180000 || result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.CurrentRows != 400000 {
		t.Fatalf("unexpected largest txn rows: %+v", result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta)
	}

	if len(result.AlertChanges.Added) != 2 {
		t.Fatalf("expected tonight large txns to be added, got %d: %+v", len(result.AlertChanges.Added), result.AlertChanges.Added)
	}
	if len(result.AlertChanges.Removed) != 1 {
		t.Fatalf("expected last_week large txn to be removed, got %d: %+v", len(result.AlertChanges.Removed), result.AlertChanges.Removed)
	}

	foundLargest := false
	for _, alert := range result.AlertChanges.Added {
		if alert.TxnKey == "txn-1" {
			foundLargest = true
			break
		}
	}
	if !foundLargest {
		t.Fatalf("expected tonight txn-1 large_transaction in Added Alerts, got %+v", result.AlertChanges.Added)
	}
}

func TestBuildCompareResultSameContentLargeTxnIsNotNew(t *testing.T) {
	txn := InputTransaction{
		TxnKey:          "txn-1",
		TotalRows:       180000,
		Tables:          map[string]int{"shop.orders": 180000},
		Operations:      map[string]int{"INSERT": 180000},
		BinlogFileStart: "mysql-bin.000010",
		PosStart:        4,
		PosEnd:          88000,
	}
	alert := InputAlert{
		Type:     "large_transaction",
		Severity: "warning",
		Message:  "txn-1 exceeds row threshold",
		TxnKey:   "txn-1",
		Details:  map[string]any{"rows": float64(180000), "tables": []any{"shop.orders"}},
	}
	baseline := InputReport{
		Summary:     InputSummary{TotalRows: 180000, TotalTransactions: 1, TotalEvents: 10},
		Diagnostics: InputDiagnostics{LargestTransactions: []InputTransaction{txn}},
		Alerts:      []InputAlert{alert},
	}
	currentTxn := txn
	currentTxn.TxnKey = "txn-9"
	currentAlert := alert
	currentAlert.TxnKey = "txn-9"
	currentAlert.Message = "txn-9 exceeds row threshold"
	current := InputReport{
		Summary:     InputSummary{TotalRows: 180000, TotalTransactions: 1, TotalEvents: 10},
		Diagnostics: InputDiagnostics{LargestTransactions: []InputTransaction{currentTxn}},
		Alerts:      []InputAlert{currentAlert},
	}

	result := BuildCompareResult(current, baseline)
	if result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta.IdentityNew {
		t.Fatal("same table/op/rows/file+span should not be marked NEW")
	}
	if len(result.AlertChanges.Added) != 0 || len(result.AlertChanges.Removed) != 0 {
		t.Fatalf("same content large_transaction should not be added/removed, got added=%+v removed=%+v", result.AlertChanges.Added, result.AlertChanges.Removed)
	}
}

func TestRenderTextIncludesLargestTxnAndMarksNew(t *testing.T) {
	output, err := RenderText(CompareResult{
		CurrentLabel:  "tonight",
		BaselineLabel: "last_week",
		DiagnosticsDelta: DiagnosticsDelta{
			TxnDiagnostics: TxnDiagnosticDelta{
				LargestTxnDelta: TxnSizeCompare{
					BaselineRows:  180000,
					CurrentRows:   400000,
					DeltaRows:     220000,
					BaselineTable: "dogfood_week.orders",
					CurrentTable:  "dogfood_big.t",
					BaselineOp:    "INSERT",
					CurrentOp:     "INSERT",
					IdentityNew:   true,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("render text: %v", err)
	}
	want := "Largest txn: dogfood_week.orders INSERT 180000 -> dogfood_big.t INSERT 400000 NEW"
	if !strings.Contains(output, want) {
		t.Fatalf("expected %q, got:\n%s", want, output)
	}
}
