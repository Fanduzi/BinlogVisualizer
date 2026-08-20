package compare

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDeltaPercentTreatsZeroBaselineGrowthAsNew(t *testing.T) {
	if got := deltaPercent(282348, 0); got != nil {
		t.Fatalf("0 -> 282348 should be undefined/new, got %v", *got)
	}
	if got := deltaPercent(0, 0); got == nil || *got != 0 {
		t.Fatalf("0 -> 0 should stay 0, got %v", got)
	}
	got := deltaPercent(0, 58493)
	if got == nil || *got != -100 {
		t.Fatalf("58493 -> 0 should stay -100, got %v", got)
	}
	got = deltaPercent(2400000, 40000)
	if got == nil || *got != 5900 {
		t.Fatalf("40000 -> 2400000 should be 5900, got %v", got)
	}
}

func TestBuildCompareResultOmitsPercentForNewWriteShape(t *testing.T) {
	result := BuildCompareResult(
		InputReport{
			Summary: InputSummary{TotalRows: 282348, TotalTransactions: 605, TotalEvents: 700},
			Tables: []InputTable{{
				Schema: "dogfood_big", Table: "t", TotalRows: 282348, UpdateRows: 282348, TxnCount: 605,
			}},
			Alerts: []InputAlert{},
		},
		InputReport{
			Summary: InputSummary{TotalRows: 0, TotalTransactions: 0, TotalEvents: 0, Duration: "0s"},
			Tables:  []InputTable{},
			Alerts:  []InputAlert{},
		},
	)
	if len(result.TableChanges) != 1 {
		t.Fatalf("expected 1 table change, got %+v", result.TableChanges)
	}
	change := result.TableChanges[0]
	if change.BaselineRows != 0 || change.CurrentRows != 282348 {
		t.Fatalf("unexpected rows: %+v", change)
	}
	if change.DeltaPercent != nil {
		t.Fatalf("expected null/new percent for 0 -> 282348, got %v", *change.DeltaPercent)
	}

	encoded, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("render json: %v", err)
	}
	if !strings.Contains(encoded, `"delta_percent": null`) {
		t.Fatalf("expected JSON delta_percent null, got:\n%s", encoded)
	}
	var decoded CompareResult
	if err := json.Unmarshal([]byte(encoded), &decoded); err != nil {
		t.Fatalf("unmarshal compare json: %v", err)
	}
}

func TestRenderTextShowsNewForZeroBaselineGrowth(t *testing.T) {
	output, err := RenderText(CompareResult{
		CurrentLabel:  "tonight",
		BaselineLabel: "last_week",
		TableChanges: []TableChange{{
			Schema: "dogfood_big", Table: "t", BaselineRows: 0, CurrentRows: 282348, DeltaRows: 282348,
		}},
		PatternChanges: []PatternChange{{
			Label: "dogfood_big.t / UPDATE / large batch", BaselineRows: 0, CurrentRows: 282348, DeltaRows: 282348, CurrentTxnCount: 605,
		}},
	})
	if err != nil {
		t.Fatalf("render text: %v", err)
	}
	if !strings.Contains(output, "0 -> 282348 (+282348, new)") {
		t.Fatalf("expected table line to say new, got:\n%s", output)
	}
	if !strings.Contains(output, "0 -> 282348 (+282348, new), txns 0 -> 605") {
		t.Fatalf("expected pattern line to say new, got:\n%s", output)
	}
	if strings.Contains(output, "0.0%") {
		t.Fatalf("new write shape must not render as 0.0%%:\n%s", output)
	}
}

func TestFormatDeltaPercentKeepsMinus100(t *testing.T) {
	pct := percentValue(-100)
	if got := formatDeltaPercent(0, 400, pct); got != "-100.0%" {
		t.Fatalf("got %q", got)
	}
	if got := formatDeltaPercent(282348, 0, nil); got != "new" {
		t.Fatalf("got %q", got)
	}
}
