package compare

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestRenderJSONProducesStableCompareContract(t *testing.T) {
	current, _ := LoadReport(filepath.Join("testdata", "current.json"))
	baseline, _ := LoadReport(filepath.Join("testdata", "baseline.json"))

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

	var decoded map[string]any
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatalf("expected valid json: %v", err)
	}
	if _, ok := decoded["summary"]; !ok {
		t.Fatal("expected summary field")
	}
	if _, ok := decoded["table_changes"]; !ok {
		t.Fatal("expected table_changes field")
	}
	if _, ok := decoded["operation_mix"]; !ok {
		t.Fatal("expected operation_mix field")
	}
	if _, ok := decoded["alert_changes"]; !ok {
		t.Fatal("expected alert_changes field")
	}
	if decoded["current_label"] != "current" {
		t.Fatalf("expected current_label=current, got %v", decoded["current_label"])
	}
	if decoded["baseline_label"] != "baseline" {
		t.Fatalf("expected baseline_label=baseline, got %v", decoded["baseline_label"])
	}
	alerts := decoded["alert_changes"].(map[string]any)
	if len(alerts["added"].([]any)) != 2 {
		t.Fatalf("expected 2 added alerts, got %d", len(alerts["added"].([]any)))
	}
}
