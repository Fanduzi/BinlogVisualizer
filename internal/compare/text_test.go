package compare

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderTextIncludesSummaryAndTopTables(t *testing.T) {
	current, _ := LoadReport(filepath.Join("testdata", "current.json"))
	baseline, _ := LoadReport(filepath.Join("testdata", "baseline.json"))

	output, err := RenderText(BuildCompareResult(current, baseline))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"Compare Summary",
		"Current Label: current",
		"Baseline Label: baseline",
		"Rows: 1500 -> 2400 (+900)",
		"Transactions: 90 -> 120 (+30)",
		"Top Table Changes",
		"orders.refunds",
		"Operation Mix",
		"INSERT: 600 -> 1000 (+400)",
		"Alert Changes",
		"Added Alerts (2)",
		"Removed Alerts (1)",
		"large transaction detected",
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected output to contain %q, got %s", token, output)
		}
	}
}
