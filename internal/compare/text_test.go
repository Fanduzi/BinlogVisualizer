package compare

import (
	"path/filepath"
	"testing"
)

func TestRenderTextIncludesSummaryAndTopTables(t *testing.T) {
	current, err := LoadReport(filepath.Join("testdata", "current.json"))
	if err != nil {
		t.Fatalf("load current report: %v", err)
	}
	baseline, err := LoadReport(filepath.Join("testdata", "baseline.json"))
	if err != nil {
		t.Fatalf("load baseline report: %v", err)
	}

	output, err := RenderText(BuildCompareResult(current, baseline))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := `Compare Summary
Current Label: current
Baseline Label: baseline
Rows: 1500 -> 2400 (+900)
Transactions: 90 -> 120 (+30)
Warnings: 0 -> 0 (+0)

Top Table Changes
- orders.refunds: 0 -> 900 (+900, 0.0%)
- orders.chargebacks: 400 -> 0 (-400, -100.0%)
- orders.payments: 800 -> 1200 (+400, 50.0%)

Operation Mix
- INSERT: 600 -> 1000 (+400)
- UPDATE: 500 -> 900 (+400)
- DELETE: 100 -> 200 (+100)

Alert Changes
Added Alerts (2)
- [LARGE_TRANSACTION] large transaction detected
- [SPIKE] rows spiked at 10:12
Removed Alerts (1)
- [SPIKE] rows spiked at 10:05
`

	if output != expected {
		t.Fatalf("unexpected text output:\n%s", output)
	}
}
