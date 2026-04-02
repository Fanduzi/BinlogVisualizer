package compare

import (
	"path/filepath"
	"strings"
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

func TestRenderTextIncludesSnapshotIdentityWhenPresent(t *testing.T) {
	output, err := RenderText(CompareResult{
		Summary:       SummaryDelta{},
		CurrentLabel:  "Current Snapshot (current-snap)",
		BaselineLabel: "Baseline Snapshot (baseline-snap)",
		CurrentSnapshot: &InputSnapshot{
			InputMode: "files",
			Input: InputSnapshotInput{
				Files: []string{"mysql-bin.000123", "mysql-bin.000124"},
			},
			Filters: InputSnapshotFilters{
				IncludeSchemas: []string{"shop"},
			},
			Window: InputSnapshotWindow{
				StartTime: "2026-03-20T10:00:00Z",
				EndTime:   "2026-03-20T10:30:00Z",
			},
		},
		BaselineSnapshot: &InputSnapshot{
			InputMode: "discovery",
			Input: InputSnapshotInput{
				FromDir: "/var/lib/mysql",
				Prefix:  "mysql-bin.",
			},
			Filters: InputSnapshotFilters{
				ExcludeSchemas: []string{"mysql"},
			},
			Window: InputSnapshotWindow{
				StartTime: "2026-03-13T10:00:00Z",
				EndTime:   "2026-03-13T10:30:00Z",
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"Current Label: Current Snapshot (current-snap)",
		"Baseline Label: Baseline Snapshot (baseline-snap)",
		"Current Window: 2026-03-20T10:00:00Z -> 2026-03-20T10:30:00Z",
		"Baseline Window: 2026-03-13T10:00:00Z -> 2026-03-13T10:30:00Z",
		"Current Input Mode: files",
		"Baseline Input Mode: discovery",
		"Current Source: files=2",
		"Baseline Source: from_dir=/var/lib/mysql prefix=mysql-bin.",
		"Current Filters: include_schema=shop",
		"Baseline Filters: exclude_schema=mysql",
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected text output to contain %q, got %s", token, output)
		}
	}
}
