// Package compare verifies human-readable compare rendering and byte coverage labels.
// input: fixture-backed compare reports and built CompareResult values.
// output: regression coverage for compare text sections, byte metrics, and snapshot context.
// pos: renderer regression suite for the compare text output path.
// note: if this file changes, update this header and internal/compare/README.md.
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
Input File Size: baseline=N/A -> current=N/A
Counted Event Bytes: baseline=0 B -> current=0 B

Key Findings
1. [volume_change] rows grew sharply
2. [table_driver] orders.refunds had the largest row change
   evidence: orders.refunds

Recommended Next Checks
1. [high] Check table hotspot
   orders.refunds had the largest row change; review the table owner, recent jobs, and whether this table-specific write movement is expected.
   evidence: orders.refunds

Top Table Changes
- orders.refunds: 0 -> 900 (+900, new)
- orders.chargebacks: 400 -> 0 (-400, -100.0%)
- orders.payments: 800 -> 1200 (+400, 50.0%)

Top Pattern Changes
- none

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

func TestRenderTextIncludesInputAndCountedBytes(t *testing.T) {
	result := BuildCompareResult(
		InputReport{Diagnostics: InputDiagnostics{
			FileCoverage:      InputFileCoverage{Selected: []InputFileCoverageItem{{Size: 3000}}},
			CountedEventBytes: 250,
		}},
		InputReport{Diagnostics: InputDiagnostics{
			FileCoverage:      InputFileCoverage{Selected: []InputFileCoverageItem{{Size: 6000}}},
			CountedEventBytes: 500,
		}},
	)

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{
		"Input File Size: baseline=5.9 KB -> current=2.9 KB",
		"Counted Event Bytes: baseline=500 B -> current=250 B",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected compare text to contain %q, got:\n%s", want, out)
		}
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

func TestRenderTextIncludesPatternChangesSection(t *testing.T) {
	current, err := LoadReport(filepath.Join("testdata", "current_patterns.json"))
	if err != nil {
		t.Fatalf("load current report: %v", err)
	}
	baseline, err := LoadReport(filepath.Join("testdata", "baseline_patterns.json"))
	if err != nil {
		t.Fatalf("load baseline report: %v", err)
	}

	output, err := RenderText(BuildCompareResult(current, baseline))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"Top Pattern Changes",
		"payments.update_status",
		"txns 9 -> 18 (+9)",
		"query: update payments set status = ?",
		"Recommended Next Checks",
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected text output to contain %q, got %s", token, output)
		}
	}
	if strings.Index(output, "Top Pattern Changes") < strings.Index(output, "Top Table Changes") {
		t.Fatalf("expected pattern section after table changes, got %s", output)
	}
}

func TestRenderTextRendersPatternEmptyState(t *testing.T) {
	output, err := RenderText(CompareResult{
		CurrentLabel:   "current",
		BaselineLabel:  "baseline",
		PatternChanges: []PatternChange{},
		TableChanges:   []TableChange{},
		OperationMix:   []OperationDelta{},
		AlertChanges:   AlertDelta{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(output, "Top Pattern Changes\n- none") {
		t.Fatalf("expected empty pattern section, got %s", output)
	}
}

func TestRenderTextNoDrilldownWhenEmpty(t *testing.T) {
	output, err := RenderText(CompareResult{
		CurrentLabel:    "current",
		BaselineLabel:   "baseline",
		PatternChanges:  []PatternChange{},
		PatternDrilldowns: []PatternDrilldown{},
		TableChanges:    []TableChange{},
		OperationMix:    []OperationDelta{},
		AlertChanges:    AlertDelta{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(output, "drilldown:") {
		t.Fatalf("expected no drilldown block for empty drilldowns, got:\n%s", output)
	}
}

func TestRenderTextDrilldownBeneathSelectedPattern(t *testing.T) {
	output, err := RenderText(CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary:       SummaryDelta{TotalRowsDelta: 1000},
		PatternChanges: []PatternChange{
			{PatternKey: "orders.insert", Label: "orders.insert", CurrentRows: 1200, BaselineRows: 200, DeltaRows: 1000, DeltaPercent: percentValue(500), CurrentTxnCount: 140, BaselineTxnCount: 20, DeltaTxnCount: 120},
		},
		PatternDrilldowns: []PatternDrilldown{
			{
				PatternKey:  "orders.insert",
				Label:       "orders.insert",
				WhySelected: "dominant driver of the row delta between windows",
				BaselineRows: 200,
				CurrentRows:  1200,
				DeltaRows:    1000,
				BaselineTxns: 20,
				CurrentTxns:  140,
				DeltaTxns:    120,
				SignalFlags:  CompareDrilldownSignals{DominantDelta: true},
				KeyPoints: []CompareKeyPoint{
					{Label: "baseline context", Summary: "rows 200→1200 (+1000), txns 20→140 (+120)"},
					{Label: "current context", Summary: "rows 200→1200 (+1000), txns 20→140 (+120)"},
				},
			},
		},
		TableChanges: []TableChange{},
		OperationMix: []OperationDelta{},
		AlertChanges: AlertDelta{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(output, "drilldown:") {
		t.Fatalf("expected drilldown block, got:\n%s", output)
	}
	if !strings.Contains(output, "why: dominant driver") {
		t.Fatalf("expected why_selected in drilldown, got:\n%s", output)
	}
	if !strings.Contains(output, "baseline context:") {
		t.Fatalf("expected baseline context key point, got:\n%s", output)
	}
	// Drilldown should appear after the pattern entry line
	idx := strings.Index(output, "orders.insert:")
	if idx < 0 {
		t.Fatal("expected orders.insert pattern entry")
	}
	didx := strings.Index(output, "drilldown:")
	if didx < idx {
		t.Fatal("drilldown should appear after the pattern entry")
	}
}

func TestRenderTextDrilldownUsesCompareScopedWording(t *testing.T) {
	output, err := RenderText(CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary:       SummaryDelta{TotalRowsDelta: 1000},
		PatternChanges: []PatternChange{
			{PatternKey: "p1", Label: "p1", CurrentRows: 1200, BaselineRows: 200, DeltaRows: 1000, DeltaPercent: percentValue(500), CurrentTxnCount: 140, BaselineTxnCount: 20, DeltaTxnCount: 120},
		},
		PatternDrilldowns: []PatternDrilldown{
			{
				PatternKey:  "p1",
				Label:       "p1",
				WhySelected: "dominant driver of the row delta between windows",
				KeyPoints: []CompareKeyPoint{
					{Label: "baseline context", Summary: "rows 200→1200 (+1000), txns 20→140 (+120)"},
				},
			},
		},
		TableChanges: []TableChange{},
		OperationMix: []OperationDelta{},
		AlertChanges: AlertDelta{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should reference baseline/current, not raw pattern ownership
	if !strings.Contains(output, "baseline context") || !strings.Contains(output, "current") {
		t.Fatalf("expected compare-scoped wording (baseline/current), got:\n%s", output)
	}
}
