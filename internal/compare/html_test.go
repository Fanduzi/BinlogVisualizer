package compare

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderHTMLIncludesCompareSections(t *testing.T) {
	current, err := LoadReport(filepath.Join("testdata", "current.json"))
	if err != nil {
		t.Fatalf("load current report: %v", err)
	}
	baseline, err := LoadReport(filepath.Join("testdata", "baseline.json"))
	if err != nil {
		t.Fatalf("load baseline report: %v", err)
	}

	output, err := RenderHTML(BuildCompareResult(current, baseline))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"<html",
		"Compare Summary",
		"Top Table Changes",
		"Operation Mix",
		"Alert Changes",
		`id="compare-summary-chart"`,
		`id="compare-top-tables"`,
		`id="compare-ops-mix"`,
		`id="compare-alerts"`,
		"echarts.init(document.getElementById('compare-summary-chart'))",
		"window.compareSummaryPairs =",
		"window.compareTopTables =",
		"window.compareOpsMix =",
		"window.compareAlertCounts =",
		"orders.refunds",
		"large transaction detected",
		"Added Alerts (2)",
		"Removed Alerts (1)",
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected html to contain %q", token)
		}
	}

	if strings.Contains(output, `"label":"Alerts"`) {
		t.Fatalf("summary chart should not treat alerts as baseline/current pairs: %s", output)
	}
	for _, token := range []string{`"name":"Added Alerts","value":2`, `"name":"Removed Alerts","value":1`} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected alert chart contract token %q", token)
		}
	}
}

func TestRenderHTMLEscapesHostileCompareContent(t *testing.T) {
	result := CompareResult{
		Summary: SummaryDelta{
			CurrentTotalRows:          10,
			BaselineTotalRows:         5,
			TotalRowsDelta:            5,
			CurrentTotalTransactions:  4,
			BaselineTotalTransactions: 2,
			TotalTransactionsDelta:    2,
		},
		TableChanges: []TableChange{{
			Schema:       `orders<script>alert("schema")</script>`,
			Table:        `payments<img src=x onerror=alert("table")>`,
			CurrentRows:  10,
			BaselineRows: 5,
			DeltaRows:    5,
			DeltaPercent: 100,
		}},
		OperationMix: []OperationDelta{{Operation: "INSERT", Current: 10, Baseline: 5, Delta: 5}},
		AlertChanges: AlertDelta{
			Added: []InputAlert{{
				Type:    "spike",
				Message: `</script><script>alert("alert")</script>`,
			}},
		},
		CurrentLabel:  `current<script>alert("current")</script>`,
		BaselineLabel: `baseline<img src=x onerror=alert("baseline")>`,
	}

	output, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, forbidden := range []string{
		`<script>alert("schema")</script>`,
		`<img src=x onerror=alert("table")>`,
		`</script><script>alert("alert")</script>`,
	} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("expected hostile content to be escaped, found %q in output", forbidden)
		}
	}
	for _, expected := range []string{
		`orders&lt;script&gt;alert`,
		`payments&lt;img`,
		`current&lt;script&gt;alert`,
		`baseline&lt;img`,
		`&lt;/script&gt;&lt;script&gt;alert`,
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected escaped hostile token %q in output", expected)
		}
	}
}

func TestRenderHTMLIncludesSnapshotIdentityWhenPresent(t *testing.T) {
	output, err := RenderHTML(CompareResult{
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
		"Current Snapshot (current-snap) vs Baseline Snapshot (baseline-snap)",
		">Current Snapshot (current-snap)<",
		">Baseline Snapshot (baseline-snap)<",
		"2026-03-20T10:00:00Z -&gt; 2026-03-20T10:30:00Z",
		"2026-03-13T10:00:00Z -&gt; 2026-03-13T10:30:00Z",
		"Current Input Mode: files",
		"Baseline Input Mode: discovery",
		"Current Source: files=2",
		"Baseline Source: from_dir=/var/lib/mysql prefix=mysql-bin.",
		"Current Filters: include_schema=shop",
		"Baseline Filters: exclude_schema=mysql",
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected html output to contain %q", token)
		}
	}
}
