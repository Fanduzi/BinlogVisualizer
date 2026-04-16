// Package compare verifies HTML compare output sections and localized context.
// input: fixture-backed compare reports and built CompareResult values.
// output: assertions for compare HTML sections, labels, and regression-sensitive content.
// pos: renderer regression coverage for the compare HTML output path.
// note: if this file changes, keep internal/compare/README.md synchronized.
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
		"window.compareKeyFindings =",
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

func TestRenderHTMLIncludesPatternChangeSection(t *testing.T) {
	current, err := LoadReport(filepath.Join("testdata", "current_patterns.json"))
	if err != nil {
		t.Fatalf("load current report: %v", err)
	}
	baseline, err := LoadReport(filepath.Join("testdata", "baseline_patterns.json"))
	if err != nil {
		t.Fatalf("load baseline report: %v", err)
	}

	output, err := RenderHTML(BuildCompareResult(current, baseline))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"Pattern Changes",
		`id="compare-pattern-changes"`,
		"window.comparePatternChanges =",
		"payments.update_status",
		"refunds.create",
		"chargebacks.create",
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected html to contain %q", token)
		}
	}
}

func TestRenderHTMLPatternChangesUsesTallStackedLayout(t *testing.T) {
	current, err := LoadReport(filepath.Join("testdata", "current_patterns.json"))
	if err != nil {
		t.Fatalf("load current report: %v", err)
	}
	baseline, err := LoadReport(filepath.Join("testdata", "baseline_patterns.json"))
	if err != nil {
		t.Fatalf("load baseline report: %v", err)
	}

	output, err := RenderHTML(BuildCompareResult(current, baseline))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		".chart-box-tall { height: 420px; }",
		`<div class="section-body pattern-stack">`,
		`id="compare-pattern-changes" class="chart-box chart-box-tall"`,
		"legend: { data: ['Baseline', 'Current'], bottom: 0",
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected compare pattern layout token %q", token)
		}
	}
}

func TestRenderHTMLEscapesHostilePatternContent(t *testing.T) {
	result := CompareResult{
		PatternChanges: []PatternChange{{
			PatternKey:         `p<script>alert("key")</script>`,
			Label:              `label<img src=x onerror=alert("label")>`,
			CurrentRows:        10,
			BaselineRows:       5,
			DeltaRows:          5,
			CurrentTxnCount:    4,
			BaselineTxnCount:   2,
			DeltaTxnCount:      2,
			SampleQuerySummary: `</script><script>alert("query")</script>`,
		}},
	}

	output, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, forbidden := range []string{
		`<script>alert("key")</script>`,
		`<img src=x onerror=alert("label")>`,
		`</script><script>alert("query")</script>`,
	} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("expected hostile pattern content to be escaped, found %q", forbidden)
		}
	}
}

func TestRenderHTMLContainsCompareEvidenceRefAnchors(t *testing.T) {
	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary: SummaryDelta{
			CurrentTotalRows: 10, BaselineTotalRows: 5, TotalRowsDelta: 5,
			CurrentTotalTransactions: 4, BaselineTotalTransactions: 2, TotalTransactionsDelta: 2,
		},
		KeyFindings: []CompareFinding{
			{
				Kind:    "pattern_driver",
				Title:   "Top pattern driver",
				Summary: "orders.insert_batch drove most row growth",
				EvidenceRefs: []EvidenceRef{{
					Section: "pattern_changes",
					Key:     "orders.insert_batch",
					Label:   "orders.insert_batch",
					Anchor:  "section-pattern-changes",
				}},
			},
			{
				Kind:    "table_driver",
				Title:   "Top table driver",
				Summary: "shop.orders had the largest row change",
				EvidenceRefs: []EvidenceRef{{
					Section: "table_changes",
					Key:     "shop.orders",
					Label:   "shop.orders",
					Anchor:  "section-table-changes",
				}},
			},
			{
				Kind:    "operation_mix_drift",
				Title:   "Operation mix drift",
				Summary: "INSERT share increased significantly",
				EvidenceRefs: []EvidenceRef{{
					Section: "operation_mix",
					Label:   "Operation Mix",
					Anchor:  "section-operation-mix",
				}},
			},
		},
		PatternChanges: []PatternChange{{PatternKey: "orders.insert_batch", Label: "orders.insert_batch"}},
		TableChanges:   []TableChange{{Schema: "shop", Table: "orders", DeltaRows: 1000, CurrentRows: 1000, BaselineRows: 0, DeltaPercent: 0}},
		OperationMix:   []OperationDelta{{Operation: "INSERT", Current: 1000, Baseline: 100, Delta: 900}},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML returned error: %v", err)
	}

	for _, anchor := range []string{"section-pattern-changes", "section-table-changes", "section-operation-mix"} {
		if !strings.Contains(html, `id="`+anchor+`"`) {
			t.Fatalf("html missing anchor %q", anchor)
		}
	}
}

func TestRenderHTMLLabelsComparePatternDrilldowns(t *testing.T) {
	result := CompareResult{
		PatternChanges: []PatternChange{{
			PatternKey:       "orders.insert_batch",
			Label:            "orders.insert_batch",
			CurrentRows:      900,
			BaselineRows:     100,
			DeltaRows:        800,
			CurrentTxnCount:  90,
			BaselineTxnCount: 10,
			DeltaTxnCount:    80,
		}},
		PatternDrilldowns: []PatternDrilldown{{
			PatternKey:  "orders.insert_batch",
			Label:       "orders.insert_batch",
			WhySelected: "dominant share of cross-window row movement",
			KeyPoints:   []CompareKeyPoint{{Label: "baseline context", Summary: "rows 100→900 (+800), txns 10→90 (+80)"}},
		}},
	}

	output, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{
		`id="compare-pattern-drilldowns"`,
		`title.className = 'drilldown-label'`,
		"orders.insert_batch",
		"dominant share of cross-window row movement",
		"baseline context",
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected html to contain %q", token)
		}
	}
}

func TestRenderHTMLCompareFindingsEscapeMaliciousContent(t *testing.T) {
	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary: SummaryDelta{
			CurrentTotalRows: 10, BaselineTotalRows: 5, TotalRowsDelta: 5,
			CurrentTotalTransactions: 4, BaselineTotalTransactions: 2, TotalTransactionsDelta: 2,
		},
		KeyFindings: []CompareFinding{{
			Kind:    "pattern_driver",
			Title:   `<img src=x onerror=alert(1)>`,
			Summary: `<script>alert(1)</script>`,
			EvidenceRefs: []EvidenceRef{{
				Section: "pattern_changes",
				Key:     "evil",
				Label:   `<img src=x onerror=alert(1)>`,
				Anchor:  "section-pattern-changes",
			}},
		}},
		PatternChanges: []PatternChange{{PatternKey: "evil", Label: `<img src=x onerror=alert(1)>`}},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML returned error: %v", err)
	}
	if strings.Contains(html, "findingsEl.innerHTML") {
		t.Fatalf("findings renderer should not use innerHTML")
	}
	if !strings.Contains(html, "textContent") {
		t.Fatalf("findings renderer should use textContent")
	}
	if strings.Contains(html, `<script>alert(1)</script>`) {
		t.Fatalf("html contains raw script tag from finding summary")
	}
	if strings.Contains(html, `<img src=x onerror=alert(1)>`) {
		t.Fatalf("html contains raw img injection from finding title or label")
	}
}

func TestRenderHTMLUsesDOMAPINotInnerHTMLForFindings(t *testing.T) {
	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary: SummaryDelta{
			CurrentTotalRows: 10, BaselineTotalRows: 5, TotalRowsDelta: 5,
			CurrentTotalTransactions: 4, BaselineTotalTransactions: 2, TotalTransactionsDelta: 2,
		},
		KeyFindings: []CompareFinding{
			{
				Kind:    "table_driver",
				Summary: `rows grew<script>alert("xss")</script>`,
				Evidence: map[string]any{
					"table": `evil<img src=x onerror=alert(1)>`,
				},
				EvidenceRefs: []EvidenceRef{{
					Section: "table_changes",
					Key:     `evil<img src=x onerror=alert(1)>`,
					Label:   `evil<img src=x onerror=alert(1)>`,
					Anchor:  "section-table-changes",
				}},
			},
		},
		TableChanges: []TableChange{{
			Schema: "evil", Table: `table<img src=x onerror=alert(1)>`,
			CurrentRows: 10, BaselineRows: 5, DeltaRows: 5, DeltaPercent: 100,
		}},
		OperationMix: []OperationDelta{},
		AlertChanges: AlertDelta{},
	}

	output, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The JS must use DOM APIs (textContent, createElement) not innerHTML
	for _, forbidden := range []string{
		`<script>alert("xss")</script>`,
		`<img src=x onerror=alert(1)>`,
		`findingsEl.innerHTML`,
	} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("expected hostile content to be escaped / no innerHTML usage, found %q", forbidden)
		}
	}

	// Verify the findings rendering uses createElement + textContent
	if !strings.Contains(output, "document.createElement") {
		t.Fatalf("expected findings JS to use document.createElement")
	}
	if !strings.Contains(output, ".textContent") {
		t.Fatalf("expected findings JS to use textContent")
	}
}

func TestRenderHTMLIncludesRecommendationSection(t *testing.T) {
	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary: SummaryDelta{
			CurrentTotalRows: 10, BaselineTotalRows: 5, TotalRowsDelta: 5,
			CurrentTotalTransactions: 4, BaselineTotalTransactions: 2, TotalTransactionsDelta: 2,
		},
		KeyFindings: []CompareFinding{{
			Kind:    "pattern_driver",
			Summary: "orders.insert_batch drove most row growth",
			Evidence: map[string]any{
				"pattern_key":          "orders.insert_batch",
				"delta_rows":           5,
				"share_of_total_delta": 0.80,
			},
		}},
		Recommendations: []Recommendation{{
			Kind:                "check_pattern_driver",
			Priority:            "high",
			Title:               "Check pattern driver",
			Summary:             "orders.insert_batch drove most row growth; confirm whether a deploy changed.",
			Rationale:           "A single write pattern explains a significant share of the row delta.",
			RelatedFindingKinds: []string{"pattern_driver"},
			EvidenceRefs: []EvidenceRef{{
				Section: "pattern_changes",
				Key:     "orders.insert_batch",
				Label:   "orders.insert_batch",
				Anchor:  "section-pattern-changes",
			}},
		}},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML returned error: %v", err)
	}

	for _, token := range []string{
		`id="compare-recommendations"`,
		"Recommended Next Checks",
		"window.compareRecommendations =",
	} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected html to contain %q", token)
		}
	}
}

func TestRenderHTMLUsesDOMAPINotInnerHTMLForRecommendations(t *testing.T) {
	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary: SummaryDelta{
			CurrentTotalRows: 10, BaselineTotalRows: 5, TotalRowsDelta: 5,
		},
		Recommendations: []Recommendation{{
			Kind:     "check_pattern_driver",
			Priority: "high",
			Title:    `<img src=x onerror=alert(1)>`,
			Summary:  `<script>alert(1)</script>`,
			EvidenceRefs: []EvidenceRef{{
				Label:  `<img src=x onerror=alert(1)>`,
				Anchor: "section-pattern-changes",
			}},
		}},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML returned error: %v", err)
	}
	if strings.Contains(html, "recommendationsEl.innerHTML") {
		t.Fatal("recommendation rendering should not use innerHTML")
	}
	if strings.Contains(html, `<script>alert(1)</script>`) {
		t.Fatal("html contains raw malicious recommendation content")
	}
	if strings.Contains(html, `<img src=x onerror=alert(1)>`) {
		t.Fatal("html contains raw malicious recommendation content")
	}
}

func TestRenderHTMLIncludesCompareDrilldownForSelectedPatterns(t *testing.T) {
	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary:       SummaryDelta{TotalRowsDelta: 1000},
		PatternChanges: []PatternChange{
			{PatternKey: "orders.insert", Label: "orders.insert", CurrentRows: 1200, BaselineRows: 200, DeltaRows: 1000, DeltaPercent: 500, CurrentTxnCount: 140, BaselineTxnCount: 20, DeltaTxnCount: 120},
		},
		PatternDrilldowns: []PatternDrilldown{
			{
				PatternKey:   "orders.insert",
				Label:        "orders.insert",
				WhySelected:  "dominant driver of the row delta between windows",
				BaselineRows: 200, CurrentRows: 1200, DeltaRows: 1000,
				BaselineTxns: 20, CurrentTxns: 140, DeltaTxns: 120,
				SignalFlags: CompareDrilldownSignals{DominantDelta: true},
				KeyPoints: []CompareKeyPoint{
					{Label: "baseline context", Summary: "rows 200 to 1200"},
				},
			},
		},
		OperationMix: []OperationDelta{},
		AlertChanges: AlertDelta{},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}

	if !strings.Contains(html, "window.comparePatternDrilldowns") {
		t.Fatal("expected pattern_drilldowns data in HTML")
	}
	if !strings.Contains(html, "drilldown-details") {
		t.Fatal("expected drilldown detail rendering in HTML")
	}
}

func TestRenderHTMLNoDrilldownSectionWhenEmpty(t *testing.T) {
	result := CompareResult{
		CurrentLabel:      "current",
		BaselineLabel:     "baseline",
		Summary:           SummaryDelta{TotalRowsDelta: 0},
		PatternDrilldowns: []PatternDrilldown{},
		OperationMix:      []OperationDelta{},
		AlertChanges:      AlertDelta{},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}

	// The JS should still include the data but render nothing visible
	if !strings.Contains(html, "window.comparePatternDrilldowns") {
		t.Fatal("expected pattern_drilldowns data to always be present")
	}
}

func TestRenderHTMLDrilldownBoundedKeyPoints(t *testing.T) {
	result := CompareResult{
		CurrentLabel:  "current",
		BaselineLabel: "baseline",
		Summary:       SummaryDelta{TotalRowsDelta: 2000},
		PatternChanges: []PatternChange{
			{PatternKey: "p1", Label: "p1", CurrentRows: 2200, BaselineRows: 200, DeltaRows: 2000, DeltaPercent: 1000, CurrentTxnCount: 200, BaselineTxnCount: 20, DeltaTxnCount: 180},
		},
		PatternDrilldowns: []PatternDrilldown{
			{
				PatternKey:  "p1",
				WhySelected: "dominant driver",
				KeyPoints: []CompareKeyPoint{
					{Label: "point 1", Summary: "first"},
					{Label: "point 2", Summary: "second"},
				},
			},
		},
		OperationMix: []OperationDelta{},
		AlertChanges: AlertDelta{},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML: %v", err)
	}

	// Should have at most 2 key_points rendered
	// Count occurrences of "kp-label" to verify bounding
	count := strings.Count(html, "kp-label")
	if count > 2 {
		t.Fatalf("expected at most 2 key point labels, got %d", count)
	}
}
