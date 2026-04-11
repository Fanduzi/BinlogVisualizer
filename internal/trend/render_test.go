package trend

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	comparepkg "binlogviz/internal/compare"
)

func TestRenderTextIncludesSectionsAndBaseline(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{Path: "/tmp/a.json", Report: testInputReport("a", "Alpha", "2026-03-20T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 1)},
			{Path: "/tmp/b.json", Report: testInputReport("b", "Beta", "2026-03-21T10:00:00Z", 2000, 90, 2400, 1200, 800, 400, 2)},
		},
		Baseline: &BuildInput{
			Path:   "/tmp/baseline.json",
			Report: testInputReport("baseline", "Baseline", "2026-03-19T10:00:00Z", 800, 40, 1000, 400, 300, 100, 0),
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	text, err := RenderText(result)
	if err != nil {
		t.Fatalf("render text: %v", err)
	}
	for _, token := range []string{"Trend Summary", "Baseline Snapshot: Baseline (baseline)", "Ordered Points", "Top Table Movement"} {
		if !strings.Contains(text, token) {
			t.Fatalf("expected text output to contain %q, got %q", token, text)
		}
	}
}

func TestRenderTextIncludesPatternTrends(t *testing.T) {
	result := mustBuildPatternTrendResult(t)

	text, err := RenderText(result)
	if err != nil {
		t.Fatalf("render text: %v", err)
	}
	for _, token := range []string{
		"Top Pattern Trends",
		"payments.update_status (orders.payments|UPDATE|medium)",
		"share=60.0% -> 43.3% (-16.7%)",
		"rows=600 -> 1300 (+700)",
	} {
		if !strings.Contains(text, token) {
			t.Fatalf("expected text output to contain %q, got %q", token, text)
		}
	}
}

func TestRenderTextShowsNoneForLegacyPatternTrends(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{
				Path:   "/tmp/snapshots/legacy-a.json",
				Report: testInputReport("legacy-a", "Legacy A", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0),
			},
			{
				Path:   "/tmp/snapshots/legacy-b.json",
				Report: testInputReport("legacy-b", "Legacy B", "2026-03-20T10:00:00Z", 1600, 80, 1800, 700, 500, 200, 1),
			},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	text, err := RenderText(result)
	if err != nil {
		t.Fatalf("render text: %v", err)
	}
	if !strings.Contains(text, "Top Pattern Trends") {
		t.Fatalf("expected pattern trend section, got %q", text)
	}
	parts := strings.Split(text, "\nTop Pattern Trends\n")
	if len(parts) != 2 {
		t.Fatalf("expected dedicated pattern trend section, got %q", text)
	}
	patternSection := parts[1]
	if !strings.HasPrefix(patternSection, "- none\n") {
		t.Fatalf("expected empty pattern trend fallback at section start, got %q", patternSection)
	}
}

func TestRenderJSONTrendSummaryShape(t *testing.T) {
	// trend_summary is always present, evidence_refs omitted when empty
	result := Result{
		TrendSummary: []TrendFinding{
			{Kind: "rising_pattern", Title: "Rising", Summary: "rose", Evidence: map[string]any{"pattern_key": "p0"}},
			{
				Kind: "table_trend", Title: "Table", Summary: "grew",
				Evidence: map[string]any{"table": "s.t"},
				EvidenceRefs: []EvidenceRef{{Section: "table_trends", Key: "s.t", Label: "s.t", Anchor: "table-0"}},
			},
		},
		PatternTrends: []PatternTrend{{PatternKey: "p0", Label: "p0"}},
		TableTrends:   []TableTrend{{Schema: "s", Table: "t", DeltaRows: 100}},
		Points:        []Point{{Snapshot: SnapshotMeta{Name: "w1"}}},
	}

	raw, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("render json: %v", err)
	}

	// Top-level trend_summary must be present
	if !strings.Contains(raw, `"trend_summary"`) {
		t.Fatalf("expected trend_summary in JSON output")
	}

	// Parse to verify evidence_refs behavior
	var payload struct {
		TrendSummary []map[string]any `json:"trend_summary"`
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(payload.TrendSummary) != 2 {
		t.Fatalf("expected 2 findings, got %d", len(payload.TrendSummary))
	}

	// rising_pattern: no evidence_refs → omitempty
	if _, ok := payload.TrendSummary[0]["evidence_refs"]; ok {
		t.Fatalf("expected evidence_refs omitted for finding without refs, got %v", payload.TrendSummary[0]["evidence_refs"])
	}

	// table_trend: has evidence_refs → must be present
	if _, ok := payload.TrendSummary[1]["evidence_refs"]; !ok {
		t.Fatalf("expected evidence_refs present for finding with refs")
	}
}

func TestRenderJSONIncludesPatternTrendSeries(t *testing.T) {
	result := mustBuildPatternTrendResult(t)

	raw, err := RenderJSON(result)
	if err != nil {
		t.Fatalf("render json: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("unmarshal json: %v", err)
	}

	patternTrends, ok := payload["pattern_trends"].([]any)
	if !ok {
		t.Fatalf("expected top-level pattern_trends array, got %#v", payload["pattern_trends"])
	}
	if len(patternTrends) != 1 {
		t.Fatalf("expected 1 pattern trend, got %#v", patternTrends)
	}

	trend, ok := patternTrends[0].(map[string]any)
	if !ok {
		t.Fatalf("expected pattern trend object, got %#v", patternTrends[0])
	}
	if _, ok := trend["rows_series"].([]any); !ok {
		t.Fatalf("expected rows_series array, got %#v", trend["rows_series"])
	}
	if _, ok := trend["share_of_rows_series"].([]any); !ok {
		t.Fatalf("expected share_of_rows_series array, got %#v", trend["share_of_rows_series"])
	}
}

func TestRenderHTMLIncludesTrendChartAnchors(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{Path: "/tmp/a.json", Report: testInputReport("a", "Alpha", "2026-03-20T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 1)},
			{Path: "/tmp/b.json", Report: testInputReport("b", "Beta", "2026-03-21T10:00:00Z", 2000, 90, 2400, 1200, 800, 400, 2)},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("render html: %v", err)
	}
	for _, token := range []string{`id="trend-overall-chart"`, `id="trend-ops-chart"`, `id="trend-tables-chart"`} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected html output to contain %q, got %q", token, html)
		}
	}
}

func TestRenderHTMLIncludesPatternTrendsSectionAndToggle(t *testing.T) {
	result := mustBuildPatternTrendResult(t)

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("render html: %v", err)
	}
	for _, token := range []string{
		"Pattern Trends",
		`id="trend-pattern-chart"`,
		`data-pattern-view="share"`,
		`data-pattern-view="rows"`,
		`aria-pressed="true">Share of Rows</button>`,
		`aria-pressed="false">Rows</button>`,
	} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected html output to contain %q, got %q", token, html)
		}
	}
	if !strings.Contains(html, "Default view shows share of rows") {
		t.Fatalf("expected html output to explain default view, got %q", html)
	}
}

func TestRenderHTMLEmptyPatternTrendsSectionShowsEmptyState(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{
				Path:   "/tmp/snapshots/legacy-a.json",
				Report: testInputReport("legacy-a", "Legacy A", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0),
			},
			{
				Path:   "/tmp/snapshots/legacy-b.json",
				Report: testInputReport("legacy-b", "Legacy B", "2026-03-20T10:00:00Z", 1600, 80, 1800, 700, 500, 200, 1),
			},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("render html: %v", err)
	}
	if !strings.Contains(html, "Pattern Trends") {
		t.Fatalf("expected pattern trend section, got %q", html)
	}
	if !strings.Contains(html, "No pattern trends available for the selected snapshots.") {
		t.Fatalf("expected empty state, got %q", html)
	}
	if strings.Contains(html, `id="trend-pattern-chart"`) {
		t.Fatalf("expected no chart container for empty pattern trends, got %q", html)
	}
}

func mustBuildPatternTrendResult(t *testing.T) Result {
	t.Helper()

	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{
				Path: "/tmp/snapshots/earlier.json",
				Report: testInputReportWithPatterns(
					"earlier",
					"Earlier",
					"2026-03-19T10:00:00Z",
					1000,
					50,
					1200,
					500,
					350,
					150,
					0,
					[]comparepkg.InputPattern{{
						PatternKey:  "orders.payments|UPDATE|medium",
						Label:       "payments.update_status",
						TotalRows:   600,
						TxnCount:    9,
						EventCount:  45,
						ShareOfRows: 0.999,
						ShareOfTxns: 0.1,
						Tables: map[string]int{
							"orders.payments": 600,
						},
						Operations: map[string]int{
							"UPDATE": 600,
						},
						AvgRowsPerTxn:      66.7,
						SampleQuerySummary: "update payments set status = ?",
					}},
				),
			},
			{
				Path: "/tmp/snapshots/later.json",
				Report: testInputReportWithPatterns(
					"later",
					"Later",
					"2026-03-21T10:00:00Z",
					3000,
					150,
					3600,
					1600,
					900,
					500,
					3,
					[]comparepkg.InputPattern{{
						PatternKey:  "orders.payments|UPDATE|medium",
						Label:       "payments.update_status",
						TotalRows:   1300,
						TxnCount:    18,
						EventCount:  90,
						ShareOfRows: 0.4333333333,
						ShareOfTxns: 0.12,
						Tables: map[string]int{
							"orders.payments": 1300,
						},
						Operations: map[string]int{
							"UPDATE": 1300,
						},
						AvgRowsPerTxn:      72.2,
						SampleQuerySummary: "update payments set status = ?",
					}},
				),
			},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	return result
}

func TestRenderHTMLContainsAllTrendEvidenceRefAnchors(t *testing.T) {
	result := Result{
		TrendSummary: []TrendFinding{
			{Kind: "rising_pattern", Title: "Rising", Summary: "pattern rose", EvidenceRefs: []EvidenceRef{{Section: "pattern_trends", Key: "p0", Label: "p0", Anchor: "pattern-0"}}},
			{Kind: "table_trend", Title: "Table", Summary: "table grew", EvidenceRefs: []EvidenceRef{{Section: "table_trends", Key: "shop.orders", Label: "shop.orders", Anchor: "table-0"}}},
			{Kind: "spike_outlier", Title: "Spike", Summary: "week3 spiked", EvidenceRefs: []EvidenceRef{{Section: "ordered_points", Key: "week3", Label: "week3", Anchor: "point-2"}}},
		},
		PatternTrends: []PatternTrend{{PatternKey: "p0", Label: "p0"}},
		TableTrends:   []TableTrend{{Schema: "shop", Table: "orders", DeltaRows: 1000}},
		Points: []Point{
			{Snapshot: SnapshotMeta{Name: "week1"}},
			{Snapshot: SnapshotMeta{Name: "week2"}},
			{Snapshot: SnapshotMeta{Name: "week3"}},
		},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML returned error: %v", err)
	}
	for _, anchor := range []string{"pattern-0", "table-0", "point-2"} {
		if !strings.Contains(html, `id="`+anchor+`"`) {
			t.Fatalf("html missing anchor %q", anchor)
		}
	}
}

func TestRenderHTMLTrendFindingsEscapeMaliciousContent(t *testing.T) {
	result := Result{
		TrendSummary: []TrendFinding{{
			Kind:    "rising_pattern",
			Title:   `<img src=x onerror=alert(1)>`,
			Summary: `<script>alert(1)</script>`,
			EvidenceRefs: []EvidenceRef{{
				Section: "pattern_trends",
				Key:     "evil",
				Label:   `<img src=x onerror=alert(1)>`,
				Anchor:  "pattern-0",
			}},
		}},
		PatternTrends: []PatternTrend{{PatternKey: "evil", Label: `<img src=x onerror=alert(1)>`}},
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML returned error: %v", err)
	}
	if strings.Contains(html, "trendFindingsEl.innerHTML") {
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

func TestRenderHTMLContainsEvidenceRefAnchors(t *testing.T) {
	result := mustBuildPatternTrendResult(t)

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("render html: %v", err)
	}

	// Collect all evidence ref anchors from the result
	for _, finding := range result.TrendSummary {
		for _, ref := range finding.EvidenceRefs {
			anchor := `id="` + ref.Anchor + `"`
			if !strings.Contains(html, anchor) {
				t.Fatalf("evidence ref anchor %q not found in HTML output for finding %q", ref.Anchor, finding.Kind)
			}
		}
	}

	// Also verify section-level anchors exist
	for _, id := range []string{`id="section-pattern-trends"`, `id="section-table-trends"`, `id="section-ordered-points"`} {
		if !strings.Contains(html, id) {
			t.Fatalf("expected html output to contain %q", id)
		}
	}
}

func TestRenderHTMLAnchorsAreUniqueAcrossManyItems(t *testing.T) {
	// Build a result with 7 patterns to verify items beyond index 5 get unique anchors.
	reportWith7Patterns := func(name, label, start string, rows, txns, events, inserts, updates, deletes, alerts int) InputReport {
		patterns := make([]comparepkg.InputPattern, 7)
		for i := range patterns {
			key := fmt.Sprintf("pat%02d", i)
			patterns[i] = comparepkg.InputPattern{
				PatternKey:  key,
				Label:       key,
				TotalRows:   rows/7 + i*10,
				TxnCount:    txns / 7,
				EventCount:  events / 7,
				ShareOfRows: 1.0 / 7,
				ShareOfTxns: 1.0 / 7,
				Tables:      map[string]int{key: rows/7 + i*10},
				Operations:  map[string]int{"UPDATE": rows/7 + i*10},
			}
		}
		return testInputReportWithPatterns(name, label, start, rows, txns, events, inserts, updates, deletes, alerts, patterns)
	}

	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{Path: "/tmp/a.json", Report: reportWith7Patterns("a", "A", "2026-03-19T10:00:00Z", 700, 70, 840, 350, 250, 100, 0)},
			{Path: "/tmp/b.json", Report: reportWith7Patterns("b", "B", "2026-03-21T10:00:00Z", 1400, 140, 1680, 700, 500, 200, 1)},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("render html: %v", err)
	}

	// Verify all 7 pattern anchors exist and are unique
	for i := 0; i < 7; i++ {
		anchor := fmt.Sprintf(`id="pattern-%d"`, i)
		if !strings.Contains(html, anchor) {
			t.Fatalf("expected html to contain %q for pattern index %d", anchor, i)
		}
	}

	// Verify point anchors exist for each snapshot
	for i := 0; i < len(result.Points); i++ {
		anchor := fmt.Sprintf(`id="point-%d"`, i)
		if !strings.Contains(html, anchor) {
			t.Fatalf("expected html to contain %q for point index %d", anchor, i)
		}
	}
}

func TestRenderHTMLUsesDOMAPINotInnerHTMLForFindings(t *testing.T) {
	result := mustBuildPatternTrendResult(t)

	// Inject a hostile summary into a finding to verify it cannot execute
	for i := range result.TrendSummary {
		result.TrendSummary[i].Summary = `growth<script>alert("xss")</script>`
		result.TrendSummary[i].EvidenceRefs = []EvidenceRef{{
			Section: "pattern_trends",
			Key:     `evil<img src=x onerror=alert(1)>`,
			Label:   `evil<img src=x onerror=alert(1)>`,
			Anchor:  "pattern-0",
		}}
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("render html: %v", err)
	}

	for _, forbidden := range []string{
		`<script>alert("xss")</script>`,
		`<img src=x onerror=alert(1)>`,
		`trendFindingsEl.innerHTML`,
	} {
		if strings.Contains(html, forbidden) {
			t.Fatalf("expected hostile content to be escaped / no innerHTML usage, found %q", forbidden)
		}
	}

	if !strings.Contains(html, "document.createElement") {
		t.Fatalf("expected findings JS to use document.createElement")
	}
	if !strings.Contains(html, ".textContent") {
		t.Fatalf("expected findings JS to use textContent")
	}
}
