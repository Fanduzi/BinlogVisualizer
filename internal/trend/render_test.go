package trend

import (
	"encoding/json"
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
