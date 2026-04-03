package trend

import (
	"strings"
	"testing"
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
