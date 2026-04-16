package trend

import (
	"strings"
	"testing"
)

func TestRenderHTMLIncludesDiagnosticsTrendsSections(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{
				Path:   "/tmp/a.json",
				Report: testInputReportWithTimeseries("a", "Alpha", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0, 33.5),
			},
			{
				Path:   "/tmp/b.json",
				Report: testInputReportWithTimeseries("b", "Beta", "2026-03-20T10:00:00Z", 2000, 90, 2400, 800, 600, 200, 1, 55.0),
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

	// Verify section containers exist
	for _, token := range []string{
		`id="trend-tps-chart"`,
		`id="trend-ddl-chart"`,
		`id="trend-txn-chart"`,
		`id="trend-event-mix-chart"`,
		`id="trend-hot-interval-chart"`,
		"TPS Trends",
		"DDL Trends",
		"Transaction Trends",
		"Event Mix Trends",
		"Hot Interval Trends",
	} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected html to contain %q", token)
		}
	}

	// Verify data contracts
	if !strings.Contains(html, "window.trendTPSSeries =") {
		t.Fatalf("expected TPS data contract")
	}
	if !strings.Contains(html, "window.trendDDLSeries =") {
		t.Fatalf("expected DDL data contract")
	}
	if !strings.Contains(html, "window.trendTxnSeries =") {
		t.Fatalf("expected txn data contract")
	}
	if !strings.Contains(html, "window.trendEventMixSeries =") {
		t.Fatalf("expected event mix data contract")
	}
	if !strings.Contains(html, "window.trendHotIntervalSeries =") {
		t.Fatalf("expected hot interval data contract")
	}
}

func TestRenderHTMLDiagnosticsTrendsGracefulWhenEmpty(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{Path: "/tmp/a.json", Report: testInputReport("a", "Alpha", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0)},
			{Path: "/tmp/b.json", Report: testInputReport("b", "Beta", "2026-03-20T10:00:00Z", 2000, 90, 2400, 800, 600, 200, 1)},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("render html: %v", err)
	}

	// Sections should exist even with zero-valued data
	for _, token := range []string{
		`id="trend-tps-chart"`,
		`id="trend-ddl-chart"`,
	} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected section container %q even with zero-valued data", token)
		}
	}
}

func TestRenderHTMLDiagnosticsChartsResizeOnWindowResize(t *testing.T) {
	result, err := BuildResult(BuildOptions{
		InputMode:   "explicit",
		SnapshotDir: "/tmp/snapshots",
		Points: []BuildInput{
			{Path: "/tmp/a.json", Report: testInputReport("a", "Alpha", "2026-03-19T10:00:00Z", 1000, 50, 1200, 500, 350, 150, 0)},
			{Path: "/tmp/b.json", Report: testInputReport("b", "Beta", "2026-03-20T10:00:00Z", 2000, 90, 2400, 800, 600, 200, 1)},
		},
	})
	if err != nil {
		t.Fatalf("build result: %v", err)
	}

	html, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("render html: %v", err)
	}

	// Verify the diagnostics charts are included in the resize handler
	for _, chartVar := range []string{
		"tpsChart",
		"ddlChart",
		"txnChart",
		"eventMixChart",
		"hotIntervalChart",
	} {
		if !strings.Contains(html, chartVar+".resize()") {
			t.Fatalf("expected %s.resize() in window resize handler", chartVar)
		}
	}
}
