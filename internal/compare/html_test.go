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
		"window.compareSummaryPairs =",
		"window.compareTopTables =",
		"window.compareOpsMix =",
		"current",
		"baseline",
		"orders.refunds",
		"large transaction detected",
	} {
		if !strings.Contains(output, token) {
			t.Fatalf("expected html to contain %q", token)
		}
	}
}
