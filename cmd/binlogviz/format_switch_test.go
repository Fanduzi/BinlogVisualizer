// input: the compare and trend render-format lists and their render functions.
// output: a failure when a listed format has no render case or an unlisted format silently renders.
// pos: pins the exhaustive-switch contract for compare/trend --format, the same shape report.Write has.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"testing"
	"time"

	comparepkg "binlogviz/internal/compare"
	trendpkg "binlogviz/internal/trend"
)

// Every format in compareFormats must have a render case, and a format the
// list does not know must error instead of falling back to text.
func TestCompareRendersEveryListedFormat(t *testing.T) {
	for _, format := range compareFormats {
		if _, err := renderCompare(comparepkg.CompareResult{}, format, time.UTC); err != nil {
			t.Errorf("compare format %q has no render case: %v", format, err)
		}
	}
	if _, err := renderCompare(comparepkg.CompareResult{}, "markdown", time.UTC); err == nil {
		t.Error("an unlisted compare format silently rendered instead of erroring")
	}
}

func TestTrendRendersEveryListedFormat(t *testing.T) {
	for _, format := range trendFormats {
		if _, err := renderTrend(trendpkg.Result{}, format, time.UTC); err != nil {
			t.Errorf("trend format %q has no render case: %v", format, err)
		}
	}
	if _, err := renderTrend(trendpkg.Result{}, "markdown", time.UTC); err == nil {
		t.Error("an unlisted trend format silently rendered instead of erroring")
	}
}
