package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

// A minute chart label that is only HH:MM appears once per day. When the
// report spans midnight the labels must carry the date so the same time
// cannot stand for two different days; a single-day report keeps HH:MM.
func TestHTMLChartLabelsCarryDateAcrossDays(t *testing.T) {
	forceEnglishReportLocale(t)
	day1 := time.Date(2026, 3, 15, 23, 58, 0, 0, time.UTC)
	day2 := day1.AddDate(0, 0, 1)
	mk := func(minutes ...time.Time) model.AnalysisResult {
		buckets := make([]model.MinuteBucket, 0, len(minutes))
		for _, m := range minutes {
			buckets = append(buckets, model.MinuteBucket{Minute: m, TotalRows: 1, TxnCount: 1})
		}
		return model.AnalysisResult{Minutes: buckets}
	}

	multi, err := RenderHTMLWithOptions(mk(day1, day2), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(multi, `"03-15 23:58 UTC"`) || !strings.Contains(multi, `"03-16 23:58 UTC"`) {
		t.Fatalf("multi-day labels should carry the date")
	}

	noon := time.Date(2026, 3, 15, 12, 5, 0, 0, time.UTC)
	single, err := RenderHTMLWithOptions(mk(noon, noon.Add(10*time.Minute)), DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(single, `"12:05 UTC"`) || strings.Contains(single, `"03-15 12:05 UTC"`) {
		t.Fatalf("single-day labels should stay HH:MM")
	}
}
