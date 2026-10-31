package report

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"binlogviz/internal/model"
)

// The executive summary strip surfaces at most four alerts, taken from the
// head of the full findings list; the Risks & Findings section keeps all of
// them.
func TestBuildHTMLAlerts_TopAlertsCappedAtFour(t *testing.T) {
	alerts := make([]model.Alert, 6)
	for i := range alerts {
		alerts[i] = model.Alert{Severity: "warning", Message: fmt.Sprintf("alert-%d", i)}
	}

	var d htmlReportData
	buildHTMLAlerts(&d, alerts)

	if len(d.Alerts) != 6 {
		t.Fatalf("expected all 6 alerts in the full list, got %d", len(d.Alerts))
	}
	if len(d.TopAlerts) != 4 {
		t.Fatalf("expected the executive summary capped at 4 alerts, got %d", len(d.TopAlerts))
	}
	for i := 0; i < 4; i++ {
		if want := fmt.Sprintf("alert-%d", i); d.TopAlerts[i].Message != want {
			t.Errorf("TopAlerts[%d]: expected %q (source order), got %q", i, want, d.TopAlerts[i].Message)
		}
	}
}

func TestBuildHTMLAlerts_TopAlertsShortList(t *testing.T) {
	var d htmlReportData
	buildHTMLAlerts(&d, []model.Alert{{Severity: "critical", Message: "one"}})

	if len(d.TopAlerts) != 1 || d.TopAlerts[0].Message != "one" {
		t.Fatalf("expected TopAlerts to hold the single alert, got %+v", d.TopAlerts)
	}
}

// --sql-context off must hide primary-key values without hiding the ranking:
// the row keeps its position, table, and counts, and the report says keys are
// hidden.
func TestBuildHTMLHotRows_SQLContextOffHidesKeys(t *testing.T) {
	result := model.AnalysisResult{
		HotRows: model.HotRowReport{
			Rows: []model.HotRow{{
				Schema: "shop", Table: "orders", PrimaryKey: "id=42",
				Touches: 9, Transactions: 3,
			}},
		},
	}

	opts := DefaultOptions()
	opts.SQLContextMode = SQLContextOff
	var d htmlReportData
	buildHTMLHotRows(&d, result, opts)

	if !d.HasHotRows || len(d.HotRows) != 1 {
		t.Fatalf("expected the hot row to stay listed, got %+v", d.HotRows)
	}
	row := d.HotRows[0]
	if row.Key == "id=42" {
		t.Fatal("primary key value leaked under --sql-context off")
	}
	if row.Key == "" {
		t.Fatal("expected a placeholder key label, not an empty cell")
	}
	if row.Touches != 9 || row.Transactions != 3 {
		t.Fatalf("counts must survive key hiding, got %+v", row)
	}
	if d.HotRowsHidden == "" {
		t.Fatal("expected the hidden-keys note to be set")
	}
}

func TestBuildHTMLHotRows_SQLContextSummaryShowsKeys(t *testing.T) {
	result := model.AnalysisResult{
		HotRows: model.HotRowReport{
			Rows: []model.HotRow{{Schema: "shop", Table: "orders", PrimaryKey: "id=42"}},
		},
	}

	var d htmlReportData
	buildHTMLHotRows(&d, result, DefaultOptions())

	if len(d.HotRows) != 1 || d.HotRows[0].Key != "id=42" {
		t.Fatalf("expected the real primary key under summary mode, got %+v", d.HotRows)
	}
	if d.HotRowsHidden != "" {
		t.Fatalf("expected no hidden-keys note, got %q", d.HotRowsHidden)
	}
}

// The throughput chart joins labels, bytes, and rows by array position, so all
// three series must have the segment count and follow segment order.
func TestBuildHTMLFileSegments_ThroughputSeriesAligned(t *testing.T) {
	t0 := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			FileSegments: []model.FileSegment{
				{StartTime: t0, EndTime: t0.Add(time.Hour), BinlogBytes: 100, Rows: 5, Events: 7},
				{StartTime: t0.Add(time.Hour), EndTime: t0.Add(2 * time.Hour), BinlogBytes: 200, Rows: 6, Events: 8},
			},
		},
	}

	var d htmlReportData
	buildHTMLFileSegments(&d, result, DefaultOptions())

	var labels []string
	var bytes []int64
	var rows []int
	for raw, dst := range map[string]any{
		string(d.ThroughputLabels): &labels,
		string(d.ThroughputBytes):  &bytes,
		string(d.ThroughputRows):   &rows,
	} {
		if err := json.Unmarshal([]byte(raw), dst); err != nil {
			t.Fatalf("throughput series is not valid JSON: %v", err)
		}
	}
	if len(labels) != 2 || len(bytes) != 2 || len(rows) != 2 {
		t.Fatalf("expected all three series to hold 2 points, got labels=%d bytes=%d rows=%d",
			len(labels), len(bytes), len(rows))
	}
	if bytes[0] != 100 || bytes[1] != 200 || rows[0] != 5 || rows[1] != 6 {
		t.Fatalf("expected segment-order values, got bytes=%v rows=%v", bytes, rows)
	}
	if labels[0] != d.FileSegments[0].StartTime || labels[1] != d.FileSegments[1].StartTime {
		t.Fatalf("expected labels to be the segment start times, got %v", labels)
	}
}

// The Top Threads table renders a column only when at least one listed thread
// carries a value for it; the flags must track presence, not row count.
func TestBuildHTMLThreads_ColumnFlagsFollowData(t *testing.T) {
	result := model.AnalysisResult{
		Threads: []model.ThreadStats{
			{ThreadID: 7, TotalRows: 10, EventCount: 4, TxnCount: 2},
		},
	}

	var d htmlReportData
	buildHTMLThreads(&d, result, DefaultOptions())

	for flag, name := range map[bool]string{
		d.ThreadShowID:     "ThreadShowID",
		d.ThreadShowRows:   "ThreadShowRows",
		d.ThreadShowEvents: "ThreadShowEvents",
		d.ThreadShowTxns:   "ThreadShowTxns",
	} {
		if !flag {
			t.Errorf("expected %s: the listed thread carries that column", name)
		}
	}
	for flag, name := range map[bool]string{
		d.ThreadShowServer: "ThreadShowServer",
		d.ThreadShowActor:  "ThreadShowActor",
		d.ThreadShowSchema: "ThreadShowSchema",
		d.ThreadShowBytes:  "ThreadShowBytes",
	} {
		if flag {
			t.Errorf("expected no %s: no listed thread carries that column", name)
		}
	}
}

// The top-tables bar mirrors the displayed (TopTables-limited) list while the
// ops pie totals every analyzed table; handing the pie the limited list would
// silently drop rows from the INSERT/UPDATE/DELETE split.
func TestBuildHTMLCharts_BarLimitedPieTotalsAll(t *testing.T) {
	all := []model.TableStats{
		{Schema: "shop", Table: "a", TotalRows: 10, InsertRows: 10},
		{Schema: "shop", Table: "b", TotalRows: 20, UpdateRows: 20},
	}
	limited := all[:1]

	var d htmlReportData
	buildHTMLTablesBar(&d, limited)
	buildHTMLOpsPie(&d, all)

	var names []string
	if err := json.Unmarshal([]byte(d.TableBarNames), &names); err != nil {
		t.Fatalf("bar names are not valid JSON: %v", err)
	}
	if len(names) != 1 || names[0] != "shop.a" {
		t.Fatalf("expected the bar to list only displayed tables, got %v", names)
	}

	var pie []struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}
	if err := json.Unmarshal([]byte(d.OpsPie), &pie); err != nil {
		t.Fatalf("ops pie is not valid JSON: %v", err)
	}
	got := map[string]int{}
	for _, p := range pie {
		got[p.Name] = p.Value
	}
	if got["INSERT"] != 10 || got["UPDATE"] != 20 || got["DELETE"] != 0 {
		t.Fatalf("expected pie totals over all tables (10/20/0), got %v", got)
	}
}
