// Package analyzer checks that a minute is one bucket when event timestamps move backwards.
// input: synthetic normalized events through Analyzer.Analyze, including a commit that lands after a later minute.
// output: one chronological entry per minute, with summed rows, transactions, and driver tables, in JSON, text, Markdown, and HTML.
// pos: regression for the streaming minute map; these tests fail if a minute is flushed before the input ends.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

// A transaction that started in an earlier minute can be written after commits
// whose timestamps have already moved on. MySQL 8.0.36 stamps that
// transaction with its start time. Those events must land in the minute that
// already exists, not a second bucket.

func TestAnalyzeOutOfOrderMinuteCountedOnce(t *testing.T) {
	m0 := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	m1 := m0.Add(time.Minute)
	busy := m0.Add(2 * time.Minute)
	m3 := m0.Add(3 * time.Minute)

	events := []model.NormalizedEvent{}
	events = append(events, closedInsert(m0.Add(10*time.Second), "t1", "orders", 10)...)
	events = append(events, closedInsert(m1.Add(10*time.Second), "t2", "orders", 10)...)
	events = append(events, beginRows(busy.Add(10*time.Second), "t3", "orders", 30)...)
	events = append(events, rowEvent(busy.Add(20*time.Second), "t3", "users", 10))
	events = append(events, xid(busy.Add(25*time.Second), "t3"))
	// A commit in the next minute, then a transaction whose events still
	// carry the busy minute. On a monotonic drain this opens a second bucket.
	events = append(events, closedInsert(m3.Add(5*time.Second), "t4", "orders", 10)...)
	events = append(events, closedInsert(busy.Add(40*time.Second), "t5", "orders", 50)...)

	result := analyzeMinutes(t, events)
	want := []minuteWant{
		{at: m0, rows: 10, txns: 1, tables: map[string]int{"shop.orders": 10}},
		{at: m1, rows: 10, txns: 1, tables: map[string]int{"shop.orders": 10}},
		{at: busy, rows: 90, txns: 2, tables: map[string]int{"shop.orders": 80, "shop.users": 10}},
		{at: m3, rows: 10, txns: 1, tables: map[string]int{"shop.orders": 10}},
	}
	assertMinutes(t, result.Minutes, want)
	assertSeriesMinutes(t, result.Timeseries.RowsSeries, want)

	peak := result.Diagnostics.HotIntervals
	if len(peak) == 0 || !peak[0].Minute.Equal(busy) || peak[0].TotalRows != 90 || peak[0].TxnCount != 2 {
		t.Fatalf("busiest minute = %+v, want %s rows=90 txns=2", peak, busy)
	}
	if peak[0].TableRows["shop.orders"] != 80 || peak[0].TableRows["shop.users"] != 10 {
		t.Fatalf("busiest minute tables = %+v", peak[0].TableRows)
	}
	assertSpike(t, result.Alerts, busy, "", 90)
	assertSpike(t, result.Alerts, busy, "shop.orders", 80)
	for _, dd := range result.PatternDrilldowns {
		seen := map[time.Time]struct{}{}
		for _, minute := range dd.BusiestMinutes {
			if _, ok := seen[minute.Minute]; ok {
				t.Fatalf("drilldown %s lists %s twice", dd.PatternKey, minute.Minute)
			}
			seen[minute.Minute] = struct{}{}
		}
		if len(dd.BusiestMinutes) == 0 || !dd.BusiestMinutes[0].Minute.Equal(busy) || dd.BusiestMinutes[0].TotalRows != 90 {
			t.Fatalf("drilldown %s busiest = %+v", dd.PatternKey, dd.BusiestMinutes)
		}
	}

	assertRenderedMinute(t, result, "2026-03-15T14:02:00Z", "2026-03-15 14:02:00 UTC", "2026-03-15 14:02 UTC", 90, 2)
}

func TestAnalyzeBackwardTimestampInsideTransactionKeepsOneMinute(t *testing.T) {
	busy := time.Date(2026, 3, 15, 14, 2, 0, 0, time.UTC)
	later := busy.Add(time.Minute)
	events := []model.NormalizedEvent{
		{Timestamp: busy, EventType: "BEGIN", TxnKey: "t1"},
		rowEvent(busy.Add(10*time.Second), "t1", "orders", 30),
		rowEvent(later.Add(5*time.Second), "t1", "orders", 10),
		rowEvent(busy.Add(20*time.Second), "t1", "orders", 40),
		xid(later.Add(10*time.Second), "t1"),
	}

	result := analyzeMinutes(t, events)
	want := []minuteWant{
		{at: busy, rows: 70, txns: 1, tables: map[string]int{"shop.orders": 70}},
		{at: later, rows: 10, txns: 1, tables: map[string]int{"shop.orders": 10}},
	}
	assertMinutes(t, result.Minutes, want)
	assertSeriesMinutes(t, result.Timeseries.RowsSeries, want)
	assertRenderedMinute(t, result, "2026-03-15T14:02:00Z", "2026-03-15 14:02:00 UTC", "2026-03-15 14:02 UTC", 70, 1)
}

type minuteWant struct {
	at     time.Time
	rows   int
	txns   int
	tables map[string]int
}

func minuteAnalyzeOptions() Options {
	return Options{
		DetectSpikes: true,
		SpikeWindow:  2,
		SpikeFactor:  3,
		SpikeMinRows: 60,
		LargeTxnRows: 10000,
		TopMinutes:   0,
	}
}

func analyzeMinutes(t *testing.T, events []model.NormalizedEvent) *model.AnalysisResult {
	t.Helper()
	a := New(minuteAnalyzeOptions())
	result, err := a.Analyze(events)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	return result
}

func assertMinutes(t *testing.T, got []model.MinuteBucket, want []minuteWant) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("minutes = %d, want %d (%+v)", len(got), len(want), got)
	}
	for i, minute := range got {
		if i > 0 && !minute.Minute.After(got[i-1].Minute) {
			t.Fatalf("minutes not chronological at %s", minute.Minute)
		}
		w := want[i]
		if !minute.Minute.Equal(w.at) || minute.TotalRows != w.rows || minute.TxnCount != w.txns {
			t.Fatalf("minute %d = %s rows=%d txns=%d, want %s rows=%d txns=%d", i, minute.Minute, minute.TotalRows, minute.TxnCount, w.at, w.rows, w.txns)
		}
		if len(minute.TableRows) != len(w.tables) {
			t.Fatalf("minute %s tables = %+v, want %+v", minute.Minute, minute.TableRows, w.tables)
		}
		for table, rows := range w.tables {
			if minute.TableRows[table] != rows {
				t.Fatalf("minute %s %s = %d, want %d", minute.Minute, table, minute.TableRows[table], rows)
			}
		}
	}
}

func assertSeriesMinutes(t *testing.T, points []model.TimeseriesPoint, want []minuteWant) {
	t.Helper()
	if len(points) != len(want) {
		t.Fatalf("rows series = %d points, want %d", len(points), len(want))
	}
	for i, point := range points {
		if !point.Minute.Equal(want[i].at) || point.Value != float64(want[i].rows) {
			t.Fatalf("rows series[%d] = %s %v, want %s %d", i, point.Minute, point.Value, want[i].at, want[i].rows)
		}
	}
}

func assertSpike(t *testing.T, alerts []model.Alert, minute time.Time, table string, rows int) {
	t.Helper()
	for _, alert := range alerts {
		if alert.Type != "spike" || !alert.Minute.Equal(minute) {
			continue
		}
		gotTable, _ := alert.Details["table"].(string)
		if gotTable != table {
			continue
		}
		if alert.Details["rows"] != rows {
			t.Fatalf("spike table=%q rows=%v, want %d", table, alert.Details["rows"], rows)
		}
		return
	}
	t.Fatalf("missing spike minute=%s table=%q rows=%d in %+v", minute, table, rows, alerts)
}

func assertRenderedMinute(t *testing.T, result *model.AnalysisResult, jsonMinute, fullStamp, minuteStamp string, rows, txns int) {
	t.Helper()
	if err := i18n.Init("en"); err != nil {
		t.Fatalf("i18n: %v", err)
	}
	opts := report.DefaultOptions()
	opts.ShowMinutes = true

	raw, err := report.RenderJSONWithOptions(*result, opts)
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	var doc struct {
		ReportVersion int `json:"report_version"`
		Minutes       []struct {
			Minute    string         `json:"minute"`
			TotalRows int            `json:"total_rows"`
			TxnCount  int            `json:"txn_count"`
			TableRows map[string]int `json:"table_rows"`
		} `json:"minutes"`
		Timeseries struct {
			RowsSeries []struct {
				Minute string  `json:"minute"`
				Value  float64 `json:"value"`
			} `json:"rows_series"`
		} `json:"timeseries"`
		Diagnostics struct {
			HotIntervals []struct {
				Minute    string `json:"minute"`
				TotalRows int    `json:"total_rows"`
			} `json:"hot_intervals"`
		} `json:"diagnostics"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if doc.ReportVersion != 4 {
		t.Fatalf("report_version = %d, want 4", doc.ReportVersion)
	}
	if strings.Count(raw, `"minute": "`+jsonMinute+`"`) < 1 {
		t.Fatalf("json missing %s", jsonMinute)
	}
	seen := map[string]int{}
	for _, minute := range doc.Minutes {
		seen[minute.Minute]++
	}
	if seen[jsonMinute] != 1 {
		t.Fatalf("json minutes list %s %d times: %+v", jsonMinute, seen[jsonMinute], doc.Minutes)
	}
	var found bool
	for _, minute := range doc.Minutes {
		if minute.Minute == jsonMinute && minute.TotalRows == rows && minute.TxnCount == txns {
			found = true
		}
	}
	if !found {
		t.Fatalf("json minutes = %+v, want %s rows=%d txns=%d", doc.Minutes, jsonMinute, rows, txns)
	}
	seriesSeen := map[string]int{}
	for _, point := range doc.Timeseries.RowsSeries {
		seriesSeen[point.Minute]++
		if point.Minute == jsonMinute && point.Value != float64(rows) {
			t.Fatalf("json rows_series %s = %v, want %d", jsonMinute, point.Value, rows)
		}
	}
	if seriesSeen[jsonMinute] != 1 {
		t.Fatalf("json rows_series lists %s %d times", jsonMinute, seriesSeen[jsonMinute])
	}
	hotSeen := 0
	for _, interval := range doc.Diagnostics.HotIntervals {
		if interval.Minute == jsonMinute {
			hotSeen++
			if interval.TotalRows != rows {
				t.Fatalf("json hot interval %s rows=%d, want %d", jsonMinute, interval.TotalRows, rows)
			}
		}
	}
	if hotSeen != 1 {
		t.Fatalf("json hot intervals list %s %d times", jsonMinute, hotSeen)
	}

	text, err := report.RenderTextWithOptions(*result, opts)
	if err != nil {
		t.Fatalf("text: %v", err)
	}
	busiest := fullStamp + "  rows=" + strconv.Itoa(rows) + "  txns=" + strconv.Itoa(txns)
	detail := minuteStamp + ": " + strconv.Itoa(rows) + " rows, " + strconv.Itoa(txns) + " txn"
	if strings.Count(text, busiest) != 1 {
		t.Fatalf("text busiest line %q count=%d\n%s", busiest, strings.Count(text, busiest), text)
	}
	if strings.Count(text, detail) != 1 {
		t.Fatalf("text minute line %q count=%d\n%s", detail, strings.Count(text, detail), text)
	}

	md, err := report.RenderMarkdownWithOptions(*result, opts)
	if err != nil {
		t.Fatalf("markdown: %v", err)
	}
	mdLine := "| " + fullStamp + " | " + strconv.Itoa(rows) + " | " + strconv.Itoa(txns) + " |"
	if strings.Count(md, mdLine) != 2 {
		t.Fatalf("markdown line %q count=%d, want 2 (busiest and minute table)\n%s", mdLine, strings.Count(md, mdLine), md)
	}

	html, err := report.RenderHTMLWithOptions(*result, opts)
	if err != nil {
		t.Fatalf("html: %v", err)
	}
	// The minute chart is built from result.Minutes. A second bucket for the
	// same minute shows up as adjacent labels; per-table series are already keyed.
	chart := fullStamp[11:16] + " UTC"
	if strings.Contains(html, `"`+chart+`","`+chart+`"`) {
		t.Fatalf("html minute chart lists %s twice", chart)
	}
	if strings.Count(html, "Rows="+strconv.Itoa(rows)) != 1 {
		t.Fatalf("html Rows=%d count=%d", rows, strings.Count(html, "Rows="+strconv.Itoa(rows)))
	}
	if strings.Count(html, fullStamp) < 1 {
		t.Fatalf("html missing %s", fullStamp)
	}
}

func closedInsert(ts time.Time, key, table string, rows int) []model.NormalizedEvent {
	return []model.NormalizedEvent{
		{Timestamp: ts, EventType: "BEGIN", TxnKey: key},
		rowEvent(ts, key, table, rows),
		xid(ts, key),
	}
}

func beginRows(ts time.Time, key, table string, rows int) []model.NormalizedEvent {
	return []model.NormalizedEvent{
		{Timestamp: ts, EventType: "BEGIN", TxnKey: key},
		rowEvent(ts, key, table, rows),
	}
}

func rowEvent(ts time.Time, key, table string, rows int) model.NormalizedEvent {
	return model.NormalizedEvent{
		Timestamp: ts, EventType: "ROWS", TxnKey: key,
		Schema: "shop", Table: table, Operation: "INSERT", RowCount: rows,
	}
}

func xid(ts time.Time, key string) model.NormalizedEvent {
	return model.NormalizedEvent{Timestamp: ts, EventType: "XID", TxnKey: key}
}
