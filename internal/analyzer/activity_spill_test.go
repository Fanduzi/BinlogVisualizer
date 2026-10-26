package analyzer

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

// A spilled series has to render the same report as the inlined one, including
// a minute that is updated after a later minute was already seen.
func TestSpilledActivityMatchesInline(t *testing.T) {
	m0 := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	busy := m0.Add(2 * time.Minute)
	events := []model.NormalizedEvent{}
	events = append(events, closedInsert(m0.Add(10*time.Second), "t1", "orders", 10)...)
	events = append(events, closedInsert(m0.Add(time.Minute+10*time.Second), "t2", "orders", 10)...)
	events = append(events, beginRows(busy.Add(10*time.Second), "t3", "orders", 30)...)
	events = append(events, rowEvent(busy.Add(20*time.Second), "t3", "users", 10))
	events = append(events, xid(busy.Add(25*time.Second), "t3"))
	events = append(events, closedInsert(m0.Add(3*time.Minute+5*time.Second), "t4", "orders", 10)...)
	events = append(events, closedInsert(busy.Add(40*time.Second), "t5", "orders", 50)...)
	upd := rowEvent(busy.Add(50*time.Second), "t6", "orders", 4)
	upd.Operation = "UPDATE"
	events = append(events, model.NormalizedEvent{Timestamp: upd.Timestamp, EventType: "BEGIN", TxnKey: "t6"}, upd, xid(upd.Timestamp, "t6"))
	del := rowEvent(m0.Add(4*time.Second), "t7", "users", 2)
	del.Operation = "DELETE"
	events = append(events, model.NormalizedEvent{Timestamp: del.Timestamp, EventType: "BEGIN", TxnKey: "t7"}, del, xid(del.Timestamp, "t7"))

	inline := analyzeWithLimit(t, events, 1<<20)
	spilled := analyzeWithLimit(t, events, 0)
	t.Cleanup(func() {
		if spilled.MinuteTables != nil {
			spilled.MinuteTables.Close()
		}
	})
	if inline.MinuteTables != nil {
		t.Fatal("small series was spilled")
	}
	if spilled.MinuteTables == nil {
		t.Fatal("series over the limit was inlined")
	}
	for _, minute := range spilled.Minutes {
		if len(minute.TableRows) != 0 {
			t.Fatalf("spilled minute %s kept table rows on the bucket", minute.Minute)
		}
	}
	for _, table := range spilled.Tables {
		if len(table.Activity) != 0 {
			t.Fatalf("spilled table %s.%s kept activity on the table", table.Schema, table.Table)
		}
	}

	opts := report.Options{TopN: 10, TopTables: 10, ShowMinutes: true}
	for _, format := range []string{"json", "text", "markdown"} {
		got, err := report.Render(*spilled, opts, format)
		if err != nil {
			t.Fatalf("render spilled %s: %v", format, err)
		}
		want, err := report.Render(*inline, opts, format)
		if err != nil {
			t.Fatalf("render inline %s: %v", format, err)
		}
		if got != want {
			t.Fatalf("%s spilled report differs from inline\n--- spilled\n%s\n--- inline\n%s", format, got, want)
		}
	}
	gotHTML, err := report.Render(*spilled, opts, "html")
	if err != nil {
		t.Fatal(err)
	}
	wantHTML, err := report.Render(*inline, opts, "html")
	if err != nil {
		t.Fatal(err)
	}
	if seriesOf(gotHTML) != seriesOf(wantHTML) {
		t.Fatalf("html activity series differs\nspilled %s\ninline %s", seriesOf(gotHTML), seriesOf(wantHTML))
	}

	hydrated := hydrateMinuteTables(t, spilled)
	if !reflect.DeepEqual(hydrated, *inline) {
		t.Fatalf("hydrated spilled result differs from inline\nspill %#v\ninline %#v", hydrated, *inline)
	}
}

func analyzeWithLimit(t *testing.T, events []model.NormalizedEvent, limit int) *model.AnalysisResult {
	t.Helper()
	restore := SetActivityInlineLimitForTest(limit)
	defer restore()
	return analyzeMinutes(t, events)
}

func hydrateMinuteTables(t *testing.T, result *model.AnalysisResult) model.AnalysisResult {
	t.Helper()
	out := *result
	out.Minutes = append([]model.MinuteBucket(nil), result.Minutes...)
	out.Tables = append([]model.TableStats(nil), result.Tables...)
	out.Diagnostics = result.Diagnostics
	out.Diagnostics.HotIntervals = append([]model.MinuteBucket(nil), result.Diagnostics.HotIntervals...)
	if result.MinuteTables == nil {
		out.MinuteTables = nil
		return out
	}
	for i := range out.Minutes {
		out.Minutes[i].TableRows = result.MinuteTables.TableRows(out.Minutes[i].Minute)
	}
	for i := range out.Tables {
		out.Tables[i].Activity = result.MinuteTables.Activity(out.Tables[i].Schema, out.Tables[i].Table)
	}
	if err := result.MinuteTables.Err(); err != nil {
		t.Fatal(err)
	}
	out.MinuteTables = nil
	return out
}

func seriesOf(html string) string {
	const key = "window.tableActivitySeries = "
	i := strings.Index(html, key)
	if i < 0 {
		return ""
	}
	rest := html[i:]
	j := strings.Index(rest, ";")
	if j < 0 {
		return rest
	}
	return rest[:j]
}

// The streamed minutes array has to be the bytes encoding/json would emit.
func TestSpilledJSONMatchesEncoder(t *testing.T) {
	m0 := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	events := append([]model.NormalizedEvent{}, closedInsert(m0, "t1", "orders", 3)...)
	events = append(events, closedInsert(m0.Add(time.Minute), "t2", "users", 1)...)
	events = append(events, closedInsert(m0.Add(2*time.Minute), "t3", "orders", 8)...)
	inline := analyzeWithLimit(t, events, 1<<20)
	spilled := analyzeWithLimit(t, events, 0)
	t.Cleanup(func() {
		if spilled.MinuteTables != nil {
			spilled.MinuteTables.Close()
		}
	})
	opts := report.DefaultOptions()
	got, err := report.RenderJSONWithOptions(*spilled, opts)
	if err != nil {
		t.Fatal(err)
	}
	want, err := report.RenderJSONWithOptions(*inline, opts)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("RenderJSON spilled differs\n%s\n%s", got, want)
	}
	var spilledBuf, inlineBuf bytes.Buffer
	if err := report.RenderJSONToWithOptions(*spilled, &spilledBuf, opts); err != nil {
		t.Fatal(err)
	}
	if err := report.RenderJSONToWithOptions(*inline, &inlineBuf, opts); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(spilledBuf.Bytes(), inlineBuf.Bytes()) {
		t.Fatalf("RenderJSONTo spilled differs\n%s\n%s", spilledBuf.String(), inlineBuf.String())
	}
}
