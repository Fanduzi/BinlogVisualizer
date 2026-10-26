package analyzer

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strconv"
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

// Alternating minutes are the shape that used to append a whole minute on
// every switch (long transactions, lagging channels). The file stays one
// record per point across a second pass that only updates those points.
// Output matches the in-memory series. After the shared event state is
// released, live heap and RSS of what remains stay at or below the
// in-memory series main retains. VmRSS does not include the unlinked file;
// the file-size check is what bounds that tmpfs RAM.
func TestAlternatingMinuteSpillStaysBounded(t *testing.T) {
	switch os.Getenv("ACTIVITY_SPILL_SHAPE") {
	case "spill", "inline":
		measureAlternatingShape(t, os.Getenv("ACTIVITY_SPILL_SHAPE"))
		return
	case "":
	default:
		t.Fatalf("ACTIVITY_SPILL_SHAPE=%q", os.Getenv("ACTIVITY_SPILL_SHAPE"))
	}

	events, points := alternatingMinuteEvents()
	inline := analyzeWithLimit(t, events, 1<<30)
	spilled := analyzeWithLimit(t, events, 0)
	t.Cleanup(func() {
		if spilled.MinuteTables != nil {
			spilled.MinuteTables.Close()
		}
	})
	src, ok := spilled.MinuteTables.(*activityStore)
	if !ok {
		t.Fatal("alternating series was inlined")
	}
	if src.n != points {
		t.Fatalf("points = %d, want %d", src.n, points)
	}
	// One spill op per point is inside the allowance of one per point plus
	// one per minute switch. A per-cell pwrite on revisit is over it.
	// Each minute is written at least once, so a missing counter fails too.
	t.Logf("alternating spill ops %d points %d", src.ops, points)
	if src.ops < points/200 || src.ops > points {
		t.Fatalf("spill I/O ops %d, want between %d and %d", src.ops, points/200, points)
	}
	bound := int64(points) * int64(actRecSize)
	if got := src.spillBytes(); got != bound {
		t.Fatalf("spill file is %d bytes for %d points, bound %d", got, points, bound)
	}
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
		t.Fatal("alternating spilled JSON differs from in-memory")
	}

	spill := runAlternatingChild(t, "spill")
	mem := runAlternatingChild(t, "inline")
	t.Logf("spill %s", spill.raw)
	t.Logf("inline %s", mem.raw)
	if spill.points != points || mem.points != points {
		t.Fatalf("child points spill=%d inline=%d want=%d", spill.points, mem.points, points)
	}
	if spill.file != bound {
		t.Fatalf("child spill file %d, bound %d", spill.file, bound)
	}
	if mem.file != 0 {
		t.Fatalf("in-memory child wrote %d spill bytes", mem.file)
	}
	if spill.heap > mem.heap {
		t.Fatalf("spill live heap %d is above the in-memory series %d", spill.heap, mem.heap)
	}
	if spill.rss > mem.rss {
		t.Fatalf("spill RSS %d is above the in-memory series %d", spill.rss, mem.rss)
	}
}

type alternatingChild struct {
	points int
	file   int64
	heap   uint64
	rss    uint64
	raw    string
}

func runAlternatingChild(t *testing.T, mode string) alternatingChild {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAlternatingMinuteSpillStaysBounded$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), "ACTIVITY_SPILL_SHAPE="+mode)
	out, err := cmd.CombinedOutput()
	text := string(out)
	if err != nil {
		t.Fatalf("shape %s: %v\n%s", mode, err, text)
	}
	var got alternatingChild
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "ACTIVITY_SHAPE ") {
			continue
		}
		got.raw = line
		if _, scanErr := fmt.Sscanf(line, "ACTIVITY_SHAPE points=%d file=%d heap=%d rss=%d", &got.points, &got.file, &got.heap, &got.rss); scanErr != nil {
			t.Fatalf("parse %q: %v", line, scanErr)
		}
	}
	if got.raw == "" || got.heap == 0 || got.rss == 0 {
		t.Fatalf("shape %s did not report heap and RSS\n%s", mode, text)
	}
	return got
}

func measureAlternatingShape(t *testing.T, mode string) {
	t.Helper()
	events, points := alternatingMinuteEvents()
	limit := 1 << 30
	if mode == "spill" {
		limit = 0
	}
	restore := SetActivityInlineLimitForTest(limit)
	defer restore()
	a := New(minuteAnalyzeOptions())
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	res, err := a.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	// Drop the analyzer's other state (transactions, the event slice) and
	// keep only the series. Whole-process samples during Consume sit on
	// that shared state, about 100MB, and jitter by more than the series
	// itself. What main keeps for this shape, and what a tmpfs spill adds,
	// is the series: the in-memory points, or the index plus the file.
	var file int64
	var retained any
	if mode == "spill" {
		src, ok := res.MinuteTables.(*activityStore)
		if !ok {
			t.Fatal("alternating shape did not spill")
		}
		if src.n != points {
			t.Fatalf("points %d, want %d", src.n, points)
		}
		file = src.spillBytes()
		retained = src
	} else if res.MinuteTables != nil {
		res.MinuteTables.Close()
		t.Fatal("in-memory shape spilled")
	} else {
		retained = []any{res.Minutes, res.Tables}
	}
	res = nil
	a = nil
	events = nil
	debug.FreeOSMemory()
	heap := liveHeap(t)
	rss := processRSS(t)
	runtime.KeepAlive(retained)
	fmt.Printf("ACTIVITY_SHAPE points=%d file=%d heap=%d rss=%d\n", points, file, heap, rss)
	if src, ok := retained.(*activityStore); ok {
		src.Close()
	}
}

func liveHeap(t *testing.T) uint64 {
	t.Helper()
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		t.Fatalf("live heap kind %v", sample[0].Value.Kind())
	}
	return sample[0].Value.Uint64()
}

func processRSS(t *testing.T) uint64 {
	t.Helper()
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			t.Fatalf("VmRSS line %q", line)
		}
		kb, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return kb * 1024
	}
	t.Fatal("VmRSS missing")
	return 0
}

func alternatingMinuteEvents() ([]model.NormalizedEvent, int) {
	const tables = 200
	const pairs = 100
	base := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	var events []model.NormalizedEvent
	for wave := 0; wave < 2; wave++ {
		for p := 0; p < pairs; p++ {
			minute := base.Add(time.Duration(p*6) * time.Minute)
			other := minute.Add(3 * time.Minute)
			for tbl := 0; tbl < tables; tbl++ {
				name := fmt.Sprintf("t%02d", tbl)
				keyA := fmt.Sprintf("w%d-p%d-t%d-a", wave, p, tbl)
				keyB := fmt.Sprintf("w%d-p%d-t%d-b", wave, p, tbl)
				events = append(events, closedInsert(minute, keyA, name, 1)...)
				events = append(events, closedInsert(other, keyB, name, 1)...)
			}
		}
	}
	return events, tables * pairs * 2
}

// Two channels a few minutes apart are the lag shape. Both minutes stay
// resident, so the spill does a handful of reads and writes at the end
// rather than one syscall per cell on every switch. The ops budget is what
// fails that shape; the wall budget leaves room for a slow runner and still
// fails a multi-second regression against the in-memory path.
func TestLaggingChannelsSpillKeepsUp(t *testing.T) {
	events := laggingChannelEvents()
	inlineStart := time.Now()
	inline := analyzeWithLimit(t, events, 1<<30)
	inlineDur := time.Since(inlineStart)
	spillStart := time.Now()
	spilled := analyzeWithLimit(t, events, 0)
	spillDur := time.Since(spillStart)
	t.Cleanup(func() {
		if spilled.MinuteTables != nil {
			spilled.MinuteTables.Close()
		}
	})
	src, ok := spilled.MinuteTables.(*activityStore)
	if !ok {
		t.Fatal("lagging series was inlined")
	}
	t.Logf("lag shape inline %s spill %s ops %d points %d", inlineDur, spillDur, src.ops, src.n)
	if src.ops > 8 {
		t.Fatalf("spill I/O ops %d, want at most 8 for two resident minutes", src.ops)
	}
	budget := inlineDur*5 + time.Second
	if spillDur > budget {
		t.Fatalf("spill %s exceeds %s (inline %s)", spillDur, budget, inlineDur)
	}
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
		t.Fatal("lagging spilled JSON differs from in-memory")
	}
}

func laggingChannelEvents() []model.NormalizedEvent {
	const tables = 2000
	base := time.Date(2026, 4, 2, 0, 0, 0, 0, time.UTC)
	other := base.Add(3 * time.Minute)
	events := make([]model.NormalizedEvent, 0, tables*2*3)
	for tbl := 0; tbl < tables; tbl++ {
		name := fmt.Sprintf("t%04d", tbl)
		events = append(events, closedInsert(base, fmt.Sprintf("a%d", tbl), name, 1)...)
		events = append(events, closedInsert(other, fmt.Sprintf("b%d", tbl), name, 1)...)
	}
	return events
}

func TestSpillStopsBeforeFillingTempFS(t *testing.T) {
	restoreDisk := SetSpillDiskAvailForTest(func() (uint64, bool) { return 8 << 20, true })
	defer restoreDisk()
	m0 := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	var events []model.NormalizedEvent
	for i := 0; i < 6; i++ {
		events = append(events, closedInsert(m0.Add(time.Duration(i)*time.Minute), fmt.Sprintf("k%d", i), "orders", i+1)...)
	}
	inline := analyzeWithLimit(t, events, 1<<20)
	spilled := analyzeWithLimit(t, events, 0)
	if spilled.MinuteTables != nil {
		spilled.MinuteTables.Close()
		t.Fatal("spill grew a file when the filesystem was under the reserve")
	}
	if mustJSON(t, spilled) != mustJSON(t, inline) {
		t.Fatal("low-space fallback differs from the in-memory series")
	}
}

func TestSpillFileIsUnlinkedWhileOpen(t *testing.T) {
	m0 := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	events := append(closedInsert(m0, "a", "orders", 2), closedInsert(m0.Add(time.Minute), "b", "orders", 3)...)
	spilled := analyzeWithLimit(t, events, 0)
	src, ok := spilled.MinuteTables.(*activityStore)
	if !ok {
		t.Fatal("expected a spill file")
	}
	t.Cleanup(src.Close)
	if _, err := os.Stat(src.path); !os.IsNotExist(err) {
		t.Fatalf("spill file %s is still linked: %v", src.path, err)
	}
	if got := src.TableRows(m0)["shop.orders"]; got != 2 {
		t.Fatalf("unlinked spill table_rows = %d, want 2", got)
	}
}

func TestSpillWriteFailureKeepsRows(t *testing.T) {
	m0 := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	var events []model.NormalizedEvent
	for i := 0; i < 8; i++ {
		events = append(events, closedInsert(m0.Add(time.Duration(i)*time.Minute), fmt.Sprintf("k%d", i), "orders", i+1)...)
	}
	want := analyzeWithLimit(t, events, 1<<20)
	wantJSON := mustJSON(t, want)

	t.Run("first write", func(t *testing.T) {
		got := analyzeUntilWriteFails(t, events, func(a *Analyzer, consumed int) {
			if consumed == 0 {
				a.activity.failWrites = 1
			}
		})
		if got.MinuteTables != nil {
			got.MinuteTables.Close()
			t.Fatal("a failed first write still spilled")
		}
		if mustJSON(t, got) != wantJSON {
			t.Fatal("rows dropped when the first spill write failed")
		}
	})
	t.Run("later write", func(t *testing.T) {
		armed := false
		got := analyzeUntilWriteFails(t, events, func(a *Analyzer, consumed int) {
			if !armed && a.activity.file != nil {
				a.activity.failWrites = 1
				armed = true
			}
			_ = consumed
		})
		if got.MinuteTables != nil {
			got.MinuteTables.Close()
			t.Fatal("a failed later write still left the series spilled")
		}
		if mustJSON(t, got) != wantJSON {
			t.Fatal("rows dropped when a later spill write failed")
		}
	})
}

func analyzeUntilWriteFails(t *testing.T, events []model.NormalizedEvent, before func(*Analyzer, int)) *model.AnalysisResult {
	t.Helper()
	restore := SetActivityInlineLimitForTest(0)
	defer restore()
	a := New(minuteAnalyzeOptions())
	for i, ev := range events {
		before(a, i)
		if err := a.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	got, err := a.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestDumpMemDoesNotDoubleActivity(t *testing.T) {
	m0 := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	var events []model.NormalizedEvent
	for i := 0; i < 4; i++ {
		events = append(events, closedInsert(m0.Add(time.Duration(i)*time.Minute), fmt.Sprintf("k%d", i), fmt.Sprintf("t%d", i), 3)...)
	}
	inline := analyzeWithLimit(t, events, 1<<20)
	spilled := analyzeWithLimit(t, events, 2)
	src, ok := spilled.MinuteTables.(*activityStore)
	if !ok {
		t.Fatal("series over the limit was inlined")
	}
	t.Cleanup(src.Close)
	if pts := src.Activity("shop", "t0"); len(pts) != 1 {
		t.Fatalf("shop.t0 activity points = %d, want 1", len(pts))
	}
	opts := report.Options{TopN: 10, TopTables: 10, ShowMinutes: true}
	got, err := report.Render(*spilled, opts, "html")
	if err != nil {
		t.Fatal(err)
	}
	want, err := report.Render(*inline, opts, "html")
	if err != nil {
		t.Fatal(err)
	}
	if seriesOf(got) != seriesOf(want) {
		t.Fatalf("html activity duplicated\nspilled %s\ninline %s", seriesOf(got), seriesOf(want))
	}
}

func TestSpilledDDLMinuteOmitsZeroRowTable(t *testing.T) {
	m0 := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{{
		Timestamp: m0,
		EventType: "DDL",
		Schema:    "shop",
		Table:     "t",
		QuerySQL:  "CREATE TABLE shop.t (id int)",
	}}
	events = append(events, closedInsert(m0.Add(time.Minute), "r1", "orders", 4)...)
	inline := analyzeWithLimit(t, events, 1<<20)
	spilled := analyzeWithLimit(t, events, 0)
	t.Cleanup(func() {
		if spilled.MinuteTables != nil {
			spilled.MinuteTables.Close()
		}
	})
	if spilled.MinuteTables == nil {
		t.Fatal("DDL series was inlined")
	}
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
		t.Fatalf("DDL spilled JSON differs\n%s\n%s", got, want)
	}
	if strings.Contains(got, `"shop.t": 0`) {
		t.Fatalf("DDL-only minute wrote a zero row count\n%s", got)
	}
}

func TestSpillReadErrorFailsFinalize(t *testing.T) {
	m0 := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	var events []model.NormalizedEvent
	for i := 0; i < 8; i++ {
		events = append(events, closedInsert(m0.Add(time.Duration(i)*time.Minute), fmt.Sprintf("k%d", i), "orders", i+1)...)
	}
	restore := SetActivityInlineLimitForTest(0)
	defer restore()
	a := New(minuteAnalyzeOptions())
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if a.activity.file == nil {
		t.Fatal("expected a spill file")
	}
	// Truncate leaves a hole that reads back as zeros. Closing the fd makes
	// the next read fail, which is the error Finalize has to surface.
	if err := a.activity.file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Finalize(); err == nil {
		t.Fatal("Finalize swallowed a spill read error")
	}
}

func TestSpillReadErrorWritesNoJSON(t *testing.T) {
	m0 := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	events := append(closedInsert(m0, "a", "orders", 2), closedInsert(m0.Add(time.Minute), "b", "orders", 3)...)
	spilled := analyzeWithLimit(t, events, 0)
	src, ok := spilled.MinuteTables.(*activityStore)
	if !ok {
		t.Fatal("expected a spill file")
	}
	t.Cleanup(src.Close)
	if err := src.file.Truncate(0); err != nil {
		t.Fatal(err)
	}
	opts := report.Options{TopN: 10, TopTables: 10, ShowMinutes: true}
	for _, format := range []string{"json", "text", "markdown", "html"} {
		t.Run(format, func(t *testing.T) {
			src.err = nil
			var buf bytes.Buffer
			err := report.Write(&buf, *spilled, opts, format)
			if err == nil {
				t.Fatalf("%s write swallowed a spill read error", format)
			}
			var spill *SpillReadError
			if !errors.As(err, &spill) {
				t.Fatalf("%s error is %T (%v), want a spill read error", format, err, err)
			}
			if !strings.Contains(err.Error(), "read spilled activity") {
				t.Fatalf("%s error %q", format, err.Error())
			}
			if buf.Len() != 0 {
				t.Fatalf("%s wrote %d bytes on a spill read error", format, buf.Len())
			}
		})
	}
}

func mustJSON(t *testing.T, result *model.AnalysisResult) string {
	t.Helper()
	got, err := report.RenderJSONWithOptions(*result, report.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	return got
}
