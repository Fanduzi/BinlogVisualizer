package flashback_test

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/flashback"
	"binlogviz/internal/model"
)

// TestGeneratedReviewRSS is the #317 regression. An in-memory generated-column
// review retained every image, so heap grew by kilobytes per row (about 500MB
// at 100k rows). Streaming spill keeps a small conflict-key map instead, and
// resident memory stays flat as the script grows.
//
// The child measures live heap at 25k and 100k rows, at fixed points in its
// own goroutine (see genCorpus). The extra bytes per extra row must stay
// under maxRetainedBytesPerRow. That bound sits above the key map and below
// image retention, and it does not move with VmHWM, GOMAXPROCS, or the runner.
// BINLOGVIZ_GENMEM=measure only prints one size, so the test skips there.
//
// CI's verify job runs plain `go test ./...`, not -race, so it always runs
// this test (TestEveryMemoryTestRunsInCIVerify in cmd/binlogviz checks
// that). A -race build skips it with a reason, visible in `go test -race -v`.
func TestGeneratedReviewRSS(t *testing.T) {
	if reviewRSSRace() {
		t.Skip("-race build: maxRetainedBytesPerRow is calibrated on non-race builds and the 100k-row run is slow under the race detector; CI's verify job (plain go test ./...) runs this check")
	}
	mode := os.Getenv("BINLOGVIZ_GENMEM")
	if mode == "child" || mode == "measure" {
		runGenMem(t, mode)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestGeneratedReviewRSS$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(),
		"BINLOGVIZ_GENMEM=child",
		"GOMEMLIMIT=256MiB",
	)
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatalf("generated-column review retained images: %v\n%s", err, out)
	}
	if strings.Count(string(out), "heap_bytes=") < 2 {
		t.Fatalf("child printed no heap pair:\n%s", out)
	}
}

// maxRetainedBytesPerRow is the extra live heap per extra row between the
// 25k and 100k runs. Streaming measures about 75 bytes/row (the conflict-key
// map) at GOMAXPROCS 1, 2, and 8. Keeping every logged image measures about
// 380, half about 260, a quarter about 170, an eighth about 120. 100 fails
// all of those; it is not a process RSS cap.
const maxRetainedBytesPerRow int64 = 100

func runGenMem(t *testing.T, mode string) {
	t.Helper()
	// GENMEM_PLAIN only shapes a measure run; the asserting child always
	// reviews generated columns.
	plain := mode == "measure" && os.Getenv("GENMEM_PLAIN") == "1"
	if mode == "measure" {
		n, _ := strconv.Atoi(os.Getenv("GENMEM_N"))
		if n <= 0 {
			n = 100000
		}
		heap, rss, sql := genCorpus(t, n, plain)
		fmt.Printf("n=%d plain=%v sql=%d heap_bytes=%d rss_kb=%d\n", n, plain, sql, heap, rss)
		t.Skip("BINLOGVIZ_GENMEM=measure prints one size and asserts nothing")
	}

	const smallN, largeN = 25000, 100000
	smallHeap, smallRSS, smallSQL := genCorpus(t, smallN, plain)
	largeHeap, largeRSS, largeSQL := genCorpus(t, largeN, plain)
	fmt.Printf("n=%d plain=%v sql=%d heap_bytes=%d rss_kb=%d\n", smallN, plain, smallSQL, smallHeap, smallRSS)
	fmt.Printf("n=%d plain=%v sql=%d heap_bytes=%d rss_kb=%d\n", largeN, plain, largeSQL, largeHeap, largeRSS)
	if smallSQL <= 0 || largeSQL < smallSQL*3 {
		t.Fatalf("sql did not scale with rows: %d -> %d", smallSQL, largeSQL)
	}
	extraHeap := largeHeap - smallHeap
	if extraHeap < 0 {
		extraHeap = 0
	}
	extraRows := int64(largeN - smallN)
	if extraHeap > extraRows*maxRetainedBytesPerRow {
		t.Fatalf("retained heap grew %d bytes/row from %d to %d rows (%d -> %d bytes)", extraHeap/extraRows, smallN, largeN, smallHeap, largeHeap)
	}
}

func genCorpus(t *testing.T, n int, plain bool) (heap int64, rssKB int64, sqlBytes int) {
	t.Helper()
	schema := "CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `note` varchar(255) DEFAULT NULL,\n" +
		"  `g` int GENERATED ALWAYS AS ((`id` + 1)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	if plain {
		schema = "CREATE TABLE `t` (\n" +
			"  `id` int NOT NULL,\n" +
			"  `note` varchar(255) DEFAULT NULL,\n" +
			"  `g` int DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n);\n"
	}
	a := analyzer.New(analyzer.Options{Flashback: true, SchemaSQL: schema})
	cols := []model.FlashCol{
		{Base: "int", HasSign: true},
		{Base: "varchar", Charset: "utf8mb4"},
		{Base: "int", HasSign: true},
	}
	base := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		id := strconv.Itoa(i + 1)
		g := strconv.Itoa(i + 2)
		note := "'n'"
		row := model.FlashRow{
			Schema: "shop", Table: "t", Op: "UPDATE",
			Columns: []string{"id", "note", "g"}, Cols: cols,
			Before: []string{id, note, g}, After: []string{id, note, g},
			PK: []int{0},
		}
		ev := model.NormalizedEvent{
			Timestamp: base, EventType: "ROWS", Operation: "UPDATE",
			Schema: "shop", Table: "t", TxnKey: id, TxnGTID: id,
			BinlogPath: "mysql-bin.000001", PositionStart: int64(i + 1), PositionEnd: int64(i + 2),
			FlashRows: []model.FlashRow{row},
		}
		if err := a.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	// Live heap is read synchronously, in this goroutine, at three fixed
	// points: the end of the generated-column review (its state is still
	// reachable), after Prepare, and after the write. Nothing else allocates
	// between runtime.GC and the read, so GOMAXPROCS and load do not move it.
	// Images the review holds are reachable at the first point; garbage from
	// the script bytes is not, which is what made a VmHWM cap flake.
	var peak uint64
	samples, reviewSamples := 0, 0
	sample := func() {
		samples++
		if live := liveHeapBytes(t); live > peak {
			peak = live
		}
	}
	restore := flashback.SetReviewProbe(func() {
		reviewSamples++
		sample()
	})
	defer restore()
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	plan := a.Planner()
	if _, err := plan.Prepare(); err != nil {
		t.Fatal(err)
	}
	sample()
	var sink countingWriter
	if _, err := plan.WriteFlashback(&sink); err != nil {
		t.Fatal(err)
	}
	sample()
	runtime.KeepAlive(a)
	if !plain && reviewSamples == 0 {
		t.Fatalf("n=%d: the generated-column review never ran, so nothing measured it", n)
	}
	if samples < 2 || peak == 0 {
		t.Fatalf("n=%d: %d heap samples, peak %d", n, samples, peak)
	}
	return int64(peak), vmHWM(), sink.n
}

// liveHeapBytes forces a GC and returns the heap it marked live.
func liveHeapBytes(t *testing.T) uint64 {
	t.Helper()
	runtime.GC()
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(s)
	if s[0].Value.Kind() != metrics.KindUint64 {
		t.Fatalf("/gc/heap/live:bytes unsupported: kind %v", s[0].Value.Kind())
	}
	return s[0].Value.Uint64()
}

type countingWriter struct{ n int }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.n += len(p)
	return len(p), nil
}

func vmHWM() int64 {
	body, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "VmHWM:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return -1
		}
		n, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return -1
		}
		return n
	}
	return -1
}
