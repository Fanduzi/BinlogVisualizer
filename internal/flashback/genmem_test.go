package flashback_test

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/model"
)

// TestGeneratedReviewRSS is the #317 regression. An in-memory generated-column
// review retained every image, so heap grew by kilobytes per row (about 500MB
// at 100k rows). Streaming spill keeps a small conflict-key map instead, and
// resident memory stays flat as the script grows.
//
// The child measures post-GC HeapAlloc at 25k and 100k rows. The extra bytes
// per extra row must stay under maxRetainedBytesPerRow. That bound sits above
// the key map and below image retention, and it does not move with VmHWM,
// GOMAXPROCS, or the runner. BINLOGVIZ_GENMEM=measure only prints one size.
func TestGeneratedReviewRSS(t *testing.T) {
	if reviewRSSRace() {
		t.Skip("race detector puts this workload above the retained-heap check")
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

// maxRetainedBytesPerRow is the extra post-GC heap per extra row between the
// 25k and 100k runs. Streaming measured about 120–145 bytes/row (the
// conflict-key map). Keeping every logged image measured about 520. 400 sits
// between those; it is not a process RSS cap.
const maxRetainedBytesPerRow int64 = 400

func runGenMem(t *testing.T, mode string) {
	t.Helper()
	plain := os.Getenv("GENMEM_PLAIN") == "1"
	if mode == "measure" {
		n, _ := strconv.Atoi(os.Getenv("GENMEM_N"))
		if n <= 0 {
			n = 100000
		}
		heap, rss, sql := genCorpus(t, n, plain)
		fmt.Printf("n=%d plain=%v sql=%d heap_bytes=%d rss_kb=%d\n", n, plain, sql, heap, rss)
		return
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
	// HeapAlloc after a GC, sampled while the review and the write run. Images
	// the review still holds are reachable from that stack. Garbage from the
	// script bytes is not, which is what made a whole-process VmHWM cap flake.
	var peak atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var m runtime.MemStats
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				runtime.GC()
				runtime.ReadMemStats(&m)
				for {
					old := peak.Load()
					if m.HeapAlloc <= old || peak.CompareAndSwap(old, m.HeapAlloc) {
						break
					}
				}
			}
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	var sink countingWriter
	if _, err := a.Planner().WriteFlashback(&sink); err != nil {
		t.Fatal(err)
	}
	runtime.KeepAlive(a)
	return int64(peak.Load()), vmHWM(), sink.n
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
