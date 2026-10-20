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
// review retained every image, so heap grew by hundreds of bytes per row.
// Streaming spill keeps a small conflict-key map instead.
//
// The child samples live heap once the review finishes inside Prepare, on
// that same goroutine, and once more after the script write. Extra bytes per
// extra row must stay under maxRetainedBytesPerRow. A background sampler is
// the wrong instrument: at GOMAXPROCS=1 the writer allocates between the
// sampler's GC and its read, and the test kept the max.
func TestGeneratedReviewRSS(t *testing.T) {
	if reviewRSSRace() {
		t.Skip("race detector puts this workload above the retained-heap check")
	}
	if os.Getenv("BINLOGVIZ_GENMEM") == "child" {
		runGenMem(t)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestGeneratedReviewRSS$", "-test.count=1", "-test.v")
	cmd.Env = append(withoutEnv("BINLOGVIZ_GENMEM"),
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
// 25k and 100k runs. Streaming keeps a conflict-key map of 74 bytes/row.
// Retaining a quarter of the logged images is 168, and half is 260.
// 140 sits between the map and a quarter of the images.
const maxRetainedBytesPerRow int64 = 140

func withoutEnv(key string) []string {
	prefix := key + "="
	env := os.Environ()
	out := make([]string, 0, len(env))
	for _, e := range env {
		if strings.HasPrefix(e, prefix) {
			continue
		}
		out = append(out, e)
	}
	return out
}

func runGenMem(t *testing.T) {
	t.Helper()
	const smallN, largeN = 25000, 100000
	smallHeap, smallRSS, smallSQL := genCorpus(t, smallN)
	largeHeap, largeRSS, largeSQL := genCorpus(t, largeN)
	fmt.Printf("n=%d sql=%d heap_bytes=%d rss_kb=%d\n", smallN, smallSQL, smallHeap, smallRSS)
	fmt.Printf("n=%d sql=%d heap_bytes=%d rss_kb=%d\n", largeN, largeSQL, largeHeap, largeRSS)
	if smallHeap <= 0 || largeHeap <= 0 {
		t.Fatalf("live heap was not sampled: %d -> %d", smallHeap, largeHeap)
	}
	if smallSQL <= 0 || largeSQL < smallSQL*3 {
		t.Fatalf("sql did not scale with rows: %d -> %d", smallSQL, largeSQL)
	}
	extraHeap := largeHeap - smallHeap
	if extraHeap < 0 {
		extraHeap = 0
	}
	extraRows := int64(largeN - smallN)
	fmt.Printf("retained_bytes_per_row=%d\n", extraHeap/extraRows)
	if extraHeap > extraRows*maxRetainedBytesPerRow {
		t.Fatalf("retained heap grew %d bytes/row from %d to %d rows (%d -> %d bytes)", extraHeap/extraRows, smallN, largeN, smallHeap, largeHeap)
	}
}

func genCorpus(t *testing.T, n int) (heap int64, rssKB int64, sqlBytes int) {
	t.Helper()
	schema := "CREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `note` varchar(255) DEFAULT NULL,\n" +
		"  `g` int GENERATED ALWAYS AS ((`id` + 1)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
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
	// Prepare runs the review before the first Write. Sample there, while a
	// retained image is still reachable, then again after the write so the
	// peak cannot stay 0 when the write itself retains memory.
	var peak uint64
	sawReview := false
	flashback.SetReviewSample(func(held any) {
		sawReview = true
		if n := liveHeap(); n > peak {
			peak = n
		}
		runtime.KeepAlive(held)
	})
	defer flashback.SetReviewSample(nil)
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	var sink countingWriter
	if _, err := a.Planner().WriteFlashback(&sink); err != nil {
		t.Fatal(err)
	}
	if n := liveHeap(); n > peak {
		peak = n
	}
	runtime.KeepAlive(a)
	if !sawReview {
		t.Fatal("generated-column review finished without a live-heap sample")
	}
	if peak == 0 {
		t.Fatal("live heap sample was 0")
	}
	return int64(peak), vmHWM(), sink.n
}

// liveHeap is the heap the last GC marked. Reading it after runtime.GC in
// this goroutine does not race a writer the way HeapAlloc does.
func liveHeap() uint64 {
	runtime.GC()
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return sample[0].Value.Uint64()
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
