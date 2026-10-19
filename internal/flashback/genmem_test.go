package flashback_test

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/model"
)

// TestGeneratedReviewRSS is the #317 regression. 100k generated-column rows
// used about 500MB on the in-memory review and about 38MB after the spill.
// The child asserts VmHWM stays under 64MB, which the old review misses, and
// runs with GOMEMLIMIT set. BINLOGVIZ_GENMEM=measure only prints VmHWM.
func TestGeneratedReviewRSS(t *testing.T) {
	mode := os.Getenv("BINLOGVIZ_GENMEM")
	if mode == "child" || mode == "measure" {
		n, _ := strconv.Atoi(os.Getenv("GENMEM_N"))
		if n <= 0 {
			n = 100000
		}
		rss, sql := genCorpus(t, n, os.Getenv("GENMEM_PLAIN") == "1")
		fmt.Printf("n=%d plain=%v sql=%d rss_kb=%d\n", n, os.Getenv("GENMEM_PLAIN") == "1", sql, rss)
		if mode == "child" && rss > 64*1024 {
			t.Fatalf("VmHWM %d KB is above 64MB", rss)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestGeneratedReviewRSS$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(),
		"BINLOGVIZ_GENMEM=child",
		"GOMEMLIMIT=256MiB",
		"GENMEM_N=100000",
		"GENMEM_PLAIN=",
	)
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatalf("generated-column review exceeded 192MiB: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "rss_kb=") {
		t.Fatalf("child printed no rss:\n%s", out)
	}
}

func genCorpus(t *testing.T, n int, plain bool) (rssKB int64, sqlBytes int) {
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
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	var sink countingWriter
	if _, err := a.Planner().WriteFlashback(&sink); err != nil {
		t.Fatal(err)
	}
	return vmHWM(), sink.n
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
