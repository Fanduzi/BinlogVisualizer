package binlogviz

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

// TestWideActivityHeap is the E2 gate for the per-(table, minute) series
// (#389). The 4-tables-per-minute template in TestLargeInputBaseline folds
// that series into a fixed per-minute cost, so a binlog that writes many
// tables every minute can grow the heap by hundreds of bytes per point
// without failing it. This test keeps the transaction count fixed and
// varies only how many tables share each minute.
//
// It runs in CI's verify job (plain go test ./...). A -race build skips
// it: two multi-minute parses under the race detector are too slow there,
// and verify is not a race build.
func TestWideActivityHeap(t *testing.T) {
	if largeInputRace {
		t.Skip("-race build: parsing many tables over a long span under the race detector is too slow for this gate; CI's verify job runs this check in plain go test ./...")
	}
	switch mode := os.Getenv("BINLOGVIZ_WIDE_HEAP"); mode {
	case "child":
		wideHeapChild(t)
		return
	case "":
	default:
		t.Fatalf("BINLOGVIZ_WIDE_HEAP=%q: only this test sets it, to child", mode)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestWideActivityHeap$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(),
		"BINLOGVIZ_WIDE_HEAP=child",
		// Above a passing run so the collector stays on the unlimited path
		// while a retained series still reaches the assertion.
		"GOMEMLIMIT=1GiB",
	)
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil || !strings.Contains(string(out), "wide-heap pass") {
		t.Fatalf("wide activity heap: %v\n%s", err, out)
	}
}

// wideHeapTxns is the same transaction count on both files, one commit a
// second, so both span the same minutes and flashback keeps the same number
// of conflict keys. The wide file writes every one of its tables in every
// minute; the narrow file writes four.
const (
	wideHeapTxns      = 24000
	wideHeapTables    = 60
	wideHeapPerMinute = 60
	narrowPerMinute   = 4
)

// maxWideExtraHeap is how many more live-heap bytes the wide run may peak
// at than the narrow run. On main the wide file keeps about 220 bytes per
// extra (table, minute) point, several megabytes over this span. A spilled
// series keeps a byte-capped buffer and merges runs at the end.
const maxWideExtraHeap int64 = 2 << 20

func wideHeapChild(t *testing.T) {
	t.Helper()
	tp := loadSyntheticTemplate(t)
	tp.stepMicros = largeInputStepMicros
	dir := t.TempDir()

	wideShape := newLargeShape(t, tp, wideHeapTables)
	wideShape.perMinute = wideHeapPerMinute
	narrowShape := newLargeShape(t, tp, narrowPerMinute)
	narrowShape.perMinute = narrowPerMinute

	warm := writeScaledBinlog(t, wideShape, dir+"/warm.bin", 300)
	narrowPath := dir + "/narrow.bin"
	widePath := dir + "/wide.bin"
	narrowBytes := writeScaledBinlog(t, narrowShape, narrowPath, wideHeapTxns)
	wideBytes := writeScaledBinlog(t, wideShape, widePath, wideHeapTxns)
	fmt.Printf("wide-heap files warm=%d narrow=%d wide=%d txns=%d minutes=%d tables=%d,%d\n",
		warm, narrowBytes, wideBytes, wideHeapTxns, spanMinutes(wideHeapTxns), narrowPerMinute, wideHeapTables)

	// One-time report and runtime caches land on the warmup, not the gap.
	wideAnalyze(t, dir+"/warm.bin", 300, 0)

	narrowA := wideAnalyze(t, narrowPath, wideHeapTxns, narrowPerMinute)
	wideA := wideAnalyze(t, widePath, wideHeapTxns, wideHeapTables)
	narrowF := measureFlashback(t, narrowPath, "").run.peak
	wideF := measureFlashback(t, widePath, "").run.peak

	fmt.Printf("wide-heap analyze narrow_peak=%d wide_peak=%d extra=%d\n", narrowA, wideA, int64(wideA)-int64(narrowA))
	fmt.Printf("wide-heap flashback narrow_peak=%d wide_peak=%d extra=%d\n", narrowF, wideF, int64(wideF)-int64(narrowF))

	fail := false
	if extra := int64(wideA) - int64(narrowA); extra > maxWideExtraHeap {
		t.Errorf("analyze live heap grew %d bytes from %d tables/minute to %d (peaks %d -> %d); limit %d",
			extra, narrowPerMinute, wideHeapTables, narrowA, wideA, maxWideExtraHeap)
		fail = true
	}
	if extra := int64(wideF) - int64(narrowF); extra > maxWideExtraHeap {
		t.Errorf("flashback live heap grew %d bytes from %d tables/minute to %d (peaks %d -> %d); limit %d",
			extra, narrowPerMinute, wideHeapTables, narrowF, wideF, maxWideExtraHeap)
		fail = true
	}
	if !fail {
		fmt.Println("wide-heap pass")
	}
}

// wideAnalyze runs one file through analyze, renders every format, and
// returns the peak live heap. wantTables is the number of tables that must
// appear in a minute (0 skips that check, for the warmup).
func wideAnalyze(t *testing.T, path string, txns, wantTables int) uint64 {
	t.Helper()
	devnull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	oldStdout := os.Stdout
	os.Stdout = devnull
	defer func() {
		os.Stdout = oldStdout
		_ = devnull.Close()
	}()

	var sa *sampledAnalyzer
	factory := func(opts analyzer.Options, _ *analyzer.DuckDBStore) commandAnalyzer {
		sa = &sampledAnalyzer{inner: analyzer.New(opts), every: analyzeMemSampleEvery}
		return sa
	}
	if err := runAnalysisStreamingWithDeps([]string{path}, analyzer.DefaultOptions(), report.DefaultOptions(), "json", binlog.NewParser(), nil, factory, nil, ""); err != nil {
		t.Fatalf("analyze %s: %v", path, err)
	}
	if sa == nil || sa.result == nil || !sa.finalizeSampled {
		t.Fatalf("analyze %s did not finish", path)
	}
	if got := sa.result.Summary.TotalTransactions; got != txns {
		t.Fatalf("analyze %s saw %d transactions, want %d", path, got, txns)
	}
	if wantTables > 0 {
		got := widestMinuteTables(sa.result)
		if got < wantTables {
			t.Fatalf("analyze %s minute has %d tables, want at least %d", path, got, wantTables)
		}
	}
	peak := sa.peak
	renderFormats(t, sa.result, txns, &peak)
	if sa.result.MinuteTables != nil {
		sa.result.MinuteTables.Close()
	}
	return peak
}

func widestMinuteTables(result *model.AnalysisResult) int {
	best := 0
	for _, minute := range result.Minutes {
		n := len(minute.TableRows)
		if n == 0 && result.MinuteTables != nil {
			n = len(result.MinuteTables.TableRows(minute.Minute))
		}
		if n > best {
			best = n
		}
	}
	return best
}
