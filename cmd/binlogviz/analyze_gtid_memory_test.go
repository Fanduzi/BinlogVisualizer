package binlogviz

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/analyzer"
)

// TestAnalyzeGTIDFilterRetainedHeap is the E1 bound for analyze under
// --include-gtids / --exclude-gtids (#376). Before #376 the analyzer held
// every in-window event of the open group until the group closed (about
// 4.3KB per event), so one large transaction could exhaust memory.
//
// The child measures live heap the way TestAnalyzeRetainedHeap does (see
// sampledAnalyzer), on synthetic MySQL 8.0 binlogs from the hot-rows fixture:
//
//   - many: 25k and then 100k one-row transactions, --include-gtids
//     selecting the same 2000 of them. The 100k run may peak at most
//     maxAnalyzeExtraHeap above the 25k run.
//   - giant: one transaction of 75k and then 300k UPDATE_ROWS events (each
//     after its TABLE_MAP), selected by --include-gtids, and the 300k one
//     also with --exclude-gtids of another GTID. Each 300k run may peak at
//     most maxAnalyzeExtraHeap above the 75k run.
//
// The matched set is fixed on purpose: JSON selection.matched_gtids lists
// every matched GTID, so that list grows with the matches, like the alerts
// list grows with alerts.
//
// It runs in the plain `go test ./...` of CI's verify job and does not skip
// under -race.
func TestAnalyzeGTIDFilterRetainedHeap(t *testing.T) {
	switch mode := os.Getenv("BINLOGVIZ_ANAMEM_GTID"); mode {
	case "child":
		analyzeGTIDMemChild(t)
		return
	case "":
	default:
		t.Fatalf("BINLOGVIZ_ANAMEM_GTID=%q: only this test sets it, to child", mode)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestAnalyzeGTIDFilterRetainedHeap$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(),
		"BINLOGVIZ_ANAMEM_GTID=child",
		"GOMEMLIMIT=256MiB",
	)
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatalf("analyze retained heap grows under GTID filters: %v\n%s", err, out)
	}
	if strings.Count(string(out), "anamem-gtid case=") != 5 || !strings.Contains(string(out), "--- PASS: TestAnalyzeGTIDFilterRetainedHeap") {
		t.Fatalf("child did not measure every run and pass:\n%s", out)
	}
}

func analyzeGTIDMemChild(t *testing.T) {
	t.Helper()
	tp := loadSyntheticTemplate(t)
	dir := t.TempDir()
	selector := func(include, exclude []string) analyzer.Options {
		sel, err := analyzer.ParseGTIDSelector(include, exclude)
		if err != nil {
			t.Fatal(err)
		}
		opts := analyzer.DefaultOptions()
		opts.GTIDSelector = sel
		return opts
	}
	sid := tp.sid()
	const picked = 2000
	many := selector([]string{fmt.Sprintf("%s:1-%d", sid, picked)}, nil)
	first := selector([]string{sid + ":1"}, nil)
	notOther := selector([]string{sid + ":1"}, []string{sid + ":2"})

	run := func(name, file string, opts analyzer.Options, wantTxns, wantRows int) analyzeMemRun {
		path := filepath.Join(dir, file)
		r := measureAnalyzeHeap(t, path, opts, wantTxns)
		if r.rows != wantRows || r.matched != wantTxns {
			t.Fatalf("%s: %d rows and %d matched GTIDs, want %d and %d", name, r.rows, r.matched, wantRows, wantTxns)
		}
		fmt.Printf("anamem-gtid case=%s events=%d samples=%d peak_bytes=%d\n", name, r.events, r.samples, r.peak)
		return r
	}
	limit := func(what string, base, big analyzeMemRun, units int, unit string) {
		extra := int64(big.peak) - int64(base.peak)
		per := float64(extra) / float64(units)
		fmt.Printf("anamem-gtid %s extra_heap=%d per_%s=%.2f limit=%d\n", what, extra, unit, per, maxAnalyzeExtraHeap)
		if extra > maxAnalyzeExtraHeap {
			t.Fatalf("%s: analyze live heap grew %d bytes (%d -> %d; %.1f bytes/%s); limit %d bytes",
				what, extra, base.peak, big.peak, per, unit, maxAnalyzeExtraHeap)
		}
	}

	for _, n := range []int{picked, 25000, 100000} {
		writeSyntheticFile(t, filepath.Join(dir, fmt.Sprintf("many.%d", n)), func(emit func([]byte)) {
			for i := 0; i < n; i++ {
				for j := range tp.tmpl {
					emit(tp.event(j, i, i))
				}
			}
		})
	}
	measureAnalyzeHeap(t, filepath.Join(dir, fmt.Sprintf("many.%d", picked)), many, picked) // warm-up
	small := run("many-25k", "many.25000", many, picked, picked)
	large := run("many-100k", "many.100000", many, picked, picked)
	limit("many", small, large, 75000, "txn")

	const giantBase, giantN = 75000, 300000
	for _, n := range []int{giantBase, giantN} {
		writeSyntheticFile(t, filepath.Join(dir, fmt.Sprintf("giant.%d", n)), func(emit func([]byte)) {
			emit(tp.event(0, 0, 0)) // GTID :1
			emit(tp.event(1, 0, 0)) // BEGIN
			for r := 0; r < n; r++ {
				emit(tp.event(2, 0, r)) // TABLE_MAP
				emit(tp.event(3, 0, r)) // UPDATE_ROWS, primary key r+1
			}
			emit(tp.event(4, 0, 0)) // XID
			for j := range tp.tmpl {
				emit(tp.event(j, 1, n)) // GTID :2, one row
			}
		})
	}
	giantSmall := run("giant-75k", "giant.75000", first, 1, giantBase)
	giantLarge := run("giant-300k", "giant.300000", first, 1, giantN)
	giantExcl := run("giant-300k-exclude", "giant.300000", notOther, 1, giantN)
	if giantLarge.events < 2*giantN {
		t.Fatalf("giant transaction had %d events, want at least %d", giantLarge.events, 2*giantN)
	}
	limit("giant", giantSmall, giantLarge, giantN-giantBase, "event")
	limit("giant-exclude", giantSmall, giantExcl, giantN-giantBase, "event")
}

func writeSyntheticFile(t *testing.T, path string, body func(emit func([]byte))) {
	t.Helper()
	var out bytes.Buffer
	emit := loadSyntheticTemplate(t).writer(&out)
	body(emit)
	if err := os.WriteFile(path, out.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}
