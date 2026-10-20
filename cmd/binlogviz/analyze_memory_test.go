package binlogviz

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"testing"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

// TestAnalyzeRetainedHeap is the E1 bound for analyze (#364): analyze live
// heap must not grow with the number of transactions.
//
// The child writes synthetic MySQL 8.0 binlogs (one-row UPDATE transactions,
// a distinct primary key per transaction, built from a transaction in the
// hot-rows fixture) and runs each through the analyze pipeline: the real
// parser, analyzer and JSON report. Live heap is read on the analyze
// goroutine itself, every analyzeMemSampleEvery events and at both sides of
// Finalize, after a forced GC (see sampledAnalyzer). There is no background
// ticker, so GOMAXPROCS and machine load do not move it.
//
// A 2k-transaction run warms the process (report encoders and other
// one-time caches), then 25k and 100k runs are measured. The 100k run may
// peak at most maxAnalyzeExtraHeap above the 25k run.
//
// It runs in the plain `go test ./...` of CI's verify job, which is not a
// -race build. It does not skip under -race either: the numbers are the same.
func TestAnalyzeRetainedHeap(t *testing.T) {
	switch mode := os.Getenv("BINLOGVIZ_ANAMEM"); mode {
	case "child":
		analyzeMemChild(t)
		return
	case "":
	default:
		t.Fatalf("BINLOGVIZ_ANAMEM=%q: only this test sets it, to child", mode)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestAnalyzeRetainedHeap$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(),
		"BINLOGVIZ_ANAMEM=child",
		"GOMEMLIMIT=256MiB",
	)
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatalf("analyze retained heap grows with transactions: %v\n%s", err, out)
	}
	if strings.Count(string(out), "anamem txns=") != 2 || !strings.Contains(string(out), "--- PASS: TestAnalyzeRetainedHeap") {
		t.Fatalf("child did not measure both runs and pass:\n%s", out)
	}
}

// maxAnalyzeExtraHeap is how much more live heap the 100k-transaction run
// may peak at than the 25k run. It is not an RSS cap.
//
// Measured (#364): the two peaks are within about 20KB of each other at
// GOMAXPROCS 1 and 8, with and without -race. Before #364 the 100k run
// peaked about 5.7MB higher: the table and minute aggregators kept every
// transaction key (about 64 bytes each), the apply-delay p95 kept every
// delay in memory (8 bytes each), and hot-row key churn grew its map by
// about 1MB. Keeping every event adds about 3800 bytes per transaction, half
// about 2000, an eighth about 580, and one more map entry per transaction
// about 35; over 75k extra transactions each is MBs and fails. 256KiB is
// about 12x the measured gap.
const maxAnalyzeExtraHeap int64 = 256 << 10

const analyzeMemSampleEvery = 2000

func analyzeMemChild(t *testing.T) {
	t.Helper()
	const warmN, smallN, largeN = 2000, 25000, 100000
	dir := t.TempDir()
	runAnalyzeMem(t, dir, warmN)
	small := runAnalyzeMem(t, dir, smallN)
	large := runAnalyzeMem(t, dir, largeN)
	for _, r := range []analyzeMemRun{small, large} {
		fmt.Printf("anamem txns=%d events=%d file=%d samples=%d peak_bytes=%d\n", r.txns, r.events, r.fileBytes, r.samples, r.peak)
	}
	extraEvents := int64(large.events - small.events)
	if extraEvents < int64(largeN-smallN)*4 {
		t.Fatalf("events did not scale with transactions: %d -> %d", small.events, large.events)
	}
	extraHeap := int64(large.peak) - int64(small.peak)
	extraTxns := int64(largeN - smallN)
	perTxn := float64(extraHeap) / float64(extraTxns)
	fmt.Printf("anamem extra_heap=%d per_txn=%.2f limit=%d\n", extraHeap, perTxn, maxAnalyzeExtraHeap)
	if extraHeap > maxAnalyzeExtraHeap {
		t.Fatalf("analyze live heap grew %d bytes from %d to %d transactions (%d -> %d; %.1f bytes/transaction); limit %d bytes",
			extraHeap, smallN, largeN, small.peak, large.peak, perTxn, maxAnalyzeExtraHeap)
	}
}

type analyzeMemRun struct {
	txns, events int
	fileBytes    int64
	samples      int
	peak         uint64
}

func runAnalyzeMem(t *testing.T, dir string, n int) analyzeMemRun {
	t.Helper()
	path := filepath.Join(dir, "mysql-bin."+strconv.Itoa(n))
	size := writeSyntheticBinlog(t, path, n)
	defer os.Remove(path)

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
	runtime.GC()
	if err := runAnalysisStreamingWithDeps([]string{path}, analyzer.DefaultOptions(), report.DefaultOptions(), "json", binlog.NewParser(), nil, factory, nil, ""); err != nil {
		t.Fatalf("analyze %d transactions: %v", n, err)
	}
	if sa == nil || sa.result == nil {
		t.Fatalf("analyze %d transactions never finalized, so nothing was measured", n)
	}
	if got := sa.result.Summary.TotalTransactions; got != n {
		t.Fatalf("analyze saw %d transactions, want %d", got, n)
	}
	if !sa.finalizeSampled || sa.samples < sa.consumed/analyzeMemSampleEvery || sa.peak == 0 {
		t.Fatalf("n=%d: %d heap samples over %d events (finalize sampled %v), peak %d", n, sa.samples, sa.consumed, sa.finalizeSampled, sa.peak)
	}
	return analyzeMemRun{txns: n, events: sa.consumed, fileBytes: size, samples: sa.samples, peak: sa.peak}
}

// sampledAnalyzer reads live heap on the goroutine that feeds the analyzer:
// every `every` events, just before Finalize (all streaming state still
// reachable), and just after it (the result reachable). Nothing else
// allocates between runtime.GC and the read.
type sampledAnalyzer struct {
	inner           *analyzer.Analyzer
	every           int
	consumed        int
	samples         int
	peak            uint64
	finalizeSampled bool
	result          *model.AnalysisResult
}

func (s *sampledAnalyzer) sample() {
	s.samples++
	runtime.GC()
	m := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(m)
	if m[0].Value.Kind() != metrics.KindUint64 {
		panic(fmt.Sprintf("/gc/heap/live:bytes unsupported: kind %v", m[0].Value.Kind()))
	}
	if live := m[0].Value.Uint64(); live > s.peak {
		s.peak = live
	}
}

func (s *sampledAnalyzer) Consume(ev model.NormalizedEvent) error {
	if err := s.inner.Consume(ev); err != nil {
		return err
	}
	s.consumed++
	if s.consumed%s.every == 0 {
		s.sample()
	}
	return nil
}

func (s *sampledAnalyzer) Finalize() (*model.AnalysisResult, error) {
	s.sample()
	res, err := s.inner.Finalize()
	if err != nil {
		return nil, err
	}
	s.sample()
	s.finalizeSampled = true
	s.result = res
	runtime.KeepAlive(s.inner)
	return res, nil
}

// writeSyntheticBinlog writes a MySQL 8.0.46 binlog with n one-row UPDATE
// transactions on shop.counters. It copies the header events and one
// GTID/BEGIN/TABLE_MAP/UPDATE_ROWS/XID transaction from the hot-rows
// fixture, then gives each copy its own GNO, sequence number, commit
// timestamps (10ms apart), thread id, primary key and xid, fixes end_log_pos
// and recomputes the CRC32.
func writeSyntheticBinlog(t *testing.T, path string, n int) int64 {
	t.Helper()
	raw, err := os.ReadFile(mustFixturePath(t, "mysql-8.0.46-hot-rows-full.binlog"))
	if err != nil {
		t.Fatal(err)
	}
	type rawEvent struct {
		pos int
		ev  []byte
	}
	var events []rawEvent
	for pos := 4; pos+19 <= len(raw); {
		size := int(binary.LittleEndian.Uint32(raw[pos+9:]))
		if size < 19 || pos+size > len(raw) {
			t.Fatalf("fixture event at %d has size %d", pos, size)
		}
		events = append(events, rawEvent{pos, raw[pos : pos+size]})
		pos += size
	}
	at := func(pos int) []byte {
		for _, e := range events {
			if e.pos == pos {
				return e.ev
			}
		}
		t.Fatalf("no fixture event at %d", pos)
		return nil
	}
	// Header: FORMAT_DESCRIPTION at 4, PREVIOUS_GTIDS at 126. Template:
	// GTID :12 at 3269 through its XID ending at 3603.
	header := [][]byte{at(4), at(126)}
	var tmpl [][]byte
	for pos := 3269; pos < 3603; {
		ev := at(pos)
		tmpl = append(tmpl, ev)
		pos += len(ev)
	}
	wantTypes := []byte{33, 2, 19, 31, 16} // GTID, QUERY, TABLE_MAP, UPDATE_ROWS_V2, XID
	if len(tmpl) != len(wantTypes) {
		t.Fatalf("template has %d events, want %d", len(tmpl), len(wantTypes))
	}
	for i, ev := range tmpl {
		if ev[4] != wantTypes[i] {
			t.Fatalf("template event %d has type %d, want %d", i, ev[4], wantTypes[i])
		}
	}
	// 'hot' then id=7 (INT UNSIGNED), in the before and the after image.
	idMarker := []byte{3, 'h', 'o', 't', 7, 0, 0, 0}
	rows := tmpl[3]
	first := bytes.Index(rows, idMarker)
	second := first + 1 + bytes.Index(rows[first+1:], idMarker)
	if first < 0 || second <= first {
		t.Fatalf("UPDATE_ROWS template has no before/after id: %x", rows)
	}
	idOffsets := []int{first + 4, second + 4}

	baseTS := binary.LittleEndian.Uint32(tmpl[0][0:])
	baseMicros := uint64(baseTS) * 1_000_000

	var out bytes.Buffer
	out.Write(raw[:4])
	pos := uint32(4)
	emit := func(ev []byte) {
		pos += uint32(len(ev))
		binary.LittleEndian.PutUint32(ev[13:], pos)
		binary.LittleEndian.PutUint32(ev[len(ev)-4:], crc32.ChecksumIEEE(ev[:len(ev)-4]))
		out.Write(ev)
	}
	for _, ev := range header {
		emit(append([]byte(nil), ev...))
	}
	put7 := func(b []byte, v uint64) {
		for k := 0; k < 7; k++ {
			b[k] = byte(v >> (8 * k))
		}
	}
	for i := 0; i < n; i++ {
		micros := baseMicros + uint64(i)*10_000
		ts := uint32(micros / 1_000_000)
		for j, src := range tmpl {
			ev := append([]byte(nil), src...)
			binary.LittleEndian.PutUint32(ev[0:], ts)
			body := ev[19:]
			switch j {
			case 0: // GTID
				binary.LittleEndian.PutUint64(body[17:], uint64(i+1))
				binary.LittleEndian.PutUint64(body[26:], uint64(i))
				binary.LittleEndian.PutUint64(body[34:], uint64(i+1))
				flag := body[48] & 0x80
				put7(body[42:], micros)
				body[48] |= flag
				if flag != 0 {
					put7(body[49:], micros)
				}
			case 1: // BEGIN: thread id
				binary.LittleEndian.PutUint32(body[0:], uint32(i%32+1))
			case 3: // UPDATE_ROWS: primary key, before and after
				for _, off := range idOffsets {
					binary.LittleEndian.PutUint32(ev[off:], uint32(i+1))
				}
			case 4: // XID
				binary.LittleEndian.PutUint64(body[0:], uint64(i+1000))
			}
			emit(ev)
		}
	}
	if err := os.WriteFile(path, out.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return int64(out.Len())
}
