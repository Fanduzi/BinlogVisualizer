package binlogviz

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
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
// a distinct primary key and a replica apply delay per transaction, built
// from a transaction in the hot-rows fixture) and runs each through the analyze pipeline: the real
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
	rows         int
	matched      int // selection.matched_transactions, -1 without GTID selectors
	set          []string
}

func runAnalyzeMem(t *testing.T, dir string, n int) analyzeMemRun {
	t.Helper()
	path := filepath.Join(dir, "mysql-bin."+strconv.Itoa(n))
	size := writeSyntheticBinlog(t, path, n)
	defer os.Remove(path)
	r := measureAnalyzeHeap(t, path, analyzer.DefaultOptions(), n)
	r.txns, r.fileBytes = n, size
	return r
}

// measureAnalyzeHeap runs one file through the analyze pipeline with opts and
// returns the sampled peak live heap. wantTxns is the transaction count the
// result must report.
func measureAnalyzeHeap(t *testing.T, path string, opts analyzer.Options, wantTxns int) analyzeMemRun {
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
	runtime.GC()
	if err := runAnalysisStreamingWithDeps([]string{path}, opts, report.DefaultOptions(), "json", binlog.NewParser(), nil, factory, nil, ""); err != nil {
		t.Fatalf("analyze %s: %v", filepath.Base(path), err)
	}
	if sa == nil || sa.result == nil {
		t.Fatalf("analyze %s never finalized, so nothing was measured", filepath.Base(path))
	}
	if got := sa.result.Summary.TotalTransactions; got != wantTxns {
		t.Fatalf("analyze %s saw %d transactions, want %d", filepath.Base(path), got, wantTxns)
	}
	if !sa.finalizeSampled || sa.samples < sa.consumed/analyzeMemSampleEvery || sa.peak == 0 {
		t.Fatalf("%s: %d heap samples over %d events (finalize sampled %v), peak %d", filepath.Base(path), sa.samples, sa.consumed, sa.finalizeSampled, sa.peak)
	}
	matched := -1
	var set []string
	if sel := sa.result.Selection; sel != nil && sel.MatchedTransactions != nil {
		matched = *sel.MatchedTransactions
		set = append(set, sel.MatchedGTIDSet...)
	}
	// Keep only counts and the small set: a result kept alive would count
	// in the next run's peak.
	return analyzeMemRun{events: sa.consumed, samples: sa.samples, peak: sa.peak, rows: sa.result.Summary.TotalRows, matched: matched, set: set}
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
// timestamps (10ms apart, each with a replica apply delay of 0-3s), thread
// id, primary key and xid, fixes end_log_pos and recomputes the CRC32.
func writeSyntheticBinlog(t *testing.T, path string, n int) int64 {
	t.Helper()
	tp := loadSyntheticTemplate(t)
	var out bytes.Buffer
	emit := tp.writer(&out)
	for i := 0; i < n; i++ {
		for j := range tp.tmpl {
			emit(tp.event(j, i, i))
		}
	}
	if err := os.WriteFile(path, out.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return int64(out.Len())
}

// syntheticTemplate is the hot-rows fixture's header events and its
// GTID/BEGIN/TABLE_MAP/UPDATE_ROWS/XID transaction, with the GTID event
// widened to carry an original commit timestamp.
type syntheticTemplate struct {
	magic      []byte
	header     [][]byte
	tmpl       [][]byte
	idOffsets  []int
	baseMicros uint64
}

// sid is the server UUID of the template's GTID.
func (tp syntheticTemplate) sid() string {
	u := tp.tmpl[0][19+1 : 19+17]
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// writer writes the magic and header events to out and returns an emit
// function that appends one event with end_log_pos and CRC32 fixed.
func (tp syntheticTemplate) writer(out io.Writer) func([]byte) {
	out.Write(tp.magic)
	pos := uint32(len(tp.magic))
	emit := func(ev []byte) {
		pos += uint32(len(ev))
		binary.LittleEndian.PutUint32(ev[13:], pos)
		binary.LittleEndian.PutUint32(ev[len(ev)-4:], crc32.ChecksumIEEE(ev[:len(ev)-4]))
		out.Write(ev)
	}
	for _, ev := range tp.header {
		emit(append([]byte(nil), ev...))
	}
	return emit
}

// event is template event j (0 GTID, 1 BEGIN, 2 TABLE_MAP, 3 UPDATE_ROWS,
// 4 XID) for transaction i, with row i as its primary key: GNO i+1,
// sequence numbers, commit timestamps 10ms apart with a replica apply delay
// of 0-3s, thread id, primary key and xid.
func (tp syntheticTemplate) event(j, i, row int) []byte {
	put7 := func(b []byte, v uint64) {
		for k := 0; k < 7; k++ {
			b[k] = byte(v >> (8 * k))
		}
	}
	micros := tp.baseMicros + uint64(i)*10_000
	ev := append([]byte(nil), tp.tmpl[j]...)
	binary.LittleEndian.PutUint32(ev[0:], uint32(micros/1_000_000))
	body := ev[19:]
	switch j {
	case 0: // GTID: replica-style, the original commit 0-3s earlier
		binary.LittleEndian.PutUint64(body[17:], uint64(i+1))
		binary.LittleEndian.PutUint64(body[26:], uint64(i))
		binary.LittleEndian.PutUint64(body[34:], uint64(i+1))
		put7(body[42:], micros)
		body[48] |= 0x80
		put7(body[49:], micros-uint64(i*7919%3_000_000))
	case 1: // BEGIN: thread id
		binary.LittleEndian.PutUint32(body[0:], uint32(i%32+1))
	case 3: // UPDATE_ROWS: primary key, before and after
		for _, off := range tp.idOffsets {
			binary.LittleEndian.PutUint32(ev[off:], uint32(row+1))
		}
	case 4: // XID
		binary.LittleEndian.PutUint64(body[0:], uint64(i+1000))
	}
	return ev
}

func loadSyntheticTemplate(t *testing.T) syntheticTemplate {
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
	// The fixture GTID event has one commit timestamp (a source). Give it an
	// original_commit_timestamp, as a replica writes, so apply delay varies
	// per transaction: set the flag bit in the immediate timestamp, insert 7
	// bytes, and grow the event size and the packed transaction_length (0xfc
	// plus 2 bytes) by 7.
	gtid := tmpl[0]
	if gtid[19+48]&0x80 != 0 || gtid[19+49] != 0xfc {
		t.Fatalf("GTID template already has an original timestamp or an unexpected length: %x", gtid)
	}
	gtid = append(append(append([]byte(nil), gtid[:19+49]...), make([]byte, 7)...), gtid[19+49:]...)
	binary.LittleEndian.PutUint32(gtid[9:], uint32(len(gtid)))
	txnLen := gtid[19+57:]
	binary.LittleEndian.PutUint16(txnLen, binary.LittleEndian.Uint16(txnLen)+7)
	tmpl[0] = gtid

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
	return syntheticTemplate{
		magic:      raw[:4],
		header:     header,
		tmpl:       tmpl,
		idOffsets:  idOffsets,
		baseMicros: uint64(baseTS) * 1_000_000,
	}
}
