package binlogviz

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/flashback"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

// TestLargeInputBaseline is the E2 gate: a generated binlog of at least
// 100MB has a baseline for time and memory, and a regression fails CI.
// The design is docs/large-input-baseline.md.
//
// The child writes two MySQL 8.0 binlogs from the hot-rows transaction
// template (about 12.5MB and at least 100MB, eight times as many
// transactions) and runs analyze, then flashback generate, on each. Analyze
// goes through the real parser and aggregator and writes json, text,
// markdown, and html. Nothing is committed as a fixture.
//
// Ratio gates catch super-linear growth. Absolute gates on the large run
// (live heap after GC, bytes allocated per event, process CPU per event)
// catch a uniform slowdown and a fixed retained buffer, which a large/small
// ratio accepts. Process CPU is getrusage user+sys, not
// /cpu/classes/total:cpu-seconds (that metric is GOMAXPROCS times wall time).
//
// It runs in the plain `go test ./...` of CI's verify job. A -race build
// skips it: parsing two files this size under the race detector is too slow
// for a local -race run, and verify is not a race build.
func TestLargeInputBaseline(t *testing.T) {
	if largeInputRace {
		t.Skip("-race build: parsing a >=100MB binlog under the race detector is too slow for this gate; CI's verify job runs this check in plain go test ./...")
	}
	switch mode := os.Getenv("BINLOGVIZ_LARGE_INPUT"); mode {
	case "child":
		largeInputChild(t)
		return
	case "":
	default:
		t.Fatalf("BINLOGVIZ_LARGE_INPUT=%q: only this test sets it, to child", mode)
	}
	// The parent owns the directory. The child's t.TempDir would be left
	// behind (~118MB) if a go test timeout killed this process first.
	dir := t.TempDir()
	ctx, cancel := largeInputChildContext(t)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLargeInputBaseline$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(),
		"BINLOGVIZ_LARGE_INPUT=child",
		"BINLOGVIZ_LARGE_INPUT_DIR="+dir,
		// Above the passing heap (~9MB analyze, ~17MB flashback) so GC stays
		// on the unlimited path. High enough that a retained buffer still
		// reaches the absolute heap assertion.
		"GOMEMLIMIT=2GiB",
	)
	cmd.WaitDelay = 3 * time.Second
	setChildDeathSignal(cmd)
	out, err := cmd.CombinedOutput()
	t.Logf("%s", out)
	if err != nil {
		t.Fatalf("large-input baseline: %v\n%s", err, out)
	}
	text := string(out)
	if strings.Count(text, "large-input analyze scale=") != 2 || strings.Count(text, "large-input flashback scale=") != 2 {
		t.Fatalf("child did not measure both analyze and flashback runs:\n%s", text)
	}
	if !strings.Contains(text, "--- PASS: TestLargeInputBaseline") {
		t.Fatalf("child did not pass:\n%s", text)
	}
	const marker = "large-input large_bytes="
	i := strings.Index(text, marker)
	if i < 0 {
		t.Fatalf("child did not report the large file size:\n%s", text)
	}
	size, convErr := strconv.ParseInt(strings.Fields(text[i+len(marker):])[0], 10, 64)
	if convErr != nil || size < largeInputMinBytes {
		t.Fatalf("large file is %d bytes, want at least %d", size, largeInputMinBytes)
	}
}

// largeInputChildContext stops the child a few seconds before t.Deadline so
// this test can return and remove the temp dir. go test's timeout kills the
// parent without running cleanup.
func largeInputChildContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	if d, ok := t.Deadline(); ok {
		stop := d.Add(-5 * time.Second)
		if !stop.After(time.Now()) {
			stop = time.Now().Add(time.Second)
		}
		return context.WithDeadline(context.Background(), stop)
	}
	return context.WithCancel(context.Background())
}

// largeInputMinBytes is the E2 floor. 100 << 20, not 100e6.
const largeInputMinBytes = 100 << 20

// largeInputSampleEvery is how often the analyze goroutine forces a GC and
// reads the live heap. 20k events is a few dozen samples on the large file,
// enough to see a buffer that grows through the run, without the samples
// becoming the work.
const largeInputSampleEvery = 20000

// maxLargeInputExtraHeap is how many more live-heap bytes the large run may
// peak at than the small run, in the same process, after a warmup.
// Retaining a 1KB copy of every event is about 1.3GB over this span and fails.
// A fixed buffer held on both runs does not move this gap; the absolute caps
// below catch that.
const maxLargeInputExtraHeap int64 = 1 << 20

// largeInputCPUSlack multiplies the input-size ratio to cap process CPU.
// The ratio gate fails when the large run's process CPU exceeds this times
// the linear extrapolation from the small run (slack × small × byte ratio).
// A uniform slowdown stays near the size ratio and does not trip it; the
// per-event cap does. An O(n^2) step moves the ratio toward the square of
// the size ratio.
const largeInputCPUSlack = 2.0

// largeInputAllocSlack caps (bytes allocated per event on the large run)
// divided by the same figure on the small run.
const largeInputAllocSlack = 1.5

// Absolute caps are the large run alone, after GC. Headroom over a clean
// run is several times the measurement and still far under a 256MiB retain
// or an ~11× per-event slowdown. Values are filled from the measurement
// table in docs/large-input-baseline.md; the constants below are the gates.
//
// CPU is seconds of process user+sys per event (getrusage RUSAGE_SELF).
const (
	maxAnalyzeLiveHeap        uint64 = 32 << 20
	maxFlashbackLiveHeap      uint64 = 64 << 20
	maxAnalyzeAllocPerEvent          = 8000.0
	maxFlashbackAllocPerEvent        = 20000.0
	maxAnalyzeCPUPerEvent            = 20e-6
	maxFlashbackCPUPerEvent          = 50e-6
)

// maxDelaySpillPerTxn caps the analyze apply-delay temp file. The file is 8
// bytes per spilled transaction. 64 still fails a spill of the row image.
const maxDelaySpillPerTxn = 64.0

// maxFlashbackBytesPerTxn is the extra live heap per extra transaction on the
// flashback runs. Measured about 31 bytes: the conflict-key map, one primary
// key per row. Keeping the row images is hundreds of bytes per row. 100 fails
// that and sits above the measured map.
const maxFlashbackBytesPerTxn = 100.0

// slowFaultIters is fixed extra CPU per event for the slow fault. It is
// enough that analyze's process CPU per event clears maxAnalyzeCPUPerEvent
// while the large/small ratio stays near the size ratio.
const slowFaultIters = 170000

func largeInputChild(t *testing.T) {
	t.Helper()
	fault := os.Getenv("BINLOGVIZ_LARGE_INPUT_FAULT")
	switch fault {
	case "", "retain", "quadratic", "slow", "retain256", "pause":
	default:
		t.Fatalf("BINLOGVIZ_LARGE_INPUT_FAULT=%q, want retain, quadratic, slow, retain256, pause, or unset", fault)
	}
	if fault != "" {
		fmt.Printf("large-input fault=%s\n", fault)
	}

	tp := loadSyntheticTemplate(t)
	smallN, largeN := scaledTxnCounts(t, tp)
	dir := os.Getenv("BINLOGVIZ_LARGE_INPUT_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	warm := writeScaledBinlog(t, tp, dir+"/warm.bin", 2000)
	smallPath := dir + "/small.bin"
	largePath := dir + "/large.bin"
	smallBytes := writeScaledBinlog(t, tp, smallPath, smallN)
	largeBytes := writeScaledBinlog(t, tp, largePath, largeN)
	fmt.Printf("large-input large_bytes=%d small_bytes=%d warm_bytes=%d txns=%d,%d\n", largeBytes, smallBytes, warm, smallN, largeN)
	if largeBytes < largeInputMinBytes {
		t.Fatalf("large binlog is %d bytes, want at least %d", largeBytes, largeInputMinBytes)
	}
	byteRatio := float64(largeBytes) / float64(smallBytes)
	if byteRatio < 7 || byteRatio > 9 {
		t.Fatalf("size ratio %.2f, want about 8 (small %d, large %d)", byteRatio, smallBytes, largeBytes)
	}

	measureAnalyze(t, dir+"/warm.bin", 2000, fault) // warmup: one-time report and runtime caches
	small := measureAnalyze(t, smallPath, smallN, fault)
	large := measureAnalyze(t, largePath, largeN, fault)
	printAnalyze("small", small)
	printAnalyze("large", large)
	checkScale(t, "analyze", small, large, byteRatio, true, 0)

	measureFlashback(t, dir+"/warm.bin") // same warmup, on the flashback path
	smallFlash := measureFlashback(t, smallPath)
	largeFlash := measureFlashback(t, largePath)
	smallFlash.run.txns = smallN
	largeFlash.run.txns = largeN
	printFlash("small", smallFlash)
	printFlash("large", largeFlash)
	checkScale(t, "flashback", smallFlash.run, largeFlash.run, byteRatio, false, maxFlashbackBytesPerTxn)
	sqlRatio := float64(largeFlash.sql) / float64(smallFlash.sql)
	fmt.Printf("large-input flashback sql_ratio=%.2f\n", sqlRatio)
	if sqlRatio < byteRatio/2 || sqlRatio > byteRatio*2 {
		t.Fatalf("flashback SQL scaled %.2f from the small file to the large one, want between %.2f and %.2f (bytes %d -> %d)",
			sqlRatio, byteRatio/2, byteRatio*2, smallFlash.sql, largeFlash.sql)
	}
	if !spillStatAvailable() {
		fmt.Printf("large-input flashback spill skipped: /proc/self/fd is not available\n")
		return
	}
	if largeFlash.run.spill <= 0 || smallFlash.run.spill <= 0 {
		t.Fatalf("flashback spill missing (small %d, large %d)", smallFlash.run.spill, largeFlash.run.spill)
	}
	perByte := func(r largeRun) float64 { return float64(r.spill) / float64(r.file) }
	fmt.Printf("large-input flashback spill_per_byte=%.3f\n", perByte(largeFlash.run))
	if perByte(largeFlash.run) > perByte(smallFlash.run)*2 {
		t.Fatalf("flashback spill grew super-linearly: %.3f bytes of spill per input byte, small run %.3f",
			perByte(largeFlash.run), perByte(smallFlash.run))
	}
}

type largeRun struct {
	txns, events int
	file         int64
	peak         uint64
	cpu          float64
	alloc        uint64
	spill        int64
	formats      [4]int
}

type flashRun struct {
	run largeRun
	sql int64
}

func printAnalyze(scale string, r largeRun) {
	fmt.Printf("large-input analyze scale=%s txns=%d events=%d file=%d peak=%d cpu_s=%.3f alloc=%d spill=%d text=%d json=%d markdown=%d html=%d\n",
		scale, r.txns, r.events, r.file, r.peak, r.cpu, r.alloc, r.spill, r.formats[0], r.formats[1], r.formats[2], r.formats[3])
}

func printFlash(scale string, r flashRun) {
	fmt.Printf("large-input flashback scale=%s events=%d file=%d peak=%d cpu_s=%.3f alloc=%d spill=%d sql=%d\n",
		scale, r.run.events, r.run.file, r.run.peak, r.run.cpu, r.run.alloc, r.run.spill, r.sql)
}

func absoluteCaps(what string) (maxHeap uint64, maxAlloc, maxCPU float64) {
	if what == "analyze" {
		return maxAnalyzeLiveHeap, maxAnalyzeAllocPerEvent, maxAnalyzeCPUPerEvent
	}
	return maxFlashbackLiveHeap, maxFlashbackAllocPerEvent, maxFlashbackCPUPerEvent
}

func checkScale(t *testing.T, what string, small, large largeRun, byteRatio float64, analyzeSpill bool, heapPerTxn float64) {
	t.Helper()
	extra := int64(large.peak) - int64(small.peak)
	cpuRatio := large.cpu / small.cpu
	smallPer := float64(small.alloc) / float64(small.events)
	largePer := float64(large.alloc) / float64(large.events)
	allocRatio := largePer / smallPer
	limitCPU := byteRatio * largeInputCPUSlack
	// floor_vs_linear is the slack: the ratio gate fails only when large
	// process CPU exceeds this multiple of (small CPU × byte ratio).
	// floor_vs_measured is that limit divided by the measured ratio.
	floorMeasured := limitCPU / cpuRatio
	fmt.Printf("large-input %s extra_heap=%d cpu_ratio=%.2f linear=%.2f limit_cpu=%.2f floor_vs_linear=%.2f floor_vs_measured=%.2f alloc_event_ratio=%.3f\n",
		what, extra, cpuRatio, byteRatio, limitCPU, largeInputCPUSlack, floorMeasured, allocRatio)
	if heapPerTxn == 0 {
		if extra > maxLargeInputExtraHeap {
			t.Fatalf("%s live heap grew %d bytes (%d -> %d); limit %d", what, extra, small.peak, large.peak, maxLargeInputExtraHeap)
		}
	} else {
		extraTxns := large.txns - small.txns
		per := float64(extra) / float64(extraTxns)
		fmt.Printf("large-input %s heap_per_txn=%.2f limit_per_txn=%.0f\n", what, per, heapPerTxn)
		if per > heapPerTxn {
			t.Fatalf("%s live heap grew %.1f bytes/transaction (%d -> %d over %d transactions); limit %.0f",
				what, per, small.peak, large.peak, extraTxns, heapPerTxn)
		}
	}
	if cpuRatio > limitCPU {
		t.Fatalf("%s process CPU ratio %.2f exceeds %.2f (%.2fx the %.2f size ratio; fails only above %.2fx the linear extrapolation from the small run); small %.3fs large %.3fs",
			what, cpuRatio, limitCPU, largeInputCPUSlack, byteRatio, largeInputCPUSlack, small.cpu, large.cpu)
	}
	if allocRatio > largeInputAllocSlack {
		t.Fatalf("%s allocated %.0f bytes/event on the large run and %.0f on the small one (ratio %.2f, limit %.2f)",
			what, largePer, smallPer, allocRatio, largeInputAllocSlack)
	}

	maxHeap, maxAlloc, maxCPU := absoluteCaps(what)
	cpuPer := large.cpu / float64(large.events)
	fmt.Printf("large-input %s absolute peak=%d limit_peak=%d alloc_per_event=%.0f limit_alloc=%.0f cpu_ns_per_event=%.0f limit_cpu_ns=%.0f\n",
		what, large.peak, maxHeap, largePer, maxAlloc, cpuPer*1e9, maxCPU*1e9)
	if large.peak > maxHeap {
		t.Fatalf("%s live heap after GC is %d bytes; limit %d", what, large.peak, maxHeap)
	}
	if largePer > maxAlloc {
		t.Fatalf("%s allocated %.0f bytes/event; limit %.0f", what, largePer, maxAlloc)
	}
	if cpuPer > maxCPU {
		t.Fatalf("%s process CPU is %.0f ns/event (getrusage user+sys, %.3fs over %d events); limit %.0f ns/event",
			what, cpuPer*1e9, large.cpu, large.events, maxCPU*1e9)
	}

	if !analyzeSpill {
		return
	}
	if !spillStatAvailable() {
		fmt.Printf("large-input %s spill skipped: /proc/self/fd is not available\n", what)
		return
	}
	if small.spill <= 0 || large.spill <= 0 {
		t.Fatalf("apply-delay spill missing (small %d, large %d); delays would be staying in memory", small.spill, large.spill)
	}
	perTxn := float64(large.spill) / float64(large.txns)
	smallPerTxn := float64(small.spill) / float64(small.txns)
	fmt.Printf("large-input analyze spill_per_txn=%.2f small_per_txn=%.2f\n", perTxn, smallPerTxn)
	if perTxn > maxDelaySpillPerTxn || perTxn > smallPerTxn*4 {
		t.Fatalf("apply-delay spill is %.1f bytes/transaction (small run %.1f); limit %.0f and 4x the small run",
			perTxn, smallPerTxn, maxDelaySpillPerTxn)
	}
}

func measureAnalyze(t *testing.T, path string, wantTxns int, fault string) largeRun {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	cpu0 := processCPUSeconds(t)
	var sa *largeInputAnalyzer
	jsonBody := captureStdout(t, func() {
		factory := func(opts analyzer.Options, _ *analyzer.DuckDBStore) commandAnalyzer {
			sa = &largeInputAnalyzer{inner: analyzer.New(opts), every: largeInputSampleEvery, fault: fault}
			return sa
		}
		if err := runAnalysisStreamingWithDeps([]string{path}, analyzer.DefaultOptions(), report.DefaultOptions(), "json", binlog.NewParser(), nil, factory, nil, ""); err != nil {
			t.Fatalf("analyze %s: %v", path, err)
		}
	})
	if sa == nil || sa.result == nil || !sa.finalizeSampled {
		t.Fatalf("analyze %s did not finish", path)
	}
	if sa.result.Summary.TotalTransactions != wantTxns {
		t.Fatalf("analyze %s saw %d transactions, want %d", path, sa.result.Summary.TotalTransactions, wantTxns)
	}
	formats := renderFormats(t, sa.result, wantTxns, &sa.peak)
	formats[1] = len(jsonBody)
	if !bytes.Contains(jsonBody, []byte(`"report_version"`)) || !reportHasTxnCount(jsonBody, wantTxns) {
		t.Fatalf("streaming json report missing report_version or transaction count %d (%d bytes)", wantTxns, len(jsonBody))
	}
	notePeak(&sa.peak)
	cpu := processCPUSeconds(t) - cpu0
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if sa.samples < 1 || sa.peak == 0 {
		t.Fatalf("analyze %s took no heap samples", path)
	}
	return largeRun{
		txns: wantTxns, events: sa.consumed, file: fi.Size(), peak: sa.peak,
		cpu: cpu, alloc: after.TotalAlloc - before.TotalAlloc, spill: sa.spill, formats: formats,
	}
}

func renderFormats(t *testing.T, result *model.AnalysisResult, txns int, peak *uint64) (out [4]int) {
	t.Helper()
	formats := []struct {
		name, marker string
	}{
		{"text", "==="},
		{"json", `"report_version"`},
		{"markdown", "| "},
		{"html", "<!DOCTYPE html>"},
	}
	for i, format := range formats {
		var buf bytes.Buffer
		if err := report.Write(&buf, *result, report.DefaultOptions(), format.name); err != nil {
			t.Fatalf("render %s: %v", format.name, err)
		}
		if !bytes.Contains(buf.Bytes(), []byte(format.marker)) || !reportHasTxnCount(buf.Bytes(), txns) {
			t.Fatalf("%s report missing %q or transaction count %d (%d bytes)", format.name, format.marker, txns, buf.Len())
		}
		out[i] = buf.Len()
		notePeak(peak)
		runtime.KeepAlive(&buf)
	}
	return out
}

// reportHasTxnCount accepts the raw decimal and the comma form used by
// markdown and HTML cards (38,437).
func reportHasTxnCount(body []byte, n int) bool {
	raw := strconv.Itoa(n)
	if bytes.Contains(body, []byte(raw)) {
		return true
	}
	var b []byte
	for i, c := range raw {
		if i > 0 && (len(raw)-i)%3 == 0 {
			b = append(b, ',')
		}
		b = append(b, byte(c))
	}
	return bytes.Contains(body, b)
}

func measureFlashback(t *testing.T, path string) flashRun {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	cpu0 := processCPUSeconds(t)
	hp := &heapParser{inner: binlog.NewParser(), every: largeInputSampleEvery}
	opts := analyzer.DefaultOptions()
	opts.Flashback = true
	opts.FlashbackConflict = flashback.FlashbackConflictRefuse
	sqlN, hasShop := streamStdout(t, []byte("shop"), func() {
		factory := func(opts analyzer.Options, _ *analyzer.DuckDBStore) commandAnalyzer {
			return analyzer.New(opts)
		}
		if err := runAnalysisStreamingWithDeps([]string{path}, opts, report.Options{}, "text", hp, nil, factory, nil, ""); err != nil {
			t.Fatalf("flashback %s: %v", path, err)
		}
	})
	// hp.peak was taken on the parse goroutine. The SQL is counted and
	// discarded as it is written, so it is not part of this heap.
	cpu := processCPUSeconds(t) - cpu0
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if hp.events < 1 || hp.peak == 0 {
		t.Fatalf("flashback %s: events %d peak %d", path, hp.events, hp.peak)
	}
	// A short warmup stays under ChunkRows and never opens the row spill.
	// The measured runs check the spill themselves.
	if !hasShop {
		t.Fatalf("flashback %s SQL does not mention shop (%d bytes)", path, sqlN)
	}
	return flashRun{
		run: largeRun{
			events: hp.events, file: fi.Size(), peak: hp.peak, cpu: cpu,
			alloc: after.TotalAlloc - before.TotalAlloc, spill: hp.spill,
		},
		sql: sqlN,
	}
}

// largeInputAnalyzer samples live heap on the goroutine that feeds analyze.
type largeInputAnalyzer struct {
	inner           *analyzer.Analyzer
	every           int
	fault           string
	consumed        int
	samples         int
	peak            uint64
	spill           int64
	finalizeSampled bool
	result          *model.AnalysisResult
	kept            [][]byte
	pad             []byte
	sink            int
	paused          bool
}

func (s *largeInputAnalyzer) Consume(ev model.NormalizedEvent) error {
	if err := s.inner.Consume(ev); err != nil {
		return err
	}
	s.consumed++
	switch s.fault {
	case "retain":
		// 1KB owned by the analyzer, standing in for a regression that keeps
		// every event. The copy is the point: a pointer into a reused parser
		// buffer would not grow the heap.
		s.kept = append(s.kept, bytes.Repeat([]byte{1}, 1024))
	case "retain256":
		// One buffer for the whole run. Both sizes hold it, so large−small
		// stays flat and only the absolute heap cap fails.
		if s.pad == nil {
			s.pad = make([]byte, 256<<20)
			s.pad[0] = 1
		}
	case "quadratic":
		for i := 0; i < s.consumed; i += 32 {
			s.sink += i
		}
	case "slow":
		acc := s.sink
		for i := 0; i < slowFaultIters; i++ {
			acc += i
		}
		s.sink = acc
	case "pause":
		// Once, on the large file only (the small file stays under this
		// event count). Sleep is not process CPU.
		if !s.paused && s.consumed == 500_000 {
			s.paused = true
			time.Sleep(7 * time.Second)
		}
	}
	if s.consumed%s.every == 0 {
		s.sample()
	}
	return nil
}

func (s *largeInputAnalyzer) Finalize() (*model.AnalysisResult, error) {
	s.spill = deletedTempBytes("binlogviz-apply-delay-")
	s.sample()
	res, err := s.inner.Finalize()
	if err != nil {
		return nil, err
	}
	s.sample()
	s.finalizeSampled = true
	s.result = res
	runtime.KeepAlive(s.kept)
	runtime.KeepAlive(s.pad)
	runtime.KeepAlive(s.sink)
	return res, nil
}

func (s *largeInputAnalyzer) sample() {
	s.samples++
	notePeak(&s.peak)
}

func notePeak(peak *uint64) {
	runtime.GC()
	m := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(m)
	if m[0].Value.Kind() != metrics.KindUint64 {
		panic(fmt.Sprintf("/gc/heap/live:bytes unsupported: kind %v", m[0].Value.Kind()))
	}
	if live := m[0].Value.Uint64(); live > *peak {
		*peak = live
	}
}

// heapParser samples on the parse goroutine. Flashback's SQL writer type-asserts
// *analyzer.Analyzer, so the analyzer itself cannot be wrapped.
type heapParser struct {
	inner  binlog.Parser
	every  int
	events int
	peak   uint64
	spill  int64
}

func (p *heapParser) wrap(handler func(binlog.RawEvent) error) func(binlog.RawEvent) error {
	return func(raw binlog.RawEvent) error {
		if err := handler(raw); err != nil {
			return err
		}
		p.events++
		if p.events%p.every == 0 {
			notePeak(&p.peak)
			if n := deletedTempBytes("binlogviz-flashback-"); n > p.spill {
				p.spill = n
			}
		}
		return nil
	}
}

func (p *heapParser) ParseFiles(paths []string, handler func(binlog.RawEvent) error) error {
	err := p.inner.ParseFiles(paths, p.wrap(handler))
	p.finishParse()
	return err
}

func (p *heapParser) ParseFilesWithProgress(paths []string, onProgress func(binlog.ParseProgress), handler func(binlog.RawEvent) error) error {
	err := p.inner.(binlog.ProgressParser).ParseFilesWithProgress(paths, onProgress, p.wrap(handler))
	p.finishParse()
	return err
}

func (p *heapParser) SetCaptureFlashback(on bool) {
	p.inner.(binlog.FlashbackParser).SetCaptureFlashback(on)
}

func (p *heapParser) SetCaptureRowImages(on bool) {
	p.inner.(binlog.RowImageParser).SetCaptureRowImages(on)
}

func (p *heapParser) finishParse() {
	notePeak(&p.peak)
	if n := deletedTempBytes("binlogviz-flashback-"); n > p.spill {
		p.spill = n
	}
}

// processCPUSeconds is this process's user+sys time. /cpu/classes/total:cpu-seconds
// is GOMAXPROCS times wall time, so a pause during the run moves it.
func processCPUSeconds(t *testing.T) float64 {
	t.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Fatal(err)
	}
	return float64(ru.Utime.Nano()+ru.Stime.Nano()) / 1e9
}

// spillStatAvailable reports whether deleted temp files can be sized.
// The check reads /proc/self/fd. Off Linux that directory is absent and the
// spill assertions are skipped; heap and CPU gates still run.
func spillStatAvailable() bool {
	if runtime.GOOS != "linux" {
		return false
	}
	f, err := os.Open("/proc/self/fd")
	if err != nil {
		return false
	}
	f.Close()
	return true
}

func deletedTempBytes(substr string) int64 {
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	var total int64
	for _, fd := range fds {
		link := "/proc/self/fd/" + fd.Name()
		target, err := os.Readlink(link)
		if err != nil || !strings.Contains(target, substr) {
			continue
		}
		// The script temp is created only after parse. Ignore it here so a
		// flashback sample is the row spill, not the generated SQL.
		if strings.Contains(target, "binlogviz-flashback-script-") {
			continue
		}
		fi, err := os.Stat(link)
		if err != nil {
			continue
		}
		total += fi.Size()
	}
	return total
}

func captureStdout(t *testing.T, fn func()) []byte {
	t.Helper()
	body, _ := readStdout(t, nil, fn)
	return body
}

// streamStdout counts what the command writes and reports whether needle
// appears. The body is not retained (a large flashback script is ~140MB).
func streamStdout(t *testing.T, needle []byte, fn func()) (n int64, found bool) {
	t.Helper()
	_, st := readStdout(t, needle, fn)
	return st.n, st.found
}

type stdoutStat struct {
	n     int64
	found bool
	body  []byte
}

func readStdout(t *testing.T, needle []byte, fn func()) ([]byte, stdoutStat) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	defer func() {
		os.Stdout = old
		_ = w.Close()
		_ = r.Close()
	}()
	done := make(chan stdoutStat, 1)
	go func() {
		var st stdoutStat
		buf := make([]byte, 256<<10)
		hold := 0
		if needle != nil {
			hold = len(needle) - 1
		}
		var prev []byte
		for {
			nr, readErr := r.Read(buf)
			if nr > 0 {
				chunk := buf[:nr]
				st.n += int64(nr)
				if needle == nil {
					st.body = append(st.body, chunk...)
				} else if !st.found {
					if bytes.Contains(chunk, needle) || (len(prev) > 0 && bytes.Contains(append(append([]byte{}, prev...), chunk...), needle)) {
						st.found = true
					}
					if hold > 0 {
						if nr >= hold {
							prev = append(prev[:0], chunk[nr-hold:]...)
						} else {
							prev = append(prev, chunk...)
							if len(prev) > hold {
								prev = append([]byte(nil), prev[len(prev)-hold:]...)
							}
						}
					}
				}
			}
			if readErr != nil {
				break
			}
		}
		done <- st
	}()
	fn()
	os.Stdout = old
	_ = w.Close()
	st := <-done
	return st.body, st
}

func scaledTxnCounts(t *testing.T, tp syntheticTemplate) (smallN, largeN int) {
	t.Helper()
	header := len(tp.magic)
	for _, ev := range tp.header {
		header += len(ev)
	}
	txn := 0
	for _, ev := range tp.tmpl {
		txn += len(ev)
	}
	if txn <= 0 || header >= largeInputMinBytes {
		t.Fatalf("template header %d txn %d", header, txn)
	}
	largeN = (largeInputMinBytes - header + txn - 1) / txn
	smallN = largeN / 8
	if smallN < 8192 {
		t.Fatalf("small run has %d transactions, want at least 8192 so the delay spill is in use", smallN)
	}
	return smallN, largeN
}

func writeScaledBinlog(t *testing.T, tp syntheticTemplate, path string, n int) int64 {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)
	if _, err := w.Write(tp.magic); err != nil {
		t.Fatal(err)
	}
	pos := uint32(len(tp.magic))
	emit := func(ev []byte) {
		pos += uint32(len(ev))
		binary.LittleEndian.PutUint32(ev[13:], pos)
		binary.LittleEndian.PutUint32(ev[len(ev)-4:], crc32.ChecksumIEEE(ev[:len(ev)-4]))
		if _, err := w.Write(ev); err != nil {
			t.Fatal(err)
		}
	}
	for _, ev := range tp.header {
		emit(append([]byte(nil), ev...))
	}
	for i := 0; i < n; i++ {
		for j := range tp.tmpl {
			emit(tp.event(j, i, i))
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}
