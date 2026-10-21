package binlogviz

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/exec"
	"runtime"
	"runtime/metrics"
	"sort"
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
// transactions), spread over 256 tables and one commit a second, so the
// small file spans about 641 minutes and the large one about 5126. It
// runs analyze, then flashback generate, on each. Analyze goes through the real parser and aggregator and writes json, text,
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
		// Above the passing heap (~25MB analyze and flashback) so GC stays
		// on the unlimited path. High enough that a retained buffer still
		// reaches the absolute heap assertion.
		"GOMEMLIMIT=2GiB",
	)
	cmd.WaitDelay = 3 * time.Second
	setChildDeathSignal(cmd)
	out, runErr := cmd.CombinedOutput()
	t.Logf("%s", out)
	// Judge first, then write the summary, so the summary carries the
	// parent's verdict and the failing check.
	outcome := evaluateLargeInputChild(string(out), runErr)
	body, alert := outcome.summary()
	t.Logf("%s", body)
	if alert {
		fmt.Fprintln(os.Stderr, body)
	}
	if err := appendStepSummary(os.Getenv("GITHUB_STEP_SUMMARY"), body); err != nil {
		t.Errorf("TestLargeInputBaseline could not write the CI step summary: %v", err)
	}
	if outcome.err != nil {
		t.Fatalf("large-input baseline: %v\n%s", outcome.err, out)
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
// key per row. Keeping the row images is hundreds of bytes per row. 40 fails
// that, and a leak of 8 bytes per event, together with the per-minute budget.
const maxFlashbackBytesPerTxn = 40.0

// Live heap allowed per minute of time span, on top of the fixed caps. The
// analyzer keeps a minute bucket per minute and an activity point per table
// per minute; on this template (4 tables a minute) analyze measures about
// 2925 bytes per extra minute, with the same number on every run, and
// flashback about 1360 beside its 31 bytes per transaction. The budgets are
// about 1.2x and 1.5x: 16KB per minute fails by far, and with 60
// transactions a minute a leak of more than about 15 bytes per transaction
// (3 per event) exceeds the analyze slack.
const (
	maxAnalyzeHeapPerMinute   = 3584.0
	maxFlashbackHeapPerMinute = 2048.0
)

// slowFaultIters is fixed extra CPU per event for the slow fault. It is
// enough that analyze's process CPU per event clears maxAnalyzeCPUPerEvent
// while the large/small ratio stays near the size ratio.
const slowFaultIters = 170000

// largeInputFaults are the regressions BINLOGVIZ_LARGE_INPUT_FAULT can
// inject. Each one must fail the gate except pause; docs/large-input-baseline.md
// lists the measurements.
var largeInputFaults = map[string]bool{
	"":           true,
	"retain":     true, // analyze: 1KB copy of every event
	"retain256":  true, // analyze: one 256MiB buffer for the whole run
	"quadratic":  true, // analyze: O(n) extra work per event
	"slow":       true, // analyze: fixed extra CPU per event
	"pause":      true, // analyze: 7s sleep once (must pass)
	"slowfirst":  true, // analyze: slow on the first measured pair only (must pass, attempts=2)
	"pertable":   true, // analyze: 512KB kept per distinct table
	"minuteheap": true, // analyze: 16KB kept per minute bucket
	"minutescan": true, // analyze: every event walks every minute bucket
	"flashslow":  true, // flashback: fixed extra CPU per event
	"flashleak":  true, // flashback: 8 bytes kept per event
}

func largeInputFaultNames() string {
	var names []string
	for n := range largeInputFaults {
		if n != "" {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func largeInputChild(t *testing.T) {
	t.Helper()
	fault := os.Getenv("BINLOGVIZ_LARGE_INPUT_FAULT")
	if !largeInputFaults[fault] {
		t.Fatalf("BINLOGVIZ_LARGE_INPUT_FAULT=%q, want one of %s, or unset", fault, largeInputFaultNames())
	}
	if fault != "" {
		fmt.Printf("large-input fault=%s\n", fault)
	}

	tp := loadSyntheticTemplate(t)
	tp.stepMicros = largeInputStepMicros
	shape := newLargeShape(t, tp, largeInputTables)
	smallN, largeN := scaledTxnCounts(t, tp)
	dir := os.Getenv("BINLOGVIZ_LARGE_INPUT_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	warm := writeScaledBinlog(t, shape, dir+"/warm.bin", 2000)
	smallPath := dir + "/small.bin"
	largePath := dir + "/large.bin"
	smallBytes := writeScaledBinlog(t, shape, smallPath, smallN)
	largeBytes := writeScaledBinlog(t, shape, largePath, largeN)
	fmt.Printf("large-input large_bytes=%d small_bytes=%d warm_bytes=%d txns=%d,%d tables=%d minutes=%d,%d\n",
		largeBytes, smallBytes, warm, smallN, largeN, largeInputTables, spanMinutes(smallN), spanMinutes(largeN))
	fmt.Println(largeInputInput{Fault: fault, LargeBytes: largeBytes, SmallBytes: smallBytes, Txns: [2]int{smallN, largeN},
		Minutes: [2]int{spanMinutes(smallN), spanMinutes(largeN)}, Tables: largeInputTables}.line())
	if largeBytes < largeInputMinBytes {
		t.Fatalf("large binlog is %d bytes, want at least %d", largeBytes, largeInputMinBytes)
	}
	byteRatio := float64(largeBytes) / float64(smallBytes)
	if byteRatio < 7 || byteRatio > 9 {
		t.Fatalf("size ratio %.2f, want about 8 (small %d, large %d)", byteRatio, smallBytes, largeBytes)
	}

	// slowfirst is the slow fault on the first measured analyze pair only:
	// a host stall in miniature. The retry must absorb it and pass.
	analyzeFault := func(attempt int) string {
		if fault != "slowfirst" {
			return fault
		}
		if attempt == 1 {
			return "slow"
		}
		return ""
	}
	measureAnalyze(t, dir+"/warm.bin", 2000, analyzeFault(0)) // warmup: one-time report and runtime caches
	analyze := runGatedPair("analyze", func(attempt int) (largeRun, largeRun) {
		small := measureAnalyze(t, smallPath, smallN, analyzeFault(attempt))
		large := measureAnalyze(t, largePath, largeN, analyzeFault(attempt))
		small.minutes, large.minutes = spanMinutes(smallN), spanMinutes(largeN)
		printAnalyze("small", small)
		printAnalyze("large", large)
		return small, large
	}, func(small, large largeRun) pairResult {
		return checkScale("analyze", small, large, byteRatio, true, 0)
	}, func(msg string) { fmt.Printf("large-input analyze retry: %s\n", msg) })
	fmt.Println(analyze.line())
	if err := analyze.err(); err != nil {
		t.Fatal(err)
	}

	measureFlashback(t, dir+"/warm.bin", fault) // same warmup, on the flashback path
	flash := runGatedPair("flashback", func(int) (largeRun, largeRun) {
		smallFlash := measureFlashback(t, smallPath, fault)
		largeFlash := measureFlashback(t, largePath, fault)
		smallFlash.run.txns = smallN
		largeFlash.run.txns = largeN
		smallFlash.run.minutes, largeFlash.run.minutes = spanMinutes(smallN), spanMinutes(largeN)
		printFlash("small", smallFlash)
		printFlash("large", largeFlash)
		smallFlash.run.sql = smallFlash.sql
		largeFlash.run.sql = largeFlash.sql
		return smallFlash.run, largeFlash.run
	}, func(small, large largeRun) pairResult {
		r := checkScale("flashback", small, large, byteRatio, false, maxFlashbackBytesPerTxn)
		r.add(checkFlashbackOutput(small, large, byteRatio))
		return r
	}, func(msg string) { fmt.Printf("large-input flashback retry: %s\n", msg) })
	fmt.Println(flash.line())
	if err := flash.err(); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("large-input attempts analyze=%d flashback=%d\n", analyze.Attempts, flash.Attempts)
}

// largeInputTables is how many tables the generated transactions rotate
// through. Both files touch every table, so state kept per table is the
// same on both runs and only the absolute caps see it: 256 tables turn a
// 512KB-per-table buffer into 128MiB.
const largeInputTables = 256

// largeInputTablesPerMinute is how many tables each minute's transactions
// write; see largeShape.table.
const largeInputTablesPerMinute = 4

// largeInputStepMicros is the commit-time gap between transactions. At 1s
// the small file spans about 641 minutes and the large one about 5125
// (3.6 days), so per-minute-bucket state differs by about 4500 buckets
// between the runs and a scan of every bucket on every event is about
// 64 times more work on the large run.
const largeInputStepMicros = 1_000_000

func spanMinutes(txns int) int {
	return int(uint64(txns)*largeInputStepMicros/60_000_000) + 1
}

// largeShape spreads the template transaction over tables: transaction i
// writes table t_%06d (i mod tables), with its own table id in TABLE_MAP
// and UPDATE_ROWS. The name has the same length as the fixture's
// "counters", so event sizes, and the file sizes, do not change.
type largeShape struct {
	tp      syntheticTemplate
	tables  int
	nameOff int
}

func newLargeShape(t *testing.T, tp syntheticTemplate, tables int) largeShape {
	t.Helper()
	m, r := tp.tmpl[2], tp.tmpl[3]
	name := append([]byte{8}, "counters"...)
	off := bytes.Index(m, name)
	if off < 0 {
		t.Fatalf("TABLE_MAP template has no table name %q: %x", name[1:], m)
	}
	if !bytes.Equal(m[19:25], r[19:25]) {
		t.Fatalf("TABLE_MAP table id %x and UPDATE_ROWS table id %x differ", m[19:25], r[19:25])
	}
	if tables < 1 || tables > 999_999 {
		t.Fatalf("tables %d", tables)
	}
	return largeShape{tp: tp, tables: tables, nameOff: off + 1}
}

// table is the table transaction i writes. Each minute writes
// largeInputTablesPerMinute tables, and the next minute moves on to the next
// ones, so both files touch all of them but the number of (table, minute)
// activity points is a fixed multiple of the minutes, not of the
// transactions. The analyzer keeps one point per table per minute (about
// 220 bytes); rotating every transaction through all tables would make that
// a per-transaction cost and hide a per-transaction leak inside it.
func (s largeShape) table(i int) int {
	minute := uint64(i) * s.tp.stepMicros / 60_000_000
	return int((minute*largeInputTablesPerMinute + uint64(i%largeInputTablesPerMinute)) % uint64(s.tables))
}

func (s largeShape) event(j, i int) []byte {
	ev := s.tp.event(j, i, i)
	if j != 2 && j != 3 {
		return ev
	}
	tbl := s.table(i)
	id := uint64(1000 + tbl)
	for k := 0; k < 6; k++ {
		ev[19+k] = byte(id >> (8 * k))
	}
	if j == 2 {
		copy(ev[s.nameOff:], fmt.Sprintf("t_%06d", tbl))
	}
	return ev
}

// gateFailure is one failed gate: the check's name, the measured value and
// its cap, and the message. CPU failures (the process CPU ratio and the
// per-event CPU cap) are the only ones a noisy host can produce: heap,
// allocation, spill and output size do not move under load.
type gateFailure struct {
	CPU   bool    `json:"cpu,omitempty"`
	Check string  `json:"check"`
	Value float64 `json:"value"`
	Cap   float64 `json:"cap"`
	Msg   string  `json:"msg"`
}

func (f gateFailure) String() string {
	if f.CPU {
		return "cpu: " + f.Msg
	}
	return f.Msg
}

func joinFailures(fs []gateFailure) string {
	parts := make([]string, len(fs))
	for i, f := range fs {
		parts[i] = f.String()
	}
	return strings.Join(parts, "; ")
}

// pairResult is one small/large pair's gauges and failed gates.
type pairResult struct {
	gauges []gauge
	fails  []gateFailure
}

func (r *pairResult) add(o pairResult) {
	r.gauges = append(r.gauges, o.gauges...)
	r.fails = append(r.fails, o.fails...)
}

// largeInputVerdict is the child's final record for one path, printed as
// one "large-input verdict {json}" line. The parent reads these instead of
// counting measurement lines, so a retried pair (four scale= lines) is
// judged on its verdict. Gauges and Failures are the last pair's; Retried
// holds the first pair's failures when it was measured again.
type largeInputVerdict struct {
	Path     string        `json:"path"`
	Attempts int           `json:"attempts"`
	Pass     bool          `json:"pass"`
	Gauges   []gauge       `json:"gauges"`
	Failures []gateFailure `json:"failures,omitempty"`
	Retried  []gateFailure `json:"retried,omitempty"`
}

func (v largeInputVerdict) err() error {
	if v.Pass {
		return nil
	}
	return fmt.Errorf("%s attempt %d: %s", v.Path, v.Attempts, joinFailures(v.Failures))
}

const largeInputVerdictPrefix = "large-input verdict "

func (v largeInputVerdict) line() string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return largeInputVerdictPrefix + string(b)
}

// runGatedPair measures a small/large pair and checks it. When every
// failure is a CPU failure, it measures the whole pair again, small and
// large, and fails if the second pair fails any gate. Any other failure
// fails at once. One host stall inflates one pair's CPU (QA saw 1 in 82
// runs under 8 saturated cores, ratio 17.23 against 16); a super-linear
// step fails every pair, so a second pair cannot hide it. measure gets the
// attempt number (1 or 2).
func runGatedPair(path string, measure func(attempt int) (largeRun, largeRun), check func(small, large largeRun) pairResult, retry func(string)) largeInputVerdict {
	v := largeInputVerdict{Path: path}
	for v.Attempts = 1; ; v.Attempts++ {
		small, large := measure(v.Attempts)
		r := check(small, large)
		v.Gauges, v.Failures = r.gauges, r.fails
		if len(r.fails) == 0 {
			v.Pass = true
			return v
		}
		if v.Attempts > 1 || !allCPU(r.fails) {
			return v
		}
		v.Retried = r.fails
		retry("CPU-only failure on attempt 1, measuring the pair again: " + joinFailures(r.fails))
	}
}

func allCPU(fs []gateFailure) bool {
	for _, f := range fs {
		if !f.CPU {
			return false
		}
	}
	return true
}

func checkFlashbackOutput(small, large largeRun, byteRatio float64) pairResult {
	var r pairResult
	sqlRatio := float64(large.sql) / float64(small.sql)
	fmt.Printf("large-input flashback sql_ratio=%.2f\n", sqlRatio)
	r.gauges = append(r.gauges, gauge{"sql_ratio", sqlRatio, byteRatio * 2, true})
	if sqlRatio < byteRatio/2 || sqlRatio > byteRatio*2 {
		r.fails = append(r.fails, gateFailure{Check: "sql_ratio", Value: sqlRatio, Cap: byteRatio * 2, Msg: fmt.Sprintf("flashback SQL scaled %.2f from the small file to the large one, want between %.2f and %.2f (bytes %d -> %d)",
			sqlRatio, byteRatio/2, byteRatio*2, small.sql, large.sql)})
	}
	if !spillStatAvailable() {
		fmt.Printf("large-input flashback spill skipped: /proc/self/fd is not available\n")
		return r
	}
	if large.spill <= 0 || small.spill <= 0 {
		r.fails = append(r.fails, gateFailure{Check: "flashback_spill", Value: float64(large.spill), Msg: fmt.Sprintf("flashback spill missing (small %d, large %d)", small.spill, large.spill)})
		return r
	}
	perByte := func(r largeRun) float64 { return float64(r.spill) / float64(r.file) }
	fmt.Printf("large-input flashback spill_per_byte=%.3f\n", perByte(large))
	r.gauges = append(r.gauges, gauge{"spill_per_byte", perByte(large), perByte(small) * 2, true})
	if perByte(large) > perByte(small)*2 {
		r.fails = append(r.fails, gateFailure{Check: "spill_per_byte", Value: perByte(large), Cap: perByte(small) * 2, Msg: fmt.Sprintf("flashback spill grew super-linearly: %.3f bytes of spill per input byte, small run %.3f",
			perByte(large), perByte(small))})
	}
	return r
}

type largeRun struct {
	txns, events int
	minutes      int
	file         int64
	peak         uint64
	cpu          float64
	alloc        uint64
	spill        int64
	sql          int64
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

func absoluteCaps(what string) (maxHeap uint64, maxAlloc, maxCPU, heapPerMinute float64) {
	if what == "analyze" {
		return maxAnalyzeLiveHeap, maxAnalyzeAllocPerEvent, maxAnalyzeCPUPerEvent, maxAnalyzeHeapPerMinute
	}
	return maxFlashbackLiveHeap, maxFlashbackAllocPerEvent, maxFlashbackCPUPerEvent, maxFlashbackHeapPerMinute
}

// nearCapFactor marks a measurement as near its cap in the summary: within
// 1.5x, that is measured × 1.5 ≥ cap.
const nearCapFactor = 1.5

// gauge is one gated value and its cap, for the summary line.
type gauge struct {
	Name  string  `json:"name"`
	Value float64 `json:"value"`
	Cap   float64 `json:"cap"`
	// Tight marks a deterministic gate set on purpose close to the
	// measurement (the heap growth budget, the allocation, spill and SQL
	// ratios). It is shown but never reported as near its cap.
	Tight bool `json:"tight,omitempty"`
}

// over is a value past its cap, tight or not.
func (g gauge) over() bool { return g.Value > g.Cap }

func (g gauge) near() bool { return !g.Tight && !g.over() && g.Value*nearCapFactor >= g.Cap }

// summaryLine is the one line per pair that the parent copies to the CI
// step summary. Values within 1.5x of their cap are listed after near_cap=.
func summaryLine(what string, gs []gauge) string {
	var b strings.Builder
	fmt.Fprintf(&b, "large-input summary %s", what)
	var near, over []string
	for _, g := range gs {
		fmt.Fprintf(&b, " %s=%s/%s(%.2fx)", g.Name, fmtGauge(g.Value), fmtGauge(g.Cap), g.Cap/g.Value)
		switch {
		case g.over():
			over = append(over, g.Name)
		case g.near():
			near = append(near, g.Name)
		}
	}
	if len(over) > 0 {
		fmt.Fprintf(&b, " over_cap=%s", strings.Join(over, ","))
	}
	if len(near) > 0 {
		fmt.Fprintf(&b, " near_cap=%s", strings.Join(near, ","))
	}
	return b.String()
}

func fmtGauge(v float64) string {
	if v >= 1000 || v == float64(int64(v)) {
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
	return strconv.FormatFloat(v, 'f', 2, 64)
}

func checkScale(what string, small, large largeRun, byteRatio float64, analyzeSpill bool, heapPerTxn float64) pairResult {
	var fails []gateFailure
	fail := func(cpu bool, check string, value, cap float64, format string, args ...any) {
		fails = append(fails, gateFailure{CPU: cpu, Check: check, Value: value, Cap: cap, Msg: fmt.Sprintf(format, args...)})
	}
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
	maxHeap, maxAlloc, maxCPU, maxHeapPerMinute := absoluteCaps(what)
	maxHeap += uint64(maxHeapPerMinute * float64(large.minutes))
	cpuPer := large.cpu / float64(large.events)
	gauges := []gauge{
		{"cpu_ratio", cpuRatio, limitCPU, false},
		{"cpu_ns_per_event", cpuPer * 1e9, maxCPU * 1e9, false},
		{"peak_heap", float64(large.peak), float64(maxHeap), false},
		{"alloc_per_event", largePer, maxAlloc, false},
		{"alloc_ratio", allocRatio, largeInputAllocSlack, true},
	}
	// Live heap is modelled as fixed + per minute of time span (+ per
	// transaction for flashback's conflict keys). The two files span
	// different minutes, so the per-minute state is budgeted, not ignored:
	// growing it fails too.
	extraMinutes := large.minutes - small.minutes
	extraTxns := large.txns - small.txns
	allowExtra := maxLargeInputExtraHeap + int64(maxHeapPerMinute*float64(extraMinutes)) + int64(heapPerTxn*float64(extraTxns))
	gauges = append(gauges, gauge{"extra_heap", float64(max(extra, 1)), float64(allowExtra), true})
	fmt.Printf("large-input %s extra_heap=%d allowed=%d (1MiB + %.0f B x %d minutes + %.0f B x %d transactions) per_extra_minute=%.0f per_extra_txn=%.2f\n",
		what, extra, allowExtra, maxHeapPerMinute, extraMinutes, heapPerTxn, extraTxns, float64(extra)/float64(max(extraMinutes, 1)), float64(extra)/float64(max(extraTxns, 1)))
	if extra > allowExtra {
		fail(false, "extra_heap", float64(extra), float64(allowExtra), "%s live heap grew %d bytes (%d -> %d) over %d more minutes and %d more transactions; limit %d (1MiB + %.0f B/minute + %.0f B/transaction)",
			what, extra, small.peak, large.peak, extraMinutes, extraTxns, allowExtra, maxHeapPerMinute, heapPerTxn)
	}
	if cpuRatio > limitCPU {
		fail(true, "cpu_ratio", cpuRatio, limitCPU, "%s process CPU ratio %.2f exceeds %.2f (%.2fx the %.2f size ratio; fails only above %.2fx the linear extrapolation from the small run); small %.3fs large %.3fs",
			what, cpuRatio, limitCPU, largeInputCPUSlack, byteRatio, largeInputCPUSlack, small.cpu, large.cpu)
	}
	if allocRatio > largeInputAllocSlack {
		fail(false, "alloc_ratio", allocRatio, largeInputAllocSlack, "%s allocated %.0f bytes/event on the large run and %.0f on the small one (ratio %.2f, limit %.2f)",
			what, largePer, smallPer, allocRatio, largeInputAllocSlack)
	}
	fmt.Printf("large-input %s absolute peak=%d limit_peak=%d alloc_per_event=%.0f limit_alloc=%.0f cpu_ns_per_event=%.0f limit_cpu_ns=%.0f\n",
		what, large.peak, maxHeap, largePer, maxAlloc, cpuPer*1e9, maxCPU*1e9)
	if large.peak > maxHeap {
		fail(false, "peak_heap", float64(large.peak), float64(maxHeap), "%s live heap after GC is %d bytes; limit %d (fixed cap + %.0f B x %d minutes)", what, large.peak, maxHeap, maxHeapPerMinute, large.minutes)
	}
	if largePer > maxAlloc {
		fail(false, "alloc_per_event", largePer, maxAlloc, "%s allocated %.0f bytes/event; limit %.0f", what, largePer, maxAlloc)
	}
	if cpuPer > maxCPU {
		fail(true, "cpu_ns_per_event", cpuPer*1e9, maxCPU*1e9, "%s process CPU is %.0f ns/event (getrusage user+sys, %.3fs over %d events); limit %.0f ns/event",
			what, cpuPer*1e9, large.cpu, large.events, maxCPU*1e9)
	}
	defer func() { fmt.Println(summaryLine(what, gauges)) }()
	result := func() pairResult { return pairResult{gauges: gauges, fails: fails} }

	if !analyzeSpill {
		return result()
	}
	if !spillStatAvailable() {
		fmt.Printf("large-input %s spill skipped: /proc/self/fd is not available\n", what)
		return result()
	}
	if small.spill <= 0 || large.spill <= 0 {
		fail(false, "delay_spill", float64(large.spill), 0, "apply-delay spill missing (small %d, large %d); delays would be staying in memory", small.spill, large.spill)
		return result()
	}
	perTxn := float64(large.spill) / float64(large.txns)
	smallPerTxn := float64(small.spill) / float64(small.txns)
	fmt.Printf("large-input analyze spill_per_txn=%.2f small_per_txn=%.2f\n", perTxn, smallPerTxn)
	if perTxn > maxDelaySpillPerTxn || perTxn > smallPerTxn*4 {
		fail(false, "spill_per_txn", perTxn, min(maxDelaySpillPerTxn, smallPerTxn*4), "apply-delay spill is %.1f bytes/transaction (small run %.1f); limit %.0f and 4x the small run",
			perTxn, smallPerTxn, maxDelaySpillPerTxn)
	}
	gauges = append(gauges, gauge{"spill_per_txn", perTxn, min(maxDelaySpillPerTxn, smallPerTxn*4), true})
	return result()
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
	// The JSON is scanned as it is written, not kept: it grows with the
	// time span (about 13MB on the large file), and the live heap sampled
	// below is the analyzer's, not the test's copy of the output.
	jsonScan := newReportScan(wantTxns, `"report_version"`)
	streamStdout(t, jsonScan, func() {
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
	formats[1] = int(jsonScan.n)
	if !jsonScan.ok() {
		t.Fatalf("streaming json report missing report_version or transaction count %d (%d bytes)", wantTxns, jsonScan.n)
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
		scan := newReportScan(txns, format.marker)
		if err := report.Write(scan, *result, report.DefaultOptions(), format.name); err != nil {
			t.Fatalf("render %s: %v", format.name, err)
		}
		if !scan.ok() {
			t.Fatalf("%s report missing %q or transaction count %d (%d bytes)", format.name, format.marker, txns, scan.n)
		}
		out[i] = int(scan.n)
		notePeak(peak)
	}
	return out
}

// reportScan counts the bytes of a report and looks for needles as they
// stream past, so the report is never held in memory. It keeps the last
// (longest needle − 1) bytes to find a needle split across writes.
type reportScan struct {
	needles [][]byte
	found   []bool
	tail    []byte
	n       int64
}

// newReportScan wants marker and the transaction count, as the raw decimal
// or the comma form markdown and HTML cards use (38,437).
func newReportScan(txns int, marker string) *reportScan {
	raw := strconv.Itoa(txns)
	var comma []byte
	for i, c := range raw {
		if i > 0 && (len(raw)-i)%3 == 0 {
			comma = append(comma, ',')
		}
		comma = append(comma, byte(c))
	}
	return newNeedleScan([]byte(marker), []byte(raw), comma)
}

func newNeedleScan(needles ...[]byte) *reportScan {
	return &reportScan{needles: needles, found: make([]bool, len(needles))}
}

// ok is the marker plus either form of the count.
func (s *reportScan) ok() bool { return s.found[0] && (s.found[1] || s.found[2]) }

func (s *reportScan) Write(p []byte) (int, error) {
	s.n += int64(len(p))
	window := append(s.tail, p...)
	keep := 0
	for i, nd := range s.needles {
		if !s.found[i] && bytes.Contains(window, nd) {
			s.found[i] = true
		}
		keep = max(keep, len(nd)-1)
	}
	if len(window) > keep {
		window = window[len(window)-keep:]
	}
	s.tail = append(s.tail[:0:0], window...)
	return len(p), nil
}

func measureFlashback(t *testing.T, path, fault string) flashRun {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	cpu0 := processCPUSeconds(t)
	hp := &heapParser{inner: binlog.NewParser(), every: largeInputSampleEvery, fault: fault}
	opts := analyzer.DefaultOptions()
	opts.Flashback = true
	opts.FlashbackConflict = flashback.FlashbackConflictRefuse
	sqlScan := newNeedleScan([]byte("shop"))
	streamStdout(t, sqlScan, func() {
		factory := func(opts analyzer.Options, _ *analyzer.DuckDBStore) commandAnalyzer {
			return analyzer.New(opts)
		}
		if err := runAnalysisStreamingWithDeps([]string{path}, opts, report.Options{}, "text", hp, nil, factory, nil, ""); err != nil {
			t.Fatalf("flashback %s: %v", path, err)
		}
	})
	runtime.KeepAlive(hp.kept)
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
	if !sqlScan.found[0] {
		t.Fatalf("flashback %s SQL does not mention shop (%d bytes)", path, sqlScan.n)
	}
	return flashRun{
		run: largeRun{
			events: hp.events, file: fi.Size(), peak: hp.peak, cpu: cpu,
			alloc: after.TotalAlloc - before.TotalAlloc, spill: hp.spill,
		},
		sql: sqlScan.n,
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
	tables          map[string][]byte
	minutes         map[int64]int
	minuteBufs      map[int64][]byte
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
	case "pertable":
		// State sized per table rather than per event: invisible with one
		// table, 128MiB across the template's 256.
		if ev.Table != "" {
			key := ev.Schema + "." + ev.Table
			if s.tables == nil {
				s.tables = map[string][]byte{}
			}
			if _, ok := s.tables[key]; !ok {
				b := make([]byte, 512<<10)
				b[0] = 1
				s.tables[key] = b
			}
		}
	case "minuteheap":
		// 16KB per minute bucket: about 70MiB more on the large run, whose
		// span has about 4500 more minutes.
		m := ev.Timestamp.Unix() / 60
		if s.minuteBufs == nil {
			s.minuteBufs = map[int64][]byte{}
		}
		if _, ok := s.minuteBufs[m]; !ok {
			b := make([]byte, 16<<10)
			b[0] = 1
			s.minuteBufs[m] = b
		}
	case "minutescan":
		// A per-event walk over every minute bucket seen so far.
		m := ev.Timestamp.Unix() / 60
		if s.minutes == nil {
			s.minutes = map[int64]int{}
		}
		s.minutes[m]++
		for _, c := range s.minutes {
			s.sink += c
		}
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
	runtime.KeepAlive(s.tables)
	runtime.KeepAlive(s.minuteBufs)
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
	fault  string
	kept   [][8]byte
	sink   int
}

func (p *heapParser) wrap(handler func(binlog.RawEvent) error) func(binlog.RawEvent) error {
	return func(raw binlog.RawEvent) error {
		if err := handler(raw); err != nil {
			return err
		}
		p.events++
		switch p.fault {
		case "flashslow":
			acc := p.sink
			for i := 0; i < slowFaultIters; i++ {
				acc += i
			}
			p.sink = acc
		case "flashleak":
			// 8 bytes per event kept to the end of the parse, about 40
			// bytes per transaction: the leak QA's #382 review saw pass
			// the old 100 B/transaction cap.
			p.kept = append(p.kept, [8]byte{1})
		}
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

// streamStdout runs fn with os.Stdout on a pipe and copies what fn writes
// into w. Nothing is retained beyond w (a large flashback script is ~140MB).
func streamStdout(t *testing.T, w io.Writer, fn func()) {
	t.Helper()
	r, pw, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = pw
	defer func() {
		os.Stdout = old
		_ = pw.Close()
		_ = r.Close()
	}()
	done := make(chan error, 1)
	go func() {
		_, err := io.CopyBuffer(w, r, make([]byte, 256<<10))
		done <- err
	}()
	fn()
	os.Stdout = old
	_ = pw.Close()
	if err := <-done; err != nil {
		t.Fatalf("read stdout: %v", err)
	}
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

func writeScaledBinlog(t *testing.T, shape largeShape, path string, n int) int64 {
	t.Helper()
	tp := shape.tp
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
			emit(shape.event(j, i))
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
