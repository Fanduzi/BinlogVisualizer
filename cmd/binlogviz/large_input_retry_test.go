package binlogviz

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

const testByteRatio = 8.0

// syntheticAnalyzePair is a clean analyze pair shaped like the measured
// runs, with the CPU seconds given.
func syntheticAnalyzePair(cpuSmall, cpuLarge float64) (largeRun, largeRun) {
	small := largeRun{txns: 38437, events: 192185, minutes: 641, peak: 11_450_000, cpu: cpuSmall, alloc: 192185 * 1966, file: 13107174, spill: 262144}
	large := largeRun{txns: 307500, events: 1537500, minutes: 5126, peak: 24_580_000, cpu: cpuLarge, alloc: 1537500 * 1966, file: 104857657, spill: 2424832}
	return small, large
}

func syntheticFlashPair() (largeRun, largeRun) {
	small := largeRun{txns: 38437, events: 192185, minutes: 641, peak: 10_170_000, cpu: 3.3, alloc: 192185 * 4439, file: 13107174, spill: 7_600_000, sql: 17_500_000}
	large := largeRun{txns: 307500, events: 1537500, minutes: 5126, peak: 24_590_000, cpu: 26.4, alloc: 1537500 * 4439, file: 104857657, spill: 60_800_000, sql: 140_823_325}
	return small, large
}

type scriptedPair struct{ small, large largeRun }

func pairOf(small, large largeRun) scriptedPair { return scriptedPair{small, large} }

func checkAnalyzePair(small, large largeRun) pairResult {
	return checkScale("analyze", small, large, testByteRatio, true, 0)
}

func checkFlashPair(small, large largeRun) pairResult {
	r := checkScale("flashback", small, large, testByteRatio, false, maxFlashbackBytesPerTxn)
	r.add(checkFlashbackOutput(small, large, testByteRatio))
	return r
}

// runScripted runs runGatedPair over scripted pairs and reports how many
// pairs it measured and how many retries it announced.
func runScripted(t *testing.T, path string, pairs []scriptedPair, check func(small, large largeRun) pairResult) (v largeInputVerdict, measured, retries int) {
	t.Helper()
	v = runGatedPair(path, func(attempt int) (largeRun, largeRun) {
		if attempt != measured+1 {
			t.Fatalf("attempt %d after %d pairs", attempt, measured)
		}
		if measured >= len(pairs) {
			t.Fatalf("measured %d pairs, only %d scripted", measured+1, len(pairs))
		}
		p := pairs[measured]
		measured++
		return p.small, p.large
	}, check, func(string) { retries++ })
	return v, measured, retries
}

// TestLargeInputCPURetry pins the retry rule of TestLargeInputBaseline: a
// CPU-only failure measures the whole small/large pair once more and fails
// if that pair fails too; a heap, allocation or output failure is never
// retried. The quadratic case uses the shape QA measured for the quadratic
// fault (CPU ratio about 37 against a cap of 16): it fails on every pair, so
// the retry cannot turn it into a pass.
func TestLargeInputCPURetry(t *testing.T) {
	heapFail := func() []scriptedPair {
		s, l := syntheticAnalyzePair(1.319, 22.724)
		l.peak = 277_257_288
		return []scriptedPair{{s, l}, pairOf(syntheticAnalyzePair(0.85, 6.6))}
	}
	allocFail := func() []scriptedPair {
		s, l := syntheticAnalyzePair(0.85, 6.6)
		l.alloc *= 2
		return []scriptedPair{{s, l}, pairOf(syntheticAnalyzePair(0.85, 6.6))}
	}
	heapSecond := func() []scriptedPair {
		s, l := syntheticAnalyzePair(0.85, 6.6)
		l.peak = 60 << 20
		return []scriptedPair{pairOf(syntheticAnalyzePair(1.319, 22.724)), {s, l}}
	}
	cases := []struct {
		name         string
		pairs        []scriptedPair
		wantErr      string
		wantCheck    string
		wantAttempts int
	}{
		{name: "clean pair passes once", pairs: []scriptedPair{pairOf(syntheticAnalyzePair(0.85, 6.6))}, wantAttempts: 1},
		{
			// QA's flake: 8 saturated cores, small 1.319s, large 22.724s.
			name:         "one CPU flake then a clean pair passes",
			pairs:        []scriptedPair{pairOf(syntheticAnalyzePair(1.319, 22.724)), pairOf(syntheticAnalyzePair(0.85, 6.6))},
			wantAttempts: 2,
		},
		{
			name:         "quadratic fails both pairs",
			pairs:        []scriptedPair{pairOf(syntheticAnalyzePair(0.85, 0.85*37)), pairOf(syntheticAnalyzePair(0.85, 0.85*37.9))},
			wantErr:      "attempt 2: cpu: analyze process CPU ratio 37.90",
			wantCheck:    "cpu_ratio",
			wantAttempts: 2,
		},
		{
			// A slow per-event regression fails the absolute CPU cap, also CPU-only.
			name:         "uniform slowdown fails both pairs",
			pairs:        []scriptedPair{pairOf(syntheticAnalyzePair(9.5, 75)), pairOf(syntheticAnalyzePair(9.5, 75))},
			wantErr:      "attempt 2: cpu: analyze process CPU is 48780 ns/event",
			wantCheck:    "cpu_ns_per_event",
			wantAttempts: 2,
		},
		{name: "heap failure is not retried, even next to a CPU failure", pairs: heapFail(), wantErr: "attempt 1: analyze live heap grew", wantCheck: "extra_heap", wantAttempts: 1},
		{name: "allocation failure is not retried", pairs: allocFail(), wantErr: "attempt 1: analyze allocated 3932 bytes/event", wantCheck: "alloc_ratio", wantAttempts: 1},
		{name: "a second pair failing on heap fails", pairs: heapSecond(), wantErr: "attempt 2: analyze live heap", wantCheck: "extra_heap", wantAttempts: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, measured, retries := runScripted(t, "analyze", tc.pairs, checkAnalyzePair)
			if v.Attempts != tc.wantAttempts || measured != tc.wantAttempts {
				t.Fatalf("attempts %d, pairs measured %d; want %d", v.Attempts, measured, tc.wantAttempts)
			}
			if retries != tc.wantAttempts-1 || (len(v.Retried) > 0) != (tc.wantAttempts == 2) {
				t.Fatalf("retry callback ran %d times, %d retried failures; want %d", retries, len(v.Retried), tc.wantAttempts-1)
			}
			err := v.err()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected failure: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error %v, want it to contain %q", err, tc.wantErr)
			case tc.wantCheck != "" && v.Failures[0].Check != tc.wantCheck:
				t.Fatalf("first failed check %q, want %q (%s)", v.Failures[0].Check, tc.wantCheck, joinFailures(v.Failures))
			}
		})
	}
}

// childOutput renders what the child prints for the given verdicts: the
// human measurement lines (scale= once per measured pair), the records,
// and go test's result, as the real child does.
func childOutput(analyze largeInputVerdict, flash *largeInputVerdict) (string, error) {
	var b strings.Builder
	in := largeInputInput{LargeBytes: 104857657, SmallBytes: 13107174, Txns: [2]int{38437, 307500}, Minutes: [2]int{641, 5126}, Tables: 256}
	fmt.Fprintf(&b, "=== RUN   TestLargeInputBaseline\nlarge-input large_bytes=%d small_bytes=%d\n%s\n", in.LargeBytes, in.SmallBytes, in.line())
	for i := 0; i < analyze.Attempts; i++ {
		b.WriteString("large-input analyze scale=small txns=38437\nlarge-input analyze scale=large txns=307500\n")
		if i == 0 && len(analyze.Retried) > 0 {
			b.WriteString("large-input analyze retry: CPU-only failure on attempt 1, measuring the pair again: " + joinFailures(analyze.Retried) + "\n")
		}
	}
	b.WriteString(analyze.line() + "\n")
	if err := analyze.err(); err != nil {
		fmt.Fprintf(&b, "    large_input_baseline_test.go:330: %v\n--- FAIL: TestLargeInputBaseline (1.00s)\nFAIL\n", err)
		return b.String(), errors.New("exit status 1")
	}
	for i := 0; i < flash.Attempts; i++ {
		b.WriteString("large-input flashback scale=small events=192185\nlarge-input flashback scale=large events=1537500\n")
	}
	b.WriteString(flash.line() + "\n")
	if err := flash.err(); err != nil {
		fmt.Fprintf(&b, "    large_input_baseline_test.go:352: %v\n--- FAIL: TestLargeInputBaseline (1.00s)\nFAIL\n", err)
		return b.String(), errors.New("exit status 1")
	}
	fmt.Fprintf(&b, "large-input attempts analyze=%d flashback=%d\n--- PASS: TestLargeInputBaseline (1.00s)\nPASS\n", analyze.Attempts, flash.Attempts)
	return b.String(), nil
}

// TestLargeInputParentVerdict feeds the parent's judge child output built
// from real runGatedPair verdicts. #388 at b0f2ebff counted scale= lines
// and so failed every retried pair, even one that passed (QA's slowfirst
// mutant), and wrote "pass" to the step summary before failing.
func TestLargeInputParentVerdict(t *testing.T) {
	cleanFlash, _, _ := runScripted(t, "flashback", []scriptedPair{pairOf(syntheticFlashPair())}, checkFlashPair)
	if !cleanFlash.Pass {
		t.Fatalf("synthetic flashback pair does not pass: %s", joinFailures(cleanFlash.Failures))
	}
	heapPairs := func() []scriptedPair {
		s, l := syntheticAnalyzePair(0.85, 6.6)
		l.peak = 293_033_280
		return []scriptedPair{{s, l}}
	}()
	cases := []struct {
		name         string
		analyze      []scriptedPair
		wantPass     bool
		wantAttempts int
		wantCheck    string
		wantHead     string
		// wantOver must appear in over_cap=. extra_heap is a tight gauge;
		// b0f2ebff never flagged those, and labelled an over-cap value near_cap.
		wantOver string
	}{
		{name: "clean run", analyze: []scriptedPair{pairOf(syntheticAnalyzePair(0.85, 6.6))}, wantPass: true, wantAttempts: 1, wantHead: "### ✅ TestLargeInputBaseline pass"},
		{
			// slowfirst: the first pair at about 56µs/event, the retry clean.
			// Four analyze scale= lines. The parent at b0f2ebff required
			// exactly two, so this output failed there.
			name:         "CPU retry that then passes",
			analyze:      []scriptedPair{pairOf(syntheticAnalyzePair(9.4, 86.6)), pairOf(syntheticAnalyzePair(0.85, 6.6))},
			wantPass:     true,
			wantAttempts: 2,
			wantHead:     "### ⚠️ TestLargeInputBaseline pass, retried or near a cap",
		},
		{
			name:         "CPU retry that fails again",
			analyze:      []scriptedPair{pairOf(syntheticAnalyzePair(0.85, 0.85*34.2)), pairOf(syntheticAnalyzePair(0.85, 0.85*34.5))},
			wantAttempts: 2,
			wantCheck:    "cpu_ratio",
			wantHead:     "### ❌ TestLargeInputBaseline FAIL: analyze attempt 2: cpu: analyze process CPU ratio 34.50 exceeds 16.00",
			wantOver:     "cpu_ratio",
		},
		{
			name:         "heap failure is never retried",
			analyze:      heapPairs,
			wantAttempts: 1,
			wantCheck:    "extra_heap",
			wantHead:     "### ❌ TestLargeInputBaseline FAIL: analyze attempt 1: analyze live heap grew 281583280 bytes",
			wantOver:     "extra_heap",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			analyze, _, _ := runScripted(t, "analyze", tc.analyze, checkAnalyzePair)
			out, runErr := childOutput(analyze, &cleanFlash)
			scales := strings.Count(out, "large-input analyze scale=")
			if scales != 2*tc.wantAttempts {
				t.Fatalf("analyze scale= lines = %d, want %d", scales, 2*tc.wantAttempts)
			}
			o := evaluateLargeInputChild(out, runErr, false)
			if (o.err == nil) != tc.wantPass {
				t.Fatalf("parent verdict %v, want pass=%v\n%s", o.err, tc.wantPass, out)
			}
			if got := o.verdicts[0].Attempts; got != tc.wantAttempts {
				t.Fatalf("analyze attempts %d, want %d", got, tc.wantAttempts)
			}
			if tc.wantCheck != "" && (o.failing == nil || o.failing.Check != tc.wantCheck) {
				t.Fatalf("failing check %+v, want %q", o.failing, tc.wantCheck)
			}
			body, alert := o.summary()
			head, _, _ := strings.Cut(body, "\n")
			if !strings.HasPrefix(head, tc.wantHead) || (tc.wantPass && head != tc.wantHead) {
				t.Fatalf("summary heading:\n%s\nwant prefix %q", head, tc.wantHead)
			}
			if alert == (tc.wantAttempts == 1 && tc.wantPass) {
				t.Fatalf("alert=%v for %s", alert, tc.name)
			}
			if tc.wantAttempts == 2 && !strings.Contains(body, "analyze attempt 1 (retried): cpu:") {
				t.Fatalf("summary does not show the retried attempt:\n%s", body)
			}
			if !tc.wantPass && !strings.Contains(body, "Parent verdict: ") {
				t.Fatalf("failing summary has no parent verdict:\n%s", body)
			}
			if tc.wantOver != "" {
				over := flagList(body, "over_cap")
				if !strings.Contains(","+over+",", ","+tc.wantOver+",") {
					t.Fatalf("over_cap=%q, want %s\n%s", over, tc.wantOver, body)
				}
				if strings.Contains(","+flagList(body, "near_cap")+",", ","+tc.wantOver+",") {
					t.Fatalf("%s is listed near_cap\n%s", tc.wantOver, body)
				}
			} else if strings.Contains(body, "over_cap=") {
				t.Fatalf("passing summary flags over_cap:\n%s", body)
			}
			if tc.wantOver == "extra_heap" && !gaugeTight(o, "extra_heap") {
				t.Fatal("extra_heap is the tight heap-growth gauge")
			}
		})
	}

	// The parent does not take the child's word alone.
	cleanAnalyze, _, _ := runScripted(t, "analyze", []scriptedPair{pairOf(syntheticAnalyzePair(0.85, 6.6))}, checkAnalyzePair)
	good, _ := childOutput(cleanAnalyze, &cleanFlash)
	bad := []struct {
		name, out, want string
		runErr          error
	}{
		{"no flashback verdict", strings.Replace(good, cleanFlash.line()+"\n", "", 1), "printed 1 verdicts", nil},
		{"no PASS line", strings.Replace(good, "--- PASS: TestLargeInputBaseline", "", 1), "did not print --- PASS", nil},
		{"exit status without a failing verdict", good, "child exited with exit status 1", errors.New("exit status 1")},
		{"small large file", strings.Replace(good, `"large_bytes":104857657`, `"large_bytes":104857`, 1), "large file is 104857 bytes", nil},
		{"pass with a gauge over its cap", strings.Replace(good, `{"name":"cpu_ratio","value":7.`, `{"name":"cpu_ratio","value":17.`, 1), "says pass but cpu_ratio", nil},
		{"three attempts", strings.Replace(good, `"path":"flashback","attempts":1`, `"path":"flashback","attempts":3`, 1), "took 3 attempts", nil},
		{"garbled verdict", strings.Replace(good, `"path":"flashback"`, `"path":`, 1), "verdict line", nil},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			if tc.out == good && tc.runErr == nil {
				t.Fatalf("replacement did not apply")
			}
			o := evaluateLargeInputChild(tc.out, tc.runErr, false)
			if o.err == nil || !strings.Contains(o.err.Error(), tc.want) {
				t.Fatalf("parent verdict %v, want it to contain %q", o.err, tc.want)
			}
			if body, _ := o.summary(); !strings.Contains(body, "❌") {
				t.Fatalf("failing summary has no ❌:\n%s", body)
			}
		})
	}
}

func flagList(body, key string) string {
	token := key + "="
	i := strings.Index(body, token)
	if i < 0 {
		return ""
	}
	rest := body[i+len(token):]
	if j := strings.IndexAny(rest, " \n"); j >= 0 {
		rest = rest[:j]
	}
	return rest
}

func gaugeTight(o largeInputOutcome, name string) bool {
	for _, g := range o.verdicts[0].Gauges {
		if g.Name == name {
			return g.Tight
		}
	}
	return false
}

func TestLargeInputStepSummaryWriteError(t *testing.T) {
	if err := appendStepSummary("", "x"); err != nil {
		t.Fatalf("empty path: %v", err)
	}
	if err := appendStepSummary(t.TempDir()+"/missing/dir/summary.md", "x"); err == nil {
		t.Fatal("writing under a missing directory succeeded")
	}
}
