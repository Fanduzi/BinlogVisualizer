package binlogviz

import (
	"strings"
	"testing"
)

// TestLargeInputCPURetry pins the retry rule of TestLargeInputBaseline: a
// CPU-only failure measures the whole small/large pair once more and fails
// if that pair fails too; a heap, allocation or output failure is never
// retried. The quadratic case uses the shape QA measured for the quadratic
// fault (CPU ratio about 37 against a cap of 16): it fails on every pair, so
// the retry cannot turn it into a pass.
func TestLargeInputCPURetry(t *testing.T) {
	const byteRatio = 8.0
	clean := func(cpuSmall, cpuLarge float64) (largeRun, largeRun) {
		small := largeRun{txns: 38437, events: 192185, peak: 8_300_000, cpu: cpuSmall, alloc: 192185 * 1883}
		large := largeRun{txns: 307500, events: 1537500, peak: 8_400_000, cpu: cpuLarge, alloc: 1537500 * 1883}
		return small, large
	}
	check := func(small, large largeRun) []gateFailure {
		return checkScale("analyze", small, large, byteRatio, false, 0)
	}
	type pair struct{ small, large largeRun }
	cases := []struct {
		name         string
		pairs        []pair
		wantErr      string
		wantAttempts int
	}{
		{
			name:         "clean pair passes once",
			pairs:        []pair{largeRunPair(clean(0.85, 6.6))},
			wantAttempts: 1,
		},
		{
			// QA's flake: 8 saturated cores, small 1.319s, large 22.724s.
			name:         "one CPU flake then a clean pair passes",
			pairs:        []pair{largeRunPair(clean(1.319, 22.724)), largeRunPair(clean(0.85, 6.6))},
			wantAttempts: 2,
		},
		{
			name:         "quadratic fails both pairs",
			pairs:        []pair{largeRunPair(clean(0.85, 0.85*37)), largeRunPair(clean(0.85, 0.85*37.9))},
			wantErr:      "attempt 2: cpu: analyze process CPU ratio 37.90",
			wantAttempts: 2,
		},
		{
			// A slow per-event regression fails the absolute CPU cap, also CPU-only.
			name:         "uniform slowdown fails both pairs",
			pairs:        []pair{largeRunPair(clean(9.5, 75)), largeRunPair(clean(9.5, 75))},
			wantErr:      "attempt 2: cpu: analyze process CPU is 48780 ns/event",
			wantAttempts: 2,
		},
		{
			name: "heap failure is not retried, even next to a CPU failure",
			pairs: func() []pair {
				s, l := clean(1.319, 22.724)
				l.peak = 277_257_288
				return []pair{{s, l}, largeRunPair(clean(0.85, 6.6))}
			}(),
			wantErr:      "attempt 1: analyze live heap grew",
			wantAttempts: 1,
		},
		{
			name: "allocation failure is not retried",
			pairs: func() []pair {
				s, l := clean(0.85, 6.6)
				l.alloc = l.alloc * 2
				return []pair{{s, l}, largeRunPair(clean(0.85, 6.6))}
			}(),
			wantErr:      "attempt 1: analyze allocated 3766 bytes/event",
			wantAttempts: 1,
		},
		{
			name: "a second pair failing on heap fails",
			pairs: func() []pair {
				s, l := clean(0.85, 6.6)
				l.peak = 40 << 20
				return []pair{largeRunPair(clean(1.319, 22.724)), {s, l}}
			}(),
			wantErr:      "attempt 2: analyze live heap",
			wantAttempts: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			measured := 0
			retries := 0
			attempts, err := runGatedPair(func() (largeRun, largeRun) {
				if measured >= len(tc.pairs) {
					t.Fatalf("measured %d pairs, only %d scripted", measured+1, len(tc.pairs))
				}
				p := tc.pairs[measured]
				measured++
				return p.small, p.large
			}, check, func(string) { retries++ })
			if attempts != tc.wantAttempts || measured != tc.wantAttempts {
				t.Fatalf("attempts %d, pairs measured %d; want %d", attempts, measured, tc.wantAttempts)
			}
			if retries != tc.wantAttempts-1 {
				t.Fatalf("retry callback ran %d times, want %d", retries, tc.wantAttempts-1)
			}
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected failure: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func largeRunPair(small, large largeRun) struct{ small, large largeRun } {
	return struct{ small, large largeRun }{small, large}
}
