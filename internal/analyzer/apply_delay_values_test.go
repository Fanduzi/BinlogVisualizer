package analyzer

import (
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func sortedP95(values []int64) int64 {
	s := append([]int64(nil), values...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return percentile95(s)
}

// TestDelayValuesSpillMatchesSort is the #364 contract: spilling delays to
// disk and selecting the rank there gives the same p95 a full sort does.
func TestDelayValuesSpillMatchesSort(t *testing.T) {
	rng := rand.New(rand.NewSource(364))
	gens := map[string]func() int64{
		"wide":     func() int64 { return rng.Int63n(1<<40) - 1<<39 },
		"narrow":   func() int64 { return rng.Int63n(5) },
		"zero":     func() int64 { return 0 },
		"extremes": func() int64 { return []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}[rng.Intn(5)] },
		"ms":       func() int64 { return rng.Int63n(3_600_000) * 1000 },
	}
	for name, gen := range gens {
		for _, n := range []int{1, 2, 7, 8, 19, 20, 21, 99, 100, 101, 1000, 4097} {
			for _, spillAt := range []int{1, 3, 8, 64} {
				d := delayValues{spillAt: spillAt}
				var all []int64
				for i := 0; i < n; i++ {
					v := gen()
					all = append(all, v)
					d.add(v)
				}
				got, err := d.p95()
				if err != nil {
					t.Fatalf("%s n=%d spillAt=%d: %v", name, n, spillAt, err)
				}
				if want := sortedP95(all); got != want {
					t.Fatalf("%s n=%d spillAt=%d: p95 %d, sort gives %d", name, n, spillAt, got, want)
				}
				if n >= spillAt && d.spilled == 0 {
					t.Fatalf("%s n=%d spillAt=%d: nothing spilled", name, n, spillAt)
				}
				if len(d.mem) >= spillAt {
					t.Fatalf("%s n=%d spillAt=%d: %d values left in memory", name, n, spillAt, len(d.mem))
				}
				d.close()
			}
		}
	}
}

// A temp dir that cannot be used keeps every delay in memory: same answer.
func TestDelayValuesWithoutTempDirStayInMemory(t *testing.T) {
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "missing"))
	d := delayValues{spillAt: 4}
	var all []int64
	for i := 0; i < 50; i++ {
		v := int64((i * 7919) % 97)
		all = append(all, v)
		d.add(v)
	}
	if d.spilled != 0 || len(d.mem) != 50 {
		t.Fatalf("spilled %d, in memory %d", d.spilled, len(d.mem))
	}
	if got, err := d.p95(); err != nil || got != sortedP95(all) {
		t.Fatalf("p95 %d %v, want %d", got, err, sortedP95(all))
	}
}

// The spill file is gone once the tracker is closed, and on Unix at once.
func TestDelayValuesLeavesNoFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	d := delayValues{spillAt: 2}
	for i := 0; i < 10; i++ {
		d.add(int64(i))
	}
	if d.spilled == 0 {
		t.Fatal("nothing spilled")
	}
	d.close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spill file left behind: %v", entries)
	}
	if _, err := d.selectRank(1); err == nil {
		t.Fatal("reading a closed spill file did not fail")
	}
}
