package analyzer

import (
	"errors"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// refRanges renders sorted unique sequence numbers the way the set should.
func refRanges(seqs map[uint64]bool, single func(uint64) string, span func(a, b uint64) string) []string {
	var sorted []uint64
	for s := range seqs {
		sorted = append(sorted, s)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var out []string
	for i := 0; i < len(sorted); {
		j := i
		for j+1 < len(sorted) && sorted[j+1] == sorted[j]+1 {
			j++
		}
		if i == j {
			out = append(out, single(sorted[i]))
		} else {
			out = append(out, span(sorted[i], sorted[j]))
		}
		i = j + 1
	}
	return out
}

// TestMatchedGTIDSetMatchesReference is the #379 contract: the interval set
// counts and renders exactly the distinct GTIDs added, in any order.
func TestMatchedGTIDSetMatchesReference(t *testing.T) {
	const a, b = "24bc7850-2c16-11e6-a073-0242ac110002", "0aaaaaaa-2c16-11e6-a073-0242ac110002"
	rng := rand.New(rand.NewSource(379))
	for round := 0; round < 200; round++ {
		set := newMatchedGTIDSet()
		ref := map[string]map[uint64]bool{a: {}, b: {}}
		for i := 0; i < 1+rng.Intn(300); i++ {
			sid := a
			if rng.Intn(4) == 0 {
				sid = b
			}
			seq := uint64(1 + rng.Intn(120))
			if round%2 == 0 { // mostly binlog order
				seq = uint64(i + 1 + rng.Intn(3))
			}
			ref[sid][seq] = true
			if err := set.add(sid + ":" + strconv.FormatUint(seq, 10)); err != nil {
				t.Fatal(err)
			}
		}
		var want []string
		count := 0
		for _, sid := range []string{b, a} { // sorted by UUID
			if len(ref[sid]) == 0 {
				continue
			}
			count += len(ref[sid])
			parts := refRanges(ref[sid], func(s uint64) string { return strconv.FormatUint(s, 10) },
				func(x, y uint64) string { return strconv.FormatUint(x, 10) + "-" + strconv.FormatUint(y, 10) })
			want = append(want, sid+":"+strings.Join(parts, ":"))
		}
		if got := set.strings(); !reflect.DeepEqual(got, want) || set.count != count {
			t.Fatalf("round %d: got %v count %d, want %v count %d", round, got, set.count, want, count)
		}
	}
}

func TestMatchedGTIDSetMariaDBRanges(t *testing.T) {
	set := newMatchedGTIDSet()
	for _, id := range []string{"1-7-5", "0-7-3", "0-7-1", "0-7-2", "0-10-9", "0-7-3", "1-7-6", "0-7-8"} {
		if err := set.add(id); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{"0-7-1..3", "0-7-8", "0-10-9", "1-7-5..6"}
	if got := set.strings(); !reflect.DeepEqual(got, want) || set.count != 7 {
		t.Fatalf("got %v count %d, want %v count 7", got, set.count, want)
	}
}

// One million matches with one gap is two intervals; a new gap past the
// ceiling is a MatchedGTIDLimitError, which the CLI maps to exit 10.
func TestMatchedGTIDSetIntervalCeiling(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	set := newMatchedGTIDSet()
	for i := 1; i <= 1_000_000; i++ {
		if i == 5 {
			continue
		}
		if err := set.add(sid + ":" + strconv.Itoa(i)); err != nil {
			t.Fatal(err)
		}
	}
	if set.intervals != 2 || set.count != 999_999 {
		t.Fatalf("intervals %d count %d", set.intervals, set.count)
	}
	old := MatchedGTIDIntervalLimit
	MatchedGTIDIntervalLimit = 3
	defer func() { MatchedGTIDIntervalLimit = old }()
	if err := set.add(sid + ":2000000"); err != nil {
		t.Fatalf("third interval: %v", err)
	}
	if err := set.add(sid + ":1999999"); err != nil { // joins, no new interval
		t.Fatalf("joining add: %v", err)
	}
	err := set.add(sid + ":3000000")
	var limit *MatchedGTIDLimitError
	if !errors.As(err, &limit) || limit.Limit != 3 || limit.Intervals != 4 {
		t.Fatalf("fourth interval: %v", err)
	}
	if err := set.add(sid + ":5"); err != nil || set.intervals != 2 { // fills the gap
		t.Fatalf("fill gap: %v intervals %d", err, set.intervals)
	}
}
