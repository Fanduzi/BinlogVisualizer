package analyzer

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
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
// renders exactly the distinct GTIDs added, in any order, and counts every
// matched transaction, repeats included.
func TestMatchedGTIDSetMatchesReference(t *testing.T) {
	const a, b = "24bc7850-2c16-11e6-a073-0242ac110002", "0aaaaaaa-2c16-11e6-a073-0242ac110002"
	rng := rand.New(rand.NewSource(379))
	for round := 0; round < 200; round++ {
		set := newMatchedGTIDSet()
		ref := map[string]map[uint64]bool{a: {}, b: {}}
		adds := 1 + rng.Intn(300)
		for i := 0; i < adds; i++ {
			sid := a
			if rng.Intn(4) == 0 {
				sid = b
			}
			seq := uint64(1 + rng.Intn(120))
			switch round % 3 {
			case 0: // mostly binlog order
				seq = uint64(i + 1 + rng.Intn(3))
			case 1: // descending
				seq = uint64(1000 - i - rng.Intn(3))
			}
			ref[sid][seq] = true
			if err := set.add(sid + ":" + strconv.FormatUint(seq, 10)); err != nil {
				t.Fatal(err)
			}
		}
		var want []string
		for _, sid := range []string{b, a} { // sorted by UUID
			if len(ref[sid]) == 0 {
				continue
			}
			parts := refRanges(ref[sid], func(s uint64) string { return strconv.FormatUint(s, 10) },
				func(x, y uint64) string { return strconv.FormatUint(x, 10) + "-" + strconv.FormatUint(y, 10) })
			want = append(want, sid+":"+strings.Join(parts, ":"))
		}
		if got := set.strings(); !reflect.DeepEqual(got, want) || set.count != adds {
			t.Fatalf("round %d: got %v count %d, want %v count %d", round, got, set.count, want, adds)
		}
	}
}

// TestRangeListMatchesReference covers chunk splits, joins across chunk
// boundaries, and the uint64 edges, in ascending, descending and random order.
func TestRangeListMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(381))
	for round := 0; round < 60; round++ {
		var l rangeList
		ref := map[uint64]bool{}
		base := uint64(0)
		if round%4 == 3 {
			base = ^uint64(0) - 3000
		}
		for i := 0; i < 3000; i++ {
			var seq uint64
			switch round % 3 {
			case 0:
				seq = base + uint64(2*i)
			case 1:
				seq = base + uint64(6000-2*i)
			default:
				seq = base + uint64(rng.Intn(3001))
			}
			if round%4 == 3 && i == 0 {
				seq = ^uint64(0)
			}
			before := l.n
			delta, present := l.add(seq)
			if present != ref[seq] || l.n != before+delta {
				t.Fatalf("round %d add %d: present %v delta %d", round, seq, present, delta)
			}
			ref[seq] = true
		}
		var got []string
		l.each(func(r sequenceRange) { got = append(got, fmt.Sprintf("%d-%d", r.start, r.end)) })
		want := refRanges(ref, func(s uint64) string { return fmt.Sprintf("%d-%d", s, s) },
			func(x, y uint64) string { return fmt.Sprintf("%d-%d", x, y) })
		if !reflect.DeepEqual(got, want) || l.n != len(want) {
			t.Fatalf("round %d: %d ranges (n=%d), want %d", round, len(got), l.n, len(want))
		}
		for _, ch := range l.chunks {
			if len(ch) == 0 || len(ch) > rangeChunkMax {
				t.Fatalf("round %d: chunk of %d", round, len(ch))
			}
		}
	}
}

// matchedSetFrom reads every identity and adds the matched ones in the same
// step, as the analyzer does at drain.
func matchedSetFrom(t *testing.T, all []string, matched map[string]bool) (*matchedGTIDSet, error) {
	t.Helper()
	set := newMatchedGTIDSet()
	for _, id := range all {
		if err := set.read(id, matched[id]); err != nil {
			return set, err
		}
	}
	return set, nil
}

func TestMatchedGTIDSetMariaDBRanges(t *testing.T) {
	all := []string{"1-7-5", "0-7-3", "0-7-1", "0-7-2", "0-10-9", "0-7-3", "1-7-6", "0-7-8"}
	matched := map[string]bool{}
	for _, id := range all {
		matched[id] = true
	}
	set, err := matchedSetFrom(t, all, matched)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"0-7-1..3", "0-7-8", "0-10-9", "1-7-5..6"}
	if got := set.strings(); !reflect.DeepEqual(got, want) || set.count != 8 {
		t.Fatalf("got %v count %d, want %v count 8", got, set.count, want)
	}
}

// TestMatchedGTIDSetMariaDBAlternatingWriters is #381 QA F1: two servers
// writing one domain alternately is one shared range per server, not one
// interval per transaction, so it stays far below the ceiling.
func TestMatchedGTIDSetMariaDBAlternatingWriters(t *testing.T) {
	old := MatchedGTIDIntervalLimit
	MatchedGTIDIntervalLimit = 4
	defer func() { MatchedGTIDIntervalLimit = old }()
	var all []string
	matched := map[string]bool{}
	for seq := 1; seq <= 200_000; seq++ {
		id := fmt.Sprintf("0-%d-%d", 7+seq%2, seq)
		all = append(all, id)
		matched[id] = seq != 5
	}
	set, err := matchedSetFrom(t, all, matched)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"0-7-1..4", "0-7-6..200000", "0-8-1..4", "0-8-6..200000"}
	if got := set.strings(); !reflect.DeepEqual(got, want) || set.count != 199_999 {
		t.Fatalf("got %v count %d, want %v", got, set.count, want)
	}
	// A third writer in the same domain pushes the shared form to 6 entries,
	// over the lowered ceiling; exact per server is gone (dropped at 5).
	err = set.observe("0-9-200001")
	if err == nil {
		err = set.add("0-9-200001")
	}
	var limit *MatchedGTIDLimitError
	if !errors.As(err, &limit) || limit.Intervals != 6 {
		t.Fatalf("third writer: %v", err)
	}
}

// TestMatchedGTIDSetMariaDBSequenceConflict: when a sequence number of a
// domain appears twice (another server, or the same identity in two files),
// the shared form could name an unmatched transaction, so the domain is
// rendered per server.
func TestMatchedGTIDSetMariaDBSequenceConflict(t *testing.T) {
	all := []string{"0-7-1", "0-8-2", "0-7-3", "0-8-3", "0-7-4"}
	matched := map[string]bool{"0-7-1": true, "0-8-2": true, "0-7-3": true, "0-7-4": true}
	set, err := matchedSetFrom(t, all, matched)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"0-7-1", "0-7-3..4", "0-8-2"}
	if got := set.strings(); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v (0-8-3 must not be covered)", got, want)
	}
	// With the per-server form gone, a conflict leaves no exact form.
	old := MatchedGTIDIntervalLimit
	MatchedGTIDIntervalLimit = 2
	defer func() { MatchedGTIDIntervalLimit = old }()
	// The error reports the real per-server count (0-7-1, 0-7-3, 0-8-2),
	// derived from the shared ranges labeled by server (#383 QA).
	var limit *MatchedGTIDLimitError
	if _, err := matchedSetFrom(t, all, matched); !errors.As(err, &limit) || limit.Intervals != 3 || limit.Limit != 2 {
		t.Fatalf("conflict without exact form: %v", err)
	}
}

// TestMatchedGTIDSetRoundTripsAsSelector is the #381 QA F2 property: fed back
// as --include-gtids, matched_gtid_set selects exactly the matched
// identities of the same input, for random MariaDB inputs with alternating
// writers, repeated identities and same-sequence conflicts, and for MySQL.
func TestMatchedGTIDSetRoundTripsAsSelector(t *testing.T) {
	rng := rand.New(rand.NewSource(3811))
	for round := 0; round < 300; round++ {
		var all []string
		matched := map[string]bool{}
		n := 1 + rng.Intn(400)
		for i := 0; i < n; i++ {
			var id string
			if round%5 == 4 {
				id = fmt.Sprintf("24bc7850-2c16-11e6-a073-0242ac11000%d:%d", rng.Intn(2), 1+rng.Intn(500))
			} else {
				domain, server, seq := rng.Intn(2), 7+rng.Intn(2+round%3), i+1
				if round%3 == 1 && rng.Intn(10) == 0 {
					seq = 1 + rng.Intn(i+1) // conflict or repeat
				}
				id = fmt.Sprintf("%d-%d-%d", domain, server, seq)
			}
			all = append(all, id)
			if _, seen := matched[id]; !seen {
				matched[id] = rng.Intn(5) != 0
			}
		}
		set, err := matchedSetFrom(t, all, matched)
		if err != nil {
			t.Fatal(err)
		}
		rendered := set.strings()
		if len(rendered) == 0 {
			continue
		}
		selector, err := ParseGTIDSelector(rendered, nil)
		if err != nil {
			t.Fatalf("round %d: %v cannot be parsed back: %v", round, rendered, err)
		}
		for _, id := range all {
			got, err := selector.Match(id)
			if err != nil || got != matched[id] {
				t.Fatalf("round %d: %s selected=%v err=%v, matched=%v; set %v", round, id, got, err, matched[id], rendered)
			}
		}
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
	if set.cost != 2 || set.count != 999_999 {
		t.Fatalf("intervals %d count %d", set.cost, set.count)
	}
	old := MatchedGTIDIntervalLimit
	MatchedGTIDIntervalLimit = 3
	defer func() { MatchedGTIDIntervalLimit = old }()
	if err := set.add(sid + ":5"); err != nil || set.cost != 1 { // fills the gap
		t.Fatalf("fill gap: %v intervals %d", err, set.cost)
	}
	for _, seq := range []string{"2000000", "1999999", "3000000"} { // 1999999 joins
		if err := set.add(sid + ":" + seq); err != nil {
			t.Fatalf("add %s: %v", seq, err)
		}
	}
	err := set.add(sid + ":4000000")
	var limit *MatchedGTIDLimitError
	if !errors.As(err, &limit) || limit.Limit != 3 || limit.Intervals != 4 {
		t.Fatalf("fourth interval: %v", err)
	}
}

// TestMariaDBRangeSelectorGrammar pins the d-s-first..last selector form
// (#381 QA F2): canonical tokens, merging, exclude-wins, and refusals.
func TestMariaDBRangeSelectorGrammar(t *testing.T) {
	sel, err := ParseGTIDSelector([]string{"0-7-1..3, 0-7-2", "0-07-5..5", "1-7-10..18446744073709551615"}, []string{"0-7-2..2"})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := sel.Include(), []string{"0-7-1..3", "0-7-2", "0-7-5", "1-7-10..18446744073709551615"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("include %v, want %v", got, want)
	}
	for id, want := range map[string]bool{"0-7-1": true, "0-7-2": false, "0-7-3": true, "0-7-4": false, "0-7-5": true, "0-8-1": false, "1-7-18446744073709551615": true, "1-7-9": false} {
		if got, err := sel.Match(id); err != nil || got != want {
			t.Fatalf("%s: %v %v, want %v", id, got, err, want)
		}
	}
	for _, bad := range []string{"0-7-5..4", "0-7-5..", "0-7..5", "0-7-1..2..3", "0-7-a..3", "0-7-1..-3"} {
		if _, err := ParseGTIDSelector([]string{bad}, nil); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}

// TestSharedRangesCountPerServerRanges: without sequence repeats, the shared
// ranges labeled by server give the exact per-server range count, in any
// insertion order. The exit-10 error relies on it once the per-server form
// is gone (#383 QA: no placeholder count).
func TestSharedRangesCountPerServerRanges(t *testing.T) {
	rng := rand.New(rand.NewSource(383))
	for round := 0; round < 300; round++ {
		set := newMatchedGTIDSet()
		seqs := rng.Perm(1 + rng.Intn(500))
		for _, i := range seqs {
			if rng.Intn(6) == 0 {
				continue // gap
			}
			server := 7 + rng.Intn(1+round%4)
			if round%3 == 0 {
				server = 7 + i%2 // alternating writers
			}
			id := fmt.Sprintf("%d-%d-%d", round%2, server, i+1)
			if err := set.observe(id); err != nil {
				t.Fatal(err)
			}
			if err := set.add(id); err != nil {
				t.Fatal(err)
			}
		}
		for domain, d := range set.maria {
			if got := d.shared.n + d.shared.changes; got != d.exactN {
				t.Fatalf("round %d domain %d: shared-derived %d, per-server %d", round, domain, got, d.exactN)
			}
		}
	}
}

// Once the per-server form is dropped for every domain, a repeat in another
// domain fails safe with a count below the limit; the message says the form
// was dropped instead of claiming the limit was passed (#383 QA).
func TestMatchedGTIDSetDroppedFormReportsRealCount(t *testing.T) {
	old := MatchedGTIDIntervalLimit
	MatchedGTIDIntervalLimit = 4
	defer func() { MatchedGTIDIntervalLimit = old }()
	all := []string{"1-7-1", "1-8-2", "1-7-3", "1-8-4", "1-7-5", "0-7-1", "0-8-1"}
	matched := map[string]bool{"1-7-1": true, "1-8-2": true, "1-7-3": true, "1-8-4": true, "1-7-5": true, "0-7-1": true}
	var limit *MatchedGTIDLimitError
	_, err := matchedSetFrom(t, all, matched)
	if !errors.As(err, &limit) || limit.Intervals != 3 || limit.Limit != 4 {
		t.Fatalf("repeat after the drop: %v", err)
	}
	if !strings.Contains(err.Error(), "at least 3 ") || !strings.Contains(err.Error(), "dropped when all domains together passed 4") {
		t.Fatalf("message: %v", err)
	}
}

// withLimit lowers MatchedGTIDIntervalLimit for one test.
func withLimit(t *testing.T, limit int) {
	t.Helper()
	old := MatchedGTIDIntervalLimit
	MatchedGTIDIntervalLimit = limit
	t.Cleanup(func() { MatchedGTIDIntervalLimit = old })
}

// alternating returns 0-7-even/0-8-odd identities for seq 1..n, all matched
// except skip.
func alternating(n int, skip ...string) ([]string, map[string]bool) {
	var all []string
	matched := map[string]bool{}
	for seq := 1; seq <= n; seq++ {
		id := fmt.Sprintf("0-%d-%d", 7+seq%2, seq)
		all = append(all, id)
		matched[id] = true
	}
	for _, id := range skip {
		matched[id] = false
	}
	return all, matched
}

func limitOf(t *testing.T, err error) *MatchedGTIDLimitError {
	t.Helper()
	var limit *MatchedGTIDLimitError
	if !errors.As(err, &limit) {
		t.Fatalf("want a MatchedGTIDLimitError, got %v", err)
	}
	return limit
}

// TestMatchedGTIDSetRepeatCountsStoppingTransaction is #384 QA F2: the
// count of a repeat after the per-server form was dropped includes the
// matched transaction that repeats, so "at least N" is never too high. QA's
// repro at the real limit: 200,002 alternating writers, 0-8-3 excluded, and a
// matched 0-8-6, which joins 0-8-5 and 0-8-7: 200000, not 200001.
func TestMatchedGTIDSetRepeatCountsStoppingTransaction(t *testing.T) {
	all, matched := alternating(200_002, "0-8-3")
	all, matched["0-8-6"] = append(all, "0-8-6"), true
	limit := limitOf(t, func() error { _, err := matchedSetFrom(t, all, matched); return err }())
	if limit.Intervals != 200_000 || limit.Limit != 100_000 || !limit.Dropped {
		t.Fatalf("got %+v, want 200000 intervals at limit 100000, dropped", limit)
	}

	// Small cases with the same shape (0-7: 2,4,..,12; 0-8: 1,5,7,9,11),
	// against a per-server reference count that includes the repeat.
	withLimit(t, 4)
	for _, tc := range []struct {
		repeat string
		want   int  // reported
		real   int  // per-server ranges with the repeat
		exact  bool // the labels next to the repeat are known
	}{
		{"0-8-6", 10, 10, false}, // joins 0-8-5 and 0-8-7
		{"0-9-6", 12, 12, true},  // a server the domain never had: its own range
		{"0-8-2", 11, 11, true},  // 2 ends 1..2: labels of 1 and 2 are known; joins 0-8-1
		{"0-7-6", 10, 11, false}, // the same identity again: a lower bound
		{"0-8-4", 11, 11, false}, // 4 starts 4..12 (7); taken as joining 0-8-5, which it does
	} {
		all, matched := alternating(12, "0-8-3")
		all, matched[tc.repeat] = append(all, tc.repeat), true
		limit := limitOf(t, func() error { _, err := matchedSetFrom(t, all, matched); return err }())
		if limit.Intervals != tc.want || !limit.Dropped || limit.Intervals > tc.real || (tc.exact && limit.Intervals != tc.real) {
			t.Fatalf("%s: got %+v, want %d (real %d)", tc.repeat, limit, tc.want, tc.real)
		}
	}
}

// TestMatchedGTIDSetRepeatSameIdentityInShortRange: in 7..9 (7, ?, 7, with a
// change) the middle label is not kept, but its neighbors are known not to
// be 8, so 0-8-8 read again may be the same identity: it adds nothing, and
// the count stays 8 (0-7: 1,3,5,7,9; 0-8: 2,4,8), not 9.
func TestMatchedGTIDSetRepeatSameIdentityInShortRange(t *testing.T) {
	withLimit(t, 4)
	all := []string{"0-7-1", "0-8-2", "0-7-3", "0-8-4", "0-7-5", "0-7-6", "0-7-7", "0-8-8", "0-7-9", "0-8-8"}
	matched := map[string]bool{"0-7-1": true, "0-8-2": true, "0-7-3": true, "0-8-4": true, "0-7-5": true, "0-7-7": true, "0-8-8": true, "0-7-9": true}
	limit := limitOf(t, func() error { _, err := matchedSetFrom(t, all, matched); return err }())
	if limit.Intervals != 8 || !limit.Dropped {
		t.Fatalf("got %+v, want 8 dropped", limit)
	}
}

// TestMatchedGTIDSetRepeatCountIsLowerBound: for random inputs that drop
// the per-server form and then repeat a sequence number, the reported count
// never passes the real per-server count of the matched transactions read,
// the stopping one included.
func TestMatchedGTIDSetRepeatCountIsLowerBound(t *testing.T) {
	withLimit(t, 6)
	rng := rand.New(rand.NewSource(384))
	checked := 0
	for round := 0; round < 2000; round++ {
		var all []string
		matched := map[string]bool{}
		servers := 2 + rng.Intn(3)
		n := 10 + rng.Intn(40)
		for seq := 1; seq <= n; seq++ {
			id := fmt.Sprintf("0-%d-%d", 7+rng.Intn(servers), seq)
			if rng.Intn(4) == 0 {
				id = fmt.Sprintf("0-%d-%d", 7+(seq%servers), seq)
			}
			all = append(all, id)
			matched[id] = rng.Intn(8) != 0
		}
		repeat := fmt.Sprintf("0-%d-%d", 7+rng.Intn(servers+1), 1+rng.Intn(n))
		if _, seen := matched[repeat]; !seen {
			matched[repeat] = true
		}
		all = append(all, repeat)
		set, err := matchedSetFrom(t, all, matched)
		var limit *MatchedGTIDLimitError
		if !errors.As(err, &limit) || !limit.Dropped || !set.exactOff {
			continue
		}
		perServer := map[string]map[uint64]bool{}
		for _, id := range all {
			if !matched[id] {
				continue
			}
			_, server, seq, _ := mariaIdentity(id)
			key := strconv.FormatUint(server, 10)
			if perServer[key] == nil {
				perServer[key] = map[uint64]bool{}
			}
			perServer[key][seq] = true // repeat is the last identity read
		}
		real := 0
		for _, seqs := range perServer {
			real += len(refRanges(seqs, func(uint64) string { return "" }, func(a, b uint64) string { return "" }))
		}
		if limit.Intervals > real {
			t.Fatalf("round %d: reported %d, real %d; input %v matched %v", round, limit.Intervals, real, all, matched)
		}
		checked++
	}
	if checked < 50 {
		t.Fatalf("only %d rounds reached a repeat after the drop", checked)
	}
}

// TestMatchedGTIDSetDropExactCountsConflictedDomain (#384 QA F1): a domain
// already in conflict when the per-server form is dropped reports its
// per-server count taken before the ranges are freed (1-7-1, 1-8-1: 2), not
// one derived from shared, which holds sequence 1 once.
func TestMatchedGTIDSetDropExactCountsConflictedDomain(t *testing.T) {
	withLimit(t, 4)
	all, matched := alternating(5)
	all = append([]string{"1-7-1", "1-8-1"}, all...)
	matched["1-7-1"], matched["1-8-1"] = true, true
	limit := limitOf(t, func() error { _, err := matchedSetFrom(t, all, matched); return err }())
	// domain 0 renders shared (1..3 on 7 and 8: 2), domain 1 needs 2.
	if limit.Intervals != 4 || !limit.Dropped {
		t.Fatalf("got %+v, want 4 dropped", limit)
	}
}

// TestMatchedGTIDSetDropSharedCountsFormlessDomain (#384 QA F1, #390 QA
// P3): when the shared form is dropped after the per-server one, the count
// is taken before the shared ranges are freed, not 0. 1..10 alternating
// needs 10 per server but 2 shared (1..10 on 7 and 8), and no sequence
// number repeats, so the count is 2 and the message says the form was
// dropped, not that a sequence number repeats.
func TestMatchedGTIDSetDropSharedCountsFormlessDomain(t *testing.T) {
	withLimit(t, 4)
	all, matched := alternating(10)
	for _, seq := range []int{12, 14, 16, 18} { // unmatched: the seen ranges pass 4
		all = append(all, fmt.Sprintf("0-7-%d", seq))
	}
	err := func() error { _, err := matchedSetFrom(t, all, matched); return err }()
	limit := limitOf(t, err)
	if limit.Intervals != 2 || !limit.Dropped || limit.Reason != MatchedGTIDLimitFormDropped {
		t.Fatalf("got %+v, want 2 form_dropped", limit)
	}
	if msg := err.Error(); strings.Contains(msg, "repeat") || !strings.Contains(msg, "at least 2 separate GTID intervals, within the limit of 4, but the form that holds them was dropped") {
		t.Fatalf("message: %v", msg)
	}
}

// TestMatchedGTIDSetTiePrefersPerServer (#383 QA survivor): when both forms
// need the same number of entries, the per-server one is rendered, because
// it names only identities that were read.
func TestMatchedGTIDSetTiePrefersPerServer(t *testing.T) {
	all := []string{"0-7-1", "0-8-2"}
	set, err := matchedSetFrom(t, all, map[string]bool{"0-7-1": true, "0-8-2": true})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := set.strings(), []string{"0-7-1", "0-8-2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// TestMatchedGTIDSetSeenRangesAreBounded (#383 QA survivor): the repeat
// check holds at most MatchedGTIDIntervalLimit ranges of sequence numbers
// read; past that it is freed with the shared form, whether an unmatched or
// a matched transaction passes it, and domains then use the per-server form.
func TestMatchedGTIDSetSeenRangesAreBounded(t *testing.T) {
	withLimit(t, 4)
	for _, last := range []bool{false, true} {
		set := newMatchedGTIDSet()
		for _, seq := range []int{1, 3, 5, 7} {
			if err := set.read(fmt.Sprintf("0-7-%d", seq), false); err != nil {
				t.Fatal(err)
			}
		}
		if set.sharedOff || set.seenTotal != 4 {
			t.Fatalf("at the limit: sharedOff=%v seen=%d", set.sharedOff, set.seenTotal)
		}
		if err := set.read("0-7-9", last); err != nil {
			t.Fatal(err)
		}
		if !set.sharedOff || set.seenTotal != 0 || set.maria[0].seen.n != 0 || set.maria[0].shared.n != 0 {
			t.Fatalf("matched=%v past the limit: sharedOff=%v seen=%d/%d shared=%d", last, set.sharedOff, set.seenTotal, set.maria[0].seen.n, set.maria[0].shared.n)
		}
		// Alternating writers then need one range per transaction.
		var err error
		for seq := 10; seq <= 14 && err == nil; seq++ {
			err = set.read(fmt.Sprintf("1-%d-%d", 7+seq%2, seq), true)
		}
		if limit := limitOf(t, err); !limit.Dropped {
			t.Fatalf("matched=%v: %+v", last, limit)
		}
	}
}

// TestMatchedGTIDSetDropExactRecosts (#383 QA survivor): dropping the
// per-server form recounts every domain, so a domain that rendered
// per-server and now renders shared (3 servers x 3 ranges) is counted, and
// the set fails instead of rendering 11 entries over a limit of 10. The
// count is what the transactions need (#390 QA P2): 3 per server for domain
// 1 and 2 shared for domain 0, 5, within the limit, with the per-server form
// dropped.
func TestMatchedGTIDSetDropExactRecosts(t *testing.T) {
	withLimit(t, 10)
	all, matched := alternating(8)
	all = append([]string{"1-7-1", "1-8-3", "1-9-5"}, all...)
	for _, id := range all[:3] {
		matched[id] = true
	}
	set, err := matchedSetFrom(t, all, matched)
	limit := limitOf(t, err)
	if limit.Intervals != 5 || limit.Reason != MatchedGTIDLimitFormDropped {
		t.Fatalf("got %+v (set %v), want 5 form_dropped", limit, set.strings())
	}
}

// TestMatchedGTIDSetSeenFirstThreeWriters is #390 QA P2 (seen_gappy_3writers
// at a lower limit): writers 7, 8, 9 in turn on sequence numbers 2, 4, 6, ...
// leave one range per transaction in every form; shared needs 3 entries per
// range. The matched transaction is stored, then the seen overflow drops
// the shared form, and the run stops on the per-server count, 11 (the
// stopping transaction included), not 33 (the shared cost).
func TestMatchedGTIDSetSeenFirstThreeWriters(t *testing.T) {
	withLimit(t, 10)
	var all []string
	matched := map[string]bool{}
	for i := 1; i <= 30; i++ {
		id := fmt.Sprintf("0-%d-%d", 7+(i-1)%3, 2*i)
		all = append(all, id)
		matched[id] = true
	}
	set, err := matchedSetFrom(t, all, matched)
	limit := limitOf(t, err)
	if limit.Intervals != 11 || limit.Reason != MatchedGTIDLimitPerServer || set.count != 11 {
		t.Fatalf("got %+v after %d matched, want 11 per_server after 11", limit, set.count)
	}
	if real := trueNeed(all[:11], matched); limit.Intervals != real {
		t.Fatalf("reported %d, real %d", limit.Intervals, real)
	}
	if msg := err.Error(); strings.Contains(msg, "repeat") || !strings.Contains(msg, "at least 11 separate GTID intervals kept per server; at most 10 can be tracked, and the shared form, which can need fewer, was dropped") {
		t.Fatalf("message: %v", msg)
	}
}

// TestMatchedGTIDSetDropExactReportsPerServerCount is the same root cause
// on main (#390 QA P2, O1): 7 then 8 write the odd sequence numbers, an
// unmatched 9 the even ones, so the seen ranges stay one range. Every
// matched transaction is its own range: 11 per server, 22 shared. When the
// per-server ranges pass the limit they are freed, the shared cost passes
// it too, and the error reports 11, derived from the shared ranges, not 22.
func TestMatchedGTIDSetDropExactReportsPerServerCount(t *testing.T) {
	withLimit(t, 10)
	var all []string
	matched := map[string]bool{}
	for k := 0; k < 20; k++ {
		writer := 7
		if k >= 10 {
			writer = 8
		}
		odd, even := fmt.Sprintf("0-%d-%d", writer, 2*k+1), fmt.Sprintf("0-9-%d", 2*k+2)
		all = append(all, odd, even)
		matched[odd] = true
	}
	set, err := matchedSetFrom(t, all, matched)
	limit := limitOf(t, err)
	if limit.Intervals != 11 || limit.Reason != MatchedGTIDLimitPlain || limit.Dropped || set.count != 11 {
		t.Fatalf("got %+v after %d matched, want 11 plain after 11", limit, set.count)
	}
	if real := trueNeed(all[:21], matched); limit.Intervals != real {
		t.Fatalf("reported %d, real %d", limit.Intervals, real)
	}
}

// TestMatchedGTIDSetRepeatBoundsAreExact pins the repeat count where the
// labels next to the repeated sequence number are known (#390 QA P3, the
// rd_* survivors): a looser lower bound would still be safe, but less
// precise. Each case drops the per-server form on the alternating domain,
// then repeats a sequence number of the other one. The reported count
// equals the real need of the matched transactions read.
func TestMatchedGTIDSetRepeatBoundsAreExact(t *testing.T) {
	withLimit(t, 8)
	alt := func(domain int, skip string) ([]string, map[string]bool) {
		var all []string
		matched := map[string]bool{}
		for seq := 1; seq <= 12; seq++ {
			id := fmt.Sprintf("%d-%d-%d", domain, 7+seq%2, seq)
			all = append(all, id)
			matched[id] = id != skip
		}
		return all, matched
	}
	join := func(head []string, domain int, skip string, tail ...string) ([]string, map[string]bool) {
		all, matched := alt(domain, skip)
		for _, id := range head {
			matched[id] = true
		}
		for _, id := range tail {
			matched[id] = true
		}
		return append(append(append([]string{}, head...), all...), tail...), matched
	}
	run := []int{}
	for i := 1; i <= 10; i++ {
		run = append(run, i)
	}
	sevenRun := make([]string, 0, 11)
	for _, i := range run {
		sevenRun = append(sevenRun, fmt.Sprintf("0-7-%d", i))
	}
	sevenRun = append(sevenRun, "0-8-20")
	for _, tc := range []struct {
		name  string
		input func() ([]string, map[string]bool)
		want  int
	}{
		// 0-7-1..10 has no label change, so every label in it is known:
		// 0-8-5 is a range of its own (3 for domain 0, 4 shared for domain 1).
		{"no change in the range", func() ([]string, map[string]bool) { return join(sevenRun, 1, "1-8-3", "0-8-5") }, 7},
		// 10..14 is 7,7,8,8,7: the label of 10, the range start, is known
		// to be 7, so 0-8-11 cannot join it and joins 0-8-12 only.
		{"known range start", func() ([]string, map[string]bool) {
			return join([]string{"0-7-10", "0-7-11", "0-8-12", "0-8-13", "0-7-14"}, 1, "1-8-3", "0-8-11")
		}, 7},
		// 1-8-1 repeats 1, the start of 1-7-1's range: found there, it is a
		// range of its own (2 for domain 1, 4 shared for domain 0).
		{"repeat at the range start", func() ([]string, map[string]bool) { return join(nil, 0, "0-8-3", "1-7-1", "1-8-1") }, 6},
	} {
		all, matched := tc.input()
		limit := limitOf(t, func() error { _, err := matchedSetFrom(t, all, matched); return err }())
		if limit.Intervals != tc.want || limit.Reason != MatchedGTIDLimitRepeat {
			t.Fatalf("%s: got %+v, want %d repeat", tc.name, limit, tc.want)
		}
		if real := trueNeed(all, matched); limit.Intervals != real {
			t.Fatalf("%s: reported %d, real %d", tc.name, limit.Intervals, real)
		}
	}
}

// TestMatchedGTIDSetLimitErrorIsTruthful: for random MariaDB inputs, every
// limit error reports at most the real need of the matched transactions read
// up to the stop (a per-server count when the shared form was dropped
// earlier), a count over the limit for the plain message and within it for
// form_dropped, and the repeat message only when a sequence number repeated.
func TestMatchedGTIDSetLimitErrorIsTruthful(t *testing.T) {
	withLimit(t, 8)
	rng := rand.New(rand.NewSource(390))
	seen := map[MatchedGTIDLimitReason]int{}
	for round := 0; round < 3000; round++ {
		var all []string
		matched := map[string]bool{}
		servers, n := 1+rng.Intn(3), 5+rng.Intn(40)
		for i := 1; i <= n; i++ {
			domain, seq := rng.Intn(2), i*(1+round%2)
			if rng.Intn(15) == 0 {
				seq = 1 + rng.Intn(seq) // may repeat
			}
			id := fmt.Sprintf("%d-%d-%d", domain, 7+rng.Intn(servers), seq)
			if round%3 == 0 {
				id = fmt.Sprintf("%d-%d-%d", domain, 7+i%servers, seq)
			}
			all = append(all, id)
			if _, ok := matched[id]; !ok {
				matched[id] = rng.Intn(6) != 0
			}
		}
		set := newMatchedGTIDSet()
		var err error
		stop := 0
		for stop < len(all) && err == nil {
			err = set.read(all[stop], matched[all[stop]])
			stop++
		}
		if err == nil {
			continue
		}
		limit := limitOf(t, err)
		seen[limit.Reason]++
		prefix := all[:stop]
		repeats := map[string]bool{}
		repeated := false
		for _, id := range prefix {
			d, _, q, _ := mariaIdentity(id)
			key := fmt.Sprint(d, "-", q)
			repeated = repeated || repeats[key]
			repeats[key] = true
		}
		real := trueNeed(prefix, matched)
		bad := ""
		switch limit.Reason {
		case MatchedGTIDLimitPlain:
			if limit.Intervals <= limit.Limit || limit.Intervals > real {
				bad = "plain"
			}
		case MatchedGTIDLimitFormDropped:
			if limit.Intervals > limit.Limit || limit.Intervals > real {
				bad = "form_dropped"
			}
		case MatchedGTIDLimitRepeat:
			if !repeated || limit.Intervals > real {
				bad = "repeat"
			}
		case MatchedGTIDLimitPerServer:
			if limit.Intervals <= limit.Limit {
				bad = "per_server"
			}
		}
		if bad != "" || limit.Dropped != (limit.Reason != MatchedGTIDLimitPlain) {
			t.Fatalf("round %d: %s: %+v, real %d; input %v matched %v", round, bad, limit, real, prefix, matched)
		}
	}
	for _, reason := range []MatchedGTIDLimitReason{MatchedGTIDLimitPlain, MatchedGTIDLimitFormDropped, MatchedGTIDLimitRepeat, MatchedGTIDLimitPerServer} {
		if seen[reason] < 20 {
			t.Fatalf("only %d rounds stopped with %q: %v", seen[reason], reason, seen)
		}
	}
}

// trueNeed is the reference count (#390 QA): per domain, the per-server ranges when a sequence number repeats among all identities read, else min(per-server, shared ranges x servers).
func trueNeed(all []string, matched map[string]bool) int {
	type dom struct {
		seen    map[uint64]int
		per     map[uint64]map[uint64]bool
		shared  map[uint64]bool
		repeats bool
	}
	doms := map[uint64]*dom{}
	for _, id := range all {
		d, s, q, _ := mariaIdentity(id)
		x := doms[d]
		if x == nil {
			x = &dom{seen: map[uint64]int{}, per: map[uint64]map[uint64]bool{}, shared: map[uint64]bool{}}
			doms[d] = x
		}
		x.seen[q]++
		if x.seen[q] > 1 {
			x.repeats = true
		}
		if !matched[id] {
			continue
		}
		if x.per[s] == nil {
			x.per[s] = map[uint64]bool{}
		}
		x.per[s][q] = true
		x.shared[q] = true
	}
	none := func(uint64) string { return "" }
	span := func(a, b uint64) string { return "" }
	total := 0
	for _, x := range doms {
		if len(x.per) == 0 {
			continue
		}
		per := 0
		for _, seqs := range x.per {
			per += len(refRanges(seqs, none, span))
		}
		shared := len(refRanges(x.shared, none, span)) * len(x.per)
		if x.repeats || per <= shared {
			total += per
		} else {
			total += shared
		}
	}
	return total
}

// TestMatchedGTIDSetMariaDBPositionWindow (#390 QA P3, read_keep_matched_only):
// with a position window, a MariaDB transaction that matches the selector
// but has no event inside the window is read for the repeat check only; it
// is not added to matched_gtid_set or counted.
func TestMatchedGTIDSetMariaDBPositionWindow(t *testing.T) {
	base := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	var events []model.NormalizedEvent
	pos := int64(4)
	add := func(ev model.NormalizedEvent) {
		ev.Timestamp, ev.ServerFlavor, ev.BinlogPath = base, "mariadb", "mariadb-bin.000001"
		ev.PositionStart, ev.PositionEnd = pos, pos+20
		events = append(events, ev)
		pos += 20
	}
	for seq := 1; seq <= 6; seq++ {
		add(model.NormalizedEvent{EventType: "GTID", GTID: fmt.Sprintf("0-%d-%d", 7+seq/4, seq)})
		add(model.NormalizedEvent{EventType: "BEGIN"})
		add(model.NormalizedEvent{EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1})
		add(model.NormalizedEvent{EventType: "XID"})
	}
	selector, err := ParseGTIDSelector(nil, []string{"99-99-99"})
	if err != nil {
		t.Fatal(err)
	}
	// Each transaction is 80 bytes from 4: 1-3 end at 244, 4-6 after it.
	stop, start := int64(244), int64(244)
	for _, tc := range []struct {
		name        string
		start, stop *int64
		want        []string
	}{
		{"stop position", nil, &stop, []string{"0-7-1..3"}},
		{"start position", &start, nil, []string{"0-8-4..6"}},
	} {
		opts := DefaultOptions()
		opts.GTIDSelector = selector
		opts.StartPosition, opts.StopPosition = tc.start, tc.stop
		result, err := New(opts).Analyze(events)
		if err != nil {
			t.Fatal(err)
		}
		sel := result.Selection
		if sel == nil || !reflect.DeepEqual(sel.MatchedGTIDSet, tc.want) || sel.MatchedTransactions == nil || *sel.MatchedTransactions != 3 {
			t.Fatalf("%s: selection %+v", tc.name, sel)
		}
	}
}

// gappyKept is writers 7, 8, ... round-robin with sequence 2i, every
// transaction kept. i=100001 is 0-9-200002 for three writers and
// 0-8-200002 for two.
func gappyKept(n, writers int) ([]string, map[string]bool) {
	all := make([]string, 0, n)
	matched := make(map[string]bool, n)
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("0-%d-%d", 7+(i%writers), 2*i)
		all = append(all, id)
		matched[id] = true
	}
	return all, matched
}

// TestMatchedGTIDSetGappyWritersReportPerServer is the #390 P2 repro at the
// real limit. 100,001 kept transactions, sequence 2i, stop at 0-9-200002
// (three writers) or 0-8-200002 (two). The shared cost is 300003 or 200002.
// The reported count is 100001, and nothing repeated.
func TestMatchedGTIDSetGappyWritersReportPerServer(t *testing.T) {
	for _, writers := range []int{2, 3} {
		all, matched := gappyKept(100_001, writers)
		set, err := matchedSetFrom(t, all, matched)
		limit := limitOf(t, err)
		if limit.Intervals != 100_001 || limit.Reason == MatchedGTIDLimitRepeat || set.count != 100_001 {
			t.Fatalf("%d writers: got %+v after %d, want 100001", writers, limit, set.count)
		}
		if strings.Contains(limit.Error(), "sequence number repeats") || !strings.Contains(limit.Error(), "at least 100001 separate GTID intervals kept per server") {
			t.Fatalf("%d writers message: %v", writers, limit)
		}
	}
}

// TestMatchedGTIDSetSeenOverflowCountsStoppingInterval: the per-server form
// is already gone, then one new matched sequence (not a repeat) pushes the
// seen list over the limit. The count includes that sequence. Returning from
// the seen check before the transaction is stored reports the smaller number.
func TestMatchedGTIDSetSeenOverflowCountsStoppingInterval(t *testing.T) {
	withLimit(t, 4)
	all, matched := alternating(8)
	for _, seq := range []int{20, 22, 24} { // seen reaches the limit, does not pass it
		all = append(all, fmt.Sprintf("0-7-%d", seq))
	}
	all = append(all, "0-7-30")
	matched["0-7-30"] = true
	limit := limitOf(t, func() error { _, err := matchedSetFrom(t, all, matched); return err }())
	withStopping := trueNeed(all, matched)
	without := trueNeed(all[:len(all)-1], matched)
	if limit.Intervals != withStopping || limit.Intervals == without || limit.Reason != MatchedGTIDLimitFormDropped {
		t.Fatalf("got %+v, want %d form_dropped (without the stopping transaction: %d)", limit, withStopping, without)
	}
	if strings.Contains(limit.Error(), "repeat") {
		t.Fatalf("message: %v", limit)
	}
}
