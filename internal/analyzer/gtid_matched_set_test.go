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

// TestMatchedGTIDSetDropSharedCountsFormlessDomain (#384 QA F1): when the
// shared form is dropped after the per-server one, the per-server count is
// derived from the shared ranges before they are freed (1..10 alternating:
// 10), not 0.
func TestMatchedGTIDSetDropSharedCountsFormlessDomain(t *testing.T) {
	withLimit(t, 4)
	all, matched := alternating(10)
	for _, seq := range []int{12, 14, 16, 18} { // unmatched: the seen ranges pass 4
		all = append(all, fmt.Sprintf("0-7-%d", seq))
	}
	limit := limitOf(t, func() error { _, err := matchedSetFrom(t, all, matched); return err }())
	if limit.Intervals != 10 || !limit.Dropped {
		t.Fatalf("got %+v, want 10 dropped", limit)
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
// the set fails instead of rendering 11 entries over a limit of 10.
func TestMatchedGTIDSetDropExactRecosts(t *testing.T) {
	withLimit(t, 10)
	all, matched := alternating(8)
	all = append([]string{"1-7-1", "1-8-3", "1-9-5"}, all...)
	for _, id := range all[:3] {
		matched[id] = true
	}
	set, err := matchedSetFrom(t, all, matched)
	limit := limitOf(t, err)
	if limit.Intervals != 11 || limit.Dropped {
		t.Fatalf("got %+v (set %v), want 11 not dropped", limit, set.strings())
	}
}
