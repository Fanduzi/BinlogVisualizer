package codehealth

import (
	"fmt"
	"testing"
)

// kindSpec is the documented allowance of each ledger
// (docs/development/coding-standards.md): growth above a ceiling, extra room
// per split piece after the first, and the drop a change must record.
type kindSpec struct {
	name                      string
	tolerance, slack, paydown int
}

var kindSpecs = []kindSpec{
	{"files", 20, 40, 50},
	{"test_files", 20, 40, 50},
	{"complex_functions", 2, 1, 5},
	{"test_complex_functions", 2, 1, 5},
	{"long_functions", 5, 3, 20},
	{"statement_functions", 5, 2, 10},
	{"issue_refs", 0, 0, 3},
}

type entry struct {
	v    int
	text []string
}

func kindNamed(t *testing.T, name string) kind {
	t.Helper()
	for _, k := range kinds {
		if k.name == name {
			return k
		}
	}
	t.Fatalf("no kind %s", name)
	return kind{}
}

// synth is a measurement holding only the given entries of one kind.
func synth(name string, entries map[string]entry) Measurement {
	m := Measurement{Files: map[string]int{}, TestFiles: map[string]int{}, Text: map[string][]string{}, Funcs: map[string]Func{}, TestFuncs: map[string]Func{}, Refs: map[string][]string{}}
	for key, e := range entries {
		switch name {
		case "files":
			m.Files[key], m.Text[key] = e.v, e.text
		case "test_files":
			m.TestFiles[key], m.Text[key] = e.v, e.text
		case "complex_functions":
			m.Funcs[key] = Func{Complexity: e.v, Body: e.text}
		case "test_complex_functions":
			m.TestFuncs[key] = Func{Complexity: e.v, Body: e.text}
		case "long_functions":
			m.Funcs[key] = Func{Lines: e.v, Body: e.text}
		case "statement_functions":
			m.Funcs[key] = Func{Statements: e.v, Body: e.text}
		case "issue_refs":
			for i := range e.v {
				m.Refs[key] = append(m.Refs[key], fmt.Sprintf("%d: #%d", i+1, i+1))
			}
			m.Text[key] = e.text
		}
	}
	return m
}

func text(prefix string, from, to int) []string {
	var out []string
	for i := from; i < to; i++ {
		out = append(out, fmt.Sprintf("%s-%d", prefix, i))
	}
	return out
}

func baselineWith(t *testing.T, name string, l Ledger) Baseline {
	t.Helper()
	b := Baseline{Limits: DefaultLimits}.normalized()
	*kindNamed(t, name).ledger(&b) = l
	return b
}

// check passes a value up to its ceiling plus the kind's tolerance, and no
// further.
func TestKindTolerance(t *testing.T) {
	for _, s := range kindSpecs {
		t.Run(s.name, func(t *testing.T) {
			ceiling := kindNamed(t, s.name).limit(DefaultLimits) + 10
			b := baselineWith(t, s.name, Ledger{"k": ceiling})
			at := func(v int) []string { return Check(b, synth(s.name, map[string]entry{"k": {v, text("k", 0, 10)}})) }
			expect(t, at(ceiling+s.tolerance), "")
			expect(t, at(ceiling+s.tolerance+1), s.name+": k grew to")
		})
	}
}

// A two-way split may add the kind's slack once, whether the original is
// gone or keeps a part.
func TestKindSplitSlack(t *testing.T) {
	for _, s := range kindSpecs {
		t.Run(s.name, func(t *testing.T) {
			lim := kindNamed(t, s.name).limit(DefaultLimits)
			ceiling := 2*lim + 100
			prev := baselineWith(t, s.name, Ledger{"o": ceiling})
			base := synth(s.name, map[string]entry{"o": {ceiling, text("o", 0, 100)}})
			for _, extra := range []int{0, 1} {
				want := ""
				if extra > 0 {
					want = s.name + ": o became"
				}
				a := lim + 50
				rest := ceiling + s.tolerance + s.slack - a + extra
				for _, pr := range []Measurement{
					synth(s.name, map[string]entry{"a": {a, text("o", 0, 50)}, "b": {rest, text("o", 50, 100)}}),
					synth(s.name, map[string]entry{"o": {a, text("o", 0, 50)}, "b": {rest, text("o", 50, 100)}}),
				} {
					next, refused := Update(prev, pr)
					expect(t, refused, "")
					expect(t, Compare(prev, base, next, pr, false), want)
				}
			}
		})
	}
}

// A pull request that lowers a value by the kind's paydown must lower the
// ceiling; one less may stay unrecorded.
func TestKindPaydown(t *testing.T) {
	for _, s := range kindSpecs {
		t.Run(s.name, func(t *testing.T) {
			ceiling := kindNamed(t, s.name).limit(DefaultLimits) + 100
			prev := baselineWith(t, s.name, Ledger{"k": ceiling})
			base := synth(s.name, map[string]entry{"k": {ceiling, text("k", 0, 10)}})
			pr := func(v int) Measurement { return synth(s.name, map[string]entry{"k": {v, text("k", 0, 10)}}) }
			expect(t, Compare(prev, base, prev, pr(ceiling-s.paydown+1), true), "")
			expect(t, Compare(prev, base, prev, pr(ceiling-s.paydown), true), s.name+": k is down to")
			expect(t, Compare(prev, base, prev, pr(ceiling-s.paydown), false), "")
		})
	}
}

// A deleted dead function must leave the baseline in the same change,
// unless deadcode was not measured.
func TestDeadcodePaydown(t *testing.T) {
	prev := baselineWith(t, "deadcode", Ledger{"x:F": 1})
	base := Measurement{Deadcode: map[string]string{"x:F": "x/a.go:3:6"}}
	expect(t, Compare(prev, base, prev, Measurement{Deadcode: map[string]string{}}, true), "deadcode: x:F is down to 0")
	expect(t, Compare(prev, base, prev, Measurement{}, true), "")
	expect(t, Compare(prev, base, prev, base, true), "")
}

// 80% of a new entry's lines from one baseline entry is enough; less is not.
func TestLineageThreshold(t *testing.T) {
	prev := baselineWith(t, "files", Ledger{"o.go": 1000})
	base := synth("files", map[string]entry{"o.go": {1000, text("o", 0, 10)}})
	pr := func(shared int) Measurement {
		return synth("files", map[string]entry{"n.go": {900, append(text("o", 0, shared), text("new", shared, 10)...)}})
	}
	for shared, want := range map[int]string{8: "", 7: "files: adds n.go (900 lines), a new file over the limit; at most 70% of its lines come from one baseline entry (o.go), 80% needed"} {
		next, _ := Update(prev, pr(shared))
		expect(t, Compare(prev, base, next, pr(shared), false), want)
	}
}

// Every new entry is judged, and one without known content cannot claim
// an origin.
func TestLineageJudgesEachNewEntry(t *testing.T) {
	prev := baselineWith(t, "files", Ledger{"o.go": 1000})
	base := synth("files", map[string]entry{"o.go": {1000, text("o", 0, 10)}})
	pr := synth("files", map[string]entry{"a.go": {900, text("a", 0, 10)}, "b.go": {900, text("b", 0, 10)}, "c.go": {900, nil}})
	next, _ := Update(prev, pr)
	got := Compare(prev, base, next, pr, false)
	for _, want := range []string{"adds a.go", "adds b.go", "adds c.go (900 lines), a new file over the limit; at most 0% of its lines come from one baseline entry (none)"} {
		expect(t, got, want)
	}
}

// Each line of the origin matches at most once.
func TestSharedShareCountsEachLineOnce(t *testing.T) {
	if got := sharedShare([]string{"a", "a"}, map[string]int{"a": 1}); got != 0.5 {
		t.Fatalf("share = %v, want 0.5", got)
	}
}
