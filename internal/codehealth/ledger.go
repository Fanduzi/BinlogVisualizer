package codehealth

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
)

// Entry is the ceiling for one file or function. SplitFrom names the entry,
// in the same ledger of the previous baseline, that this one was split or
// renamed from. It is the only way an entry can join a baseline.
type Entry struct {
	Value     int
	SplitFrom string
}

type entryJSON struct {
	Value     int    `json:"value"`
	SplitFrom string `json:"split_from"`
}

// MarshalJSON writes a plain number, or {"value", "split_from"} for an entry
// with lineage.
func (e Entry) MarshalJSON() ([]byte, error) {
	if e.SplitFrom == "" {
		return json.Marshal(e.Value)
	}
	return json.Marshal(entryJSON(e))
}

// UnmarshalJSON reads what MarshalJSON writes.
func (e *Entry) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &e.Value); err == nil {
		return nil
	}
	var j entryJSON
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&j); err != nil {
		return err
	}
	if j.SplitFrom == "" {
		return errors.New("an entry without split_from is a plain number")
	}
	*e = Entry(j)
	return nil
}

// Ledger maps a file path or function key to its ceiling.
type Ledger map[string]Entry

// kind describes one ledger: where its values come from, the limit above
// which a value needs an entry, how far a value may sit above its ceiling,
// and how much a split may add per extra piece (negative: no lineage).
type kind struct {
	name      string
	noun      string
	unit      string
	fix       string
	ledger    func(*Baseline) *Ledger
	values    func(Measurement) map[string]int
	limit     func(Limits) int
	tolerance func(Limits) int
	slack     int
}

// fileSplitSlack is what each extra file of a split may add: its package
// clause, imports and doc comment.
const fileSplitSlack = 20

func zero(Limits) int { return 0 }

var kinds = []kind{
	{
		name: "files", noun: "file", unit: "lines", fix: "split it",
		ledger:    func(b *Baseline) *Ledger { return &b.Files },
		values:    func(m Measurement) map[string]int { return m.Files },
		limit:     func(l Limits) int { return l.FileLines },
		tolerance: func(l Limits) int { return l.GrowthTolerance },
		slack:     fileSplitSlack,
	},
	{
		name: "test_files", noun: "test file", unit: "lines", fix: "split it",
		ledger:    func(b *Baseline) *Ledger { return &b.TestFiles },
		values:    func(m Measurement) map[string]int { return m.TestFiles },
		limit:     func(l Limits) int { return l.FileLines },
		tolerance: func(l Limits) int { return l.GrowthTolerance },
		slack:     fileSplitSlack,
	},
	{
		name: "complex_functions", noun: "function", unit: "complexity", fix: "extract a branch into its own function",
		ledger:    func(b *Baseline) *Ledger { return &b.ComplexFunctions },
		values:    func(m Measurement) map[string]int { return complexityOf(m.Funcs) },
		limit:     func(l Limits) int { return l.Complexity },
		tolerance: zero,
		slack:     1,
	},
	{
		name: "test_complex_functions", noun: "test function", unit: "complexity", fix: "extract a helper or split the cases",
		ledger:    func(b *Baseline) *Ledger { return &b.TestComplexFunctions },
		values:    func(m Measurement) map[string]int { return complexityOf(m.TestFuncs) },
		limit:     func(l Limits) int { return l.Complexity },
		tolerance: zero,
		slack:     1,
	},
	{
		name: "long_functions", noun: "function", unit: "lines", fix: "extract a phase into its own function",
		ledger:    func(b *Baseline) *Ledger { return &b.LongFunctions },
		values:    func(m Measurement) map[string]int { return linesOf(m.Funcs) },
		limit:     func(l Limits) int { return l.FunctionLines },
		tolerance: zero,
		slack:     2,
	},
	{
		name: "issue_refs", noun: "file", unit: "issue references in comments", fix: "put the history in the commit message or ADR (list them: codehealth refs)",
		ledger:    func(b *Baseline) *Ledger { return &b.IssueRefs },
		values:    func(m Measurement) map[string]int { return refCounts(m.Refs) },
		limit:     zero,
		tolerance: zero,
		slack:     0,
	},
	{
		name: "deadcode", noun: "function", unit: "unreachable", fix: "delete it or call it from non-test code; test-only use counts as dead (list them: codehealth -deadcode deadcode)",
		ledger:    func(b *Baseline) *Ledger { return &b.Deadcode },
		values:    func(m Measurement) map[string]int { return deadcodeOf(m.Deadcode) },
		limit:     zero,
		tolerance: zero,
		slack:     -1,
	},
}

func complexityOf(fns map[string]Func) map[string]int {
	out := make(map[string]int, len(fns))
	for k, f := range fns {
		out[k] = f.Complexity
	}
	return out
}

func linesOf(fns map[string]Func) map[string]int {
	out := make(map[string]int, len(fns))
	for k, f := range fns {
		out[k] = f.Lines
	}
	return out
}

func refCounts(refs map[string][]string) map[string]int {
	out := make(map[string]int, len(refs))
	for k, r := range refs {
		out[k] = len(r)
	}
	return out
}

func deadcodeOf(dead map[string]string) map[string]int {
	if dead == nil {
		return nil
	}
	out := make(map[string]int, len(dead))
	for k := range dead {
		out[k] = 1
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
