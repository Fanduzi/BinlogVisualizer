package codehealth

import "sort"

// Ledger maps a file path or function key to its ceiling.
type Ledger map[string]int

// kind describes one ledger: where its values and content come from, the
// limit above which a value needs an entry, how far a value may sit above
// its ceiling, how much each piece after the first may add when an entry is
// split (package clause and imports for a file, a signature for a function),
// and how big a drop the pull request that causes it must record. content is
// nil when an entry cannot move (deadcode: delete the function).
type kind struct {
	name, noun, unit, fix string
	ledger                func(*Baseline) *Ledger
	values                func(Measurement) map[string]int
	content               func(Measurement, string) []string
	limit                 func(Limits) int
	tolerance             func(Limits) int
	slack, paydown        int
}

// fileSplitSlack is what each extra file of a split may add: its package
// clause, imports and doc comment.
const fileSplitSlack = 40

// functionLineTolerance lets a baselined long function take a small fix
// without being split first.
const functionLineTolerance = 5

// complexityTolerance lets a baselined complex function take a one-line
// fix, such as a nil or error guard, without extracting a branch first. It
// is measured against the stored ceiling, which a pull request cannot
// raise, so two fixes in a row cannot add up past it.
const complexityTolerance = 2

func fixed(n int) func(Limits) int { return func(Limits) int { return n } }

func fileText(m Measurement, key string) []string { return m.Text[key] }

func funcBody(m Measurement, key string) []string { return m.Funcs[key].Body }

func testFuncBody(m Measurement, key string) []string { return m.TestFuncs[key].Body }

var kinds = []kind{
	{
		name: "files", noun: "file", unit: "lines", fix: "split it",
		ledger:    func(b *Baseline) *Ledger { return &b.Files },
		values:    func(m Measurement) map[string]int { return m.Files },
		content:   fileText,
		limit:     func(l Limits) int { return l.FileLines },
		tolerance: func(l Limits) int { return l.GrowthTolerance },
		slack:     fileSplitSlack, paydown: 50,
	},
	{
		name: "test_files", noun: "test file", unit: "lines", fix: "split it",
		ledger:    func(b *Baseline) *Ledger { return &b.TestFiles },
		values:    func(m Measurement) map[string]int { return m.TestFiles },
		content:   fileText,
		limit:     func(l Limits) int { return l.FileLines },
		tolerance: func(l Limits) int { return l.GrowthTolerance },
		slack:     fileSplitSlack, paydown: 50,
	},
	{
		name: "complex_functions", noun: "function", unit: "complexity", fix: "extract a branch into its own function",
		ledger:    func(b *Baseline) *Ledger { return &b.ComplexFunctions },
		values:    func(m Measurement) map[string]int { return funcValues(m.Funcs, complexityOf) },
		content:   funcBody,
		limit:     func(l Limits) int { return l.Complexity },
		tolerance: fixed(complexityTolerance),
		slack:     1, paydown: 5,
	},
	{
		name: "test_complex_functions", noun: "test function", unit: "complexity", fix: "extract a helper or split the cases",
		ledger:    func(b *Baseline) *Ledger { return &b.TestComplexFunctions },
		values:    func(m Measurement) map[string]int { return funcValues(m.TestFuncs, complexityOf) },
		content:   testFuncBody,
		limit:     func(l Limits) int { return l.Complexity },
		tolerance: fixed(complexityTolerance),
		slack:     1, paydown: 5,
	},
	{
		name: "long_functions", noun: "function", unit: "lines", fix: "extract a phase into its own function",
		ledger:    func(b *Baseline) *Ledger { return &b.LongFunctions },
		values:    func(m Measurement) map[string]int { return funcValues(m.Funcs, linesOf) },
		content:   funcBody,
		limit:     func(l Limits) int { return l.FunctionLines },
		tolerance: fixed(functionLineTolerance),
		slack:     3, paydown: 20,
	},
	{
		name: "statement_functions", noun: "function", unit: "statements", fix: "extract a phase into its own function",
		ledger:    func(b *Baseline) *Ledger { return &b.StatementFunctions },
		values:    func(m Measurement) map[string]int { return funcValues(m.Funcs, statementsOf) },
		content:   funcBody,
		limit:     func(l Limits) int { return l.FunctionStatements },
		tolerance: fixed(functionLineTolerance),
		slack:     2, paydown: 10,
	},
	{
		name: "issue_refs", noun: "file", unit: "issue references in comments", fix: "put the history in the commit message or ADR (list them: codehealth refs)",
		ledger:    func(b *Baseline) *Ledger { return &b.IssueRefs },
		values:    func(m Measurement) map[string]int { return refCounts(m.Refs) },
		content:   fileText,
		limit:     fixed(0),
		tolerance: fixed(0),
		slack:     0, paydown: 3,
	},
	{
		name: "deadcode", noun: "function", unit: "unreachable", fix: "delete it, or call it from non-test code in this same pull request; no dead code lands and test-only use counts as dead, so code added now and wired up in a later pull request fails (list them: codehealth deadcode)",
		ledger:    func(b *Baseline) *Ledger { return &b.Deadcode },
		values:    func(m Measurement) map[string]int { return deadcodeOf(m.Deadcode) },
		limit:     fixed(0),
		tolerance: fixed(0),
		paydown:   1,
	},
}

func complexityOf(f Func) int { return f.Complexity }

func linesOf(f Func) int { return f.Lines }

func statementsOf(f Func) int { return f.Statements }

func funcValues(fns map[string]Func, metric func(Func) int) map[string]int {
	out := make(map[string]int, len(fns))
	for k, f := range fns {
		out[k] = metric(f)
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
