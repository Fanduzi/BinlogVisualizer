package codehealth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
)

// Baseline is the checked-in ceiling for every file and function over a
// limit. Generated maps a generated file to the command that regenerates
// it; that list may not change in a pull request.
type Baseline struct {
	Limits               Limits            `json:"limits"`
	Generated            map[string]string `json:"generated_files"`
	Files                Ledger            `json:"files"`
	TestFiles            Ledger            `json:"test_files"`
	ComplexFunctions     Ledger            `json:"complex_functions"`
	TestComplexFunctions Ledger            `json:"test_complex_functions"`
	LongFunctions        Ledger            `json:"long_functions"`
	StatementFunctions   Ledger            `json:"statement_functions"`
	IssueRefs            Ledger            `json:"issue_refs"`
	Deadcode             Ledger            `json:"deadcode"`
}

// Load reads a baseline file. Unknown fields and trailing data are errors.
func Load(path string) (Baseline, error) {
	var b Baseline
	data, err := os.ReadFile(path)
	if err != nil {
		return b, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
		return b, fmt.Errorf("%s: %w", path, err)
	}
	if dec.More() {
		return b, fmt.Errorf("%s: data after the baseline object", path)
	}
	if l := b.Limits; min(l.FileLines, l.GrowthTolerance, l.Complexity, l.FunctionLines, l.FunctionStatements) <= 0 {
		return b, fmt.Errorf("%s: every limit must be set and positive, got %+v", path, l)
	}
	return b.normalized(), nil
}

// Save writes b as indented JSON with sorted keys.
func Save(path string, b Baseline) error {
	var data bytes.Buffer
	enc := json.NewEncoder(&data)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(b.normalized()); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data.Bytes(), 0o644)
}

func (b Baseline) normalized() Baseline {
	if b.Generated == nil {
		b.Generated = map[string]string{}
	}
	for _, k := range kinds {
		if *k.ledger(&b) == nil {
			*k.ledger(&b) = Ledger{}
		}
	}
	return b
}

// Initial is the first baseline for a tree: every value over a limit
// becomes its ceiling.
func Initial(lim Limits, generated map[string]string, m Measurement) Baseline {
	b := Baseline{Limits: lim, Generated: generated}.normalized()
	for _, k := range kinds {
		out := Ledger{}
		for key, v := range k.values(m) {
			if v > k.limit(lim) {
				out[key] = v
			}
		}
		*k.ledger(&b) = out
	}
	return b
}

// Check compares a tree with its baseline and returns one message per
// problem: a value over a limit with no entry, a value above its ceiling
// plus tolerance, and the measurement's own problems (nested modules, token
// //nolint reasons). A value at or below its ceiling always passes, so main
// stays green when two changes pay down the same entry; Compare asks the
// change that paid down to record it. Ledgers that m did not measure
// (deadcode without -deadcode) are not checked.
func Check(b Baseline, m Measurement) []string {
	b = b.normalized()
	out := append([]string{}, m.Problems...)
	for _, k := range kinds {
		vals, l := k.values(m), *k.ledger(&b)
		for _, key := range sortedKeys(vals) {
			v := vals[key]
			ceiling, ok := l[key]
			switch {
			case !ok && v > k.limit(b.Limits):
				out = append(out, newEntryMessage(k, b.Limits, m, key, v))
			case ok && v > ceiling+k.tolerance(b.Limits):
				out = append(out, fmt.Sprintf("%s: %s grew to %d %s, ceiling %d (+%d allowed); %s", k.name, key, v, k.unit, ceiling, k.tolerance(b.Limits), k.fix))
			}
		}
	}
	return out
}

func newEntryMessage(k kind, lim Limits, m Measurement, key string, v int) string {
	switch k.name {
	case "deadcode":
		return fmt.Sprintf("deadcode: %s at %s is unreachable; %s", key, m.Deadcode[key], k.fix)
	case "issue_refs":
		return fmt.Sprintf("issue_refs: %s has %d issue references in comments, none allowed in a new file; %s. If the file was renamed or split from a baselined file, run make codehealth-baseline", key, v, k.fix)
	}
	return fmt.Sprintf("%s: new %s over %d %s: %s has %d; %s. If it was split or renamed from a baseline entry, run make codehealth-baseline; compare checks that its content came from that entry", k.name, k.noun, k.limit(lim), k.unit, key, v, k.fix)
}

// Update lowers every ceiling in b to what m measured, drops entries that
// are no longer over a limit, and adds the new ones. Compare decides in CI
// whether a new entry is a split or rename of an old one. It refuses,
// returning the problems, when a value is above its ceiling plus tolerance.
func Update(b Baseline, m Measurement) (Baseline, []string) {
	b = b.normalized()
	next := Baseline{Limits: b.Limits, Generated: b.Generated}.normalized()
	var problems []string
	for _, k := range kinds {
		vals, l := k.values(m), *k.ledger(&b)
		if vals == nil {
			*k.ledger(&next) = maps.Clone(l)
			continue
		}
		out := Ledger{}
		for _, key := range sortedKeys(vals) {
			v := vals[key]
			ceiling, ok := l[key]
			switch {
			case v <= k.limit(b.Limits):
			case !ok:
				out[key] = v
			case v > ceiling+k.tolerance(b.Limits):
				problems = append(problems, fmt.Sprintf("%s: %s grew to %d %s, ceiling %d (+%d allowed)", k.name, key, v, k.unit, ceiling, k.tolerance(b.Limits)))
			default:
				out[key] = min(v, ceiling)
			}
		}
		*k.ledger(&next) = out
	}
	if len(problems) > 0 {
		return b, problems
	}
	return next, nil
}

// Compare returns one message per way the pull request's baseline next is
// looser than prev, the baseline at the base revision. base and pr are the
// two trees, measured. Limits may not loosen and generated_files may not
// change. An entry already in prev may not rise. A new entry must be
// derived from a prev entry (lineage). With paydown set (pull requests), an
// entry whose value this change lowered by at least its kind's paydown must
// be lowered in next too.
func Compare(prev Baseline, base Measurement, next Baseline, pr Measurement, paydown bool) []string {
	prev, next = prev.normalized(), next.normalized()
	out := compareLimits(prev.Limits, next.Limits)
	if !maps.Equal(prev.Generated, next.Generated) {
		out = append(out, fmt.Sprintf("generated_files: changed from %v to %v; the list of generated files and their commands may not change in a pull request", prev.Generated, next.Generated))
	}
	for _, k := range kinds {
		p, n := *k.ledger(&prev), *k.ledger(&next)
		for _, key := range sortedKeys(n) {
			if was, ok := p[key]; ok && n[key] > was {
				out = append(out, fmt.Sprintf("%s: %s raised %d -> %d", k.name, key, was, n[key]))
			}
		}
		out = append(out, lineage(k, next.Limits, p, n, base, pr)...)
		if paydown {
			out = append(out, paidDown(k, n, base, pr)...)
		}
	}
	return out
}

func compareLimits(prev, next Limits) []string {
	var out []string
	for _, f := range []struct {
		name       string
		prev, next int
	}{
		{"file_lines", prev.FileLines, next.FileLines},
		{"growth_tolerance_lines", prev.GrowthTolerance, next.GrowthTolerance},
		{"complexity", prev.Complexity, next.Complexity},
		{"function_lines", prev.FunctionLines, next.FunctionLines},
		{"function_statements", prev.FunctionStatements, next.FunctionStatements},
	} {
		if f.next > f.prev {
			out = append(out, fmt.Sprintf("limits: %s loosened %d -> %d", f.name, f.prev, f.next))
		}
	}
	return out
}

// paidDown asks the change that lowered a value by at least the kind's
// paydown to lower its ceiling too. Only values this change moved count,
// so a drop that reached main through another merge never fails here.
func paidDown(k kind, n Ledger, base, pr Measurement) []string {
	var out []string
	before, after := k.values(base), k.values(pr)
	if before == nil || after == nil || k.paydown == 0 {
		return nil
	}
	for _, key := range sortedKeys(n) {
		v := after[key]
		if before[key] != v && n[key]-v >= k.paydown {
			out = append(out, fmt.Sprintf("%s: %s is down to %d %s, ceiling %d; run make codehealth-baseline to record the paydown", k.name, key, v, k.unit, n[key]))
		}
	}
	return out
}
