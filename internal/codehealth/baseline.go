package codehealth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
)

// DeadcodeTool is the pinned deadcode command CountDeadcode runs.
const DeadcodeTool = "golang.org/x/tools/cmd/deadcode@v0.52.0"

// Baseline is the checked-in ceiling for every ratcheted number.
type Baseline struct {
	Limits           Limits         `json:"limits"`
	OversizedFiles   map[string]int `json:"oversized_files"`
	ComplexFunctions map[string]int `json:"complex_functions"`
	IssueRefs        int            `json:"issue_refs"`
	Deadcode         int            `json:"deadcode"`
}

// Load reads a baseline file.
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
	if b.OversizedFiles == nil {
		b.OversizedFiles = map[string]int{}
	}
	if b.ComplexFunctions == nil {
		b.ComplexFunctions = map[string]int{}
	}
	return b, nil
}

// Save writes b as indented JSON with sorted keys.
func Save(path string, b Baseline) error {
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

// Initial is the first baseline for a tree: today's numbers become the ceilings.
func Initial(lim Limits, m Measurement) Baseline {
	return Baseline{
		Limits:           lim,
		OversizedFiles:   copyMap(m.OversizedFiles),
		ComplexFunctions: copyMap(m.ComplexFunctions),
		IssueRefs:        len(m.IssueRefs),
		Deadcode:         max(m.Deadcode, 0),
	}
}

// Check returns one message per number that is worse than the baseline.
// A Deadcode of -1 means it was not measured and is not checked.
func Check(b Baseline, m Measurement) []string {
	var out []string
	for _, path := range sortedKeys(m.OversizedFiles) {
		n := m.OversizedFiles[path]
		ceiling, known := b.OversizedFiles[path]
		switch {
		case !known:
			out = append(out, fmt.Sprintf("file over %d lines: %s has %d lines; split it instead of adding to the baseline", b.Limits.FileLines, path, n))
		case n > ceiling+b.Limits.GrowthTolerance:
			out = append(out, fmt.Sprintf("oversized file grew: %s has %d lines, baseline %d (+%d allowed); move code out before adding more", path, n, ceiling, b.Limits.GrowthTolerance))
		}
	}
	for _, key := range sortedKeys(m.ComplexFunctions) {
		c := m.ComplexFunctions[key]
		ceiling, known := b.ComplexFunctions[key]
		switch {
		case !known:
			out = append(out, fmt.Sprintf("function over complexity %d: %s is %d; split it into phase functions", b.Limits.Complexity, key, c))
		case c > ceiling:
			out = append(out, fmt.Sprintf("complex function grew: %s is %d, baseline %d; extract the new branch into its own function", key, c, ceiling))
		}
	}
	if n := len(m.IssueRefs); n > b.IssueRefs {
		out = append(out, fmt.Sprintf("issue references in comments: %d, baseline %d; put issue numbers and section history in the commit message or ADR (list them with: codehealth refs)", n, b.IssueRefs))
	}
	if m.Deadcode > b.Deadcode {
		out = append(out, fmt.Sprintf("unreachable functions (deadcode): %d, baseline %d; delete the code instead of keeping it", m.Deadcode, b.Deadcode))
	}
	return out
}

// Stale lists baseline entries that no longer apply: a file now under the
// line cap or a function now under the complexity cap. Leaving them would let
// the debt come back without failing Check.
func Stale(b Baseline, m Measurement) []string {
	var out []string
	for _, path := range sortedKeys(b.OversizedFiles) {
		if _, ok := m.OversizedFiles[path]; !ok {
			out = append(out, "oversized file no longer over the cap: "+path)
		}
	}
	for _, key := range sortedKeys(b.ComplexFunctions) {
		if _, ok := m.ComplexFunctions[key]; !ok {
			out = append(out, "complex function no longer over the cap: "+key)
		}
	}
	return out
}

// Tighten lowers every ceiling in b to what m measured. It refuses, returning
// the Check messages, when anything in m is worse than b: the baseline only
// goes down.
func Tighten(b Baseline, m Measurement) (Baseline, []string) {
	if v := Check(b, m); len(v) > 0 {
		return b, v
	}
	next := Baseline{
		Limits:           b.Limits,
		OversizedFiles:   map[string]int{},
		ComplexFunctions: map[string]int{},
		IssueRefs:        min(b.IssueRefs, len(m.IssueRefs)),
		Deadcode:         b.Deadcode,
	}
	for path, n := range m.OversizedFiles {
		next.OversizedFiles[path] = min(n, b.OversizedFiles[path])
	}
	for key, c := range m.ComplexFunctions {
		next.ComplexFunctions[key] = min(c, b.ComplexFunctions[key])
	}
	if m.Deadcode >= 0 {
		next.Deadcode = m.Deadcode
	}
	return next, nil
}

// CompareBaselines returns one message per ceiling that next raises over
// prev. A renamed entry is fine as long as the entry count and the total do
// not go up.
func CompareBaselines(prev, next Baseline) []string {
	var out []string
	if next.Limits.FileLines > prev.Limits.FileLines || next.Limits.GrowthTolerance > prev.Limits.GrowthTolerance || next.Limits.Complexity > prev.Limits.Complexity {
		out = append(out, fmt.Sprintf("limits loosened: %+v -> %+v", prev.Limits, next.Limits))
	}
	out = append(out, compareEntries("oversized_files", prev.OversizedFiles, next.OversizedFiles)...)
	out = append(out, compareEntries("complex_functions", prev.ComplexFunctions, next.ComplexFunctions)...)
	if next.IssueRefs > prev.IssueRefs {
		out = append(out, fmt.Sprintf("issue_refs raised: %d -> %d", prev.IssueRefs, next.IssueRefs))
	}
	if next.Deadcode > prev.Deadcode {
		out = append(out, fmt.Sprintf("deadcode raised: %d -> %d", prev.Deadcode, next.Deadcode))
	}
	return out
}

func compareEntries(name string, prev, next map[string]int) []string {
	var out []string
	if len(next) > len(prev) {
		out = append(out, fmt.Sprintf("%s: %d entries -> %d", name, len(prev), len(next)))
	}
	if sum(next) > sum(prev) {
		out = append(out, fmt.Sprintf("%s: total %d -> %d", name, sum(prev), sum(next)))
	}
	for _, k := range sortedKeys(next) {
		if p, ok := prev[k]; ok && next[k] > p {
			out = append(out, fmt.Sprintf("%s: %s raised %d -> %d", name, k, p, next[k]))
		}
	}
	return out
}

// CountDeadcode runs the pinned deadcode tool on root and counts the
// functions it reports as unreachable.
func CountDeadcode(root string) (int, error) {
	cmd := exec.Command("go", "run", DeadcodeTool, "./...")
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("deadcode: %w: %s", err, stderr.String())
	}
	n := 0
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "unreachable func") {
			n++
		}
	}
	return n, nil
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sum(m map[string]int) int {
	t := 0
	for _, v := range m {
		t += v
	}
	return t
}

func copyMap(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
