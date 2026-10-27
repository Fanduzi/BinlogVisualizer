package codehealth

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const repoRoot = "../.."

// The repository is no worse than its checked-in baseline, and the baseline
// has no entries for debt that is already gone. deadcode needs a separate
// tool, so CI checks it in the lint workflow (codehealth -deadcode check).
func TestRepoWithinBaseline(t *testing.T) {
	b, err := Load("baseline.json")
	if err != nil {
		t.Fatal(err)
	}
	m, err := Measure(repoRoot, b.Limits)
	if err != nil {
		t.Fatal(err)
	}
	if v := Check(b, m); len(v) > 0 {
		t.Fatalf("code health is worse than internal/codehealth/baseline.json:\n  %s\nsee docs/development/coding-standards.md", strings.Join(v, "\n  "))
	}
	if s := Stale(b, m); len(s) > 0 {
		t.Fatalf("baseline has entries for debt that is gone; run make codehealth-baseline:\n  %s", strings.Join(s, "\n  "))
	}
}

// The limits in the baseline are the ones the coding standards document.
func TestBaselineLimitsMatchStandards(t *testing.T) {
	b, err := Load("baseline.json")
	if err != nil {
		t.Fatal(err)
	}
	if b.Limits != DefaultLimits {
		t.Fatalf("baseline limits %+v, want %+v", b.Limits, DefaultLimits)
	}
}

func TestComplexityCountsLikeGocyclo(t *testing.T) {
	m, err := Measure("testdata/complex", Limits{FileLines: 800, Complexity: 1})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{".:TooComplex": 41, ".:AtCap": 40, ".:runner.Branchy": 9}
	if !reflect.DeepEqual(m.ComplexFunctions, want) {
		t.Fatalf("complexities %v, want %v", m.ComplexFunctions, want)
	}
}

func TestCheckFailsNewComplexFunction(t *testing.T) {
	m, err := Measure("testdata/complex", DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	b := Initial(DefaultLimits, Measurement{Deadcode: -1})
	v := Check(b, m)
	if len(v) != 1 || !strings.Contains(v[0], ".:TooComplex is 41") {
		t.Fatalf("violations %q, want only TooComplex", v)
	}
	b.ComplexFunctions[".:TooComplex"] = 40
	if v := Check(b, m); len(v) != 1 || !strings.Contains(v[0], "grew") {
		t.Fatalf("violations %q, want TooComplex grew", v)
	}
	b.ComplexFunctions[".:TooComplex"] = 41
	if v := Check(b, m); len(v) != 0 {
		t.Fatalf("violations %q at the baseline", v)
	}
}

func TestCheckFailsOversizedFile(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, lines int) {
		src := "package big\n" + strings.Repeat("\n", lines-1)
		if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("ok.go", 800)
	write("big.go", 801)
	write("big_test.go", 5000)
	m, err := Measure(dir, DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.OversizedFiles, map[string]int{"big.go": 801}) {
		t.Fatalf("oversized %v, want only big.go", m.OversizedFiles)
	}
	b := Initial(DefaultLimits, Measurement{Deadcode: -1})
	if v := Check(b, m); len(v) != 1 || !strings.Contains(v[0], "big.go has 801 lines") {
		t.Fatalf("violations %q, want big.go", v)
	}
	b.OversizedFiles["big.go"] = 801 - DefaultLimits.GrowthTolerance
	if v := Check(b, m); len(v) != 0 {
		t.Fatalf("violations %q within tolerance", v)
	}
	b.OversizedFiles["big.go"]--
	if v := Check(b, m); len(v) != 1 || !strings.Contains(v[0], "grew") {
		t.Fatalf("violations %q, want big.go grew", v)
	}
}

func TestIssueRefsInComments(t *testing.T) {
	m, err := Measure("testdata", DefaultLimits)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"refs/refs.go:4: #12", "refs/refs.go:5: §3", "refs/refs.go:8: (#7"}
	if !reflect.DeepEqual(m.IssueRefs, want) {
		t.Fatalf("refs %q, want %q", m.IssueRefs, want)
	}
	b := Initial(DefaultLimits, Measurement{Deadcode: -1})
	b.ComplexFunctions = m.ComplexFunctions
	b.IssueRefs = 2
	if v := Check(b, m); len(v) != 1 || !strings.Contains(v[0], "issue references in comments: 3, baseline 2") {
		t.Fatalf("violations %q", v)
	}
}

func TestCheckFailsMoreDeadcode(t *testing.T) {
	b := Initial(DefaultLimits, Measurement{Deadcode: 5})
	if v := Check(b, Measurement{Deadcode: 6}); len(v) != 1 || !strings.Contains(v[0], "deadcode): 6, baseline 5") {
		t.Fatalf("violations %q", v)
	}
	if v := Check(b, Measurement{Deadcode: -1}); len(v) != 0 {
		t.Fatalf("unmeasured deadcode checked: %q", v)
	}
}

func TestTightenOnlyLowers(t *testing.T) {
	b := Baseline{
		Limits:           DefaultLimits,
		OversizedFiles:   map[string]int{"a.go": 900, "gone.go": 1000},
		ComplexFunctions: map[string]int{"p:F": 50},
		IssueRefs:        10,
		Deadcode:         7,
	}
	m := Measurement{
		OversizedFiles:   map[string]int{"a.go": 910},
		ComplexFunctions: map[string]int{"p:F": 45},
		IssueRefs:        []string{"x"},
		Deadcode:         3,
	}
	next, refused := Tighten(b, m)
	if refused != nil {
		t.Fatalf("refused %q", refused)
	}
	want := Baseline{Limits: DefaultLimits, OversizedFiles: map[string]int{"a.go": 900}, ComplexFunctions: map[string]int{"p:F": 45}, IssueRefs: 1, Deadcode: 3}
	if !reflect.DeepEqual(next, want) {
		t.Fatalf("tightened %+v, want %+v", next, want)
	}
	m.ComplexFunctions["p:G"] = 41
	if _, refused := Tighten(b, m); len(refused) != 1 {
		t.Fatalf("refused %q, want the new function", refused)
	}
}

func TestCompareBaselinesRejectsRaise(t *testing.T) {
	prev := Baseline{Limits: DefaultLimits, OversizedFiles: map[string]int{"a.go": 900}, ComplexFunctions: map[string]int{"p:F": 50}, IssueRefs: 3, Deadcode: 2}
	renamed := prev
	renamed.OversizedFiles = map[string]int{"b.go": 880}
	if v := CompareBaselines(prev, renamed); len(v) != 0 {
		t.Fatalf("rename rejected: %q", v)
	}
	raised := prev
	raised.Limits.Complexity = 60
	raised.OversizedFiles = map[string]int{"a.go": 901}
	raised.ComplexFunctions = map[string]int{"p:F": 50, "p:G": 41}
	raised.IssueRefs = 4
	raised.Deadcode = 3
	if v := CompareBaselines(prev, raised); len(v) != 7 {
		t.Fatalf("got %d problems, want 7: %q", len(v), v)
	}
}
