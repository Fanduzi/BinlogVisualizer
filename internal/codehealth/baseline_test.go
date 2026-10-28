package codehealth

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const repoRoot = "../.."

// The repository matches its checked-in baseline: nothing over a ceiling,
// nothing new, and no ceiling above what is there now. deadcode needs a
// separate tool, so CI checks it in the lint workflow.
func TestRepoWithinBaseline(t *testing.T) {
	b, err := Load("baseline.json")
	if err != nil {
		t.Fatal(err)
	}
	m, err := Measure(repoRoot, b.Generated)
	if err != nil {
		t.Fatal(err)
	}
	expect(t, Check(b, m), "")
}

func TestBaselineLimitsMatchStandards(t *testing.T) {
	b, err := Load("baseline.json")
	if err != nil {
		t.Fatal(err)
	}
	if b.Limits != DefaultLimits {
		t.Fatalf("baseline limits %+v, want %+v", b.Limits, DefaultLimits)
	}
}

// golangci-lint does not honor generated-code markers; it skips exactly the
// files on the codehealth allowlist, so a marker cannot hide new lint issues.
func TestGolangciSkipsOnlyAllowlistedGeneratedFiles(t *testing.T) {
	b, err := Load("baseline.json")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(repoRoot, ".golangci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	type exclusions struct {
		Generated string   `yaml:"generated"`
		Paths     []string `yaml:"paths"`
	}
	var cfg struct {
		Linters struct {
			Exclusions exclusions `yaml:"exclusions"`
		} `yaml:"linters"`
		Formatters struct {
			Exclusions exclusions `yaml:"exclusions"`
		} `yaml:"formatters"`
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	want := []string{}
	for _, path := range sortedKeys(b.Generated) {
		want = append(want, "^"+regexp.QuoteMeta(path)+"$")
	}
	for name, ex := range map[string]exclusions{"linters": cfg.Linters.Exclusions, "formatters": cfg.Formatters.Exclusions} {
		got := append([]string{}, ex.Paths...)
		if ex.Generated != "disable" || !reflect.DeepEqual(got, want) {
			t.Errorf("%s.exclusions: generated %q paths %q, want generated \"disable\" and paths %q (generated_files in baseline.json)", name, ex.Generated, got, want)
		}
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "b.json")
	lim := `"limits":{"file_lines":800,"growth_tolerance_lines":20,"complexity":40,"function_lines":150,"function_statements":100}`
	bad := []string{"{" + lim + `,"issue_ref_total":3}`, "{" + lim + `} {"files":{"a.go":9999}}`, "{" + lim + `,"files":{"a.go":{"value":1}}}`}
	// A baseline from before a limit existed reads that limit as 0; it is
	// rejected instead of measuring every function against 0.
	for _, field := range []string{`"file_lines":800,`, `"growth_tolerance_lines":20,`, `"complexity":40,`, `"function_lines":150,`, `,"function_statements":100`} {
		bad = append(bad, "{"+strings.Replace(lim, field, "", 1)+"}")
	}
	for _, bad := range bad {
		if err := os.WriteFile(path, []byte(bad), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	// Limits of 1 are allowed, and a missing ledger is written back as {}.
	if err := os.WriteFile(path, []byte(`{"limits":{"file_lines":1,"growth_tolerance_lines":1,"complexity":1,"function_lines":1,"function_statements":1}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	ones, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := Save(path, ones); err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(path); !strings.Contains(string(data), `"deadcode": {}`) {
		t.Errorf("saved %s", data)
	}
	b := Baseline{Limits: DefaultLimits, Files: Ledger{"a.go": 900}}
	if err := Save(path, b); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil || !reflect.DeepEqual(got, b.normalized()) {
		t.Fatalf("load after save %+v %v", got, err)
	}
}

func TestParseDeadcode(t *testing.T) {
	out := "go: downloading x\ninternal/a/b.go:3:6: unreachable func: helper\ninternal/a/c.go:9:18: unreachable func: Server.doc\nmain.go:1:6: unreachable func: old\n\nsomething else: unreachable\n"
	want := map[string]string{"internal/a:helper": "internal/a/b.go:3:6", "internal/a:Server.doc": "internal/a/c.go:9:18", ".:old": "main.go:1:6"}
	if got := ParseDeadcode(out); !reflect.DeepEqual(got, want) {
		t.Fatalf("parsed %v, want %v", got, want)
	}
}

func TestCheckSkipsUnmeasuredDeadcode(t *testing.T) {
	b := Baseline{Limits: DefaultLimits, Deadcode: Ledger{"x:F": 1}}
	expect(t, Check(b, Measurement{}), "")
	expect(t, Check(b, Measurement{Deadcode: map[string]string{"x:G": "x/a.go:3:6"}}), "deadcode: x:G at x/a.go:3:6 is unreachable")
	next, refused := Update(b, Measurement{})
	expect(t, refused, "")
	if !reflect.DeepEqual(next.Deadcode, b.Deadcode) {
		t.Fatalf("unmeasured deadcode changed: %v", next.Deadcode)
	}
}

func TestUpdate(t *testing.T) {
	b := Baseline{Limits: DefaultLimits, Files: Ledger{"a.go": 900, "b.go": 1000, "gone.go": 1000}}
	m := Measurement{Files: map[string]int{"a.go": 915, "b.go": 950, "gone.go": 800, "new.go": 801, "small.go": 800}}
	next, refused := Update(b, m)
	expect(t, refused, "")
	if want := (Ledger{"a.go": 900, "b.go": 950, "new.go": 801}); !reflect.DeepEqual(next.Files, want) {
		t.Fatalf("files %v, want %v", next.Files, want)
	}
	m.Files["a.go"] = 921
	same, refused := Update(b, m)
	expect(t, refused, "files: a.go grew to 921 lines, ceiling 900 (+20 allowed)")
	if !reflect.DeepEqual(same, b.normalized()) {
		t.Fatalf("refused update changed the baseline: %v", same.Files)
	}
}

// Check alone holds the tolerance: a file may sit up to growth_tolerance
// lines above its ceiling, a long function 5 lines, a complex function 2,
// issue references not even one. Below the ceiling always passes.
func TestCheckToleranceBoundary(t *testing.T) {
	b := Baseline{Limits: DefaultLimits, Files: Ledger{"a.go": 837}, ComplexFunctions: Ledger{".:Run": 60}, LongFunctions: Ledger{".:Long": 160, ".:Run": 1000}, StatementFunctions: Ledger{".:Long": 1000, ".:Run": 1000}, IssueRefs: Ledger{"a.go": 2}}
	for _, c := range []struct {
		lines, complexity, long int
		refs                    string
		want                    string
	}{
		{857, 60, 165, "#1 #2", ""},
		{600, 50, 155, "", ""},
		{858, 60, 160, "", "files: a.go grew to 858 lines, ceiling 837 (+20 allowed)"},
		{857, 62, 160, "", ""},
		{857, 63, 160, "", "complex_functions: .:Run grew to 63 complexity, ceiling 60 (+2 allowed)"},
		{857, 60, 166, "", "long_functions: .:Long grew to 166 lines, ceiling 160 (+5 allowed)"},
		{857, 60, 160, "#1 #2 #3", "issue_refs: a.go grew to 3 issue references in comments, ceiling 2 (+0 allowed)"},
	} {
		m := measure(t, tree{"a.go": file(c.lines, "// "+c.refs+"\n"+complexFunc("Run", c.complexity)+longFunc("Long", c.long))}, nil)
		expect(t, Check(b, m), c.want)
	}
}

// Initial records values over a limit, not at it.
func TestInitialBoundary(t *testing.T) {
	m := measure(t, tree{"a.go": file(800, ""), "b.go": file(801, complexFunc("At", 40)+complexFunc("Over", 41))}, nil)
	b := Initial(DefaultLimits, nil, m)
	if want := (Ledger{"b.go": 801}); !reflect.DeepEqual(b.Files, want) {
		t.Errorf("files %v, want %v", b.Files, want)
	}
	if want := (Ledger{".:Over": 41}); !reflect.DeepEqual(b.ComplexFunctions, want) {
		t.Errorf("complex %v, want %v", b.ComplexFunctions, want)
	}
}

// A new entry's hint names make codehealth-compare, which previews the
// comparison CI runs on the committed baseline without writing it, so
// following the hint cannot leave the local gates green while CI fails.
func TestNewEntryHintNamesCompare(t *testing.T) {
	b := Baseline{Limits: DefaultLimits}
	m := measure(t, tree{"a.go": file(900, ""), "b.go": "package p\n// #1\n"}, nil)
	for _, want := range []string{
		"files: new file over 800 lines: a.go has 900; split it. If it was split or renamed from a baseline entry, run make codehealth-compare to check, as CI does, that its content came from that entry, then make codehealth-baseline to record the entry",
		"issue_refs: b.go has 1 issue references in comments, none allowed in a new file; put the history in the commit message or ADR (list them: codehealth refs). If the file was renamed or split from a baselined file, run make codehealth-compare to check, as CI does, that its content came from that file, then make codehealth-baseline to record the entry",
	} {
		expect(t, Check(b, m), want)
	}
	m.Deadcode = map[string]string{"x:New": "x/a.go:3:6"}
	expect(t, Check(b, m), "deadcode: x:New at x/a.go:3:6 is unreachable; delete it, or call it from non-test code in this same pull request; no dead code lands and test-only use counts as dead")
	makefile, err := os.ReadFile(filepath.Join(repoRoot, "Makefile"))
	if err != nil {
		t.Fatal(err)
	}
	if want := "codehealth-compare:\n\tgo run ./internal/codehealth/cmd/codehealth -base $(LINT_BASE) -paydown -update compare\n"; !strings.Contains(string(makefile), want) {
		t.Fatalf("Makefile has no target %q", want)
	}
}
