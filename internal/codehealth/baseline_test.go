package codehealth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
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

func TestEntryJSON(t *testing.T) {
	l := Ledger{"a.go": {Value: 900}, "b.go": {Value: 850, SplitFrom: "c.go"}}
	data, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"a.go":900,"b.go":{"value":850,"split_from":"c.go"}}`; string(data) != want {
		t.Fatalf("marshal %s, want %s", data, want)
	}
	var back Ledger
	if err := json.Unmarshal(data, &back); err != nil || !reflect.DeepEqual(back, l) {
		t.Fatalf("round trip %v %v, want %v", back, err, l)
	}
	for _, bad := range []string{`{"a.go":{"value":1}}`, `{"a.go":{"value":1,"split_from":"b","x":1}}`, `{"a.go":"1"}`} {
		if err := json.Unmarshal([]byte(bad), &back); err == nil {
			t.Errorf("%s: no error", bad)
		}
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "b.json")
	if err := os.WriteFile(path, []byte(`{"limits":{},"issue_ref_total":3}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("unknown field accepted")
	}
	b := Baseline{Limits: DefaultLimits, Files: Ledger{"a.go": {Value: 900}}}
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
	b := Baseline{Limits: DefaultLimits, Deadcode: Ledger{"x:F": {Value: 1}}}
	expect(t, Check(b, Measurement{}), "")
	expect(t, Check(b, Measurement{Deadcode: map[string]string{}}), "deadcode: x:F is down to 0")
	next, refused := Update(b, Measurement{}, nil)
	expect(t, refused, "")
	if !reflect.DeepEqual(next.Deadcode, b.Deadcode) {
		t.Fatalf("unmeasured deadcode changed: %v", next.Deadcode)
	}
}

func TestUpdateLowersAndKeepsLineage(t *testing.T) {
	b := Baseline{Limits: DefaultLimits, Files: Ledger{"a.go": {Value: 900}, "b.go": {Value: 1000, SplitFrom: "c.go"}, "gone.go": {Value: 1000}}}
	m := Measurement{Files: map[string]int{"a.go": 915, "b.go": 950, "gone.go": 800}}
	next, refused := Update(b, m, nil)
	expect(t, refused, "")
	want := Ledger{"a.go": {Value: 900}, "b.go": {Value: 950, SplitFrom: "c.go"}}
	if !reflect.DeepEqual(next.Files, want) {
		t.Fatalf("files %v, want %v", next.Files, want)
	}
	m.Files["a.go"] = 921
	if same, refused := Update(b, m, nil); len(refused) == 0 || !reflect.DeepEqual(same, b.normalized()) {
		t.Fatalf("growth past tolerance: refused %q, baseline %v", refused, same.Files)
	}
}

// Check alone holds the tolerance: a file may sit up to growth_tolerance
// lines above its ceiling, a function not even one above.
func TestCheckToleranceBoundary(t *testing.T) {
	lim := DefaultLimits
	lim.FunctionLines = 1000 // only lines and complexity vary here
	b := Baseline{Limits: lim, Files: Ledger{"a.go": {Value: 837}}, ComplexFunctions: Ledger{".:Run": {Value: 60}}}
	for _, c := range []struct {
		lines, complexity int
		want              string
	}{
		{857, 60, ""},
		{858, 60, "files: a.go grew to 858 lines, ceiling 837 (+20 allowed)"},
		{857, 61, "complex_functions: .:Run grew to 61 complexity, ceiling 60 (+0 allowed)"},
	} {
		m := measure(t, tree{"a.go": file(c.lines, complexFunc("Run", c.complexity))}, nil)
		expect(t, Check(b, m), c.want)
	}
}
