package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const baselineRel = "internal/codehealth/baseline.json"

type fixture struct {
	t          *testing.T
	root       string
	dead       map[string]string
	deadErr    error
	deadCalled bool
	// lint maps a tree (base or this one) to its findings, as JSON.
	lint    func(root string) string
	lintErr error
}

func newFixture(t *testing.T, files map[string]string) *fixture {
	f := &fixture{t: t, root: t.TempDir()}
	if err := os.MkdirAll(filepath.Join(f.root, filepath.Dir(baselineRel)), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, src := range files {
		f.write(name, src)
	}
	return f
}

func (f *fixture) write(name, src string) {
	path := filepath.Join(f.root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) remove(name string) {
	if err := os.Remove(filepath.Join(f.root, filepath.FromSlash(name))); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) read(name string) string {
	data, err := os.ReadFile(filepath.Join(f.root, filepath.FromSlash(name)))
	if err != nil {
		f.t.Fatal(err)
	}
	return string(data)
}

// commit commits the whole tree and returns its revision.
func (f *fixture) commit() string {
	f.t.Helper()
	for _, args := range [][]string{
		{"init", "-q"},
		{"add", "-A"},
		{"-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "--allow-empty", "-m", "c"},
	} {
		if out, err := exec.Command("git", append([]string{"-C", f.root}, args...)...).CombinedOutput(); err != nil {
			f.t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	out, err := exec.Command("git", "-C", f.root, "rev-parse", "HEAD").Output()
	if err != nil {
		f.t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}

// run runs codehealth with -root set to the fixture and returns the exit
// code, stdout and stderr.
func (f *fixture) run(args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	d := deps{
		deadcode: func(string, map[string]string) (map[string]string, error) {
			f.deadCalled = true
			return f.dead, f.deadErr
		},
		lint: func(root string) ([]byte, error) {
			if f.lintErr != nil {
				return nil, f.lintErr
			}
			if f.lint == nil {
				return []byte(`{"Issues":[]}`), nil
			}
			return []byte(f.lint(root)), nil
		},
		extract: gitArchive,
		shell:   shell,
	}
	code := run(append([]string{"-root", f.root}, args...), &stdout, &stderr, d)
	return code, stdout.String(), stderr.String()
}

func (f *fixture) mustRun(args ...string) string {
	f.t.Helper()
	code, out, errOut := f.run(args...)
	if code != 0 {
		f.t.Fatalf("codehealth %v: exit %d\n%s", args, code, errOut)
	}
	return out
}

func (f *fixture) mustFail(want string, args ...string) {
	f.t.Helper()
	code, _, errOut := f.run(args...)
	if code != 1 || !strings.Contains(errOut, want) {
		f.t.Fatalf("codehealth %v: exit %d, want 1 with %q:\n%s", args, code, want, errOut)
	}
}

// goFile is a Go file of n lines whose content lines are prefix-0, prefix-1...
func goFile(n int, prefix string) string {
	var b strings.Builder
	b.WriteString("package p\n")
	for i := 1; i < n; i++ {
		fmt.Fprintf(&b, "var _ = %q\n", fmt.Sprintf("%s-%d", prefix, i))
	}
	return b.String()
}

func TestInitThenCheck(t *testing.T) {
	f := newFixture(t, map[string]string{"a.go": goFile(900, "a"), "g.go": goFile(900, "g"), "x/testdata/bad.go": "not go\n"})
	f.dead = map[string]string{".:old": "a.go:3:6"}
	f.mustRun("-deadcode", "-generated", "g.go=true", "init")
	b := f.read(baselineRel)
	for _, want := range []string{`"a.go": 900`, `"g.go": "true"`, `".:old": 1`} {
		if !strings.Contains(b, want) {
			t.Fatalf("baseline lacks %s:\n%s", want, b)
		}
	}
	f.mustFail("already exists", "init")
	out := f.mustRun("-deadcode", "check")
	if !strings.Contains(out, "ok: holds against the baseline") || !strings.Contains(out, "note: x/testdata/bad.go is not valid Go") {
		t.Fatalf("check output %q", out)
	}
	f.write("new.go", goFile(801, "n"))
	f.mustFail("files: new file over 800 lines: new.go has 801", "check")
}

func TestCheckFindsDeadcodeOnlyWithTheFlag(t *testing.T) {
	f := newFixture(t, map[string]string{"a.go": goFile(10, "a")})
	f.mustRun("init")
	f.dead = map[string]string{".:old": "a.go:3:6"}
	f.mustRun("check")
	if f.deadCalled {
		t.Fatal("deadcode ran without -deadcode")
	}
	f.mustFail("deadcode: .:old at a.go:3:6 is unreachable", "-deadcode", "check")
	if out := f.mustRun("deadcode"); out != "a.go:3:6: .:old\n" {
		t.Fatalf("deadcode output %q", out)
	}
}

func TestUpdate(t *testing.T) {
	f := newFixture(t, map[string]string{"a.go": goFile(900, "a")})
	f.mustRun("init")
	f.write("a.go", goFile(921, "a"))
	f.mustFail("refusing to raise the baseline (1)", "update")
	f.write("a.go", goFile(850, "a"))
	if out := f.mustRun("update"); out != "" {
		t.Fatalf("update printed %q", out)
	}
	if b := f.read(baselineRel); !strings.Contains(b, `"a.go": 850`) {
		t.Fatalf("baseline not lowered:\n%s", b)
	}
}

// compare extracts the base revision with git and traces new entries to
// it by content.
func TestCompare(t *testing.T) {
	f := newFixture(t, map[string]string{"a.go": goFile(2000, "a")})
	f.mustFail("usage: codehealth -base REV compare", "compare")
	first := f.commit()
	f.mustRun("init")
	if out := f.mustRun("-base", first, "compare"); !strings.Contains(out, "no baseline at "+first) {
		t.Fatalf("compare with no base baseline: %q", out)
	}
	base := f.commit()
	f.mustRun("-base", base, "-paydown", "compare")

	// A split: half of a.go moves to b.go.
	whole := strings.Split(goFile(2000, "a"), "\n")
	f.write("a.go", strings.Join(whole[:1000], "\n")+"\n")
	f.write("b.go", "package p\n"+strings.Join(whole[1000:], "\n"))
	f.mustRun("update")
	f.mustRun("-base", base, "-paydown", "compare")

	// The same new file, of new code.
	f.write("b.go", goFile(1001, "b"))
	f.mustRun("update")
	f.mustFail("files: adds b.go (1001 lines)", "-base", base, "compare")
	f.mustFail("not a valid object name: nosuchrev", "-base", "nosuchrev", "compare")

	// A tree that does not parse fails compare, on either side.
	f.write("bad.go", "package p\n\nfunc {\n")
	f.mustFail("bad.go", "-base", base, "compare")
	f.write(".golangci.yml", "version: \"2\"\n")
	f.mustFail("bad.go", "-base", base, "lint")
	broken := f.commit()
	f.remove("bad.go")
	f.mustFail("bad.go", "-base", broken, "compare")
	f.mustFail("bad.go", "-base", broken, "lint")
	f.mustFail("nosuchrev", "-base", "nosuchrev", "lint")
	f.remove(".golangci.yml")
	f.mustFail(".golangci.yml", "-base", base, "lint")
	good := f.read(baselineRel)
	f.write(baselineRel, "not json")
	broken = f.commit()
	f.write(baselineRel, good)
	f.mustFail("invalid character", "-base", broken, "compare")
	t.Setenv("TMPDIR", filepath.Join(f.root, "missing"))
	f.mustFail("missing", "-base", base, "compare")
}

// golangci runs the golangci-lint on PATH and fails when it fails.
func TestGolangci(t *testing.T) {
	bin := t.TempDir()
	t.Setenv("PATH", bin)
	tool := filepath.Join(bin, "golangci-lint")
	for _, c := range []struct{ script, out, err string }{
		{"echo '{}'", "{}\n", ""},
		{"echo broke >&2; exit 3", "", "exit status 3: broke"},
	} {
		if err := os.WriteFile(tool, []byte("#!/bin/sh\n"+c.script+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		out, err := golangci(t.TempDir())
		if string(out) != c.out || (err == nil) != (c.err == "") || err != nil && !strings.Contains(err.Error(), c.err) {
			t.Errorf("%s: got %q, %v", c.script, out, err)
		}
	}
}

// A failing tool fails the command instead of reading as no findings.
func TestToolErrorsFail(t *testing.T) {
	f := newFixture(t, map[string]string{"a.go": "package p\n", ".golangci.yml": "version: \"2\"\n"})
	f.deadErr = errors.New("deadcode broke")
	f.mustFail("deadcode broke", "-deadcode", "init")
	f.mustRun("init")
	f.mustFail("deadcode broke", "-deadcode", "check")
	f.mustFail("deadcode broke", "deadcode")
	base := f.commit()
	f.lintErr = errors.New("golangci broke")
	f.mustFail("golangci broke", "-base", base, "lint")
	f.lintErr = nil
	f.lint = func(string) string { return "not json" }
	f.mustFail("invalid character", "-base", base, "lint")
}

func TestPaydownOnlyOnRequest(t *testing.T) {
	f := newFixture(t, map[string]string{"a.go": goFile(1000, "a")})
	f.mustRun("init")
	base := f.commit()
	f.write("a.go", goFile(900, "a"))
	f.mustRun("-base", base, "compare")
	f.mustFail("files: a.go is down to 900 lines, ceiling 1000", "-base", base, "-paydown", "compare")
}

func TestRegen(t *testing.T) {
	f := newFixture(t, map[string]string{"g.go": "package p\n"})
	f.mustRun("-generated", "g.go=printf 'package p\\n' > g.go", "init")
	f.mustRun("regen")
	f.write("g.go", "package p\n\nvar edited = 1\n")
	f.mustFail("g.go is not what `printf 'package p\\n' > g.go` generates", "regen")
	if got := f.read("g.go"); got != "package p\n" {
		t.Fatalf("regen left %q", got)
	}
	f.write(baselineRel, strings.Replace(f.read(baselineRel), `> g.go"`, `> g.go; exit 3"`, 1))
	f.mustFail("exit status 3", "regen")
	f.remove("g.go")
	f.mustFail("no such file", "regen")

	// Every listed file is checked, not only the first that fails.
	two := newFixture(t, map[string]string{"g.go": "package p\n", "h.go": "package p\n"})
	two.mustRun("-generated", "g.go=exit 1", "-generated", "h.go=exit 2", "init")
	two.mustFail("exit status 2", "regen")
	two.remove("g.go")
	two.mustFail("exit status 2", "regen")
}

const lintSrc = `package p

func Run() {
	_ = 1
	_ = 2
	_ = 3
	_ = 4
	_ = 5
}
`

func issues(file string, line int, text string) string {
	return fmt.Sprintf(`{"Issues":[{"FromLinter":"errcheck","Text":%q,"Pos":{"Filename":%q,"Line":%d}}]}`, text, file, line)
}

// lint passes a finding that moved with its function, and fails a new one.
func TestLint(t *testing.T) {
	f := newFixture(t, map[string]string{"a.go": lintSrc, ".golangci.yml": "version: \"2\"\n"})
	f.mustRun("init")
	base := f.commit()
	var seenBase bool
	f.lint = func(root string) string {
		if root == f.root {
			return issues("b.go", 7, "Error return value of `RunMain` is not checked 2 times")
		}
		seenBase = true
		if got, err := os.ReadFile(filepath.Join(root, ".golangci.yml")); err != nil || string(got) != f.read(".golangci.yml") {
			t.Errorf("base tree lint config %q %v, want this tree's", got, err)
		}
		return issues("a.go", 4, "Error return value of `Run` is not checked 1 times")
	}
	f.remove("a.go")
	f.write("b.go", "package p\n\n// RunMain was Run.\n"+strings.Replace(lintSrc[len("package p\n\n"):], "Run", "RunMain", 1))
	f.write(".golangci.yml", "version: \"2\"\nlinters:\n  default: standard\n")
	f.mustRun("-base", base, "lint")
	if !seenBase {
		t.Fatal("base tree not linted")
	}
	f.lint = func(root string) string {
		if root == f.root {
			return issues("b.go", 7, "a new finding")
		}
		return issues("a.go", 4, "Error return value of `Run` is not checked")
	}
	f.mustFail("b.go:7: errcheck: a new finding", "-base", base, "lint")
	f.mustFail("usage: codehealth -base REV lint", "lint")
}

func TestRefsListsEveryReference(t *testing.T) {
	f := newFixture(t, map[string]string{"a.go": "package p\n\n// #1 and GH-2\n"})
	f.mustRun("init")
	if out := f.mustRun("refs"); out != "a.go:3: #1\na.go:3: GH-2\n" {
		t.Fatalf("refs output %q", out)
	}
}

func TestUsage(t *testing.T) {
	f := newFixture(t, nil)
	for _, args := range [][]string{nil, {"-nosuchflag", "check"}, {"check", "extra"}, {"-generated", "nopair", "init"}, {"-generated", "=cmd", "init"}, {"-generated", "g.go=", "init"}} {
		if code, _, _ := f.run(args...); code != 2 {
			t.Errorf("%v: exit %d, want 2", args, code)
		}
	}
	f.mustFail(`unknown command "frob"`, "frob")
	f.mustFail("no such file", "check")
}
