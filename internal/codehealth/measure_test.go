package codehealth

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const gocycloSrc = `package p

type runner[T any] struct{}

func Branchy(x int, ch chan int) int {
	if x > 0 && x < 10 || x == 99 {
		return 1
	}
	for i := 0; i < x; i++ {
	}
	for range []int{} {
	}
	switch x {
	case 1, 2:
	case 3:
	default:
	}
	select {
	case <-ch:
	case v := <-ch:
		_ = v
	default:
	}
	f := func() {
		if x == 5 {
		}
	}
	f()
	return 0
}

func (r *runner[T]) Method() {}

var Handler = func(x int) {
	if x > 1 {
	}
	if x > 2 {
	}
}

var a, b = 1, func() {
	if true {
	}
}
`

func TestComplexityCountsLikeGocyclo(t *testing.T) {
	m := measure(t, tree{"p.go": gocycloSrc}, nil)
	got := funcValues(m.Funcs, complexityOf)
	want := map[string]int{".:Branchy": 11, ".:runner.Method": 1, ".:Handler": 3, ".:b": 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("complexities %v, want %v", got, want)
	}
}

// funlenSrc and its numbers come from golangci-lint v2.15.0 funlen with
// ignore-comments (the repository's setting): a comment inside the function
// takes off one line however many it spans, and a select's cases add no
// statements.
const funlenSrc = `package p

func F() {
	// a comment
	_ = 1

	_ = 2 // trailing
	/* block
	   two */
	_ = 3
}

func G(ch chan int) {
	select {
	case <-ch:
		_ = 1
		_ = 2
	}
	go func() {
		_ = 1
		_ = 2
	}()
	x := func() {
		_ = 3
	}
	_ = x
	switch {
	case true:
		_ = 4
	}
	{
		_ = 5
	}
}

func H(a int,
	b int) {
	_ = a
}
`

func TestFunctionsCountLikeFunlen(t *testing.T) {
	// A's comments sit on the lines of its braces, so funlen keeps them.
	// asm has no body (it is implemented in assembly) and is not measured.
	q := "package p\n\nfunc One() {}\n\nfunc Two() { _ = 1 }\n\nfunc A() { // opening note\n\tprintln(1)\n\tprintln(2)\n} // closing note\n\nfunc Three() {\n\tprintln(3)\n}\n\nfunc asm(x int) int\n"
	m := measure(t, tree{"p.go": funlenSrc, "q.go": q}, nil)
	if got, want := funcValues(m.Funcs, linesOf), map[string]int{".:F": 4, ".:G": 20, ".:H": 2, ".:One": -1, ".:Two": -1, ".:A": 2, ".:Three": 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("lines %v, want %v", got, want)
	}
	if got, want := m.Funcs[".:A"].Body, []string{"println(1)", "println(2)"}; !reflect.DeepEqual(got, want) {
		t.Errorf("A body %q, want %q", got, want)
	}
	if got, want := m.Funcs[".:Three"].Body, []string{"println(3)"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Three body %q, want %q", got, want)
	}
	if got, want := funcValues(m.Funcs, statementsOf), map[string]int{".:F": 3, ".:G": 12, ".:H": 1, ".:One": 0, ".:Two": 1, ".:A": 2, ".:Three": 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("statements %v, want %v", got, want)
	}
	if got, want := m.Funcs[".:F"].Body, []string{"// a comment", "_ = 1", "_ = 2 // trailing", "/* block", "two */", "_ = 3"}; !reflect.DeepEqual(got, want) {
		t.Errorf("body %q, want %q", got, want)
	}
	if f := m.Funcs[".:G"]; f.File != "p.go" || f.Start != 13 || f.End != 34 {
		t.Errorf("G at %s:%d-%d, want p.go:13-34", f.File, f.Start, f.End)
	}
}

func TestCountLines(t *testing.T) {
	for src, want := range map[string]int{"": 0, "a": 1, "a\n": 1, "a\nb": 2, "a\nb\n": 2, "\n\n": 2} {
		if got := countLines([]byte(src)); got != want {
			t.Errorf("countLines(%q) = %d, want %d", src, got, want)
		}
	}
}

// Issue references count only in forms that cannot be ordinary prose.
func TestIssueRefStyles(t *testing.T) {
	cases := []struct {
		comment string
		want    int
	}{
		{"see #12", 1},
		{"(#7) and #8", 2},
		{"#358/#361", 2},
		{"fixed in issue 12", 1},
		{"Issue: 12", 1},
		{"issue number 12", 1},
		{"Issues 359", 1},
		{"issue #462", 1},
		{"issue189", 1},
		{"PR#12", 1},
		{"PR #12", 1},
		{"GH-12", 1},
		{"GH12", 1},
		{"Fanduzi/BinlogServer#12", 1},
		{"https://github.com/Fanduzi/BinlogServer/issues/12", 1},
		{"https://github.com/Fanduzi/BinlogServer/pull/12", 1},
		{"pull/446", 1},
		{"full-width ＃12", 1},
		{"full-width digits ＃１２３", 1},
		{"from QA r6", 1},
		{"the r3 review", 1},
		{"#12 and issue 12 and PR #3", 3},
		{"trx #2 hit #333", 1},
		{"fixes #333", 1},
		{"cannot change the sealed segment, #415)", 1},
		{"one per task; #12", 1},
		{"after the phase. #12", 1},
		{"(the run) #12", 1},
	}
	for _, c := range cases {
		if got := refsIn(c.comment); len(got) != c.want {
			t.Errorf("%q: refs %q, want %d", c.comment, got, c.want)
		}
	}
	m := measure(t, tree{"p_test.go": "package p\n\n/* block\n   #5 */\nvar x = \"#99 in a string is not a comment\"\n"}, nil)
	if want := []string{"4: #5"}; !reflect.DeepEqual(m.Refs["p_test.go"], want) {
		t.Errorf("refs in a test block comment %q, want %q", m.Refs["p_test.go"], want)
	}
}

// Prose, SQL and binlog positions that look like references do not count.
// The cost is recall: "round 3", "§ 9.2", "bug 12", "see 12", "PR-3" and a
// bare "(12)" are not counted, because the same words are ordinary prose.
func TestIssueRefFalsePositives(t *testing.T) {
	for _, comment := range []string{
		"fixed in round 3", "QA rounds 5", "ROUND 2", "round-3", "§ 9.2", "§3",
		"Column #3", "Field #2", "row #4", "arg #1", "step #2", "byte #7",
		"The server issues 2 events", "pr0 register", "mysql-bin/000001#4", "a r3 register",
		"&#12; is an entity", "C#12 is not a ref", "color #123abc", "color #fff", "RFC 9110 section 11.1",
		"process 12 items", "bug 12", "see 12", "ADR 0011 PR-3", "returns (12)", "##12",
		"trx #2 commits first", "Transaction #2", "thread #3 holds the lock", "worker #2", "Session #1", "Query #3",
		"text color: #333", "background #333", "border: #333", "(fill #000)",
	} {
		if got := refsIn(comment); len(got) != 0 {
			t.Errorf("%q: counted %q", comment, got)
		}
	}
	for _, word := range strings.Fields(`trx txn tx transaction thread worker session query statement stmt conn connection
		server node replica shard partition table file segment chunk batch packet record block job task run pass phase
		stage iteration round color colour background bg fg foreground border fill stroke`) {
		if got := refsIn("the " + word + " #2"); len(got) != 0 {
			t.Errorf("%s #2: counted %q", word, got)
		}
	}
}

// Every directory but .git is measured, because some tool compiles or
// ships each of them; a nested module is a problem, and a file under
// testdata that is not Go is counted by lines and reported.
func TestMeasureWalksEverything(t *testing.T) {
	big := file(801, "")
	m := measure(t, tree{
		"a.go":                   big,
		"a_test.go":              big,
		"vendor/v/v.go":          big,
		"x/testdata/t/t.go":      big,
		"x/.hidden/h.go":         big,
		"x/_under/u.go":          big,
		"x/.git/objects/o.go":    big,
		"ui/node_modules/n/n.go": big,
		"nested/go.mod":          "module nested\n",
		"nested/n.go":            big,
		"x/testdata/mod/go.mod":  "module t\n",
		"notgo.txt":              big,
		"x/testdata/bad.go":      "package\n\nnot go\n",
	}, nil)
	want := map[string]int{"a.go": 801, "vendor/v/v.go": 801, "x/testdata/t/t.go": 801, "x/.hidden/h.go": 801, "x/_under/u.go": 801, "ui/node_modules/n/n.go": 801, "nested/n.go": 801, "x/testdata/bad.go": 3}
	if !reflect.DeepEqual(m.Files, want) {
		t.Fatalf("files %v, want %v", m.Files, want)
	}
	if !reflect.DeepEqual(m.TestFiles, map[string]int{"a_test.go": 801}) {
		t.Fatalf("test files %v", m.TestFiles)
	}
	if want := []string{"x/testdata/bad.go"}; !reflect.DeepEqual(m.Unparsed, want) {
		t.Fatalf("unparsed %v, want %v", m.Unparsed, want)
	}
	if len(m.Problems) != 1 || !strings.Contains(m.Problems[0], "nested module nested/go.mod") {
		t.Fatalf("problems %q, want one nested module", m.Problems)
	}
	if _, err := Measure(tree{"bad.go": "package"}.write(t), nil); err == nil {
		t.Fatal("a file outside testdata that does not parse was not an error")
	}
}

// Only the listed paths are skipped, whatever a file's header says.
func TestGeneratedOnlyByList(t *testing.T) {
	const header = "// Code generated by gen. DO NOT EDIT.\n"
	m := measure(t, tree{
		"a/g.go":     header + file(900, complexFunc("F", 50)),
		"b/g.go":     header + file(900, complexFunc("F", 50)),
		"c/g.go":     header + file(900, ""),
		"c/plain.go": file(900, ""),
	}, map[string]string{"a/g.go": "gen", "c/g.go": "gen"})
	if want := map[string]int{"b/g.go": 901, "c/plain.go": 900}; !reflect.DeepEqual(m.Files, want) {
		t.Fatalf("files %v, want %v", m.Files, want)
	}
	if _, ok := m.Funcs["a:F"]; ok {
		t.Fatal("functions of a listed file measured")
	}
	if m.Funcs["b:F"].Complexity != 50 {
		t.Fatalf("functions of an unlisted file with a header not measured: %v", m.Funcs)
	}
}

// A //nolint needs a reason of a few words.
func TestWeakNolint(t *testing.T) {
	src := "package p\n\nvar a = 1 //nolint:gosec // fine\nvar b = 2 //nolint:gosec // test only\nvar c = 3 //nolint:gosec // the path is a constant\nvar d = 4 //nolint // ok\nvar e = 5 //nolint:gosec\nvar f = 6 // nolint\nvar g = 7 // nolintlint checks directives\nvar h = 8 //nolint:gosec // exactly three words\n"
	m := measure(t, tree{"p.go": src}, nil)
	if len(m.Problems) != 5 || !strings.Contains(m.Problems[0], `p.go:3: //nolint reason "fine"`) || !strings.Contains(m.Problems[1], `p.go:4:`) || !strings.Contains(m.Problems[2], `p.go:6:`) ||
		!strings.Contains(m.Problems[3], `p.go:7: //nolint reason ""`) || !strings.Contains(m.Problems[4], `p.go:8:`) {
		t.Fatalf("problems %q", m.Problems)
	}
}

// A function literal nested in another counts toward the outer one, and
// each literal in a composite value counts.
func TestPackageLevelLiterals(t *testing.T) {
	src := "package p\n\nvar Table = map[string]func(int) int{\n\t\"a\": " + strings.TrimSuffix(complexFunc("", 3), "\n") + ",\n\t\"b\": " + strings.TrimSuffix(complexFunc("", 45), "\n") + ",\n}\n"
	m := measure(t, tree{"p.go": src}, nil)
	if got := m.Funcs[".:Table"].Complexity; got != 45 {
		t.Fatalf("Table complexity %d, want 45 (the larger literal)", got)
	}
	nested := "package p\n\nvar Outer = func() {\n\tif true {\n\t}\n\t_ = func() {\n\t\tif true {\n\t\t}\n\t}\n}\n"
	if got := measure(t, tree{"p.go": nested}, nil).Funcs[".:Outer"].Complexity; got != 3 {
		t.Fatalf("Outer complexity %d, want 3", got)
	}
}

// Files for different build tags may declare the same function; the
// larger one counts, whichever file the walk reads last.
func TestSameKeyKeepsTheLargest(t *testing.T) {
	m := measure(t, tree{
		"a_linux.go":   "//go:build linux\n\n" + file(200, complexFunc("F", 45)),
		"a_windows.go": "//go:build windows\n\n" + file(200, complexFunc("F", 42)),
		"b.go":         file(100, complexFunc("init", 3)+complexFunc("init", 2)),
	}, nil)
	if got := m.Funcs[".:F"]; got.Complexity != 45 || got.Lines != 3*44+1 {
		t.Errorf(".:F = %+v, want complexity 45, %d lines", got, 3*44+1)
	}
	if got := m.Funcs[".:init"].Complexity; got != 3 {
		t.Errorf(".:init complexity = %d, want 3", got)
	}
	// The longer copy is read last this time.
	m = measure(t, tree{
		"c_linux.go":   "//go:build linux\n\n" + file(200, longFunc("G", 160)),
		"c_windows.go": "//go:build windows\n\n" + file(200, longFunc("G", 170)),
	}, nil)
	if got := m.Funcs[".:G"]; got.Lines != 170 || len(got.Body) != 170 {
		t.Errorf(".:G has %d lines and %d body lines, want 170", got.Lines, len(got.Body))
	}
	// The longer copy has fewer statements; each metric takes its larger value.
	m = measure(t, tree{
		"d_linux.go":   "//go:build linux\n\n" + file(200, "func H() {\n"+strings.Repeat("\n", 170)+"\t_ = 0\n}\n"),
		"d_windows.go": "//go:build windows\n\n" + file(200, longFunc("H", 160)),
	}, nil)
	if got := m.Funcs[".:H"]; got.Lines != 171 || got.Statements != 160 {
		t.Errorf(".:H = %d lines, %d statements, want 171 and 160", got.Lines, got.Statements)
	}
}

// A file's text, which lineage compares, leaves out its package clause and
// imports: every file has them.
func TestTextSkipsPackageAndImports(t *testing.T) {
	m := measure(t, tree{"p.go": "package p\n\nimport (\n\t\"fmt\"\n\t\"os\"\n)\n\nimport \"io\"\n\nvar _ = fmt.Sprint\nvar _ = os.Exit\nvar _ = io.EOF\n"}, nil)
	if got, want := m.Text["p.go"], []string{"var _ = fmt.Sprint", "var _ = os.Exit", "var _ = io.EOF"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("text %q, want %q", got, want)
	}
}

// A tree that cannot be read fails instead of measuring as empty.
func TestMeasureReadErrors(t *testing.T) {
	if _, err := Measure(filepath.Join(t.TempDir(), "missing"), nil); err == nil {
		t.Error("missing root measured")
	}
	dir := t.TempDir()
	if err := os.Symlink(filepath.Join(dir, "nowhere.go"), filepath.Join(dir, "a.go")); err != nil {
		t.Fatal(err)
	}
	if _, err := Measure(dir, nil); err == nil {
		t.Error("dangling a.go measured")
	}
}

// Only files with references have an entry.
func TestRefsOnlyFilesWithRefs(t *testing.T) {
	m := measure(t, tree{"a.go": "package p\n", "b.go": "package p\n\n// see #12\n"}, nil)
	if !reflect.DeepEqual(m.Refs, map[string][]string{"b.go": {"3: #12"}}) {
		t.Fatalf("refs = %v", m.Refs)
	}
}

// In a git work tree only what git would commit is measured: tracked files
// and untracked ones that are not ignored. A worktree under an ignored
// .worktrees/, an ignored .claude/, an untracked nested repository and a
// tracked file deleted from disk do not count.
func TestMeasureWorkTreeSkipsIgnoredAndNestedRepos(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git command")
	}
	big := file(801, "")
	root := tree{
		".gitignore":           ".worktrees/\n.claude/\n",
		"go.mod":               "module m\n",
		"a.go":                 big,
		"gone.go":              big,
		".worktrees/wt/go.mod": "module m\n",
		".worktrees/wt/a.go":   big,
		".worktrees/wt/.git":   "gitdir: elsewhere\n",
		".claude/x.go":         big,
		"vendored/r.go/go.mod": "module r\n", // a nested repository whose directory looks like a Go file
		"vendored/r.go/r.go":   big,
	}.write(t)
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t", "GIT_CONFIG_GLOBAL=/dev/null")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	git("init", "-q")
	git("init", "-q", "vendored/r.go")
	git("add", ".gitignore", "go.mod", "a.go", "gone.go")
	git("commit", "-q", "-m", "init")
	// A submodule is a directory in the index; this one's name looks like a Go file.
	git("update-index", "--add", "--cacheinfo", "160000,"+strings.Repeat("1", 40)+",sub.go")
	if err := os.Mkdir(filepath.Join(root, "sub.go"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "gone.go")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "new.go"), []byte(big), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := Measure(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]int{"a.go": 801, "new.go": 801}; !reflect.DeepEqual(m.Files, want) {
		t.Fatalf("files %v, want %v", m.Files, want)
	}
	if len(m.Problems) != 0 {
		t.Fatalf("problems %q", m.Problems)
	}
	if _, err := Measure(tree{".git": "not a repository\n", "a.go": "package p\n"}.write(t), nil); err == nil {
		t.Fatal("a broken .git measured without an error")
	}
}

// A path git lists more than once, as an unmerged file in a conflict, is
// measured once, and a listed file that cannot be read is an error.
func TestMeasureWorkTreeConflictsAndErrors(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git command")
	}
	root := tree{"go.mod": "module m\n", "nested/go.mod": "module a\n", "x/a.go": "package x\n"}.write(t)
	git := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t", "GIT_CONFIG_GLOBAL=/dev/null")
		_, _ = cmd.CombinedOutput()
	}
	write := func(name, src string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("init", "-q", "-b", "main")
	git("add", ".")
	git("commit", "-q", "-m", "base")
	git("checkout", "-q", "-b", "other")
	write("nested/go.mod", "module b\n")
	git("commit", "-q", "-am", "b")
	git("checkout", "-q", "main")
	write("nested/go.mod", "module c\n")
	git("commit", "-q", "-am", "c")
	git("merge", "-q", "other")
	m, err := Measure(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Problems) != 1 || !strings.Contains(m.Problems[0], "nested module nested/go.mod") {
		t.Fatalf("problems %q, want the conflicted nested module once", m.Problems)
	}
	if os.Getuid() == 0 {
		return // root reads anything
	}
	if err := os.Chmod(filepath.Join(root, "x"), 0); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(filepath.Join(root, "x"), 0o755) }()
	if _, err := Measure(root, nil); err == nil {
		t.Fatal("an unreadable tracked file measured without an error")
	}
}
