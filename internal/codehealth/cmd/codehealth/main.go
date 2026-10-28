// Command codehealth checks the tree against internal/codehealth/baseline.json.
//
//	codehealth [-deadcode] check          fail if the tree does not hold against the baseline
//	codehealth [-deadcode] update         lower the baseline to today's numbers; refuses to raise it
//	codehealth -base REV [-paydown] [-update] compare  fail if the baseline is looser than the one at REV; -update compares the baseline update would write, in memory
//	codehealth -base REV lint             fail on golangci-lint findings the tree at REV does not have
//	codehealth regen                      fail unless each generated file is what its command generates
//	codehealth refs                       list the issue references in comments
//	codehealth deadcode                   list the unreachable functions
//	codehealth [-deadcode] [-generated PATH=COMMAND] init  write the first baseline; refuses if one exists
//
// Run it from the module root (make codehealth, make codehealth-baseline).
// compare and lint extract REV with git archive and measure both trees, so
// a split or rename is recognized by its content, never by a declaration.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"binlogviz/internal/codehealth"
)

// deps are the outside tools a command uses, so tests can replace them.
type deps struct {
	deadcode func(root string, generated map[string]string) (map[string]string, error)
	lint     func(root string) ([]byte, error)
	extract  func(root, rev, dir string) error
	shell    func(root, command string) error
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, deps{deadcode: codehealth.RunDeadcode, lint: golangci, extract: gitArchive, shell: shell}))
}

// golangci runs golangci-lint on root with every finding reported.
func golangci(root string) ([]byte, error) {
	cmd := exec.Command("golangci-lint", "run", "--output.json.path=stdout", "--issues-exit-code=0",
		"--max-issues-per-linter=0", "--max-same-issues=0", "--show-stats=false", "./...")
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("golangci-lint in %s: %w: %s", root, err, stderr.String())
	}
	return out, nil
}

// gitArchive writes the tree at rev into dir.
func gitArchive(root, rev, dir string) error {
	tar := filepath.Join(dir, ".codehealth-base.tar")
	for _, args := range [][]string{
		{"git", "-C", root, "archive", "--format=tar", "-o", tar, rev},
		{"tar", "-x", "-f", tar, "-C", dir},
	} {
		if out, err := exec.Command(args[0], args[1:]...).CombinedOutput(); err != nil {
			return fmt.Errorf("extract %s: %w: %s", rev, err, out)
		}
	}
	return os.Remove(tar)
}

func shell(root, command string) error {
	cmd := exec.Command("sh", "-c", command)
	cmd.Dir = root
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s: %w: %s", command, err, out)
	}
	return nil
}

// pairs is a repeatable PATH=COMMAND flag.
type pairs map[string]string

func (p pairs) String() string { return fmt.Sprint(map[string]string(p)) }

func (p pairs) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" || v == "" {
		return fmt.Errorf("want PATH=COMMAND, got %q", s)
	}
	p[k] = v
	return nil
}

type options struct {
	root, baseline, base string
	deadcode, paydown    bool
	update               bool
	generated            pairs
	stdout               io.Writer
	deps                 deps
}

func run(args []string, stdout, stderr io.Writer, d deps) int {
	o := options{generated: pairs{}, stdout: stdout, deps: d}
	flags := flag.NewFlagSet("codehealth", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&o.root, "root", ".", "module root to measure")
	flags.StringVar(&o.baseline, "baseline", "internal/codehealth/baseline.json", "baseline file, relative to -root")
	flags.StringVar(&o.base, "base", "", "compare, lint: the git revision to compare with")
	flags.BoolVar(&o.deadcode, "deadcode", false, "also find unreachable functions with "+codehealth.DeadcodeTool)
	flags.BoolVar(&o.paydown, "paydown", false, "compare: require recording each paydown this change makes")
	flags.BoolVar(&o.update, "update", false, "compare: compare the baseline update would write instead of the committed one, in memory; nothing is written")
	flags.Var(o.generated, "generated", "init: PATH=COMMAND, a generated file and the command that regenerates it (repeatable)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 1 {
		flags.Usage()
		return 2
	}
	if err := o.dispatch(flags.Arg(0)); err != nil {
		// Nothing is left to report a failed write to.
		_, _ = fmt.Fprintln(stderr, "codehealth:", err)
		return 1
	}
	return 0
}

func (o options) dispatch(cmd string) error {
	path := filepath.Join(o.root, o.baseline)
	switch cmd {
	case "init":
		if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%s already exists; use update", path)
		}
		m, err := o.measure(o.root, o.generated, o.deadcode)
		if err != nil {
			return err
		}
		return codehealth.Save(path, codehealth.Initial(codehealth.DefaultLimits, o.generated, m))
	case "compare", "lint":
		if o.base == "" {
			return fmt.Errorf("usage: codehealth -base REV %s", cmd)
		}
	case "check", "update", "regen", "refs", "deadcode":
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	b, err := codehealth.Load(path)
	if err != nil {
		return err
	}
	switch cmd {
	case "regen":
		return o.report("generated files differ from their generators", o.regen(b.Generated), "generated files match their generators")
	case "compare":
		return o.compare(b)
	case "lint":
		return o.lint(b)
	}
	m, err := o.measure(o.root, b.Generated, o.deadcode || cmd == "deadcode")
	if err != nil {
		return err
	}
	switch cmd {
	case "check":
		for _, f := range m.Unparsed {
			if err := o.print("codehealth: note: " + f + " is not valid Go; only its lines are counted\n"); err != nil {
				return err
			}
		}
		return o.report("does not hold against the baseline", codehealth.Check(b, m), "holds against the baseline")
	case "update":
		next, refused := codehealth.Update(b, m)
		if len(refused) > 0 {
			return failure("refusing to raise the baseline", refused)
		}
		return codehealth.Save(path, next)
	case "refs":
		var out strings.Builder
		for _, file := range sortedKeys(m.Refs) {
			for _, r := range m.Refs[file] {
				out.WriteString(file + ":" + r + "\n")
			}
		}
		return o.print(out.String())
	}
	var out strings.Builder
	for _, key := range sortedKeys(m.Deadcode) {
		out.WriteString(m.Deadcode[key] + ": " + key + "\n")
	}
	return o.print(out.String())
}

// regen runs each generator and reports each listed file it changes. The
// regenerated files stay in the tree, so CI can also diff the whole tree.
func (o options) regen(generated map[string]string) []string {
	var problems []string
	for _, p := range sortedKeys(generated) {
		before, err := os.ReadFile(filepath.Join(o.root, p))
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		if err := o.deps.shell(o.root, generated[p]); err != nil {
			problems = append(problems, err.Error())
			continue
		}
		after, err := os.ReadFile(filepath.Join(o.root, p))
		if err != nil || !bytes.Equal(before, after) {
			problems = append(problems, fmt.Sprintf("%s is not what `%s` generates; run it and commit the result", p, generated[p]))
		}
	}
	return problems
}

// baseTree extracts o.base into a temporary directory.
func (o options) baseTree() (string, func(), error) {
	dir, err := os.MkdirTemp("", "codehealth-base-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	if err := o.deps.extract(o.root, o.base, dir); err != nil {
		cleanup()
		return "", nil, err
	}
	return dir, cleanup, nil
}

// compare compares the baseline with the one at the base revision. With
// -update it compares what the update command would write, computed in
// memory: make codehealth-compare previews the comparison CI runs on the
// committed baseline without leaving baseline.json modified on failure.
// CI runs it without -update, so only a committed baseline counts.
func (o options) compare(b codehealth.Baseline) error {
	dir, cleanup, err := o.baseTree()
	if err != nil {
		return err
	}
	defer cleanup()
	prev, err := codehealth.Load(filepath.Join(dir, o.baseline))
	if errors.Is(err, fs.ErrNotExist) {
		return o.print("codehealth: no baseline at " + o.base + "; this change adds the first one\n")
	}
	if err != nil {
		return err
	}
	baseM, err := codehealth.Measure(dir, prev.Generated)
	if err != nil {
		return err
	}
	prM, err := o.measure(o.root, b.Generated, o.deadcode || o.update)
	if err != nil {
		return err
	}
	next := b
	if o.update {
		var refused []string
		if next, refused = codehealth.Update(b, prM); len(refused) > 0 {
			return failure("refusing to raise the baseline", refused)
		}
	}
	return o.report("baseline is looser than at "+o.base, codehealth.Compare(prev, baseM, next, prM, o.paydown), "baseline is no looser than at "+o.base)
}

// lint runs golangci-lint, with this tree's configuration, on this tree and
// on the base tree, and fails on findings the base does not have.
func (o options) lint(b codehealth.Baseline) error {
	dir, cleanup, err := o.baseTree()
	if err != nil {
		return err
	}
	defer cleanup()
	config, err := os.ReadFile(filepath.Join(o.root, ".golangci.yml"))
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, ".golangci.yml"), config, 0o644); err != nil {
		return err
	}
	baseM, baseIssues, err := o.lintTree(dir, b.Generated)
	if err != nil {
		return err
	}
	prM, prIssues, err := o.lintTree(o.root, b.Generated)
	if err != nil {
		return err
	}
	return o.report("golangci-lint findings not in "+o.base, codehealth.DiffLint(baseM, baseIssues, prM, prIssues), "no golangci-lint findings beyond "+o.base)
}

func (o options) lintTree(root string, generated map[string]string) (codehealth.Measurement, []codehealth.LintIssue, error) {
	m, err := codehealth.Measure(root, generated)
	if err != nil {
		return m, nil, err
	}
	out, err := o.deps.lint(root)
	if err != nil {
		return m, nil, err
	}
	issues, err := codehealth.ParseLint(out)
	return m, issues, err
}

func (o options) print(s string) error {
	_, err := io.WriteString(o.stdout, s)
	return err
}

func (o options) measure(root string, generated map[string]string, withDeadcode bool) (codehealth.Measurement, error) {
	m, err := codehealth.Measure(root, generated)
	if err != nil || !withDeadcode {
		return m, err
	}
	m.Deadcode, err = o.deps.deadcode(root, generated)
	return m, err
}

func (o options) report(what string, problems []string, ok string) error {
	if len(problems) == 0 {
		return o.print("codehealth: ok: " + ok + "\n")
	}
	return failure(what, problems)
}

func failure(what string, problems []string) error {
	return fmt.Errorf("%s (%d); see docs/development/coding-standards.md\n  %s", what, len(problems), strings.Join(problems, "\n  "))
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
