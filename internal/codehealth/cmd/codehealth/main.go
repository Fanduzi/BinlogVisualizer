// Command codehealth checks the tree against internal/codehealth/baseline.json.
//
//	codehealth [-deadcode] check                fail if the tree does not match the baseline
//	codehealth [-deadcode] [-from NEW=OLD] update  lower the baseline to today's numbers; refuses to raise it
//	codehealth -base REV compare PREV.json      fail if the baseline is looser than PREV.json (the baseline at REV)
//	codehealth refs                             list the issue references in comments
//	codehealth deadcode                         list the unreachable functions
//	codehealth [-deadcode] [-generated PATH=GEN] init  write the first baseline; refuses if one exists
//
// -from records that NEW (a file path or "pkgdir:Func") was split or renamed
// from the baseline entry OLD; repeat it for each piece. Run it from the
// module root (make codehealth, make codehealth-baseline).
package main

import (
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
	deadcode      func(root string) (map[string]string, error)
	existedAtBase func(root, rev, path string) bool
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, deps{deadcode: codehealth.RunDeadcode, existedAtBase: gitHas}))
}

func gitHas(root, rev, path string) bool {
	return exec.Command("git", "-C", root, "cat-file", "-e", rev+":"+path).Run() == nil
}

// pairs is a repeatable NEW=OLD flag.
type pairs map[string]string

func (p pairs) String() string { return fmt.Sprint(map[string]string(p)) }

func (p pairs) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" || v == "" {
		return fmt.Errorf("want NEW=OLD, got %q", s)
	}
	p[k] = v
	return nil
}

type options struct {
	root, baseline, base string
	deadcode             bool
	from, generated      pairs
	stdout               io.Writer
	deps                 deps
}

func run(args []string, stdout, stderr io.Writer, d deps) int {
	o := options{from: pairs{}, generated: pairs{}, stdout: stdout, deps: d}
	flags := flag.NewFlagSet("codehealth", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&o.root, "root", ".", "module root to measure")
	flags.StringVar(&o.baseline, "baseline", "internal/codehealth/baseline.json", "baseline file, relative to -root")
	flags.StringVar(&o.base, "base", "", "compare: the git revision PREV.json came from")
	flags.BoolVar(&o.deadcode, "deadcode", false, "also find unreachable functions with "+codehealth.DeadcodeTool)
	flags.Var(o.from, "from", "update: NEW=OLD, NEW was split or renamed from the baseline entry OLD (repeatable)")
	flags.Var(o.generated, "generated", "init: PATH=GENERATOR, a generated file and the generator its header names (repeatable)")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() < 1 {
		flags.Usage()
		return 2
	}
	if err := o.dispatch(flags.Arg(0), flags.Args()[1:]); err != nil {
		// Nothing is left to report a failed write to.
		_, _ = fmt.Fprintln(stderr, "codehealth:", err)
		return 1
	}
	return 0
}

func (o options) dispatch(cmd string, args []string) error {
	path := filepath.Join(o.root, o.baseline)
	switch cmd {
	case "init":
		if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%s already exists; use update", path)
		}
		m, err := o.measure(o.generated, o.deadcode)
		if err != nil {
			return err
		}
		return codehealth.Save(path, codehealth.Initial(codehealth.DefaultLimits, o.generated, m))
	case "check", "update", "compare", "refs", "deadcode":
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
	b, err := codehealth.Load(path)
	if err != nil {
		return err
	}
	m, err := o.measure(b.Generated, o.deadcode || cmd == "deadcode")
	if err != nil {
		return err
	}
	switch cmd {
	case "check":
		return o.report("does not hold against the baseline", codehealth.Check(b, m), "matches the baseline")
	case "update":
		next, refused := codehealth.Update(b, m, o.from)
		if len(refused) > 0 {
			return o.report("refusing to raise the baseline", refused, "")
		}
		return codehealth.Save(path, next)
	case "compare":
		return o.compare(args, b, m)
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

func (o options) print(s string) error {
	_, err := io.WriteString(o.stdout, s)
	return err
}

func (o options) compare(args []string, next codehealth.Baseline, m codehealth.Measurement) error {
	if len(args) != 1 || o.base == "" {
		return errors.New("usage: codehealth -base REV compare PREV.json")
	}
	prev, err := codehealth.Load(args[0])
	if err != nil {
		return err
	}
	existed := func(p string) bool { return o.deps.existedAtBase(o.root, o.base, p) }
	return o.report("baseline is looser than "+o.base, codehealth.Compare(prev, next, m, existed), "baseline only went down")
}

func (o options) measure(generated map[string]string, withDeadcode bool) (codehealth.Measurement, error) {
	m, err := codehealth.Measure(o.root, generated)
	if err != nil || !withDeadcode {
		return m, err
	}
	m.Deadcode, err = o.deps.deadcode(o.root)
	return m, err
}

func (o options) report(what string, problems []string, ok string) error {
	if len(problems) == 0 {
		if ok == "" {
			return nil
		}
		return o.print("codehealth: ok: " + ok + "\n")
	}
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
