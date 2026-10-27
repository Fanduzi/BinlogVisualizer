// Command codehealth checks the tree against internal/codehealth/baseline.json.
//
//	codehealth [-deadcode] check          fail if any number is worse than the baseline
//	codehealth [-deadcode] update         lower the baseline to today's numbers; refuses to raise it
//	codehealth compare PREV.json          fail if the baseline raises a ceiling over PREV.json
//	codehealth refs                       list the issue references in comments
//	codehealth [-deadcode] init           write the first baseline; refuses if one exists
//
// Run it from the module root (make codehealth, make codehealth-baseline).
package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"binlogviz/internal/codehealth"
)

func main() {
	root := flag.String("root", ".", "module root to measure")
	baselinePath := flag.String("baseline", "internal/codehealth/baseline.json", "baseline file, relative to -root")
	withDeadcode := flag.Bool("deadcode", false, "also count unreachable functions with "+codehealth.DeadcodeTool)
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}
	path := filepath.Join(*root, *baselinePath)
	if err := run(flag.Arg(0), flag.Args()[1:], *root, path, *withDeadcode); err != nil {
		fmt.Fprintln(os.Stderr, "codehealth:", err)
		os.Exit(1)
	}
}

func run(cmd string, args []string, root, path string, withDeadcode bool) error {
	if cmd == "compare" {
		if len(args) != 1 {
			return errors.New("usage: codehealth compare PREV.json")
		}
		return compare(args[0], path)
	}
	m, err := measure(root, withDeadcode)
	if err != nil {
		return err
	}
	switch cmd {
	case "refs":
		for _, r := range m.IssueRefs {
			fmt.Println(r)
		}
		return nil
	case "init":
		if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%s already exists; use update", path)
		}
		return codehealth.Save(path, codehealth.Initial(codehealth.DefaultLimits, m))
	}
	b, err := codehealth.Load(path)
	if err != nil {
		return err
	}
	switch cmd {
	case "check":
		problems := codehealth.Check(b, m)
		for _, s := range codehealth.Stale(b, m) {
			problems = append(problems, s+"; run make codehealth-baseline")
		}
		return report("does not hold against the baseline", problems, summary(b, m))
	case "update":
		next, refused := codehealth.Tighten(b, m)
		if len(refused) > 0 {
			return report("refusing to raise the baseline", refused, "")
		}
		if err := codehealth.Save(path, next); err != nil {
			return err
		}
		fmt.Println("baseline:", summary(next, m))
		return nil
	}
	return fmt.Errorf("unknown command %q", cmd)
}

func measure(root string, withDeadcode bool) (codehealth.Measurement, error) {
	b, err := codehealth.Load(filepath.Join(root, "internal/codehealth/baseline.json"))
	lim := codehealth.DefaultLimits
	if err == nil {
		lim = b.Limits
	}
	m, err := codehealth.Measure(root, lim)
	if err != nil {
		return m, err
	}
	if withDeadcode {
		m.Deadcode, err = codehealth.CountDeadcode(root)
	}
	return m, err
}

func compare(prevPath, nextPath string) error {
	prev, err := codehealth.Load(prevPath)
	if err != nil {
		return err
	}
	next, err := codehealth.Load(nextPath)
	if err != nil {
		return err
	}
	return report("baseline raised", codehealth.CompareBaselines(prev, next), "baseline only went down")
}

func report(what string, problems []string, ok string) error {
	if len(problems) == 0 {
		if ok != "" {
			fmt.Println("codehealth: ok:", ok)
		}
		return nil
	}
	for _, p := range problems {
		fmt.Fprintln(os.Stderr, "  "+p)
	}
	return fmt.Errorf("%s (%d); see docs/development/coding-standards.md", what, len(problems))
}

func summary(b codehealth.Baseline, m codehealth.Measurement) string {
	dead := "not measured"
	if m.Deadcode >= 0 {
		dead = fmt.Sprint(m.Deadcode)
	}
	return fmt.Sprintf("files over %d lines %d/%d, functions over complexity %d %d/%d, issue refs %d/%d, deadcode %s/%d (now/baseline)",
		b.Limits.FileLines, len(m.OversizedFiles), len(b.OversizedFiles),
		b.Limits.Complexity, len(m.ComplexFunctions), len(b.ComplexFunctions),
		len(m.IssueRefs), b.IssueRefs, dead, b.Deadcode)
}
