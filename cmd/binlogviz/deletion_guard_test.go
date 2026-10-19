// input: every non-test Go file in cmd/binlogviz, internal/workflow and internal/snapshot, plus synthetic mutants.
// output: fails on any file-removing call site (direct, aliased as a function value, via an import alias, os.Root methods, or exec of a process) not in the reviewed allowlist.
// pos: ADR 0009 §8 deletion guard (QA r2 on #290, P3).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// destructive names on os / syscall / golang.org/x/sys/unix.
var guardBannedPkgFuncs = map[string]bool{
	"Remove": true, "RemoveAll": true, "Rename": true, "Truncate": true,
	"Unlink": true, "Unlinkat": true, "Rmdir": true, "Renameat": true, "Renameat2": true,
	"Exec": true, "StartProcess": true, "ForkExec": true,
}

// destructive method names (os.Root and friends), whatever the receiver.
var guardBannedMethods = map[string]bool{"Remove": true, "RemoveAll": true, "Rename": true}

var guardPkgs = map[string]bool{"os": true, "syscall": true, "golang.org/x/sys/unix": true}

// deletionSites reports "<file>:<func>:<expr>" for every reference, called
// or not, to a destructive function, and every os/exec process start.
func deletionSites(fset *token.FileSet, f *ast.File) []string {
	local := map[string]string{} // local import name -> path
	for _, imp := range f.Imports {
		path, _ := strconv.Unquote(imp.Path.Value)
		name := filepath.Base(path)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		local[name] = path
	}
	base := filepath.Base(fset.Position(f.Pos()).Filename)
	var sites []string
	add := func(fn, what string) { sites = append(sites, base+":"+fn+":"+what) }
	var visit func(fn string, n ast.Node)
	visit = func(fn string, n ast.Node) {
		ast.Inspect(n, func(n ast.Node) bool {
			if fd, ok := n.(*ast.FuncDecl); ok && fd.Body != nil && fn == "" {
				visit(fd.Name.Name, fd.Body)
				return false
			}
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok {
				if path, isPkg := local[id.Name]; isPkg {
					switch {
					case guardPkgs[path] && guardBannedPkgFuncs[sel.Sel.Name]:
						add(fn, path+"."+sel.Sel.Name)
					case path == "os/exec":
						add(fn, "os/exec."+sel.Sel.Name)
					}
					return true
				}
			}
			if guardBannedMethods[sel.Sel.Name] {
				add(fn, "."+sel.Sel.Name)
			}
			return true
		})
	}
	visit("", f)
	return sites
}

// guardAllowlist is every reviewed removal outside the clean primitive.
// Each entry is a file:function:expr site and must say why it is safe.
var guardAllowlist = map[string]string{
	"safefs.go:TrashFile:.Rename":                             "the clean primitive: rename into <root>/.trash inside os.Root",
	"safefs.go:releaseLockFile:os.Remove":                     "non-unix lock file, only <root>/.lock",
	"analyze.go:createDuckDBTempStore:os.RemoveAll":           "removes the os.MkdirTemp dir it just created",
	"analyze_output.go:writeHTMLAtomically:os.Remove":         "removes its own temp file after a failed write",
	"analyze_output.go:writeHTMLAtomically:os.Rename":         "atomic replace of the user-named output file",
	"flashback_verify.go:newFlashbackVerifyCommand:os.Remove": "removes the os.CreateTemp spool file verify just created",
	"interrupt.go:releaseStdinTemp:os.RemoveAll":              "removes the stdin spool temp dir binlogviz created",
	"interrupt.go:cleanupTrackedStdinTemps:os.RemoveAll":      "removes tracked stdin spool temp dirs on interrupt",
	"store.go:RenameSnapshot:os.Remove":                       "snapshot rename: drops the old name (or the new copy on failure) after ValidateName, inside the snapshot dir",
	"store.go:DeleteSnapshot:os.Remove":                       "user-invoked `snapshot delete` of a ValidateName-checked name inside the snapshot dir",
}

func TestDeletionGuardCatchesMutants(t *testing.T) {
	for name, src := range map[string]string{
		"direct":        `package p; import "os"; func f() { os.Remove("x") }`,
		"value alias":   `package p; import "os"; func f() { rm := os.Remove; rm("x") }`,
		"package var":   `package p; import "os"; var del = os.RemoveAll`,
		"import alias":  `package p; import xos "os"; func f() { xos.RemoveAll("x") }`,
		"root method":   `package p; import "os"; func f(r *os.Root) { r.Remove("x") }`,
		"method value":  `package p; import "os"; func f(r *os.Root) { g := r.RemoveAll; g("x") }`,
		"exec rm":       `package p; import "os/exec"; func f() { exec.Command("rm", "-rf", "x").Run() }`,
		"syscall":       `package p; import "syscall"; func f() { syscall.Unlinkat(0, "x", 0) }`,
		"unix":          `package p; import "golang.org/x/sys/unix"; func f() { unix.Unlink("x") }`,
		"start process": `package p; import "os"; func f() { os.StartProcess("/bin/rm", nil, nil) }`,
	} {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "m.go", src, 0)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(deletionSites(fset, f)) == 0 {
			t.Errorf("guard misses %s: %s", name, src)
		}
	}
	// Not destructive: a time.Duration.Truncate and strings functions.
	fset := token.NewFileSet()
	f, _ := parser.ParseFile(fset, "ok.go", `package p; import ("time"; "strings"); func f(d time.Duration) { _ = d.Truncate(time.Second); _ = strings.TrimSpace("") }`, 0)
	if got := deletionSites(fset, f); len(got) != 0 {
		t.Errorf("false positive: %v", got)
	}
}

func TestDeletionGuard(t *testing.T) {
	var files []string
	for _, dir := range []string{".", "../../internal/workflow", "../../internal/snapshot"} {
		m, _ := filepath.Glob(filepath.Join(dir, "*.go"))
		files = append(files, m...)
	}
	seen := map[string]bool{}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, site := range deletionSites(fset, f) {
			seen[site] = true
			if _, ok := guardAllowlist[site]; !ok {
				t.Errorf("unreviewed file removal %s: workflow deletions must go through workflow.TrashFile; anything else needs a guardAllowlist entry saying why it is safe", site)
			}
		}
	}
	var stale []string
	for site := range guardAllowlist {
		if !seen[site] {
			stale = append(stale, site)
		}
	}
	sort.Strings(stale)
	for _, s := range stale {
		t.Errorf("stale guardAllowlist entry %s", s)
	}
}
