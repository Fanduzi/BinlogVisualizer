// input: CLI commands run in-process plus the cmd/binlogviz Go sources.
// output: #277 exit-code contract: usage, input, corrupt and unsupported failures each get their own code, and no command returns an untyped error.
// pos: guard for the ADR 0009 exit-code taxonomy.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/clierr"
)

func TestExitTaxonomy277(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	report := write("report.json", "{}")
	bad := write("bad.json", "{bad")
	empty := write("empty.binlog", "")
	snapDir := filepath.Join(dir, "snaps")
	cases := []struct {
		name string
		args []string
		code int
	}{
		{"snapshot save without --name", []string{"snapshot", "save", report, "--snapshot-dir", snapDir}, 3},
		{"snapshot save bad JSON", []string{"snapshot", "save", bad, "--name", "x", "--snapshot-dir", snapDir}, 5},
		{"snapshot save missing file", []string{"snapshot", "save", filepath.Join(dir, "nope.json"), "--name", "x", "--snapshot-dir", snapDir}, 4},
		{"snapshot save invalid name", []string{"snapshot", "save", report, "--name", "a/b", "--snapshot-dir", snapDir}, 3},
		{"snapshot show unknown", []string{"snapshot", "show", "nope", "--snapshot-dir", snapDir}, 4},
		{"snapshot list bad format", []string{"snapshot", "list", "--format", "xml", "--snapshot-dir", snapDir}, 3},
		{"compare mixed modes", []string{"compare", "--current-snapshot", "a"}, 3},
		{"trend one snapshot", []string{"trend", "a"}, 3},
		{"analyze empty file", []string{"analyze", empty}, 5},
		{"analyze /dev/null", []string{"analyze", "/dev/null"}, 5},
		{"workflow status missing dir", []string{"workflow", "status", filepath.Join(dir, "nowf")}, 4},
		{"workflow status bad format", []string{"workflow", "status", dir, "--format", "xml"}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, stderr := runRoot(t, tc.args...)
			if code != tc.code {
				t.Fatalf("exit %d, want %d\n%s", code, tc.code, stderr)
			}
		})
	}
}

func TestStatementBinlogIsUnsupported(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	p := &mockParser{events: []binlog.RawEvent{
		{Timestamp: now, EventType: "QUERY", Query: "INSERT INTO d.t VALUES (1)"},
		{Timestamp: now.Add(time.Second), EventType: "QUERY", Query: "UPDATE d.t SET a=1"},
	}}
	_, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParser([]string{"dummy.binlog"}, analyzer.DefaultOptions(), "json", p)
	})
	if err == nil {
		t.Fatal("STATEMENT binlog must fail")
	}
	if kind, _ := ErrorKind(err); kind != clierr.KindUnsupported || ExitCode(err) != 9 {
		t.Fatalf("kind %s exit %d, want unsupported/9", kind, ExitCode(err))
	}
}

// TestNoUntypedErrors keeps #277 fixed: every error built in cmd/binlogviz
// carries a clierr kind. fmt.Errorf and errors.New are allowed only for
// package-level sentinels, which callers wrap with a kind where they return.
func TestNoUntypedErrors(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			if g, ok := decl.(*ast.GenDecl); ok && g.Tok == token.VAR {
				continue // package-level sentinel
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				pkg, ok := sel.X.(*ast.Ident)
				if !ok {
					return true
				}
				if (pkg.Name == "fmt" && sel.Sel.Name == "Errorf") || (pkg.Name == "errors" && sel.Sel.Name == "New") {
					t.Errorf("%s: untyped %s.%s; use clierr.Errorf/clierr.New with a kind (ADR 0009)", fset.Position(call.Pos()), pkg.Name, sel.Sel.Name)
				}
				return true
			})
		}
	}
}
