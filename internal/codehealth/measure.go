// Package codehealth measures the code-health debt that CI ratchets and
// compares it with the checked-in baseline.json: oversized files, complex
// and long functions, issue references in comments and unreachable
// functions. Every baseline entry is a ceiling for one file or function.
// Identity is derived, never declared: a new entry is accepted only when its
// content comes from a baselined entry at the base revision, and generated
// files are proven by regenerating them. docs/development/coding-standards.md has
// the rules.
package codehealth

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Limits are the caps the ratchet enforces. They live in the baseline so a
// change that loosens one shows up in review and fails Compare.
type Limits struct {
	FileLines          int `json:"file_lines"`
	GrowthTolerance    int `json:"growth_tolerance_lines"`
	Complexity         int `json:"complexity"`
	FunctionLines      int `json:"function_lines"`
	FunctionStatements int `json:"function_statements"`
}

// DefaultLimits are the caps in docs/development/coding-standards.md.
var DefaultLimits = Limits{FileLines: 800, GrowthTolerance: 20, Complexity: 40, FunctionLines: 150, FunctionStatements: 100}

// Func is one measured function. Lines and Statements are counted the way
// golangci-lint's funlen counts them; Body is its significant lines, the
// content lineage compares.
type Func struct {
	Complexity int
	Lines      int
	Statements int
	Body       []string
	// File and Start..End locate the declaration, to find the function an
	// issue sits in.
	File       string
	Start, End int
}

// Measurement holds every value Measure saw, over a limit or not, so a new
// entry can be traced to the content of an old one.
type Measurement struct {
	// Files and TestFiles map a slash path to its line count.
	Files     map[string]int
	TestFiles map[string]int
	// Text maps a path to its significant lines.
	Text map[string][]string
	// Funcs and TestFuncs are keyed "pkgdir:Func", "pkgdir:Recv.Func", or
	// "pkgdir:name" for a function literal assigned to a package-level var.
	Funcs     map[string]Func
	TestFuncs map[string]Func
	// Refs maps a path to "line: text" for each issue reference in a comment.
	Refs map[string][]string
	// Deadcode maps "pkgdir:Func" to its position; nil when not measured.
	Deadcode map[string]string
	// Problems are findings that have no ledger: a nested module, a
	// //nolint with a token reason.
	Problems []string
	// Unparsed lists testdata files that are not valid Go; their lines
	// count, their functions and comments cannot.
	Unparsed []string
}

// Measure walks root and measures every Go file Go could compile, which is
// every directory but .git. Paths in generated are skipped: CI regenerates
// them (Regenerate) to prove they are generator output.
func Measure(root string, generated map[string]string) (Measurement, error) {
	m := Measurement{
		Files: map[string]int{}, TestFiles: map[string]int{}, Text: map[string][]string{},
		Funcs: map[string]Func{}, TestFuncs: map[string]Func{}, Refs: map[string][]string{},
	}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if d.Name() == ".git" && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() == "go.mod" && rel != "go.mod" && !inTestdata(rel) {
			m.Problems = append(m.Problems, "nested module "+rel+": go test ./..., golangci-lint and deadcode do not cover it; move the code into the main module")
		}
		if !strings.HasSuffix(rel, ".go") {
			return nil
		}
		if _, ok := generated[rel]; ok {
			return nil
		}
		return m.addFile(fset, path, rel)
	})
	return m, err
}

func inTestdata(rel string) bool {
	return strings.Contains("/"+rel, "/testdata/")
}

func (m *Measurement) addFile(fset *token.FileSet, path, rel string) error {
	src, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	files, funcs := m.Files, m.Funcs
	if strings.HasSuffix(rel, "_test.go") {
		files, funcs = m.TestFiles, m.TestFuncs
	}
	files[rel] = countLines(src)
	lines := strings.Split(string(src), "\n")
	f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		if inTestdata(rel) {
			m.Unparsed = append(m.Unparsed, rel)
			m.Text[rel] = significant(lines, nil)
			return nil
		}
		return err
	}
	m.Text[rel] = significant(lines, skippedLines(fset, f))
	if refs := issueRefs(fset, f); len(refs) > 0 {
		m.Refs[rel] = refs
	}
	m.Problems = append(m.Problems, weakNolints(fset, f, rel)...)
	pkg := filepath.ToSlash(filepath.Dir(rel))
	for name, fn := range functions(fset, f, lines) {
		fn.File = rel
		key := pkg + ":" + name
		if old, ok := funcs[key]; ok {
			fn = larger(old, fn)
		}
		funcs[key] = fn
	}
	return nil
}

// larger keeps the bigger of two functions with one key (build-tagged files,
// several init funcs): the larger value of each metric.
func larger(a, b Func) Func {
	if b.Lines > a.Lines {
		a, b = b, a
	}
	a.Complexity = max(a.Complexity, b.Complexity)
	a.Statements = max(a.Statements, b.Statements)
	return a
}

func countLines(src []byte) int {
	n := strings.Count(string(src), "\n")
	if len(src) > 0 && src[len(src)-1] != '\n' {
		n++
	}
	return n
}

// skippedLines are the package clause and import lines (1-based): every file
// has them, so they say nothing about where code came from.
func skippedLines(fset *token.FileSet, f *ast.File) map[int]bool {
	skip := map[int]bool{fset.Position(f.Package).Line: true}
	for _, decl := range f.Decls {
		if g, ok := decl.(*ast.GenDecl); ok && g.Tok == token.IMPORT {
			for l := fset.Position(g.Pos()).Line; l <= fset.Position(g.End()).Line; l++ {
				skip[l] = true
			}
		}
	}
	return skip
}

// significant returns the trimmed lines that carry content: not blank, not
// only brackets and punctuation, and not in skip (1-based line numbers).
func significant(lines []string, skip map[int]bool) []string {
	var out []string
	for i, line := range lines {
		t := strings.TrimSpace(line)
		if skip[i+1] || strings.Trim(t, "{}()[],;") == "" {
			continue
		}
		out = append(out, t)
	}
	return out
}

// functions measures each function declared in f and each function literal
// assigned to a package-level var. Function literals inside a function count
// toward that function, as in gocyclo.
func functions(fset *token.FileSet, f *ast.File, lines []string) map[string]Func {
	cmap := ast.NewCommentMap(fset, f, f.Comments)
	out := map[string]Func{}
	add := func(name string, node ast.Node, body *ast.BlockStmt) {
		fn := Func{
			Complexity: complexity(body),
			Lines:      funlenLines(fset, node, cmap.Filter(node)),
			Statements: statements(body.List),
			Body:       bodyText(fset, body, lines),
			Start:      fset.Position(node.Pos()).Line,
			End:        fset.Position(node.End()).Line,
		}
		if old, ok := out[name]; ok {
			fn = larger(old, fn)
		}
		out[name] = fn
	}
	for _, decl := range f.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Body != nil {
				add(funcName(d), d, d.Body)
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				if vs, ok := spec.(*ast.ValueSpec); ok {
					for i, v := range vs.Values {
						forEachLiteral(v, func(lit *ast.FuncLit) { add(vs.Names[i].Name, lit, lit.Body) })
					}
				}
			}
		}
	}
	return out
}

// forEachLiteral calls fn for each outermost function literal in n; a
// literal nested in another counts toward the outer one.
func forEachLiteral(n ast.Node, fn func(*ast.FuncLit)) {
	ast.Inspect(n, func(n ast.Node) bool {
		lit, ok := n.(*ast.FuncLit)
		if ok {
			fn(lit)
		}
		return !ok
	})
}

// funlenLines counts like golangci-lint funlen with ignore-comments: the
// lines between the line of "func" and the closing brace, minus each comment
// strictly inside them.
func funlenLines(fset *token.FileSet, node ast.Node, cmap ast.CommentMap) int {
	first, last := fset.Position(node.Pos()).Line, fset.Position(node.End()).Line
	n := last - first - 1
	for _, g := range cmap.Comments() {
		if fset.Position(g.Pos()).Line > first && fset.Position(g.End()).Line < last {
			n -= len(g.List)
		}
	}
	return n
}

// statements counts like funlen: each statement, plus the statements in
// nested blocks and in function literals that are assigned, deferred or
// started with go.
func statements(list []ast.Stmt) int {
	total := 0
	for _, s := range list {
		total++
		switch s := s.(type) {
		case *ast.BlockStmt:
			total += statements(s.List) - 1
		case *ast.ForStmt:
			total += statements(s.Body.List)
		case *ast.RangeStmt:
			total += statements(s.Body.List)
		case *ast.IfStmt:
			total += statements(s.Body.List)
		case *ast.SwitchStmt:
			total += statements(s.Body.List)
		case *ast.TypeSwitchStmt:
			total += statements(s.Body.List)
		case *ast.SelectStmt:
			total += statements(s.Body.List)
		case *ast.CaseClause:
			total += statements(s.Body)
		case *ast.AssignStmt:
			total += literalStatements(s.Rhs[0])
		case *ast.GoStmt:
			total += literalStatements(s.Call.Fun)
		case *ast.DeferStmt:
			total += literalStatements(s.Call.Fun)
		}
	}
	return total
}

func literalStatements(e ast.Expr) int {
	if lit, ok := e.(*ast.FuncLit); ok {
		return statements(lit.Body.List)
	}
	return 0
}

// bodyText is the significant lines strictly between the braces.
func bodyText(fset *token.FileSet, body *ast.BlockStmt, lines []string) []string {
	first, last := fset.Position(body.Lbrace).Line, fset.Position(body.Rbrace).Line
	if last-1 <= first {
		return nil
	}
	return significant(lines[first:last-1], nil)
}
