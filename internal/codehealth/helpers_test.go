package codehealth

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// tree is a set of files to write under a temporary root.
type tree map[string]string

func (tr tree) write(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for name, src := range tr {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// with returns a copy of tr with changes applied; an empty value deletes.
func (tr tree) with(changes tree) tree {
	out := tree{}
	for k, v := range tr {
		out[k] = v
	}
	for k, v := range changes {
		if v == "" {
			delete(out, k)
		} else {
			out[k] = v
		}
	}
	return out
}

// file is a Go file of exactly n lines in package p, with body after the
// package clause.
func file(n int, body string) string {
	src := "package p\n" + body
	have := strings.Count(src, "\n")
	if have > n {
		panic(fmt.Sprintf("body needs %d lines, want %d", have, n))
	}
	return src + strings.Repeat("\n", n-have)
}

// complexFunc is a function of complexity c whose body is 3*(c-1)+1 lines.
func complexFunc(name string, c int) string {
	var b strings.Builder
	b.WriteString("func " + name + "(x int) int {\n")
	for i := 1; i < c; i++ {
		fmt.Fprintf(&b, "\tif x == %d {\n\t\treturn %d\n\t}\n", i, i)
	}
	b.WriteString("\treturn 0\n}\n")
	return b.String()
}

// longFunc is a function of complexity 1 whose body is n lines.
func longFunc(name string, n int) string {
	return "func " + name + "() {\n" + strings.Repeat("\t_ = 0\n", n) + "}\n"
}

func measure(t *testing.T, tr tree, generated map[string]string) Measurement {
	t.Helper()
	m, err := Measure(tr.write(t), generated)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func clone(t *testing.T, b Baseline) Baseline {
	t.Helper()
	data, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	var out Baseline
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return out.normalized()
}

func expect(t *testing.T, problems []string, want string) {
	t.Helper()
	joined := strings.Join(problems, "\n")
	switch {
	case want == "" && len(problems) > 0:
		t.Fatalf("want no problems, got:\n%s", joined)
	case want != "" && !strings.Contains(joined, want):
		t.Fatalf("want a problem containing %q, got:\n%s", want, joined)
	}
}

// lines is n distinct significant lines named prefix-from .. prefix-(to-1),
// so a test can move content between files and functions.
func lines(prefix string, from, to int) string {
	var b strings.Builder
	for i := from; i < to; i++ {
		fmt.Fprintf(&b, "var _ = %q\n", fmt.Sprintf("%s-%d", prefix, i))
	}
	return b.String()
}

// stmtFunc is a function with one statement per line, named prefix-i for
// each (prefix, from, to) triple in ranges.
func stmtFunc(name string, ranges ...any) string {
	var b strings.Builder
	b.WriteString("func " + name + "() {\n")
	for r := 0; r < len(ranges); r += 3 {
		for i := ranges[r+1].(int); i < ranges[r+2].(int); i++ {
			fmt.Fprintf(&b, "\tprintln(%q)\n", fmt.Sprintf("%s-%d", ranges[r], i))
		}
	}
	b.WriteString("}\n")
	return b.String()
}
