package codehealth

import (
	"cmp"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// LintIssue is one golangci-lint finding.
type LintIssue struct {
	Linter string
	File   string
	Line   int
	Text   string
}

// ParseLint reads golangci-lint's JSON output (--output.json.path).
func ParseLint(data []byte) ([]LintIssue, error) {
	var report struct {
		Issues []struct {
			FromLinter string
			Text       string
			Pos        struct {
				Filename string
				Line     int
			}
		}
	}
	if err := json.Unmarshal(data, &report); err != nil {
		return nil, fmt.Errorf("golangci-lint JSON: %w", err)
	}
	out := make([]LintIssue, 0, len(report.Issues))
	for _, i := range report.Issues {
		out = append(out, LintIssue{Linter: i.FromLinter, File: filepath.ToSlash(i.Pos.Filename), Line: i.Pos.Line, Text: i.Text})
	}
	return out, nil
}

// DiffLint returns the pull request's lint findings that the base revision
// does not have. A finding is identified by lintKey: its linter, the
// enclosing function, and its message with numbers, quoted identifiers and
// the function's own name normalized. A function in the pull request is the base function
// with the same key, else the base function whose body holds at least
// minDerived of its body (it moved or was renamed), else itself. So an
// existing finding in code that was moved, split off or renamed matches its
// base finding, and a new one does not. Line numbers play no part.
func DiffLint(base Measurement, baseIssues []LintIssue, pr Measurement, prIssues []LintIssue) []string {
	have := map[string]int{}
	baseFuncs := allFuncs(base)
	for _, i := range baseIssues {
		key, name := enclosing(baseFuncs, i)
		have[lintKey(i, key, name)]++
	}
	prFuncs := allFuncs(pr)
	origins := newOriginIndex(sortedKeys(baseFuncs), func(k string) []string { return baseFuncs[k].Body })
	identity := func(key string) string {
		if _, ok := baseFuncs[key]; ok {
			return key
		}
		if origin, share := origins.best(prFuncs[key].Body); share >= minDerived {
			return origin
		}
		return key
	}
	var out []string
	for _, i := range sortedIssues(prIssues) {
		key, name := enclosing(prFuncs, i)
		key = identity(key)
		k := lintKey(i, key, name)
		if have[k] > 0 {
			have[k]--
			continue
		}
		out = append(out, fmt.Sprintf("%s:%d: %s: %s", i.File, i.Line, i.Linter, i.Text))
	}
	return out
}

func allFuncs(m Measurement) map[string]Func {
	out := make(map[string]Func, len(m.Funcs)+len(m.TestFuncs))
	for k, f := range m.Funcs {
		out[k] = f
	}
	for k, f := range m.TestFuncs {
		out[k] = f
	}
	return out
}

// enclosing returns the key and simple name of the innermost function that
// holds the issue's line, or "-" outside any function.
func enclosing(funcs map[string]Func, i LintIssue) (string, string) {
	best, span := "-", 0
	for k, f := range funcs {
		if f.File == i.File && f.Start <= i.Line && i.Line <= f.End && (best == "-" || f.End-f.Start < span) {
			best, span = k, f.End-f.Start
		}
	}
	_, name, _ := strings.Cut(best, ":")
	return best, name[strings.LastIndex(name, ".")+1:]
}

var (
	digits = regexp.MustCompile(`\d+`)
	// quoted is a span in backticks or double quotes, or in single quotes
	// with no space inside, so an apostrophe in prose does not open one.
	quoted     = regexp.MustCompile("`[^`]*`" + `|"[^"]*"|'[^'\s]*'`)
	identifier = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_]*`)
)

// lintKey is the identity of a finding: its linter, the identity of its
// enclosing function, and its message with numbers, the function's own
// name and every identifier inside quotes normalized. Quoted identifiers
// are the code the message names (errcheck's `store.Close`), so renaming a
// local variable keeps the finding; the linter, the function and the shape
// of the message still tell findings apart, and a second finding of the
// same shape in the same function still counts as new.
func lintKey(i LintIssue, fn, name string) string {
	text := quoted.ReplaceAllStringFunc(i.Text, func(q string) string {
		return identifier.ReplaceAllString(q, "ID")
	})
	if name != "" {
		text = regexp.MustCompile(`\b`+regexp.QuoteMeta(name)+`\b`).ReplaceAllString(text, "FUNC")
	}
	return i.Linter + "\x00" + fn + "\x00" + digits.ReplaceAllString(text, "N")
}

func sortedIssues(issues []LintIssue) []LintIssue {
	out := append([]LintIssue(nil), issues...)
	slices.SortFunc(out, func(a, b LintIssue) int {
		return cmp.Or(strings.Compare(a.File, b.File), cmp.Compare(a.Line, b.Line))
	})
	return out
}
