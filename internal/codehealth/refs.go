package codehealth

import (
	"go/ast"
	"go/token"
	"regexp"
	"strconv"
	"strings"
)

// issueRef matches the ways a comment points at tracker or review history:
// issue and pull request URLs, an owner/repo reference, a hash or full-width
// hash before a number, "PR" or "issue" before a number (with or without a
// hash or dash), "GH-" before a number, the section sign before a number,
// and "round" before a number, as in a QA round. HTML entities, C-sharp and
// hex colors are not matches. TestIssueRefStyles has the examples.
var issueRef = regexp.MustCompile(`(?i)` +
	`github\.com/[\w.-]+/[\w.-]+/(?:issues|pull)/\d+` +
	`|\b[\w.-]+/[\w.-]+[#＃]\d+\b` +
	`|\b(?:pull requests?|prs?|issues?)\s*[#＃-]?\s*\d+\b` +
	`|\bgh-\d+\b` +
	`|(?:^|[^\w&#＃])[#＃]\d+\b` +
	`|§\s*\d+` +
	`|\brounds?\s+\d+\b`)

func issueRefs(fset *token.FileSet, f *ast.File) []string {
	var out []string
	for _, group := range f.Comments {
		for _, c := range group.List {
			start := fset.Position(c.Pos()).Line
			for i, line := range strings.Split(c.Text, "\n") {
				for _, hit := range issueRef.FindAllString(line, -1) {
					out = append(out, strconv.Itoa(start+i)+": "+strings.TrimSpace(hit))
				}
			}
		}
	}
	return out
}
