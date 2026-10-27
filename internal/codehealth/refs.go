package codehealth

import (
	"go/ast"
	"go/token"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// issueRef matches the clear ways a comment points at tracker or review
// history: a hash before a number after a non-word character, GH- or GH
// before a number, owner/repo plus hash and number, issues/ or pull/ plus a
// number (which covers URLs), PR plus hash, the word issue (optionally with a
// colon or the word number) before a number, Issues with a capital before a
// number (lower-case "issues 2" is a verb), QA plus r and a number, and r
// plus a number before the word review. Precision matters more than recall:
// round, the section sign, bug, see and a bare number in parentheses are left
// out because ordinary prose and SQL use them. Full-width hashes and digits
// are normalized first. TestIssueRefStyles has the examples.
var issueRef = regexp.MustCompile(`(?i:\b(?:issues|pull)/\d+\b)` +
	`|\bGH-?\d+\b` +
	`|\b[A-Za-z][\w.-]*/[A-Za-z][\w.-]*#\d+\b` +
	`|(?i:\bPRs?\s*#\s*\d+\b)` +
	`|(?i:\bissue\s*(?::|\bnumber\b)?\s*#?\s*\d+\b)` +
	`|\bIssues\s+#?\d+\b` +
	`|\bQA\s+r\d+\b` +
	`|(?i:\br\d+\s+review\b)`)

// hashRef is a hash and number after a non-word character, not part of an
// HTML entity ("&#39;") or a second hash.
var hashRef = regexp.MustCompile(`(?:^|[^\w&#])(#\d+)\b`)

// notRefWords are words that, right before "#N", make it a position, an
// instance or a colour, not an issue: "Column #3", "trx #2", "thread #3",
// "color: #333".
var notRefWords = map[string]bool{
	"column": true, "col": true, "field": true, "row": true, "item": true, "arg": true, "argument": true,
	"param": true, "parameter": true, "step": true, "line": true, "byte": true, "bit": true, "index": true,
	"slot": true, "position": true, "pos": true, "offset": true, "entry": true, "event": true, "no": true,
	"number": true, "attempt": true, "try": true, "page": true, "part": true, "case": true, "test": true,
	// Binlog and server instances.
	"trx": true, "txn": true, "tx": true, "transaction": true, "thread": true, "worker": true, "session": true,
	"query": true, "statement": true, "stmt": true, "conn": true, "connection": true, "server": true,
	"node": true, "replica": true, "shard": true, "partition": true, "table": true, "file": true,
	"segment": true, "chunk": true, "batch": true, "packet": true, "record": true, "block": true, "job": true,
	"task": true, "run": true, "pass": true, "phase": true, "stage": true, "iteration": true, "round": true,
	// Colours.
	"color": true, "colour": true, "background": true, "bg": true, "fg": true, "foreground": true,
	"border": true, "fill": true, "stroke": true,
}

var fullWidth = strings.NewReplacer("＃", "#",
	"０", "0", "１", "1", "２", "2", "３", "3", "４", "4", "５", "5", "６", "6", "７", "7", "８", "8", "９", "9")

// refsIn returns the issue references in one line of comment text.
func refsIn(line string) []string {
	line = fullWidth.Replace(line)
	spans := issueRef.FindAllStringIndex(line, -1)
	for _, loc := range hashRef.FindAllStringSubmatchIndex(line, -1) {
		start, end := loc[2], loc[3]
		if !overlaps(spans, start, end) && !notRefWords[strings.ToLower(lastWord(line[:start]))] {
			spans = append(spans, []int{start, end})
		}
	}
	sort.Slice(spans, func(a, b int) bool { return spans[a][0] < spans[b][0] })
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, line[s[0]:s[1]])
	}
	return out
}

func overlaps(spans [][]int, start, end int) bool {
	for _, s := range spans {
		if start < s[1] && s[0] < end {
			return true
		}
	}
	return false
}

// lastWord is the word right before a reference, with a colon, quotes or
// an opening parenthesis around it trimmed. Clause punctuation stays on
// it, so a word that ends a clause qualifies nothing: after "the segment,"
// a hash and a number is a reference.
func lastWord(s string) string {
	f := strings.Fields(s)
	if len(f) == 0 {
		return ""
	}
	return strings.Trim(f[len(f)-1], ":(\"'`")
}

func issueRefs(fset *token.FileSet, f *ast.File) []string {
	var out []string
	for _, group := range f.Comments {
		for _, c := range group.List {
			start := fset.Position(c.Pos()).Line
			for i, line := range strings.Split(c.Text, "\n") {
				for _, hit := range refsIn(line) {
					out = append(out, strconv.Itoa(start+i)+": "+hit)
				}
			}
		}
	}
	return out
}

// nolintDirective is a //nolint comment and the reason after it, if any.
var nolintDirective = regexp.MustCompile(`^//\s*nolint(?::[\w,-]+)?\s*(?://\s*(.*))?$`)

// minNolintWords is the shortest //nolint reason that can say why. nolintlint
// only sees directives for linters that are enabled; this also rejects a
// missing reason and token ones like "// fine" for any linter.
const minNolintWords = 3

func weakNolints(fset *token.FileSet, f *ast.File, rel string) []string {
	var out []string
	for _, group := range f.Comments {
		for _, c := range group.List {
			if m := nolintDirective.FindStringSubmatch(c.Text); m != nil && len(strings.Fields(m[1])) < minNolintWords {
				out = append(out, rel+":"+strconv.Itoa(fset.Position(c.Pos()).Line)+": //nolint reason "+strconv.Quote(m[1])+" is under "+strconv.Itoa(minNolintWords)+" words; say why the finding does not apply")
			}
		}
	}
	return out
}
