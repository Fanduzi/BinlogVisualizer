// Package binlogviz guards the shape of the project READMEs.
// input: repository README.md and README_ZH.md, plus inline bypass probes.
// output: a failure when a README grows an unlisted ## heading, a release-shaped token outside an install line or the allowed list, an over-long paragraph, an unclosed fence, or past its line or byte bound.
// pos: release-doc drift guard; release notes and behavior detail belong in docs/, not in the README (BinlogViz README bloat, v0.21.0 -> v0.35.2).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

// The v0.21.0 README was 414 lines and 19932 bytes. By v0.35.2 it was 594
// lines and 48805 bytes, almost all of it per-feature detail added by release
// and fix PRs. These bounds leave room for a new command, not for its rules.
const (
	readmeMaxLines          = 360
	readmeMaxBytes          = 16000
	readmeMaxParagraphRunes = 450
)

const readmeNotesHome = "release notes go in docs/releases/release-notes-vX.Y.Z.md and .zh-CN.md plus CHANGELOG.md; " +
	"behavior detail goes in docs/reference/analyze-behavior*.md, docs/recipe/flashback*.md, docs/recipe/compare-trend-workflow*.md, " +
	"or another page under docs/reference or docs/recipe; the README keeps the command and a link"

var (
	// GitHub opens a fence only when it is indented three spaces or less; a
	// deeper ``` is code text and what follows stays prose. A backtick
	// fence's info string cannot contain a backtick.
	readmeFenceOpen = regexp.MustCompile("^ {0,3}(`{3,}|~{3,})(.*)$")
	readmeATX       = regexp.MustCompile(`^ {0,3}(#{1,6})(?:\s+(.*?))?\s*#*\s*$`)
	readmeSetext    = regexp.MustCompile(`^ {0,3}(=+|-+)\s*$`)
	readmeHTMLHead  = regexp.MustCompile(`(?i)<(h[1-6]|summary)\b[^>]*>(.*?)(?:</(?:h[1-6]|summary)\s*>|$)`)
	readmeListItem  = regexp.MustCompile(`^\s*(?:[-*+]|\d+[.)])(?:\s|$)`)
	readmeTableRow  = regexp.MustCompile(`^\s*\|`)

	// A line that is part of an install command may name a release
	// (install.sh --version vX.Y.Z, a releases/download URL, go install,
	// brew install); a prose mention of install.sh may not — the command
	// needs a flag or the download URL.
	readmeInstallLine = regexp.MustCompile(`(?i)install\.sh[ \t]+[-/]|releases/download/|go install\b|brew install\b|--version`)
	// A non-h2 heading (h1, h3-h6, setext h1, <summary>) may not announce
	// release history either; h2 headings are pinned by readmeH2 instead.
	// Upgrade and migration words are ordinary ("### Upgrade with
	// Homebrew"), so they are not here.
	readmeNotesHeading = regexp.MustCompile(`(?i)change\s*-?\s*log|changes\s+in|what'?s\s+new|\bnew\s+in\b|\bnew\s+features?\b|release[\s-]*notes?|\bbreaking\s+changes?|\bdeprecat|\bversion\s+history|\bhistory\b|` +
		`更新日志|变更日志|变更记录|更新记录|更新说明|新特性|新功能|新版本|发行说明|发布说明|版本说明|版本历史`)
	// Release-shaped tokens allowed outside install lines, each with the
	// reason it is not BinlogViz release history. A bare two-part number
	// ("Apache 2.0", "MySQL 8.0") is never a token here; the rule is the
	// same release-shaped one the landing guard and release-check share.
	readmeAllowedVersions = map[string]string{
		"1.26.10": "Go toolchain version",
		"8.0.36":  "MySQL server version",
	}
	// The h2 (## / setext --- / <h2>) headings of each README, in page
	// order. A release or changelog section fails here whatever it says.
	readmeH2 = map[string][]string{
		"README.md": {"Screenshots", "Installation", "Quick Start", "Find and Undo Bad Rows",
			"Snapshots, Compare, Trend, and Workflow", "What BinlogViz Helps You See",
			"Language Support", "Documentation by Task", "Architecture", "Requirements", "License"},
		"README_ZH.md": {"截图", "安装", "快速开始", "找到并撤销误改的行",
			"快照、对比、趋势与 workflow", "BinlogViz 适合回答什么问题",
			"多语言支持", "按任务阅读文档", "架构", "环境要求", "License"},
	}
)

type readmeIssue struct {
	line int
	msg  string
}

func (i readmeIssue) String() string { return fmt.Sprintf("%d: %s", i.line, i.msg) }

// readmeShapeIssues parses body as Markdown just far enough to find fences,
// headings and paragraphs, and returns every rule the README breaks.
// wantH2 is the fixed h2-heading list the file must match.
func readmeShapeIssues(body string, wantH2 []string) []readmeIssue {
	var issues []readmeIssue
	add := func(line int, format string, args ...any) {
		issues = append(issues, readmeIssue{line, fmt.Sprintf(format, args...)})
	}
	body = strings.ReplaceAll(body, "\r\n", "\n")
	lines := strings.Split(strings.TrimRight(body, "\n"), "\n")
	if len(lines) > readmeMaxLines {
		add(0, "%d lines, more than %d", len(lines), readmeMaxLines)
	}
	if len(body) > readmeMaxBytes {
		add(0, "%d bytes, more than %d", len(body), readmeMaxBytes)
	}

	var gotH2 []string
	checkHeading := func(n, level int, text string) {
		text = strings.TrimSpace(text)
		if level == 2 {
			gotH2 = append(gotH2, text)
			return
		}
		if readmeNotesHeading.MatchString(text) {
			add(n, "heading %q announces a release or changelog", text)
		}
	}
	checkVersion := func(n int, line string) {
		if readmeInstallLine.MatchString(line) {
			return
		}
		for _, tok := range versionTokens(versionScanText(line)) {
			if _, ok := readmeAllowedVersions[tok]; ok || !isReleaseShaped(tok) {
				continue
			}
			add(n, "names a release version %q; install-command lines and the allowed tokens in readmeAllowedVersions are exempt", tok)
		}
	}

	var (
		fenceChar  byte
		fenceLen   int
		fenceStart int
		paraStart  int
		para       []string
	)
	flush := func() {
		if len(para) > 0 {
			joined := strings.Join(strings.Fields(strings.Join(para, " ")), " ")
			if n := utf8.RuneCountInString(joined); n > readmeMaxParagraphRunes {
				add(paraStart, "a %d-character paragraph, more than %d", n, readmeMaxParagraphRunes)
			}
		}
		para = nil
	}

	for i, line := range lines {
		n := i + 1
		if fenceChar != 0 {
			trim := strings.TrimSpace(line)
			run := len(trim) - len(strings.TrimLeft(trim, string(fenceChar)))
			if run >= fenceLen && run == len(trim) {
				fenceChar = 0
				continue
			}
			checkVersion(n, line)
			if r := utf8.RuneCountInString(line); r > readmeMaxParagraphRunes {
				add(n, "a %d-character code line, more than %d", r, readmeMaxParagraphRunes)
			}
			continue
		}
		if m := readmeFenceOpen.FindStringSubmatch(line); m != nil && !(m[1][0] == '`' && strings.Contains(m[2], "`")) {
			flush()
			fenceChar, fenceLen, fenceStart = m[1][0], len(m[1]), n
			continue
		}

		checkVersion(n, line)
		for _, m := range readmeHTMLHead.FindAllStringSubmatch(line, -1) {
			level := 3
			if strings.EqualFold(m[1], "h2") {
				level = 2
			}
			checkHeading(n, level, m[2])
		}
		if m := readmeATX.FindStringSubmatch(line); m != nil {
			flush()
			checkHeading(n, len(m[1]), m[2])
			continue
		}
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		if m := readmeSetext.FindStringSubmatch(line); m != nil && len(para) > 0 {
			// The paragraph above is a setext heading: --- makes h2, === h1.
			level := 1
			if m[1][0] == '-' {
				level = 2
			}
			checkHeading(n-1, level, strings.Join(para, " "))
			para = nil
			continue
		}
		if readmeListItem.MatchString(line) || readmeTableRow.MatchString(line) {
			flush()
		}
		if len(para) == 0 {
			paraStart = n
		}
		para = append(para, line)
	}
	flush()
	if fenceChar != 0 {
		add(fenceStart, "code fence %q is never closed, so everything after it would hide from this check", strings.Repeat(string(fenceChar), fenceLen))
	}
	if !slices.Equal(gotH2, wantH2) {
		add(0, "## headings are %q, want the fixed list %q", gotH2, wantH2)
	}
	return issues
}

func TestReadmeStaysConcise(t *testing.T) {
	for _, name := range []string{"README.md", "README_ZH.md"} {
		body, err := os.ReadFile(filepath.Join(repoRoot(t), name))
		if err != nil {
			t.Fatal(err)
		}
		for _, issue := range readmeShapeIssues(string(body), readmeH2[name]) {
			t.Errorf("%s:%s; %s", name, issue, readmeNotesHome)
		}
	}
}

// Every way QA found to slip notes past this guard, plus the shapes it
// already caught. Each is appended to the real README and must produce the
// named failure.
func TestReadmeShapeCatchesBypassProbes(t *testing.T) {
	long := strings.Repeat("word ", 120)
	wrapped := strings.Repeat(strings.Repeat("word ", 20)+"\n", 5)
	probes := map[string]struct {
		probe, want string
	}{
		// Fences: unclosed, too-short closes, and ones GitHub would not open.
		"unclosed fence hides a long line":                {"```\n" + strings.Repeat("x", 600) + "\n", "is never closed"},
		"unclosed fence hides a version heading":          {"```bash\n## v0.36.0\n", "names a release version"},
		"unclosed fence on its own":                       {"~~~\n", "is never closed"},
		"tilde fence holding backticks hides a heading":   {"~~~\n```\n~~~\n## v0.36.0\n", "## headings are"},
		"tilde fence holding backticks hides a long line": {"~~~\n```\n~~~\n" + long + "\n", "paragraph"},
		"short closing fence leaves it open":              {"````\ncode\n```\n", "is never closed"},
		"four-space-indented fence is not a fence":        {"    ```\n## v0.36.0\n    ```\n", "names a release version"},
		"indented setext-looking line is not a heading":   {"    ---\n## v0.36.0\n", "names a release version"},
		// Versions in every spelling, comments included.
		"entity-encoded version":                      {"<p>v0&#46;36&#46;0</p>\n", `names a release version "v0.36.0"`},
		"fullwidth version":                           {"<p>ｖ０．３６．０</p>\n", `names a release version "v0.36.0"`},
		"version only in a comment":                   {"<!-- v0.36.0 -->\n", `names a release version "v0.36.0"`},
		"version heading in a comment":                {"<!--\n## v0.36.0\n-->\n", "names a release version"},
		"backticked version bullet":                   {"- `v0.36.0` adds spill\n", "names a release version"},
		"version inside a bullet":                     {"- In v0.36.0 the spill bound changed\n", "names a release version"},
		"bare version bullet":                         {"- 0.36.0: spill bound\n", "names a release version"},
		"version table row":                           {"| v0.36.0 | spill bound |\n", "names a release version"},
		"bold version":                                {"**v0.36.0** fixes spill\n", "names a release version"},
		"rc version":                                  {"Fixed in 0.36.0-rc1.\n", "names a release version"},
		"version badge url":                           {"![v](https://img.shields.io/badge/v0.36.0-blue)\n", "names a release version"},
		"version in a release-notes link":             {"[notes](docs/releases/release-notes-v0.36.0.md)\n", "names a release version"},
		"version in a code block":                     {"```text\nbinlogviz v0.36.0\n```\n", "names a release version"},
		"version inside an install-lookalike mention": {"the install.sh docs mention v0.36.0\n", "names a release version"},
		// Headings: h2 is a fixed list; other levels may not announce history.
		"changelog heading":            {"## Changelog\n", "## headings are"},
		"new in heading":               {"## New in 0.36\n", "## headings are"},
		"migration heading":            {"## Migration from 0.35\n", "## headings are"},
		"upgrade heading":              {"## Upgrade notes\n", "## headings are"},
		"version heading after a rule": {"---\n\n## v0.36.0\n", "## headings are"},
		"zh changelog heading":         {"## 更新日志\n", "## headings are"},
		"zh new features heading":      {"## 0.36 新功能\n", "## headings are"},
		"zh upgrade heading":           {"## 升级须知\n", "## headings are"},
		"html h2 version":              {"<h2>v0.36.0</h2>\n", "## headings are"},
		"setext version heading":       {"v0.36.0\n-------\n", "## headings are"},
		"setext changelog heading":     {"Changelog\n=========\n", "announces a release or changelog"},
		"what's new heading":           {"### What's New\n", "announces a release or changelog"},
		"uppercase html release notes": {"<H3>Release Notes</H3>\n", "announces a release or changelog"},
		"zh new feature heading":       {"### 新特性\n", "announces a release or changelog"},
		"html summary version":         {"<details><summary>v0.36.0</summary>\n\nnotes\n\n</details>\n", "names a release version"},
		"an ordinary-looking h2":       {"## Development\n", "## headings are"},
		// Size.
		"soft-wrapped long paragraph": {wrapped, "paragraph"},
		"one long line":               {long + "\n", "450"},
	}
	base, err := os.ReadFile(filepath.Join(repoRoot(t), "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	want := readmeH2["README.md"]
	if issues := readmeShapeIssues(string(base), want); len(issues) != 0 {
		t.Fatalf("README.md must pass before probing: %v", issues)
	}
	for name, tc := range probes {
		issues := readmeShapeIssues(string(base)+"\n"+tc.probe, want)
		if !slices.ContainsFunc(issues, func(i readmeIssue) bool { return strings.Contains(i.msg, tc.want) }) {
			t.Errorf("guard misses %s (want %q), got %v:\n%s", name, tc.want, issues, tc.probe)
		}
	}
}

// The guard must not flag what a concise README needs.
func TestReadmeShapeAllowsOrdinaryContent(t *testing.T) {
	ok := strings.Join([]string{
		"# BinlogViz",
		"",
		"## Installation",
		"",
		"```bash",
		"sh ./install.sh --version v0.35.2",
		"curl -fsSLO https://example.invalid/releases/download/v0.35.2/x.tar.gz",
		"go install github.com/x/y@v0.35.2",
		"brew install --cask binlogviz",
		"```",
		"",
		"~~~sh",
		"curl -fsSLO https://example.invalid/releases/download/v0.35.2/binlogviz_0.35.2_linux_amd64.tar.gz",
		"~~~",
		"",
		"## Quick Start",
		"",
		"```bash",
		"binlogviz analyze mysql-bin.000123 --start \"2026-03-15T10:00:00Z\"",
		"```",
		"",
		"### Upgrade with Homebrew",
		"### Release and supporting material",
		"### 发布与补充资料",
		"",
		"- works on MySQL 8.0.36 and MariaDB 10.6",
		"- 从 mysqlbinlog 迁移只需要一条命令",
		"- a 10.0.0.1 address or a 1.5 GB binlog is fine",
		"- [Release Notes](https://example.invalid/docs/releases)",
		"- [Changelog](https://example.invalid/CHANGELOG.md)",
		"- Go 1.26.10+ if building from source",
		"- 如果从源码构建，需要 Go 1.26.10+",
		"![Go Version](https://img.shields.io/badge/go-1.26.10-00ADD8?logo=go)",
		strings.Repeat("- "+strings.Repeat("item ", 18)+"\n", 10),
		"| Command | Purpose |",
		"| --- | --- |",
		"| `analyze` | summarize one window |",
		"",
		"Headline",
		"--------",
	}, "\n")
	if issues := readmeShapeIssues(ok, []string{"Installation", "Quick Start", "Headline"}); len(issues) != 0 {
		t.Errorf("guard flags ordinary content: %v", issues)
	}
}
