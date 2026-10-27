// Package binlogviz guards the shape of the project READMEs.
// input: repository README.md and README_ZH.md, plus inline bypass probes.
// output: a failure when a README grows a changelog or per-version section, a version token, an over-long paragraph, an unclosed fence, or past its line or byte bound.
// pos: release-doc drift guard; release notes and behavior detail belong in docs/, not in the README (BinlogViz README bloat, v0.21.0 -> v0.35.2).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
	// A fence opens with three or more backticks or tildes. A backtick fence's
	// info string cannot contain a backtick.
	readmeFenceOpen = regexp.MustCompile("^\\s*(`{3,}|~{3,})(.*)$")
	readmeATX       = regexp.MustCompile(`^\s{0,3}#{1,6}(?:\s+(.*?))?\s*#*\s*$`)
	readmeSetext    = regexp.MustCompile(`^\s{0,3}(?:=+|-+)\s*$`)
	readmeHTMLHead  = regexp.MustCompile(`(?i)<(h[1-6]|summary)\b[^>]*>(.*?)(?:</(?:h[1-6]|summary)\s*>|$)`)
	readmeListItem  = regexp.MustCompile(`^\s*(?:[-*+]|\d+[.)])(?:\s|$)`)
	readmeTableRow  = regexp.MustCompile(`^\s*\|`)

	// Headings that announce a release, a changelog, or an upgrade.
	readmeNotesHeading = regexp.MustCompile(`(?i)change\s*-?\s*log|changes\s+in|what'?s\s+new|\bnew\s+in\b|\bnew\s+features?\b|release[\s-]*notes?|\bupgrad|\bmigrat|\bbreaking\s+changes?|\bdeprecat|\bversion\s+history|\bhistory\b|` +
		`更新日志|变更日志|变更记录|更新记录|更新说明|升级|新特性|新功能|新版本|发行说明|发布说明|版本说明|版本历史|迁移|破坏性变更`)
	// Any dotted number in a heading names a release ("## 0.36 新功能").
	readmeHeadingNumber = regexp.MustCompile(`\d+\.\d+`)
	// A release version anywhere: v0.36, v0.36.0, 0.36.0, 0.36.0-rc1.
	readmeVersionToken = regexp.MustCompile(`(?i)(?:^|[^\w.])(?:v\d+\.\d+(?:\.\d+)?|\d+\.\d+\.\d+)(?:[-+][\w.]+)?(?:$|[^\w])`)
	// The Go toolchain version is a build requirement, not a release.
	readmeGoToolchain = regexp.MustCompile(`(?i)\bgo[ -]v?\d+\.\d+\.\d+\+?`)
	// Install command lines may name a release, for example --version v0.35.2.
	readmeInstallSection = regexp.MustCompile(`(?i)^(installation|install|安装)$`)
)

type readmeIssue struct {
	line int
	msg  string
}

func (i readmeIssue) String() string { return fmt.Sprintf("%d: %s", i.line, i.msg) }

// readmeShapeIssues parses body as Markdown just far enough to find fences,
// headings and paragraphs, and returns every rule the README breaks.
func readmeShapeIssues(body string) []readmeIssue {
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

	checkHeading := func(n int, text string) {
		text = strings.TrimSpace(text)
		if readmeNotesHeading.MatchString(text) || readmeHeadingNumber.MatchString(readmeGoToolchain.ReplaceAllString(text, "")) {
			add(n, "heading %q announces a release, changelog or upgrade", text)
		}
	}
	checkVersion := func(n int, line string) {
		if readmeVersionToken.MatchString(readmeGoToolchain.ReplaceAllString(line, "")) {
			add(n, "names a release version (%q)", strings.TrimSpace(line))
		}
	}

	var (
		fenceChar  byte
		fenceLen   int
		fenceStart int
		section    string
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
			if !readmeInstallSection.MatchString(section) {
				checkVersion(n, line)
			}
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
			checkHeading(n, m[2])
		}
		if m := readmeATX.FindStringSubmatch(line); m != nil {
			flush()
			checkHeading(n, m[1])
			if strings.HasPrefix(strings.TrimSpace(line), "## ") {
				section = strings.TrimSpace(m[1])
			}
			continue
		}
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		if readmeSetext.MatchString(line) && len(para) > 0 {
			// The paragraph above is a setext heading.
			checkHeading(n-1, strings.Join(para, " "))
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
	return issues
}

func TestReadmeStaysConcise(t *testing.T) {
	for _, name := range []string{"README.md", "README_ZH.md"} {
		body, err := os.ReadFile(filepath.Join(repoRoot(t), name))
		if err != nil {
			t.Fatal(err)
		}
		for _, issue := range readmeShapeIssues(string(body)) {
			t.Errorf("%s:%s; %s", name, issue, readmeNotesHome)
		}
	}
}

// Every way QA found to slip notes past the first version of this guard,
// plus the shapes it already caught. Each is appended to the real README
// and must fail.
func TestReadmeShapeCatchesBypassProbes(t *testing.T) {
	long := strings.Repeat("word ", 120)
	wrapped := strings.Repeat(strings.Repeat("word ", 20)+"\n", 5)
	probes := map[string]string{
		// QA r1 bypasses.
		"unclosed fence hides a long line":                "```\n" + strings.Repeat("x", 600) + "\n",
		"unclosed fence hides a version heading":          "```bash\n## v0.36.0\n",
		"unclosed fence on its own":                       "~~~\n",
		"tilde fence holding backticks hides a heading":   "~~~\n```\n~~~\n## v0.36.0\n",
		"tilde fence holding backticks hides a long line": "~~~\n```\n~~~\n" + long + "\n",
		"changelog heading":                               "## Changelog\n",
		"new in heading":                                  "## New in 0.36\n",
		"migration heading":                               "## Migration from 0.35\n",
		"zh changelog heading":                            "## 更新日志\n",
		"zh new features heading":                         "## 0.36 新功能\n",
		"html h2 version":                                 "<h2>v0.36.0</h2>\n",
		"html summary version":                            "<details><summary>v0.36.0</summary>\n\nnotes\n\n</details>\n",
		"backticked version bullet":                       "- `v0.36.0` adds spill\n",
		"version inside a bullet":                         "- In v0.36.0 the spill bound changed\n",
		"bare version bullet":                             "- 0.36.0: spill bound\n",
		"version table row":                               "| v0.36.0 | spill bound |\n",
		"soft-wrapped long paragraph":                     wrapped,
		// Shapes the first guard already caught.
		"version heading after a rule":            "---\n\n## v0.36.0\n",
		"version heading in a comment":            "<!--\n## v0.36.0\n-->\n",
		"upgrade heading":                         "## Upgrade notes\n",
		"bold version":                            "**v0.36.0** fixes spill\n",
		"setext version heading":                  "v0.36.0\n-------\n",
		"setext changelog heading":                "Changelog\n=========\n",
		"what's new heading":                      "### What's New\n",
		"uppercase html release notes":            "<H3>Release Notes</H3>\n",
		"zh upgrade heading":                      "## 升级须知\n",
		"zh new feature heading":                  "### 新特性\n",
		"version in a code block outside install": "```text\nbinlogviz v0.36.0\n```\n",
		"short closing fence leaves it open":      "````\ncode\n```\n",
		"rc version":                              "Fixed in 0.36.0-rc1.\n",
		"one long line":                           long + "\n",
	}
	base, err := os.ReadFile(filepath.Join(repoRoot(t), "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if issues := readmeShapeIssues(string(base)); len(issues) != 0 {
		t.Fatalf("README.md must pass before probing: %v", issues)
	}
	for name, probe := range probes {
		if issues := readmeShapeIssues(string(base) + "\n" + probe); len(issues) == 0 {
			t.Errorf("guard misses %s:\n%s", name, probe)
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
		"```",
		"",
		"~~~sh",
		"curl -fsSLO https://example.invalid/binlogviz_0.35.2_linux_amd64.tar.gz",
		"~~~",
		"",
		"## Quick Start",
		"",
		"```bash",
		"binlogviz analyze mysql-bin.000123 --start \"2026-03-15T10:00:00Z\"",
		"```",
		"",
		"### Release and supporting material",
		"### 发布与补充资料",
		"",
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
	if issues := readmeShapeIssues(ok); len(issues) != 0 {
		t.Errorf("guard flags ordinary content: %v", issues)
	}
}
