// Package binlogviz guards the shape of the project READMEs.
// input: repository README.md and README_ZH.md.
// output: a failure when a README grows a per-version section, a version bullet, an over-long paragraph, or past its line or byte bound.
// pos: release-doc drift guard; release notes and behavior detail belong in docs/, not in the README (BinlogViz README bloat, v0.21.0 -> v0.35.2).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
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
	readmeMaxLines     = 360
	readmeMaxBytes     = 16000
	readmeMaxLineRunes = 450
)

const readmeNotesHome = "release notes go in docs/releases/release-notes-vX.Y.Z.md and .zh-CN.md plus CHANGELOG.md; " +
	"behavior detail goes in docs/reference/analyze-behavior*.md, docs/recipe/flashback*.md, docs/recipe/compare-trend-workflow*.md, " +
	"or another page under docs/reference or docs/recipe; the README keeps the command and a link"

var (
	readmeVersionHeading = regexp.MustCompile(`(?i)^#{1,6}\s.*(\bv\d+\.\d+|\brelease notes?\b|\bupgrad|\bwhat'?s new\b|\bchanges in\b|升级|发行说明|新版本|更新说明|版本说明)`)
	readmeVersionBullet  = regexp.MustCompile(`^\s*(?:[-*+]|\d+\.)?\s*(?:\*\*|__)?\[?v\d+\.\d+\.\d+`)
)

func TestReadmeStaysConcise(t *testing.T) {
	for _, name := range []string{"README.md", "README_ZH.md"} {
		body, err := os.ReadFile(filepath.Join(repoRoot(t), name))
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
		if len(lines) > readmeMaxLines {
			t.Errorf("%s has %d lines, more than %d; move detail out: %s", name, len(lines), readmeMaxLines, readmeNotesHome)
		}
		if len(body) > readmeMaxBytes {
			t.Errorf("%s has %d bytes, more than %d; move detail out: %s", name, len(body), readmeMaxBytes, readmeNotesHome)
		}
		fence := false
		for i, line := range lines {
			if strings.HasPrefix(strings.TrimSpace(line), "```") {
				fence = !fence
				continue
			}
			if fence {
				continue
			}
			if readmeVersionHeading.MatchString(line) {
				t.Errorf("%s:%d is a per-version or upgrade heading (%q); %s", name, i+1, strings.TrimSpace(line), readmeNotesHome)
			}
			if readmeVersionBullet.MatchString(line) {
				t.Errorf("%s:%d starts with a release version (%q); %s", name, i+1, strings.TrimSpace(line), readmeNotesHome)
			}
			if n := utf8.RuneCountInString(line); n > readmeMaxLineRunes {
				t.Errorf("%s:%d is a %d-character paragraph, more than %d; %s", name, i+1, n, readmeMaxLineRunes, readmeNotesHome)
			}
		}
	}
}

// The guard must catch the shapes that bloated the README.
func TestReadmeShapePatterns(t *testing.T) {
	for _, line := range []string{"## Upgrade Notes", "### v0.35.2", "## 升级须知", "## What's new in v0.36", "### Release notes"} {
		if !readmeVersionHeading.MatchString(line) {
			t.Errorf("heading guard misses %q", line)
		}
	}
	for _, line := range []string{"- v0.35.2: spill bound", "**v0.23.1** fixes", "1. v0.30.0 adds"} {
		if !readmeVersionBullet.MatchString(line) {
			t.Errorf("bullet guard misses %q", line)
		}
	}
	for _, line := range []string{"## Installation", "### Release and supporting material", "## Quick Start", "### 发布与补充资料", "- [Release Notes](https://example.invalid/docs/releases)"} {
		if readmeVersionHeading.MatchString(line) || readmeVersionBullet.MatchString(line) {
			t.Errorf("guard flags ordinary line %q", line)
		}
	}
}
