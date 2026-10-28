// input: text from a repository doc page (HTML, Markdown or inline JS), plus the shared allowed-number list.
// output: the version-like tokens in that text after folding the ways a page can spell a dotted number.
// pos: shared helper for the release-doc drift guards (landing page now; the README guard redesign in #447 should reuse it).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"html"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/text/unicode/norm"
)

var (
	// \uXXXX, \u{X...} and \xXX escapes inside JS strings.
	jsEscape = regexp.MustCompile(`\\u\{([0-9a-fA-F]{1,6})\}|\\u([0-9a-fA-F]{4})|\\x([0-9a-fA-F]{2})`)
	// A dotted number with two or three parts and an optional v, not preceded by a digit or dot.
	// Word characters before it are fine, so binlogviz_v0.35.2_linux still yields v0.35.2.
	versionToken = regexp.MustCompile(`(?:^|[^0-9.])([vV]?[ \t]*\d+[ \t]*\.[ \t]*\d+(?:[ \t]*\.[ \t]*\d+)?)`)
	tokenSpace   = regexp.MustCompile(`[ \t]+`)
)

// versionScanText folds HTML character references (also double-encoded),
// JS string escapes and Unicode compatibility forms (fullwidth digits and
// dots, the U+2024 one-dot leader) to plain ASCII, so a scan for dotted
// numbers sees what a reader sees.
func versionScanText(s string) string {
	for range 3 {
		next := html.UnescapeString(jsEscape.ReplaceAllStringFunc(s, func(m string) string {
			sub := jsEscape.FindStringSubmatch(m)
			hex := sub[1] + sub[2] + sub[3]
			r, err := strconv.ParseUint(hex, 16, 32)
			if err != nil {
				return m
			}
			return string(rune(r))
		}))
		if next == s {
			break
		}
		s = next
	}
	return norm.NFKC.String(s)
}

// versionTokens returns every version-like token in s, with inner spaces
// removed: "v 0.35.2" yields "v0.35.2". A token followed by another .digit
// is part of a longer run (10.0.0.1 is an address, not a release). Callers
// pass versionScanText output.
func versionTokens(s string) []string {
	var out []string
	for _, m := range versionToken.FindAllStringSubmatchIndex(s, -1) {
		if m[1]+1 < len(s) && s[m[1]] == '.' && s[m[1]+1] >= '0' && s[m[1]+1] <= '9' {
			continue
		}
		out = append(out, tokenSpace.ReplaceAllString(s[m[2]:m[3]], ""))
	}
	return out
}

// isReleaseShaped reports whether a token can only be a release version:
// it carries a v or has three parts. A bare two-part number ("0.45",
// "MySQL 8.0", "1.5 GB") can be anything else, so it never counts.
func isReleaseShaped(tok string) bool {
	return strings.HasPrefix(strings.ToLower(tok), "v") || strings.Count(tok, ".") == 2
}

// landingAllowedNumbersFile is the one allowlist both landing version gates
// read — this test and scripts/check-release-prep.sh — so CI and the
// tag-push gate agree. One entry per line: the exact context text, a TAB,
// then the reason; # comments and blank lines are ignored.
const landingAllowedNumbersFile = "scripts/landing-allowed-numbers.txt"

type allowedNumber struct{ context, reason string }

// loadAllowedNumbers reads the shared allowlist the release-prep check also
// applies: contexts in which a release-shaped token may appear.
func loadAllowedNumbers(t *testing.T) []allowedNumber {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), filepath.FromSlash(landingAllowedNumbersFile)))
	if err != nil {
		t.Fatal(err)
	}
	var out []allowedNumber
	for _, line := range strings.Split(string(b), "\n") {
		ctx, why, ok := strings.Cut(line, "\t")
		if ok && ctx != "" && !strings.HasPrefix(ctx, "#") {
			out = append(out, allowedNumber{ctx, strings.TrimSpace(why)})
		}
	}
	return out
}
