// input: text from a repository doc page (HTML, Markdown or inline JS).
// output: the version-like tokens in that text after folding the ways a page can spell a dotted number.
// pos: shared helper for the release-doc drift guards (landing page now; the README guard redesign in #447 should reuse it).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"html"
	"regexp"
	"strconv"
	"strings"

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
// removed: "v 0.35.2" yields "v0.35.2". Callers pass versionScanText output.
func versionTokens(s string) []string {
	var out []string
	for _, m := range versionToken.FindAllStringSubmatch(s, -1) {
		out = append(out, tokenSpace.ReplaceAllString(m[1], ""))
	}
	return out
}

// isReleaseShaped reports whether a token can only be a release version:
// it carries a v or has three parts. A bare two-part number ("0.45") can be
// a CSS value, so callers decide where those are allowed.
func isReleaseShaped(tok string) bool {
	return strings.HasPrefix(strings.ToLower(tok), "v") || strings.Count(tok, ".") == 2
}
