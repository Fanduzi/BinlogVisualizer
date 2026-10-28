// input: every file under docs/landing/, plus inline mutations of the real page.
// output: a failure when the landing page grows release history, a version string, a date, a checksum, an unlisted file, section, id, heading, link or script, a reveal threshold, an unlisted meta tag, a <template> or <noscript>, a listed heading or section that a reader cannot see as served, content hidden until JS runs, or a file or total past its size cap.
// pos: release-doc drift guard; the landing page is a version-free product page, so a release never edits it (BinlogViz #443).
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"fmt"
	"html"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
)

// The landing page is an allowlist, not a pattern ban: two rounds of banned
// patterns let release history back in 28 ways. Anything not listed here
// fails, so adding a section, link or script means editing this file on
// purpose. Release history lives in docs/releases, CHANGELOG.md and GitHub
// Releases.
const (
	landingDir = "docs/landing"
	// index.html is 91 KiB today, inline CSS and JS included. The v0.35.2
	// page with 59 release cards was 230 KiB.
	landingPageMaxBytes = 104 << 10 // any one file
	landingMaxBytes     = 112 << 10 // everything under docs/landing
)

var (
	landingFiles = []string{"index.html"}

	// Section ids, in page order.
	landingSections = []string{"overview", "why", "workflows", "how", "outputs", "trust", "install", "next"}

	// Every other element id on the page.
	landingOtherIDs = []string{
		"top", "i18n-root", "back-to-top",
		"tab-text", "tab-json", "tab-markdown", "tab-html",
		"pane-text", "pane-json", "pane-markdown", "pane-html",
	}

	// data-i18n keys of the <h2> headings, in page order.
	landingHeadings = []string{"why.title", "workflows.title", "how.title", "outputs.title", "trust.title", "install.title", "cta.title"}

	// Links that leave the page. In-page links must point at a listed id.
	landingExternalLinks = []string{
		"https://github.com/Fanduzi/BinlogVisualizer",
		"https://github.com/Fanduzi/BinlogVisualizer#readme",
		"https://github.com/Fanduzi/BinlogVisualizer/releases",
		"https://github.com/Fanduzi/BinlogVisualizer/blob/main/CHANGELOG.md",
	}

	// The only calendar dates: the sample incident (03-15), its baseline
	// (03-08) and the trend day before (03-14) in the demo commands and output.
	landingAllowedDates = []string{"2026-03-08", "2026-03-14", "2026-03-15"}

	// CSS selectors that may hide an element, with the reason it comes back.
	landingHiddenSelectors = map[string]string{
		".tab-pane":    "inactive output tabs; .tab-pane.active is shown and the tab buttons switch panes",
		".back-to-top": "the back-to-top button appears after scrolling",
	}
	// Elements that may carry the hidden attribute.
	landingHiddenAttrIDs = []string{"pane-json", "pane-markdown", "pane-html", "back-to-top"}

	// The complete set of <meta> tags, whitespace collapsed. Meta content is
	// not shown on the page, so nothing a reader is meant to see goes there.
	landingMetas = []string{
		`<meta charset="UTF-8" />`,
		`<meta name="viewport" content="width=device-width, initial-scale=1.0" />`,
		`<meta name="description" content="BinlogViz is a local CLI for MySQL ROW binlog analysis, named snapshot management, and visual compare review. Find hot tables, large transactions, write spikes, incident-window workload changes, and compare saved snapshots or exported JSON reports as text, JSON, or chart-based HTML." />`,
	}
)

var (
	styleBlock    = regexp.MustCompile(`(?is)<style\b[^>]*>(.*?)</style\s*>`)
	styleAttr     = regexp.MustCompile(`(?is)\bstyle\s*=\s*("[^"]*"|'[^']*')`)
	anyTag        = regexp.MustCompile(`(?s)<[^>]*>`)
	sectionTag    = regexp.MustCompile(`(?i)<section\b([^>]*)>`)
	h2Tag         = regexp.MustCompile(`(?i)<h2\b([^>]*)>`)
	idAttr        = regexp.MustCompile(`(?i)(?:^|[\s"'])id\s*=\s*("[^"]*"|'[^']*'|[^\s"'=>]+)`)
	i18nAttr      = regexp.MustCompile(`(?i)\bdata-i18n\s*=\s*("[^"]*"|'[^']*'|[^\s"'=>]+)`)
	hiddenAttrTag = regexp.MustCompile(`(?i)<[a-z][a-z0-9-]*\b[^>]*?\shidden(?:[\s=/][^>]*)?>`)
	linkAttr      = regexp.MustCompile(`(?i)(?:^|[\s"'])(?:href|src|srcset|action|formaction|poster|xlink:href)\s*=\s*("[^"]*"|'[^']*'|[^\s"'=>]+)`)
	cssRule       = regexp.MustCompile(`([^{}]+)\{([^{}]*)\}`)
	cssComment    = regexp.MustCompile(`(?s)/\*.*?\*/`)
	cssResource   = regexp.MustCompile(`(?i)\burl\s*\(|@import`)
	joinStrip     = regexp.MustCompile("[\\s'\"`+]")
	hexRun        = regexp.MustCompile(`(?i)[0-9a-f]{40,}`)
	isoDate       = regexp.MustCompile(`\b((?:19|20)\d\d)\s*[-/.年]\s*(1[0-2]|0?[1-9])\s*(?:[-/.月]\s*(\d{1,2}))?`)
	wordDates     = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+(?:\d{1,2}(?:st|nd|rd|th)?,?\s+)?(?:19|20)\d\d\b`),
		regexp.MustCompile(`(?i)\b\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?,?\s+(?:19|20)\d\d\b`),
	}
	// A reveal threshold above 0 never fires on a section taller than
	// 1/threshold viewports: that left a 10,000px section blank (#443).
	htmlComment = regexp.MustCompile(`(?s)<!--.*?(?:-->|$)`)
	// Elements whose content is never rendered as page text.
	unrenderedBlock = regexp.MustCompile(`(?is)<(script|style|template|noscript|textarea|title)\b[^>]*>.*?(?:</(?:script|style|template|noscript|textarea|title)\s*>|$)`)
	metaTag         = regexp.MustCompile(`(?i)<meta\b[^>]*>`)
	htmlToken       = regexp.MustCompile(`(?s)<(/?)([a-zA-Z][a-zA-Z0-9-]*)\b([^>]*?)(/?)>|[^<]+|<`)
	revealRule      = regexp.MustCompile(`(?i)IntersectionObserver|intersectionRatio|isIntersecting|\breveal\b|\bthreshold\b|rootMargin`)
	hidingDecl      = regexp.MustCompile(`(?i)(?:^|[;{\s])(?:opacity\s*:\s*0*\.?0*\s*(?:!important\s*)?(?:;|$)|visibility\s*:\s*(?:hidden|collapse)|display\s*:\s*none|content-visibility\s*:\s*hidden)`)
	landingBanned   = []struct {
		re  *regexp.Regexp
		why string
	}{
		{regexp.MustCompile(`(?i)\bfetch\s*\(|XMLHttpRequest|\bimport\s*\(|EventSource|WebSocket|sendBeacon|new\s+Worker|api\.github\.com`), "runtime loading; the page ships everything it shows"},
		{regexp.MustCompile(`(?i)<(?:link|iframe|object|embed|base)\b`), "external resource tag"},
		{regexp.MustCompile(`(?i)<(?:template|noscript)\b`), "content that is not shown as served"},
		{regexp.MustCompile(`(?i)window\.open\s*\(|location\s*=[^=]|location\.(?:assign|replace)\s*\(|\.(?:src|href|id)\s*=[^=]|setAttribute\(\s*['"](?:id|style|src|href|hidden)['"]|\.style\b|\.cssText|insertRule|adoptedStyleSheets`), "a script that sets ids, links or styles"},
	}
)

func attrValue(s string) string { return strings.Trim(s, `"'`) }

var voidElements = map[string]bool{
	"area": true, "base": true, "br": true, "col": true, "embed": true, "hr": true, "img": true,
	"input": true, "link": true, "meta": true, "source": true, "track": true, "wbr": true,
}

// landingVisible returns what a reader sees as served: the text of each
// listed section and the data-i18n keys of the <h2> headings, in order.
// Comments, <script>, <style>, <template>, <noscript>, <meta> content and
// any element with the hidden attribute or an inline style that hides it
// (and everything inside it) are left out.
func landingVisible(raw string) (sectionText map[string]string, headings []string) {
	s := htmlComment.ReplaceAllString(raw, "")
	s = unrenderedBlock.ReplaceAllString(s, "")
	s = metaTag.ReplaceAllString(s, "")
	type open struct {
		name, section string
		hides         bool
	}
	var stack []open
	hiddenDepth := 0
	sectionText = map[string]string{}
	current := func() string {
		for i := len(stack) - 1; i >= 0; i-- {
			if stack[i].section != "" {
				return stack[i].section
			}
		}
		return ""
	}
	for _, m := range htmlToken.FindAllStringSubmatch(s, -1) {
		switch {
		case m[2] == "": // text
			if hiddenDepth == 0 {
				if sec := current(); sec != "" {
					sectionText[sec] += m[0]
				}
			}
		case m[1] == "/":
			name := strings.ToLower(m[2])
			for i := len(stack) - 1; i >= 0; i-- {
				if stack[i].name == name {
					for _, o := range stack[i:] {
						if o.hides {
							hiddenDepth--
						}
					}
					stack = stack[:i]
					break
				}
			}
		default:
			name, attrs := strings.ToLower(m[2]), m[3]
			hides := hiddenAttrTag.MatchString("<x " + attrs + ">")
			if a := styleAttr.FindStringSubmatch(attrs); a != nil && hidingDecl.MatchString(attrValue(a[1])) {
				hides = true
			}
			if name == "h2" && hiddenDepth == 0 && !hides {
				key := ""
				if a := i18nAttr.FindStringSubmatch(attrs); a != nil {
					key = attrValue(a[1])
				}
				headings = append(headings, key)
			}
			if voidElements[name] || m[4] == "/" {
				continue
			}
			o := open{name: name, hides: hides}
			if name == "section" {
				if a := idAttr.FindStringSubmatch(attrs); a != nil {
					o.section = attrValue(a[1])
				}
			}
			if hides {
				hiddenDepth++
			}
			stack = append(stack, o)
		}
	}
	return sectionText, headings
}

// landingIssues returns every rule the landing files break, one line each,
// prefixed with the rule: file, size, version, section, id, heading, link,
// reveal, banned, hidden, hex, date or allowlist. allowed lists the number
// contexts a release-shaped token may appear in (the shared allowlist
// scripts/check-release-prep.sh also reads).
func landingIssues(files map[string]string, allowed []allowedNumber) []string {
	var issues []string
	add := func(rule, format string, args ...any) {
		issues = append(issues, rule+": "+fmt.Sprintf(format, args...))
	}

	names := make([]string, 0, len(files))
	total := 0
	for name, body := range files {
		names = append(names, name)
		total += len(body)
		if len(body) > landingPageMaxBytes {
			add("size", "%s/%s is %d bytes, over the %d-byte page bound; release history belongs in docs/releases and GitHub Releases", landingDir, name, len(body), landingPageMaxBytes)
		}
		if !slices.Contains(landingFiles, name) {
			add("file", "%s/%s is not a listed landing file; the landing page is one self-contained index.html (landingFiles)", landingDir, name)
		}
	}
	sort.Strings(names)
	for _, want := range landingFiles {
		if _, ok := files[want]; !ok {
			add("file", "%s/%s is missing", landingDir, want)
		}
	}
	if total > landingMaxBytes {
		add("size", "%s holds %d bytes in total, over the %d-byte bound; release history belongs in docs/releases and GitHub Releases", landingDir, total, landingMaxBytes)
	}

	ids := map[string]int{}
	for _, name := range names {
		body := files[name]
		where := landingDir + "/" + name

		// Versions: the folded text, comments included, minus the contexts
		// in the shared allowlist scripts/check-release-prep.sh also reads.
		text := versionScanText(body)
		for _, a := range allowed {
			if !strings.Contains(text, a.context) {
				add("allowlist", "%s no longer contains allowed number context %q; drop it from %s", where, a.context, landingAllowedNumbersFile)
			}
			text = strings.ReplaceAll(text, a.context, "")
		}
		var css strings.Builder
		rest := styleBlock.ReplaceAllStringFunc(text, func(m string) string {
			css.WriteString(styleBlock.FindStringSubmatch(m)[1] + "\n")
			return "<style></style>"
		})
		rest = styleAttr.ReplaceAllStringFunc(rest, func(m string) string {
			css.WriteString(styleAttr.FindStringSubmatch(m)[1] + "\n")
			return ""
		})
		seen := map[string]bool{}
		report := func(view, tok string) {
			if !seen[view+tok] {
				seen[view+tok] = true
				add("version", "%s (%s) names %q; the landing page is version-free so a release never edits it — if %q is not a BinlogViz release, add its context to %s", where, view, tok, tok, landingAllowedNumbersFile)
			}
		}
		for _, v := range []struct{ label, text string }{{"source", rest}, {"text", anyTag.ReplaceAllString(rest, "")}} {
			for _, tok := range versionTokens(v.text) {
				if isReleaseShaped(tok) {
					report(v.label, tok)
				}
			}
		}
		for _, tok := range versionTokens(css.String()) {
			if isReleaseShaped(tok) {
				report("css", tok)
			}
		}
		// Joined view: tags, whitespace, quotes and + removed, so a version or
		// checksum split across lines, tags or JS concatenation reads as one run.
		joined := joinStrip.ReplaceAllString(anyTag.ReplaceAllString(text, ""), "")
		for _, tok := range versionTokens(joined) {
			if strings.Count(tok, ".") == 2 {
				report("joined", tok)
			}
		}
		for _, view := range []struct{ label, text string }{{"source", text}, {"joined", joined}} {
			if m := hexRun.FindString(view.text); m != "" {
				add("hex", "%s (%s) holds a %d-character hex run %.16s…; checksums belong in the release's checksums.txt", where, view.label, len(m), m)
			}
		}
		for _, m := range isoDate.FindAllStringSubmatch(text, -1) {
			date := fmt.Sprintf("%s-%02s", m[1], m[2])
			if m[3] != "" {
				date += fmt.Sprintf("-%02s", m[3])
			}
			date = strings.ReplaceAll(date, " ", "0")
			if !slices.Contains(landingAllowedDates, date) {
				add("date", "%s has a date (%q); dated entries are release history — a legitimate demo date goes in landingAllowedDates", where, strings.TrimSpace(m[0]))
			}
		}
		for _, re := range wordDates {
			if m := re.FindString(text); m != "" {
				add("date", "%s has a date (%q); dated entries are release history — a legitimate demo date goes in landingAllowedDates", where, m)
			}
		}

		// Structure is read from the raw file, script strings included,
		// so markup built by JS is held to the same lists.
		raw := body
		var sections []string
		for _, m := range sectionTag.FindAllStringSubmatch(raw, -1) {
			id := ""
			if a := idAttr.FindStringSubmatch(m[1]); a != nil {
				id = attrValue(a[1])
			}
			sections = append(sections, id)
		}
		if !slices.Equal(sections, landingSections) {
			add("section", "%s sections are %q, want %q; release history goes to GitHub Releases, a new product section goes in landingSections", where, sections, landingSections)
		}
		for _, m := range idAttr.FindAllStringSubmatch(raw, -1) {
			id := attrValue(m[1])
			ids[id]++
			if !slices.Contains(landingSections, id) && !slices.Contains(landingOtherIDs, id) {
				add("id", "%s has unlisted id %q; a legitimate id goes in landingSections or landingOtherIDs", where, id)
			}
		}
		var heads []string
		for _, m := range h2Tag.FindAllStringSubmatch(raw, -1) {
			key := ""
			if a := i18nAttr.FindStringSubmatch(m[1]); a != nil {
				key = attrValue(a[1])
			}
			heads = append(heads, key)
		}
		if !slices.Equal(heads, landingHeadings) {
			add("heading", "%s h2 headings are %q, want %q; a legitimate h2 goes in landingHeadings", where, heads, landingHeadings)
		}

		// What a reader sees as served: every listed heading and some text
		// in every listed section, outside comments, <template>, <meta>,
		// scripts and hidden elements.
		sectionText, visibleHeads := landingVisible(raw)
		if !slices.Equal(visibleHeads, landingHeadings) {
			add("visible", "%s visible h2 headings are %q, want %q; a heading inside a comment, <template>, <script> or a hidden element is not shown", where, visibleHeads, landingHeadings)
		}
		for _, id := range landingSections {
			if strings.TrimSpace(html.UnescapeString(sectionText[id])) == "" {
				add("visible", "%s section %q shows no text as served", where, id)
			}
		}
		for _, m := range metaTag.FindAllString(raw, -1) {
			if m = strings.Join(strings.Fields(m), " "); !slices.Contains(landingMetas, m) {
				add("meta", "%s has unlisted meta tag %s; meta content is not shown on the page — a legitimate meta goes in landingMetas", where, m)
			}
		}
		for _, m := range linkAttr.FindAllStringSubmatch(raw, -1) {
			link := attrValue(m[1])
			if anchor, ok := strings.CutPrefix(link, "#"); ok {
				if !slices.Contains(landingSections, anchor) && !slices.Contains(landingOtherIDs, anchor) {
					add("link", "%s links to unknown anchor %q; the target id belongs in landingSections or landingOtherIDs", where, link)
				}
			} else if !slices.Contains(landingExternalLinks, link) {
				add("link", "%s links to unlisted %q; a legitimate external link goes in landingExternalLinks", where, link)
			}
		}
		if m := revealRule.FindString(raw); m != "" {
			add("reveal", "%s contains %q: no scroll reveal or reveal threshold; content must be visible as served (revealRule)", where, m)
		}
		for _, b := range landingBanned {
			if m := b.re.FindString(raw); m != "" {
				add("banned", "%s contains %q: %s; a legitimate use narrows the pattern in landingBanned", where, m, b.why)
			}
		}

		// Nothing is hidden until a script runs, except the listed controls.
		var rawCSS []string
		for _, m := range styleBlock.FindAllStringSubmatch(raw, -1) {
			rawCSS = append(rawCSS, m[1])
		}
		for _, m := range styleAttr.FindAllStringSubmatch(raw, -1) {
			rawCSS = append(rawCSS, m[1])
		}
		if m := cssResource.FindString(strings.Join(rawCSS, "\n")); m != "" {
			add("banned", "%s CSS contains %q: the page loads no stylesheet, font or image", where, m)
		}
		for _, block := range styleBlock.FindAllStringSubmatch(raw, -1) {
			for _, r := range cssRule.FindAllStringSubmatch(cssComment.ReplaceAllString(block[1], ""), -1) {
				if !hidingDecl.MatchString(r[2]) {
					continue
				}
				for _, sel := range strings.Split(r[1], ",") {
					sel = strings.Join(strings.Fields(sel), " ")
					if _, ok := landingHiddenSelectors[sel]; !ok {
						add("hidden", "%s hides %q (%s); content must be visible without JS — a selector that unhides itself (tab, hover fade, media-query fallback) goes in landingHiddenSelectors", where, sel, strings.TrimSpace(r[2]))
					}
				}
			}
		}
		for _, m := range styleAttr.FindAllStringSubmatch(raw, -1) {
			if hidingDecl.MatchString(attrValue(m[1])) {
				add("hidden", "%s hides an element with an inline style %s; move the hiding to a selector listed in landingHiddenSelectors", where, m[1])
			}
		}
		for _, tag := range hiddenAttrTag.FindAllString(raw, -1) {
			id := ""
			if a := idAttr.FindStringSubmatch(tag); a != nil {
				id = attrValue(a[1])
			}
			if !slices.Contains(landingHiddenAttrIDs, id) {
				add("hidden", "%s hides %s with the hidden attribute; a control that unhides itself goes in landingHiddenAttrIDs", where, tag)
			}
		}
	}
	for id, n := range ids {
		if n > 1 {
			add("id", "id %q appears %d times", id, n)
		}
	}
	sort.Strings(issues)
	return issues
}

func readLandingFiles(t *testing.T) map[string]string {
	t.Helper()
	return readLandingDir(t, filepath.Join(repoRoot(t), filepath.FromSlash(landingDir)))
}

// readLandingDir reads every file under root, nested ones included.
func readLandingDir(t *testing.T, root string) map[string]string {
	t.Helper()
	files := map[string]string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		files[filepath.ToSlash(rel)] = string(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func TestLandingPageHasNoReleaseHistory(t *testing.T) {
	for _, issue := range landingIssues(readLandingFiles(t), loadAllowedNumbers(t)) {
		t.Error(issue)
	}
}

// Each case is one edit to the real page and names the message it must
// produce. The first 29 cover all 28 bypasses QA's probe found against the
// r1 pattern guard (#443).
func TestLandingGuardRejectsRegrowth(t *testing.T) {
	files := readLandingFiles(t)
	page := files["index.html"]
	allowed := loadAllowedNumbers(t)
	if issues := landingIssues(files, allowed); len(issues) > 0 {
		t.Fatalf("the real landing page must pass first: %q", issues)
	}
	replace := func(old, new string) func(string) string {
		return func(p string) string {
			if !strings.Contains(p, old) {
				t.Fatalf("mutation anchor %q not on the page", old)
			}
			return strings.Replace(p, old, new, 1)
		}
	}
	footer := func(s string) func(string) string { return replace("</footer>", s+"</footer>") }
	section := func(s string) func(string) string {
		return replace(`<section id="install"`, s+"\n        <section id=\"install\"")
	}
	script := func(s string) func(string) string { return replace("</script>", s+"\n    </script>") }
	style := func(s string) func(string) string { return replace("</style>", s+"\n    </style>") }
	observer := func(opts string) func(string) string {
		return script("const o = new IntersectionObserver((es) => es.forEach((e) => e.target.classList.add('visible')), { " + opts + " });")
	}
	padTo := func(n int) func(string) string {
		return func(p string) string { return p + strings.Repeat(" ", n-len(p)) }
	}
	// sectionBody returns the start and end of the content of the section
	// that opens with open.
	sectionBody := func(p, open string) (int, int) {
		start := strings.Index(p, open)
		if start < 0 {
			t.Fatalf("mutation anchor %q not on the page", open)
		}
		start += len(open)
		return start, start + strings.Index(p[start:], "</section>")
	}
	wrapSection := func(open, before, after string) func(string) string {
		return func(p string) string {
			s, e := sectionBody(p, open)
			return p[:s] + before + p[s:e] + after + p[e:]
		}
	}
	commentOut := func(open string) func(string) string {
		return func(p string) string {
			s, e := sectionBody(p, open)
			return p[:s] + "<!--" + strings.ReplaceAll(p[s:e], "-->", "") + "-->" + p[e:]
		}
	}
	const hex = "24c9d9cc572607bfb9fb21c24fac546e4748723010fa35c733777c79adce8c8d"
	const observed = `reveal: docs/landing/index.html contains "IntersectionObserver"`
	const unlistedSection = "sections are"

	cases := []struct {
		name, want string
		edit       func(string) string
		extra      map[string]string
		allow      []allowedNumber
	}{
		// QA r1 bypasses: version strings.
		{name: "entity v0&#46;35&#46;2", want: `names "v0.35.2"`, edit: footer("<p>v0&#46;35&#46;2</p>")},
		{name: "entity &#118;0.35.2", want: `names "v0.35.2"`, edit: footer("<p>&#118;0.35.2</p>")},
		{name: "v 0.35.2 with a space", want: `names "v0.35.2"`, edit: footer("<p>v 0.35.2</p>")},
		{name: "0.35.2 without v", want: `names "0.35.2"`, edit: footer("<p>Latest: 0.35.2</p>")},
		{name: "V0.35.2 uppercase", want: `names "V0.35.2"`, edit: footer("<p>V0.35.2</p>")},
		{name: "v0.35 two-part", want: `names "v0.35"`, edit: footer("<p>v0.35</p>")},
		{name: "binlogviz_v0.35.2_linux archive name", want: `names "v0.35.2"`, edit: footer("<code>binlogviz_v0.35.2_linux_amd64.tar.gz</code>")},
		{name: "JS concat 'v0.'+'35.2'", want: `names "v0.35.2"`, edit: script("document.title += ' v0.' + '35.2';")},
		{name: `data-version="0.35.2"`, want: `names "0.35.2"`, edit: replace(`<main id="top">`, `<main id="top" data-version="0.35.2">`)},
		{name: "one-dot leader v0\u202435\u20242", want: `names "v0.35.2"`, edit: footer("<p>v0\u202435\u20242</p>")},
		// QA r1 bypasses: release sections and links.
		{name: "id='releases' single-quoted", want: unlistedSection, edit: section("<section id='releases'><h2>Releases</h2></section>")},
		{name: "id=releases unquoted", want: `unlisted id "releases"`, edit: section("<section id=releases><h2>Releases</h2></section>")},
		{name: `id="changelog" with 12000px`, want: `unlisted id "changelog"`, edit: section(`<section id="changelog"><h2>Changelog</h2><div style="height:12000px"></div></section>`)},
		{name: "<section class=history>", want: unlistedSection, edit: section("<section class=history><h2>History</h2></section>")},
		{name: "What's new section", want: unlistedSection, edit: section("<section><h2>What\u2019s new</h2><ul><li>Spill merge is tiered</li></ul></section>")},
		{name: "release-notes-latest.zh-CN.md link", want: `links to unlisted "docs/releases/release-notes-latest.zh-CN.md"`, edit: footer(`<a href="docs/releases/release-notes-latest.zh-CN.md">n</a>`)},
		// QA r1 bypasses: reveal thresholds that keep a tall section hidden.
		{name: "threshold : 0.12", want: observed, edit: observer("threshold : 0.12")},
		{name: `"threshold": 0.12`, want: observed, edit: observer(`"threshold": 0.12`)},
		{name: "threshold via const T", want: observed, edit: script("const T = 0.12;\nconst o = new IntersectionObserver(() => {}, { threshold: T });")},
		{name: "threshold: 12/100", want: observed, edit: observer("threshold: 12/100")},
		{name: "threshold: 1e-1", want: observed, edit: observer("threshold: 1e-1")},
		{name: "threshold: 1", want: observed, edit: observer("threshold: 1")},
		{name: "threshold: [0, 0.5]", want: observed, edit: observer("threshold: [0, 0.5]")},
		{name: "threshold: [0.5]", want: observed, edit: observer("threshold: [0.5]")},
		{name: "rootMargin -100% never intersects", want: observed, edit: observer("threshold: 0, rootMargin: '0px 0px -100% 0px'")},
		{name: "intersectionRatio > 0.12 gate", want: `contains "intersectionRatio"`, edit: script("if (entry.intersectionRatio > 0.12) entry.target.classList.add('visible');")},
		// QA r1 bypasses: content from outside the page.
		{name: "external releases.js", want: `links to unlisted "releases.js"`, edit: replace("</body>", `<script src="releases.js"></script></body>`)},
		{name: "external releases.css", want: `contains "<link": external resource tag`, edit: replace("</style>", "</style>\n    <link rel=\"stylesheet\" href=\"releases.css\">")},
		{name: "runtime fetch of GitHub releases", want: `contains "fetch(": runtime loading`, edit: script("fetch('https://api.github.com/repos/Fanduzi/BinlogVisualizer/releases').then((r) => r.json());")},

		// The r1 controls, still caught.
		{name: "control v0.35.2", want: `names "v0.35.2"`, edit: footer("<p>v0.35.2</p>")},
		{name: `control id="releases"`, want: `unlisted id "releases"`, edit: section(`<section id="releases"><h2>Releases</h2></section>`)},
		{name: "control release-notes link", want: `links to unlisted "docs/releases/release-notes-v0.35.2.md"`, edit: footer(`<a href="docs/releases/release-notes-v0.35.2.md">n</a>`)},
		// The r1 threshold rule, kept as its own rule: a threshold option
		// fails even without the word IntersectionObserver.
		{name: "threshold option without an observer", want: `reveal: docs/landing/index.html contains "threshold"`, edit: script("const revealOpts = { threshold: 0.12 };")},
		{name: "rootMargin option without an observer", want: `reveal: docs/landing/index.html contains "rootMargin"`, edit: script("const opts = { rootMargin: '0px' };")},

		// Version spellings.
		{name: "version in an HTML comment", want: `names "v0.35.3"`, edit: footer("<!-- v0.35.3 -->")},
		{name: "v-version in CSS content", want: `names "v0.35.3"`, edit: style(`.hero::after { content: "v0.35.3"; }`)},
		{name: "bare 0.35.3 in CSS content", want: `(css) names "0.35.3"`, edit: style(`.hero::after { content: "0.35.3"; }`)},
		{name: "V0.35 two-part in CSS content", want: `(css) names "V0.35"`, edit: style(`.hero::after { content: "V0.35"; }`)},
		{name: "version in an inline style", want: `names "v0.35"`, edit: replace(`<div class="hero-badge">`, `<div class="hero-badge" style="--v:'v0.35'">`)},
		{name: "fullwidth digits", want: `names "v0.35.3"`, edit: footer("<p>ｖ０．３５．３</p>")},
		{name: "JS \\u002e escape", want: `names "0.35.3"`, edit: script(`const v = "0\u002e35\u002e3";`)},
		{name: "version split by a tag", want: `names "v0.35"`, edit: footer("<p>v0<span>.</span>35</p>")},
		{name: "version split across lines", want: `(joined) names "0.35.3"`, edit: footer("<p>v0.35\n.3</p>")},
		{name: "double-encoded entity", want: `names "v0.35.3"`, edit: footer("<p>v0&amp;#46;35&amp;#46;3</p>")},
		{name: "multi-digit v10.200.3000", want: `names "v10.200.3000"`, edit: footer("<p>v10.200.3000</p>")},
		// Structure.
		{name: "release card div with id", want: `unlisted id "release-v-latest"`, edit: footer(`<div id="release-v-latest" class="card">Latest release</div>`)},
		{name: "duplicate id", want: `id "why" appears 2 times`, edit: replace(`<div class="hero-badge">`, `<div class="hero-badge" id="why">`)},
		{name: "extra h2 inside a section", want: "heading: docs/landing/index.html h2 headings are", edit: replace(`<section id="trust">`, `<section id="trust"><h2 data-i18n="trust.history">Recent releases</h2>`)},
		{name: "h2 built by a script", want: "heading: docs/landing/index.html h2 headings are", edit: script(`document.querySelector('main').insertAdjacentHTML('beforeend', '<h2 data-i18n="trust.history">Recent releases</h2>');`)},
		{name: "section dropped", want: unlistedSection, edit: replace(`<section id="trust">`, `<div id="trust">`)},
		{name: "unlisted external link", want: `links to unlisted "https://github.com/Fanduzi/BinlogVisualizer/releases/tag/latest"`, edit: footer(`<a href="https://github.com/Fanduzi/BinlogVisualizer/releases/tag/latest">latest</a>`)},
		{name: "single-quoted link", want: `links to unlisted "notes.html"`, edit: footer(`<a href='notes.html'>notes</a>`)},
		{name: "anchor to a missing id", want: `links to unknown anchor "#changelog"`, edit: footer(`<a href="#changelog">changes</a>`)},
		// Runtime loads and scripts that change the page.
		{name: "relative fetch", want: `contains "fetch(": runtime loading`, edit: script("fetch('releases.json');")},
		{name: "XMLHttpRequest", want: `contains "XMLHttpRequest": runtime loading`, edit: script("const x = new XMLHttpRequest();")},
		{name: "dynamic import", want: `contains "import(": runtime loading`, edit: script("import('./releases.mjs');")},
		{name: "sendBeacon", want: `contains "sendBeacon": runtime loading`, edit: script("navigator.sendBeacon('/x');")},
		{name: "CSS url() background", want: `CSS contains "url("`, edit: style(`.hero { background: url(releases.svg); }`)},
		{name: "CSS @import", want: `CSS contains "@import"`, edit: style(`@import "releases.css";`)},
		{name: "iframe of the releases page", want: `contains "<iframe": external resource tag`, edit: footer(`<iframe title="r"></iframe>`)},
		{name: "script sets a style", want: `contains ".style": a script that sets`, edit: script(`document.body.style.opacity = '0';`)},
		{name: "script opens a window", want: `contains "window.open(": a script that sets`, edit: script(`window.open('/releases');`)},
		{name: "script sets an id", want: `contains ".id = ": a script that sets`, edit: script(`document.body.firstElementChild.id = 'releases';`)},
		{name: "script sets hidden", want: `a script that sets`, edit: script(`document.body.setAttribute('hidden', '');`)},
		// Content hidden until a script runs.
		{name: "JS-class reveal rule", want: `hides ".js .card"`, edit: style(`.js .card { opacity: 0; }`)},
		{name: "opacity 0 as the last declaration", want: `hides ".card"`, edit: style(`.card { color: red; opacity: 0 }`)},
		{name: "visibility hidden rule", want: `hides "#trust .surface-card"`, edit: style(`#trust .surface-card { visibility: hidden; }`)},
		{name: "display none inside @media", want: `hides ".install-grid"`, edit: style(`@media (min-width: 1px) { .install-grid { display: none !important; } }`)},
		{name: "inline style opacity 0", want: "hides an element with an inline style", edit: replace(`<div class="hero-badge">`, `<div class="hero-badge" style="opacity:0">`)},
		{name: "bare hidden attribute names only its own tag", want: "hides <p hidden> with the hidden attribute", edit: footer("<p hidden>What changed</p>")},
		{name: "hidden attribute on a card", want: "with the hidden attribute", edit: replace(`<div class="hero-badge">`, `<div class="hero-badge" hidden>`)},
		// Visible-text view: what a reader sees as served.
		{name: "heading moved into a comment", want: `visible h2 headings are`, edit: replace(`<h2 class="section-title" data-i18n="why.title">`, `<!-- <h2 class="section-title" data-i18n="why.title"> -->`)},
		{name: "heading inside a hidden wrapper", want: `visible h2 headings are`, edit: replace(`<h2 class="section-title" data-i18n="trust.title">`, `<div hidden><h2 class="section-title" data-i18n="trust.title">`)},
		{name: "heading with an inline display none", want: `visible h2 headings are`, edit: replace(`<h2 class="section-title" data-i18n="install.title">`, `<h2 class="section-title" data-i18n="install.title" style="display:none">`)},
		{name: "heading inside a template", want: `visible h2 headings are`, edit: replace(`<h2 data-i18n="cta.title">`, `<template><h2 data-i18n="cta.title">`)},
		{name: "section emptied into a comment", want: `section "trust" shows no text as served`, edit: commentOut(`<section id="trust">`)},
		{name: "section content in a template", want: `section "install" shows no text as served`, edit: wrapSection(`<section id="install">`, "<template>", "</template>")},
		{name: "section content in a hidden div", want: `section "next" shows no text as served`, edit: wrapSection(`<section id="next">`, "<div hidden>", "</div>")},
		{name: "template element", want: `contains "<template": content that is not shown as served`, edit: footer("<template><ul><li>Spill merge is tiered</li></ul></template>")},
		{name: "noscript element", want: `contains "<noscript": content that is not shown as served`, edit: footer("<noscript>What changed lately</noscript>")},
		{name: "meta with release notes", want: `unlisted meta tag <meta name="release-notes" content="spill merge is tiered">`, edit: replace("<meta charset", `<meta name="release-notes" content="spill merge is tiered"><meta charset`)},
		{name: "meta description rewritten", want: `unlisted meta tag <meta name="description" content="What's new: spill merge" />`, edit: func(p string) string {
			return metaTag.ReplaceAllStringFunc(p, func(m string) string {
				if strings.Contains(m, "description") {
					return `<meta name="description" content="What's new: spill merge" />`
				}
				return m
			})
		}},
		// Checksums and dates.
		{name: "sha256 in a pre", want: "(source) holds a 64-character hex run", edit: footer("<pre>" + hex + "  x.tar.gz</pre>")},
		{name: "uppercase sha256", want: "(source) holds a 64-character hex run", edit: footer("<pre>" + strings.ToUpper(hex) + "</pre>")},
		{name: "sha256 split across tags", want: "(joined) holds a 64-character hex run", edit: footer("<code>" + hex[:32] + "</code><code>" + hex[32:] + "</code>")},
		{name: "sha1", want: "holds a 40-character hex run", edit: footer("<pre>da39a3ee5e6b4b0d3255bfef95601890afd80709</pre>")},
		{name: "ISO release date", want: `has a date ("2026-10-27")`, edit: footer("<p>Released 2026-10-27</p>")},
		{name: "zh release date", want: `has a date ("2026年10月27`, edit: footer("<p>2026年10月27日发布</p>")},
		{name: "month-name date", want: `has a date ("Oct 27, 2026")`, edit: footer("<p>Oct 27, 2026</p>")},
		{name: "day-first date", want: `has a date ("27 Oct 2026")`, edit: footer("<p>27 Oct 2026</p>")},
		{name: "month and year", want: `has a date ("October 2026")`, edit: footer("<p>October 2026</p>")},
		// Files, sizes and the allowlists themselves.
		{name: "allowed number context removed", want: `allowed number context "initial-scale=1.0"`, edit: replace("initial-scale=1.0", "initial-scale=1"), allow: []allowedNumber{{"initial-scale=1.0", "viewport meta tag, not a release"}}},
		{name: "second landing file", want: "docs/landing/releases.html is not a listed landing file", extra: map[string]string{"releases.html": "<p>history</p>"}},
		{name: "index.html missing", want: "docs/landing/index.html is missing", edit: func(string) string { return "" }},
		{name: "page one byte over its bound", want: "over the 106496-byte page bound", edit: padTo(landingPageMaxBytes + 1)},
		{name: "page of 106 KiB", want: "page bound", edit: padTo(106 << 10)},
		{name: "two files over the total bound", want: "bytes in total, over the 114688-byte bound", extra: map[string]string{"extra.html": strings.Repeat(" ", 30<<10)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mutated := map[string]string{"index.html": page}
			if tc.edit != nil {
				mutated["index.html"] = tc.edit(page)
			}
			if mutated["index.html"] == "" {
				delete(mutated, "index.html")
			}
			for name, body := range tc.extra {
				mutated[name] = body
			}
			al := allowed
			if tc.allow != nil {
				al = tc.allow
			}
			issues := landingIssues(mutated, al)
			for _, issue := range issues {
				if strings.Contains(issue, tc.want) {
					return
				}
			}
			t.Errorf("want a failure containing %q, got %q", tc.want, issues)
		})
	}
}

// Ordinary page content that must keep passing, so the rules stay narrow.
func TestLandingGuardAllowsProductContent(t *testing.T) {
	page := readLandingFiles(t)["index.html"]
	cases := []struct{ name, old, new string }{
		{"CSS decimals", "</style>", ".note { opacity: 0.45; line-height: 1.6; transform: scale(1.02); }\n    </style>"},
		{"inline style decimals", `<div class="hero-badge">`, `<div class="hero-badge" style="line-height:1.5">`},
		{"the sample incident date again", "</footer>", "<p>2026-03-15T10:05:00Z</p></footer>"},
		{"the sample spike ratio again", "</footer>", "<p>spike · 4.7× baseline</p></footer>"},
		{"a MySQL version in the copy", "</footer>", "<p>reads MySQL 8.0 and MariaDB 10.6 binlogs</p></footer>"},
		{"a size in GB", "</footer>", "<p>a 1.5 GB binlog</p></footer>"},
		{"an IPv4 address", "</footer>", "<p>host 10.0.0.1 port 3306</p></footer>"},
		{"a version-free release link", "</footer>", `<a href="https://github.com/Fanduzi/BinlogVisualizer/releases">Releases</a></footer>`},
		{"a page exactly at its bound", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := page + strings.Repeat(" ", landingPageMaxBytes-len(page))
			if tc.old != "" {
				if !strings.Contains(page, tc.old) {
					t.Fatalf("anchor %q not on the page", tc.old)
				}
				p = strings.Replace(page, tc.old, tc.new, 1)
			}
			if issues := landingIssues(map[string]string{"index.html": p}, nil); len(issues) > 0 {
				t.Fatalf("want no failure, got %q", issues)
			}
		})
	}
}

// A file nested under docs/landing is read and rejected, so a release card
// moved into a script or subdirectory still fails.
func TestLandingGuardReadsNestedFiles(t *testing.T) {
	dir := t.TempDir()
	page := readLandingFiles(t)["index.html"]
	if err := os.MkdirAll(filepath.Join(dir, "assets"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"index.html": page, "assets/releases.js": "const notes = [];"} {
		if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(name)), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	issues := landingIssues(readLandingDir(t, dir), nil)
	want := "file: docs/landing/assets/releases.js is not a listed landing file"
	if !slices.ContainsFunc(issues, func(s string) bool { return strings.HasPrefix(s, want) }) {
		t.Fatalf("want %q, got %q", want, issues)
	}
}

// The total bound is inclusive: an extra file that brings the total to the
// bound fails only as an unlisted file, one more byte also fails on size.
func TestLandingGuardTotalBoundary(t *testing.T) {
	page := readLandingFiles(t)["index.html"]
	for _, tc := range []struct {
		extra    int
		wantSize bool
	}{{landingMaxBytes - len(page), false}, {landingMaxBytes - len(page) + 1, true}} {
		issues := landingIssues(map[string]string{"index.html": page, "extra.txt": strings.Repeat(" ", tc.extra)}, nil)
		gotSize := false
		for _, issue := range issues {
			gotSize = gotSize || strings.Contains(issue, "bytes in total")
		}
		if gotSize != tc.wantSize {
			t.Errorf("total %d bytes: size failure %v, want %v: %q", len(page)+tc.extra, gotSize, tc.wantSize, issues)
		}
	}
}
