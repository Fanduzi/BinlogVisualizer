package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/version"
)

func TestDisplayTimezoneRoundTrip(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Cleanup(func() {
		tzFlag = ""
		errorFormat = ""
	})
	fixtures := []string{
		filepath.Join("testdata", "minimal.binlog"),
		mustFixturePath(t, "mysql-8.0.46-busiest-minute.binlog"),
		mustFixturePath(t, "mysql-8.0.46-replica-apply.binlog"),
	}
	zones := []string{"Asia/Kolkata", "America/New_York", "Etc/UTC"}
	for _, binlog := range fixtures {
		for _, format := range []string{"json", "text", "markdown", "html"} {
			code, base, stderr := runRoot(t, "analyze", binlog, "--format", format, "-q")
			if code != 0 {
				t.Fatalf("%s %s exit %d: %s", binlog, format, code, stderr)
			}
			for _, zone := range zones {
				code, got, stderr := runRoot(t, "analyze", binlog, "--format", format, "-q", "--tz", zone)
				if code != 0 {
					t.Fatalf("%s %s %s exit %d: %s", zone, binlog, format, code, stderr)
				}
				assertTimesRoundTrip(t, format, zone, base, got)
			}
		}
	}
}

func TestSnapshotFileStaysUTC(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Cleanup(func() { tzFlag = "" })
	binlog := filepath.Join("testdata", "minimal.binlog")
	dir := t.TempDir()
	code, stdout, stderr := runRoot(t, "analyze", binlog, "--format", "json", "-q", "--tz", "Pacific/Kiritimati", "--snapshot-name", "s0", "--snapshot-dir", dir)
	if code != 0 {
		t.Fatalf("save exit %d: %s", code, stderr)
	}
	if !strings.Contains(stdout, "+14:00") {
		t.Fatal("stdout did not render +14:00")
	}
	file := readNamedSnapshot(t, dir, "s0")
	if strings.Contains(file, "+14:00") {
		t.Fatal("snapshot file stored the --tz offset")
	}
	if !strings.Contains(file, "Z") {
		t.Fatal("snapshot file lost UTC timestamps")
	}
	if !sameInstants(t, collectRFC3339(t, file), collectRFC3339(t, stdout)) {
		t.Fatal("stdout instants differ from the UTC snapshot file")
	}
	code, listed, stderr := runRoot(t, "snapshot", "list", "--snapshot-dir", dir, "--format", "json")
	if code != 0 {
		t.Fatalf("list exit %d: %s", code, stderr)
	}
	if strings.Contains(listed, "+14:00") {
		t.Fatalf("default snapshot list shows the display zone:\n%s", listed)
	}
	code, shown, stderr := runRoot(t, "snapshot", "show", "s0", "--snapshot-dir", dir, "--format", "text", "--tz", "Pacific/Kiritimati")
	if code != 0 || !strings.Contains(shown, "+14:00") {
		t.Fatalf("show --tz exit %d stderr %s\n%s", code, stderr, shown)
	}
	if again := readNamedSnapshot(t, dir, "s0"); again != file {
		t.Fatal("listing with --tz rewrote the snapshot file")
	}
}

func TestRejectTimezoneLocal(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Cleanup(func() {
		tzFlag = ""
		errorFormat = ""
	})
	for _, value := range []string{"Local", "local"} {
		code, stdout, stderr := runRoot(t, "--tz", value, "version")
		if code != 3 || stdout != "" || !strings.Contains(stderr, "invalid --tz") {
			t.Fatalf("--tz %s: exit %d stdout %q stderr %q", value, code, stdout, stderr)
		}
	}
	code, stdout, stderr := runRoot(t, "--error-format", "json", "--tz", "Local", "version")
	if code != 3 || stdout != "" {
		t.Fatalf("json Local: exit %d stdout %q stderr %q", code, stdout, stderr)
	}
	var doc struct {
		Error struct {
			Kind string `json:"kind"`
			Code int    `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stderr)), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Error.Kind != "usage" || doc.Error.Code != 3 {
		t.Fatalf("Local kind: %+v", doc.Error)
	}
}

func TestVersionCommitTimeFollowsTimezone(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Cleanup(func() { tzFlag = "" })
	// The test binary has no vcs commit time. Set the linker fallback so the
	// command has an instant to render. 2026-03-08T06:30:00Z is 01:30 EST,
	// the morning before New York springs forward.
	prevCommit, prevTime := version.Commit, version.CommitTime
	version.Commit = "0123456789abcdef0123456789abcdef01234567"
	version.CommitTime = "2026-03-08T06:30:00Z"
	t.Cleanup(func() {
		version.Commit = prevCommit
		version.CommitTime = prevTime
	})
	code, base, stderr := runRoot(t, "version", "--format", "json")
	if code != 0 {
		t.Fatalf("version exit %d: %s", code, stderr)
	}
	if jsonStringField(t, base, "commit_time") != "2026-03-08T06:30:00Z" {
		t.Fatalf("default commit_time changed: %s", base)
	}
	code, shifted, stderr := runRoot(t, "--tz", "America/New_York", "version", "--format", "json")
	if code != 0 {
		t.Fatalf("version --tz exit %d: %s", code, stderr)
	}
	got := jsonStringField(t, shifted, "commit_time")
	if got != "2026-03-08T01:30:00-05:00" {
		t.Fatalf("commit_time = %s, want 2026-03-08T01:30:00-05:00", got)
	}
	if !parseRFC3339(t, got).Equal(parseRFC3339(t, "2026-03-08T06:30:00Z")) {
		t.Fatalf("commit_time instant changed: %s", got)
	}
	code, text, stderr := runRoot(t, "--tz", "America/New_York", "version")
	if code != 0 || !strings.Contains(text, "2026-03-08T01:30:00-05:00") {
		t.Fatalf("version text exit %d stderr %s\n%s", code, stderr, text)
	}
}

func TestCompareTrendHTMLWindowsFollowTimezone(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Cleanup(func() { tzFlag = "" })
	binlog := filepath.Join("testdata", "minimal.binlog")
	dir := t.TempDir()
	for _, name := range []string{"wa", "wb"} {
		code, _, stderr := runRoot(t, "analyze", binlog, "--format", "json", "-q", "--snapshot-name", name, "--snapshot-dir", dir)
		if code != 0 {
			t.Fatalf("save %s exit %d: %s", name, code, stderr)
		}
	}
	code, html, stderr := runRoot(t, "compare", "--current-snapshot", "wb", "--baseline-snapshot", "wa", "--snapshot-dir", dir, "--format", "html", "--tz", "Asia/Kolkata")
	if code != 0 {
		t.Fatalf("compare html exit %d: %s", code, stderr)
	}
	plain := strings.ReplaceAll(html, "&#43;", "+")
	for _, label := range []string{"Current Window", "Baseline Window"} {
		line := lineContaining(plain, label)
		if line == "" || strings.Contains(line, "Z") || !strings.Contains(line, "+05:30") {
			t.Fatalf("%s was not converted:\n%s", label, line)
		}
	}
	code, trendHTML, stderr := runRoot(t, "trend", "wa", "wb", "--snapshot-dir", dir, "--format", "html", "--tz", "Asia/Kolkata")
	if code != 0 {
		t.Fatalf("trend html exit %d: %s", code, stderr)
	}
	trendPlain := strings.ReplaceAll(trendHTML, "&#43;", "+")
	if strings.Contains(trendPlain, "T") && strings.Contains(trendPlain, "Z") && !strings.Contains(trendPlain, "+05:30") {
		t.Fatal("trend html kept Z window times and did not render +05:30")
	}
	// The ordered-points start cell is the snapshot window, not the generated-at line.
	if !regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+05:30`).MatchString(trendPlain) {
		t.Fatalf("trend html start column stayed UTC:\n%s", trendPlain)
	}
}

func TestWorkflowRunTimezoneOnArtifactsNotSnapshots(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Cleanup(func() { tzFlag = "" })
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")
	code, _, stderr := runRoot(t, "workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir, "--tz", "Asia/Kolkata")
	if code != 0 {
		t.Fatalf("workflow run exit %d: %s", code, stderr)
	}
	artifact, err := os.ReadFile(filepath.Join(outputDir, "analyze", "week1.json"))
	if err != nil {
		t.Fatal(err)
	}
	snap, err := os.ReadFile(filepath.Join(snapshotDir, "week1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(artifact), "+05:30") {
		t.Fatal("workflow analyze artifact ignored --tz")
	}
	if strings.Contains(string(snap), "+05:30") || !strings.Contains(string(snap), "Z") {
		t.Fatal("workflow snapshot file stored the display zone")
	}
	html, err := os.ReadFile(filepath.Join(outputDir, "compare", "week2_vs_week1.html"))
	if err != nil {
		t.Fatal(err)
	}
	plain := strings.ReplaceAll(string(html), "&#43;", "+")
	if !strings.Contains(plain, "2025-01-01T05:30:00+05:30") {
		t.Fatal("workflow compare HTML left the window in UTC")
	}
	compareJSON, err := os.ReadFile(filepath.Join(outputDir, "compare", "week2_vs_week1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(compareJSON), "2025-01-01T00:00:00Z") {
		t.Fatal("workflow compare JSON was rewritten out of UTC")
	}
}

func assertTimesRoundTrip(t *testing.T, format, zone, base, got string) {
	t.Helper()
	basePlain := unescapeOffset(base)
	gotPlain := unescapeOffset(got)
	if format != "json" && strings.Contains(stripGeneratedLines(gotPlain), " UTC") {
		t.Fatalf("%s %s still has a historical UTC timestamp", zone, format)
	}
	if format != "json" {
		assertGeneratedAtOffset(t, zone, gotPlain)
	}
	baseDated, baseClocks := splitRenderedTimes(stripGeneratedLines(basePlain))
	gotDated, gotClocks := splitRenderedTimes(stripGeneratedLines(gotPlain))
	if !sameInstants(t, baseDated, gotDated) {
		t.Fatalf("%s %s dated times do not round-trip to the default instants\nbase %v\ngot %v", zone, format, baseDated, gotDated)
	}
	if strings.Join(clocksToUTC(t, baseClocks), ",") != strings.Join(clocksToUTC(t, gotClocks), ",") {
		t.Fatalf("%s %s chart clocks do not round-trip\nbase %v\ngot %v", zone, format, baseClocks, gotClocks)
	}
	if zone == "Asia/Kolkata" {
		for _, token := range append(append([]string{}, gotDated...), gotClocks...) {
			if strings.HasSuffix(token, "Z") || strings.HasSuffix(token, " UTC") {
				t.Fatalf("%s left a timestamp in UTC spelling: %s", format, token)
			}
			if !strings.Contains(token, "+05:30") {
				t.Fatalf("%s timestamp %s is not +05:30", format, token)
			}
		}
	}
}

func assertGeneratedAtOffset(t *testing.T, zone, body string) {
	t.Helper()
	lines := strings.Split(body, "\n")
	var saw bool
	for i, line := range lines {
		if !strings.Contains(line, "Generated") {
			continue
		}
		window := line
		if i+1 < len(lines) {
			window += " " + lines[i+1]
		}
		if !regexp.MustCompile(`\d{4}-\d{2}-\d{2}`).MatchString(window) {
			continue
		}
		saw = true
		if !regexp.MustCompile(`[+-]\d{2}:\d{2}`).MatchString(window) {
			t.Fatalf("%s generated-at has no numeric offset: %s", zone, window)
		}
	}
	if !saw && strings.Contains(body, "<strong>Generated</strong>") {
		t.Fatalf("%s HTML generated-at label has no timestamp next to it", zone)
	}
}

func unescapeOffset(s string) string {
	s = strings.ReplaceAll(s, "&#43;", "+")
	// html/template writes '+' as \u002b inside script strings.
	s = strings.ReplaceAll(s, `\u002b`, "+")
	s = strings.ReplaceAll(s, `\u002B`, "+")
	return s
}

func stripGeneratedLines(s string) string {
	lines := strings.Split(s, "\n")
	var b strings.Builder
	for i := 0; i < len(lines); i++ {
		if strings.Contains(lines[i], "Generated") {
			// The HTML label is on its own line and the clock is the next line.
			// That clock is time.Now(), so two runs are not the same instant.
			if i+1 < len(lines) && isOnlyRenderedTime(lines[i+1]) {
				i++
			}
			continue
		}
		b.WriteString(lines[i])
		b.WriteByte('\n')
	}
	return b.String()
}

func isOnlyRenderedTime(line string) bool {
	trimmed := strings.TrimSpace(line)
	return trimmed != "" && datedTimeRE.FindString(trimmed) == trimmed
}

var (
	// Wall times put a space before the numeric offset ("15:04:05 -07:00").
	// JSON uses RFC3339 with no space ("15:04:05-07:00" or "Z").
	datedTimeRE  = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z| ?[+-]\d{2}:\d{2}| UTC)`)
	minuteTimeRE = regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}(?: ?[+-]\d{2}:\d{2}| UTC)`)
	clockTimeRE  = regexp.MustCompile(`(?:^|[^0-9])(\d{2}:\d{2}(?: ?[+-]\d{2}:\d{2}| UTC))`)
)

func splitRenderedTimes(s string) (dated, clocks []string) {
	dated = datedTimeRE.FindAllString(s, -1)
	rest := datedTimeRE.ReplaceAllString(s, " ")
	dated = append(dated, minuteTimeRE.FindAllString(rest, -1)...)
	rest = minuteTimeRE.ReplaceAllString(rest, " ")
	for _, m := range clockTimeRE.FindAllStringSubmatch(rest, -1) {
		clocks = append(clocks, m[1])
	}
	return dated, clocks
}

func sameInstants(t *testing.T, base, got []string) bool {
	t.Helper()
	if len(base) == 0 {
		t.Fatal("no rendered timestamps to compare")
	}
	if len(base) != len(got) {
		return false
	}
	bs := instantKeys(t, base)
	gs := instantKeys(t, got)
	for i := range bs {
		if bs[i] != gs[i] {
			return false
		}
	}
	return true
}

func instantKeys(t *testing.T, tokens []string) []int64 {
	t.Helper()
	out := make([]int64, len(tokens))
	for i, token := range tokens {
		out[i] = parseRendered(t, token).UnixNano()
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func clocksToUTC(t *testing.T, tokens []string) []string {
	t.Helper()
	out := make([]string, len(tokens))
	for i, token := range tokens {
		parsed := parseRendered(t, "2026-01-02 "+token)
		out[i] = parsed.UTC().Format("15:04")
	}
	sort.Strings(out)
	return out
}

func parseRendered(t *testing.T, value string) time.Time {
	t.Helper()
	value = strings.TrimSpace(value)
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed
		}
	}
	if bare, ok := strings.CutSuffix(value, " UTC"); ok {
		for _, layout := range []string{
			"2006-01-02 15:04:05.000000",
			"2006-01-02 15:04:05",
			"2006-01-02 15:04",
			"2006-01-02 15:04:05.000000 -07:00",
		} {
			if parsed, err := time.ParseInLocation(layout, bare, time.UTC); err == nil {
				return parsed
			}
		}
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.000000 -07:00",
		"2006-01-02 15:04:05 -07:00",
		"2006-01-02 15:04 -07:00",
		"2006-01-02 15:04:05.000000",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
	} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed
		}
	}
	t.Fatalf("cannot parse rendered time %q", value)
	return time.Time{}
}

func jsonStringField(t *testing.T, raw, key string) string {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatal(err)
	}
	s, _ := doc[key].(string)
	return s
}

func readNamedSnapshot(t *testing.T, dir, name string) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(dir, name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func lineContaining(body, needle string) string {
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, needle) {
			return line
		}
	}
	return ""
}
