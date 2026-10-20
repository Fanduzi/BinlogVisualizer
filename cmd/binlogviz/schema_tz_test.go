// Package binlogviz checks the D5 contract: every versioned JSON output matches a published schema, and --tz changes only the display of times.
// input: the schema catalog, the commands that print those documents, and minimal.binlog.
// output: a failure when a schema file drifts, a new JSON output has no schema, a field is dropped or added, --tz is ignored, or an invalid zone exits as something other than usage.
// pos: the checks named by docs/production-readiness.md item D5. They run under go test ./...
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/jsonschema"
)

func TestJSONOutputMatchesSchema(t *testing.T) {
	runs := prepareJSONOutputRuns(t)
	byName := schemaCatalogByName(t)
	if len(runs) != len(byName) {
		t.Fatalf("JSON runners %d, schema catalog %d", len(runs), len(byName))
	}
	root := repoRoot(t)
	seen := map[string]bool{}
	for _, run := range runs {
		if _, ok := byName[run.name]; !ok {
			t.Errorf("runner %s is not in schemaOutputs()", run.name)
			continue
		}
		seen[run.name] = true
		code, stdout, stderr := runRoot(t, run.args...)
		if code != run.exit {
			t.Errorf("%s: exit %d, want %d: %s", run.name, code, run.exit, stderr)
			continue
		}
		schema := readPublishedSchema(t, root, run.name)
		var inst any
		if err := json.Unmarshal([]byte(stdout), &inst); err != nil {
			t.Errorf("%s: stdout is not JSON: %v\n%s", run.name, err, stdout)
			continue
		}
		if err := jsonschema.Validate(schema, inst); err != nil {
			t.Errorf("%s: output does not match docs/schema/%s.json: %v", run.name, run.name, err)
		}
	}
	for name := range byName {
		if !seen[name] {
			t.Errorf("schema %s has no command output to validate", name)
		}
		if _, err := os.Stat(filepath.Join(root, "docs", "schema", name+".json")); err != nil {
			t.Errorf("schema %s has no docs/schema file: %v", name, err)
		}
	}
}

func TestPublishedSchemaMatchesGenerator(t *testing.T) {
	t.Cleanup(func() { errorFormat = "" })
	root := repoRoot(t)
	dir := filepath.Join(root, "docs", "schema")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, out := range schemaOutputs() {
		names = append(names, out.Name)
		body, err := out.Schema()
		if err != nil {
			t.Fatalf("generate %s: %v", out.Name, err)
		}
		path := filepath.Join(dir, out.Name+".json")
		if os.Getenv("UPDATE_SCHEMAS") == "1" {
			if err := os.WriteFile(path, body, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v (UPDATE_SCHEMAS=1 writes it)", path, err)
		}
		if !bytes.Equal(got, body) {
			t.Errorf("docs/schema/%s.json differs from the generator. UPDATE_SCHEMAS=1 rewrites the file", out.Name)
		}
		code, stdout, stderr := runRoot(t, "schema", out.Name)
		if code != 0 {
			t.Fatalf("schema %s exit %d: %s", out.Name, code, stderr)
		}
		if stdout != string(body) {
			t.Errorf("binlogviz schema %s does not print the published document", out.Name)
		}
	}
	code, stdout, stderr := runRoot(t, "schema")
	if code != 0 {
		t.Fatalf("schema list exit %d: %s", code, stderr)
	}
	if strings.TrimRight(stdout, "\n") != strings.Join(names, "\n") {
		t.Fatalf("schema list:\n%s", stdout)
	}
	if strings.HasPrefix(strings.TrimSpace(stdout), "{") {
		t.Fatal("schema list is a JSON document; it is a name list")
	}
	code, stdout, stderr = runRoot(t, "schema", "not-a-schema")
	if code != 3 || stdout != "" || !strings.Contains(stderr, "unknown schema") {
		t.Fatalf("unknown schema: exit %d stdout %q stderr %q", code, stdout, stderr)
	}
}

func TestSchemaRejectsDroppedProperty(t *testing.T) {
	schema, inst := analyzeSchemaAndOutput(t)
	props, _ := schema["properties"].(map[string]any)
	if _, ok := props["summary"]; !ok {
		t.Fatal("analyze schema has no summary")
	}
	delete(props, "summary")
	if err := jsonschema.Validate(schema, inst); err == nil {
		t.Fatal("dropping summary from the schema must fail validation of the real output")
	}
}

func TestSchemaRejectsUnknownField(t *testing.T) {
	schema, inst := analyzeSchemaAndOutput(t)
	doc, _ := inst.(map[string]any)
	doc["not_a_real_field"] = true
	if err := jsonschema.Validate(schema, doc); err == nil {
		t.Fatal("an output field the schema does not name must fail")
	}
}

func analyzeSchemaAndOutput(t *testing.T) (map[string]any, any) {
	t.Helper()
	forceEnglishRuntimeOutput(t)
	binlog := filepath.Join("testdata", "minimal.binlog")
	code, stdout, stderr := runRoot(t, "analyze", binlog, "--format", "json", "-q")
	if code != 0 {
		t.Fatalf("analyze exit %d: %s", code, stderr)
	}
	var inst any
	if err := json.Unmarshal([]byte(stdout), &inst); err != nil {
		t.Fatal(err)
	}
	return readPublishedSchema(t, repoRoot(t), "analyze"), inst
}

func readPublishedSchema(t *testing.T, root, name string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "docs", "schema", name+".json"))
	if err != nil {
		t.Fatalf("read schema %s: %v", name, err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestDisplayTimezone(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Cleanup(func() {
		errorFormat = ""
		tzFlag = ""
	})
	binlog := filepath.Join("testdata", "minimal.binlog")
	code, baseJSON, stderr := runRoot(t, "analyze", binlog, "--format", "json", "-q")
	if code != 0 {
		t.Fatalf("analyze exit %d: %s", code, stderr)
	}
	baseTimes := collectRFC3339(t, baseJSON)
	if len(baseTimes) == 0 {
		t.Fatal("analyze JSON has no RFC3339 timestamp to compare")
	}

	zones := []string{"Asia/Kolkata", "America/New_York", "Etc/UTC"}
	for _, zone := range zones {
		code, out, stderr := runRoot(t, "analyze", binlog, "--format", "json", "-q", "--tz", zone)
		if code != 0 {
			t.Fatalf("%s analyze exit %d: %s", zone, code, stderr)
		}
		got := collectRFC3339(t, out)
		if len(got) != len(baseTimes) {
			t.Fatalf("%s: %d timestamps, want %d", zone, len(got), len(baseTimes))
		}
		loc, err := time.LoadLocation(zone)
		if err != nil {
			t.Fatal(err)
		}
		for i := range baseTimes {
			bt := parseRFC3339(t, baseTimes[i])
			zt := parseRFC3339(t, got[i])
			if !bt.Equal(zt) {
				t.Fatalf("%s: instant %s became %s", zone, baseTimes[i], got[i])
			}
			_, off := zt.In(loc).Zone()
			want := rfc3339Offset(off)
			if !strings.HasSuffix(got[i], want) {
				t.Fatalf("%s: %s want offset suffix %s", zone, got[i], want)
			}
			if strings.Contains(got[i], "EDT") || strings.Contains(got[i], "EST") || strings.Contains(got[i], "IST") {
				t.Fatalf("%s: timestamp uses an abbreviation: %s", zone, got[i])
			}
		}
		if zone == "Asia/Kolkata" && out == baseJSON {
			t.Fatal("--tz Asia/Kolkata left analyze JSON unchanged")
		}

		code, text, stderr := runRoot(t, "analyze", binlog, "--format", "text", "-q", "--tz", zone)
		if code != 0 {
			t.Fatalf("%s text exit %d: %s", zone, code, stderr)
		}
		code, md, stderr := runRoot(t, "analyze", binlog, "--format", "markdown", "-q", "--tz", zone)
		if code != 0 {
			t.Fatalf("%s markdown exit %d: %s", zone, code, stderr)
		}
		code, html, stderr := runRoot(t, "analyze", binlog, "--format", "html", "-q", "--tz", zone)
		if code != 0 {
			t.Fatalf("%s html exit %d: %s", zone, code, stderr)
		}
		_, sampleOff := parseRFC3339(t, baseTimes[0]).In(loc).Zone()
		human := humanOffset(sampleOff)
		for _, body := range []string{text, md, html} {
			if !strings.Contains(body, human) {
				t.Fatalf("%s output missing human offset %s", zone, human)
			}
		}
		if zone != "Etc/UTC" && !strings.Contains(text, zone) {
			t.Fatalf("text timestamps line should name %s", zone)
		}
	}

	code, text, stderr := runRoot(t, "analyze", binlog, "--format", "text", "-q")
	if code != 0 {
		t.Fatalf("default text exit %d: %s", code, stderr)
	}
	if !strings.Contains(text, " UTC") {
		t.Fatal("default text lost the UTC spelling")
	}
	code, html, stderr := runRoot(t, "analyze", binlog, "--format", "html", "-q")
	if code != 0 {
		t.Fatalf("default html exit %d: %s", code, stderr)
	}
	if !strings.Contains(html, " UTC") {
		t.Fatal("default html lost the UTC spelling")
	}

	dir := t.TempDir()
	code, _, stderr = runRoot(t, "analyze", binlog, "--format", "json", "-q", "--snapshot-name", "tzsnap", "--snapshot-dir", dir)
	if code != 0 {
		t.Fatalf("snapshot save exit %d: %s", code, stderr)
	}
	stored := readSnapshotFile(t, dir)
	if !strings.Contains(stored, "Z") {
		t.Fatal("snapshot file saved without --tz should keep Z timestamps")
	}
	code, listed, stderr := runRoot(t, "snapshot", "list", "--snapshot-dir", dir, "--format", "json", "--tz", "Asia/Kolkata")
	if code != 0 {
		t.Fatalf("snapshot list exit %d: %s", code, stderr)
	}
	if !strings.Contains(listed, "+05:30") {
		t.Fatalf("snapshot list ignored --tz:\n%s", listed)
	}
	if again := readSnapshotFile(t, dir); again != stored {
		t.Fatal("--tz rewrote the snapshot file")
	}
	code, shown, stderr := runRoot(t, "snapshot", "show", "tzsnap", "--snapshot-dir", dir, "--format", "json", "--tz", "Asia/Kolkata")
	if code != 0 || !strings.Contains(shown, "+05:30") {
		t.Fatalf("snapshot show exit %d stderr %s\n%s", code, stderr, shown)
	}

	plan := filepath.Join(t.TempDir(), "plan.yaml")
	if err := os.WriteFile(plan, []byte("version: 1\nworkflow:\n  name: tz\n  output_dir: out\ndefaults:\n  input:\n    from_dir: binlogs\n    prefix: mysql-bin.\nwindows:\n  - name: w\n    start: 2026-08-29T11:59:13Z\n    end: 2026-08-29T12:00:00Z\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, desc, stderr := runRoot(t, "workflow", "describe", plan, "--format", "json", "--tz", "Asia/Kolkata")
	if code != 0 {
		t.Fatalf("describe exit %d: %s", code, stderr)
	}
	if !strings.Contains(desc, "2026-08-29T17:29:13+05:30") || !strings.Contains(desc, "2026-08-29T17:30:00+05:30") {
		t.Fatalf("describe ignored --tz:\n%s", desc)
	}
	code, desc0, stderr := runRoot(t, "workflow", "describe", plan, "--format", "json")
	if code != 0 || !strings.Contains(desc0, "2026-08-29T11:59:13Z") {
		t.Fatalf("default describe: exit %d %s\n%s", code, stderr, desc0)
	}

	code, _, stderr = runRoot(t, "analyze", binlog, "--format", "json", "-q", "--snapshot-name", "tza", "--snapshot-dir", dir)
	if code != 0 {
		t.Fatalf("snapshot a exit %d: %s", code, stderr)
	}
	code, _, stderr = runRoot(t, "analyze", binlog, "--format", "json", "-q", "--snapshot-name", "tzb", "--snapshot-dir", dir)
	if code != 0 {
		t.Fatalf("snapshot b exit %d: %s", code, stderr)
	}
	code, compareHTML, stderr := runRoot(t, "compare", "--current-snapshot", "tzb", "--baseline-snapshot", "tza", "--snapshot-dir", dir, "--format", "html", "--tz", "Asia/Kolkata")
	// html/template writes '+' as &#43; in element text. The page still shows +05:30.
	if code != 0 || (!strings.Contains(compareHTML, "+05:30") && !strings.Contains(compareHTML, "&#43;05:30")) {
		t.Fatalf("compare html --tz: exit %d %s", code, stderr)
	}
	code, compareDefault, stderr := runRoot(t, "compare", "--current-snapshot", "tzb", "--baseline-snapshot", "tza", "--snapshot-dir", dir, "--format", "html")
	if code != 0 || !strings.Contains(compareDefault, " UTC") {
		t.Fatalf("compare html default: exit %d %s", code, stderr)
	}
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	_, nyOff := time.Now().In(ny).Zone()
	code, trendHTML, stderr := runRoot(t, "trend", "tza", "tzb", "--snapshot-dir", dir, "--format", "html", "--tz", "America/New_York")
	if code != 0 || !strings.Contains(trendHTML, humanOffset(nyOff)) {
		t.Fatalf("trend html --tz: exit %d want %s\n%s", code, humanOffset(nyOff), stderr)
	}

	code, stdout, stderr := runRoot(t, "--tz", "Not/AZone", "version")
	if code != 3 || stdout != "" || !strings.Contains(stderr, "invalid --tz") {
		t.Fatalf("invalid zone: exit %d stdout %q stderr %q", code, stdout, stderr)
	}
	code, stdout, stderr = runRoot(t, "--error-format", "json", "--tz", "Not/AZone", "version")
	if code != 3 || stdout != "" {
		t.Fatalf("invalid zone json: exit %d stdout %q stderr %q", code, stdout, stderr)
	}
	var errDoc struct {
		Error struct {
			Kind string `json:"kind"`
			Code int    `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(stderr)), &errDoc); err != nil {
		t.Fatalf("invalid zone stderr is not JSON: %v\n%s", err, stderr)
	}
	if errDoc.Error.Kind != "usage" || errDoc.Error.Code != 3 {
		t.Fatalf("invalid zone kind: %+v", errDoc.Error)
	}
	code, stdout, stderr = runRoot(t, "--tz=", "version")
	if code != 3 || stdout != "" || !strings.Contains(stderr, "invalid --tz") {
		t.Fatalf("empty --tz: exit %d stdout %q stderr %q", code, stdout, stderr)
	}
}

func TestDefaultOutputByteIdentical(t *testing.T) {
	if os.Getenv("BINLOGVIZ_BYTE_CHILD") == "1" {
		binlog := os.Getenv("BINLOGVIZ_BYTE_BINLOG")
		format := os.Getenv("BINLOGVIZ_BYTE_FORMAT")
		code, stdout, stderr := runRoot(t, "analyze", binlog, "--format", format, "-q")
		if code != 0 {
			fmt.Fprintf(os.Stderr, "child exit %d: %s", code, stderr)
			os.Exit(1)
		}
		os.Stdout.WriteString(stdout)
		os.Exit(0)
	}
	binlog, err := filepath.Abs(filepath.Join("testdata", "minimal.binlog"))
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"json", "text", "markdown"} {
		utc := execByteChild(t, "UTC", format, binlog)
		ny := execByteChild(t, "America/New_York", format, binlog)
		if utc != ny {
			t.Fatalf("analyze --format %s without --tz differs between TZ=UTC and TZ=America/New_York", format)
		}
		switch format {
		case "json":
			if !strings.Contains(utc, "Z") {
				t.Fatal("default JSON lost the Z spelling")
			}
		default:
			if !strings.Contains(utc, " UTC") {
				t.Fatalf("default %s lost the UTC spelling", format)
			}
		}
	}
}

func execByteChild(t *testing.T, tz, format, binlog string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDefaultOutputByteIdentical$")
	cmd.Env = append(os.Environ(),
		"BINLOGVIZ_BYTE_CHILD=1",
		"TZ="+tz,
		"BINLOGVIZ_BYTE_FORMAT="+format,
		"BINLOGVIZ_BYTE_BINLOG="+binlog,
	)
	out, err := cmd.Output()
	if err != nil {
		stderr := ""
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("child TZ=%s format=%s: %v\n%s", tz, format, err, stderr)
	}
	if len(out) == 0 {
		t.Fatalf("child TZ=%s format=%s wrote nothing", tz, format)
	}
	return string(out)
}

func TestStartEndTimezone(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FILTER_CHILD") == "1" {
		var args []string
		if err := json.Unmarshal([]byte(os.Getenv("BINLOGVIZ_FILTER_ARGS")), &args); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		code, stdout, stderr := runRoot(t, args...)
		fmt.Fprint(os.Stderr, stderr)
		if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"code": code, "stdout": stdout}); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
	forceEnglishRuntimeOutput(t)
	binlog, err := filepath.Abs(filepath.Join("testdata", "minimal.binlog"))
	if err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := runRoot(t, "analyze", binlog, "--format", "json", "-q")
	if code != 0 {
		t.Fatalf("analyze exit %d: %s", code, stderr)
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatal(err)
	}
	summary, _ := doc["summary"].(map[string]any)
	start, _ := summary["start_time"].(string)
	instant := parseRFC3339(t, start)
	windowStart := instant.UTC().Truncate(time.Second)
	spaceStart := windowStart.Format("2006-01-02 15:04:05")
	spaceEnd := windowStart.Add(time.Second).Format("2006-01-02 15:04:05")
	baseArgs := []string{"analyze", binlog, "--format", "json", "-q", "--start", spaceStart, "--end", spaceEnd}

	sh, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	shStart := windowStart.In(sh).Format("2006-01-02 15:04:05")
	shEnd := windowStart.Add(time.Second).In(sh).Format("2006-01-02 15:04:05")
	utcSuffixStart := windowStart.Format("2006-01-02 15:04:05") + " UTC"
	utcSuffixEnd := windowStart.Add(time.Second).Format("2006-01-02 15:04:05") + " UTC"
	offStart := windowStart.In(sh).Format("2006-01-02 15:04:05 -07:00")
	offEnd := windowStart.Add(time.Second).In(sh).Format("2006-01-02 15:04:05 -07:00")

	utc := filterChild(t, "UTC", baseArgs)
	machineShanghai := filterChild(t, "Asia/Shanghai", baseArgs)
	displayReadsUTCWall := filterChild(t, "UTC", append([]string{"--tz", "Asia/Shanghai"}, baseArgs...))
	displayReadsShanghaiWall := filterChild(t, "America/New_York", []string{"analyze", binlog, "--format", "json", "-q", "--tz", "Asia/Shanghai", "--start", shStart, "--end", shEnd})
	pastedUTC := filterChild(t, "America/New_York", []string{"analyze", binlog, "--format", "json", "-q", "--tz", "Asia/Shanghai", "--start", utcSuffixStart, "--end", utcSuffixEnd})
	pastedOffset := filterChild(t, "America/New_York", []string{"analyze", binlog, "--format", "json", "-q", "--start", offStart, "--end", offEnd})
	if utc.Code != 0 {
		t.Fatalf("TZ=UTC window %s..%s exit %d\n%s", spaceStart, spaceEnd, utc.Code, utc.Stdout)
	}
	if machineShanghai.Code != 2 {
		t.Fatalf("without --tz, TZ=Asia/Shanghai should treat %q as local, exit %d\n%s", spaceStart, machineShanghai.Code, machineShanghai.Stdout)
	}
	if displayReadsUTCWall.Code != 2 {
		t.Fatalf("--tz Asia/Shanghai should read %q in Shanghai, not as the UTC machine zone, exit %d\n%s", spaceStart, displayReadsUTCWall.Code, displayReadsUTCWall.Stdout)
	}
	if displayReadsShanghaiWall.Code != 0 || !strings.Contains(displayReadsShanghaiWall.Stdout, "+08:00") {
		t.Fatalf("zoneless Shanghai wall with --tz on a New York machine: exit %d\n%s", displayReadsShanghaiWall.Code, displayReadsShanghaiWall.Stdout)
	}
	if pastedUTC.Code != 0 || txnCount(t, pastedUTC.Stdout) != txnCount(t, utc.Stdout) {
		t.Fatalf("trailing UTC was not that instant: exit %d\n%s", pastedUTC.Code, pastedUTC.Stdout)
	}
	if pastedOffset.Code != 0 || txnCount(t, pastedOffset.Stdout) != txnCount(t, utc.Stdout) {
		t.Fatalf("space offset was not that instant: exit %d\n%s", pastedOffset.Code, pastedOffset.Stdout)
	}
}

type filterResult struct {
	Code   int    `json:"code"`
	Stdout string `json:"stdout"`
}

func filterChild(t *testing.T, tz string, args []string) filterResult {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestStartEndTimezone$")
	cmd.Env = append(os.Environ(), "BINLOGVIZ_FILTER_CHILD=1", "TZ="+tz, "BINLOGVIZ_FILTER_ARGS="+string(raw))
	out, err := cmd.Output()
	if err != nil {
		stderr := ""
		if ee, ok := err.(*exec.ExitError); ok {
			stderr = string(ee.Stderr)
		}
		t.Fatalf("filter child TZ=%s: %v\n%s", tz, err, stderr)
	}
	var got filterResult
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("filter child output: %v\n%s", err, out)
	}
	return got
}

func txnCount(t *testing.T, stdout string) int {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatal(err)
	}
	summary, _ := doc["summary"].(map[string]any)
	n, _ := summary["total_transactions"].(float64)
	return int(n)
}

func readSnapshotFile(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "tzsnap.json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func collectRFC3339(t *testing.T, raw string) []string {
	t.Helper()
	var doc any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatal(err)
	}
	var out []string
	var walk func(any)
	walk = func(v any) {
		switch v := v.(type) {
		case map[string]any:
			keys := make([]string, 0, len(v))
			for k := range v {
				keys = append(keys, k)
			}
			sortStrings(keys)
			for _, k := range keys {
				walk(v[k])
			}
		case []any:
			for _, item := range v {
				walk(item)
			}
		case string:
			if _, err := time.Parse(time.RFC3339, v); err == nil {
				out = append(out, v)
				return
			}
			if _, err := time.Parse(time.RFC3339Nano, v); err == nil {
				out = append(out, v)
			}
		}
	}
	walk(doc)
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func parseRFC3339(t *testing.T, value string) time.Time {
	t.Helper()
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("not RFC3339: %s", value)
	}
	return parsed
}

func rfc3339Offset(off int) string {
	if off == 0 {
		return "Z"
	}
	sign := "+"
	if off < 0 {
		sign = "-"
		off = -off
	}
	return fmt.Sprintf("%s%02d:%02d", sign, off/3600, (off%3600)/60)
}

func humanOffset(off int) string {
	if off == 0 {
		return "+00:00"
	}
	sign := "+"
	if off < 0 {
		sign = "-"
		off = -off
	}
	return fmt.Sprintf("%s%02d:%02d", sign, off/3600, (off%3600)/60)
}
