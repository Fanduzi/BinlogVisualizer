package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ADR 0011 §3: --diagnostics trims stdout only. A snapshot is an archive and
// keeps full diagnostics, so comparing it with a full snapshot of the same
// binlog shows no DDL, hot-interval, or transaction-diagnostics deltas.
func TestReducedDiagnosticsSnapshotKeepsFullArchive(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-dml-full.binlog")
	dir := t.TempDir()

	for _, name := range []string{"base", "base-again"} {
		if code, _, stderr := runRoot(t, "analyze", path, "--format", "json", "-q", "--snapshot-name", name, "--snapshot-dir", dir); code != 0 {
			t.Fatalf("save %s: exit %d\n%s", name, code, stderr)
		}
	}
	want := compareDiagnosticsDelta(t, dir, "base-again", "base")
	ddl := want["ddl_changes"].(map[string]any)
	if ddl["baseline_count"].(float64) == 0 {
		t.Fatal("fixture must have DDL events for this test to mean anything")
	}

	for _, mode := range []string{"summary", "off"} {
		t.Run(mode, func(t *testing.T) {
			name := "reduced-" + mode
			code, stdout, stderr := runRoot(t, "analyze", path, "--format", "json", "-q", "--diagnostics", mode, "--snapshot-name", name, "--snapshot-dir", dir)
			if code != 0 {
				t.Fatalf("exit %d\n%s", code, stderr)
			}
			// stdout is the trimmed document and says so.
			doc := decodeObject(t, stdout)
			if doc["diagnostics_mode"] != mode {
				t.Fatalf("stdout diagnostics_mode = %#v, want %q", doc["diagnostics_mode"], mode)
			}
			// The saved snapshot is the full document.
			saved, err := os.ReadFile(filepath.Join(dir, name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			snap := decodeObject(t, string(saved))
			if _, ok := snap["diagnostics_mode"]; ok {
				t.Fatalf("snapshot must not be marked reduced: %#v", snap["diagnostics_mode"])
			}
			diag, ok := snap["diagnostics"].(map[string]any)
			if !ok {
				t.Fatal("snapshot must keep diagnostics")
			}
			if hot, _ := diag["hot_intervals"].([]any); len(hot) == 0 {
				t.Fatal("snapshot must keep hot_intervals")
			}

			got := compareDiagnosticsDelta(t, dir, name, "base")
			gotDDL := got["ddl_changes"].(map[string]any)
			if gotDDL["delta"].(float64) != 0 || len(asList(gotDDL["added"])) != 0 || len(asList(gotDDL["removed"])) != 0 {
				t.Fatalf("spurious ddl delta: %#v", gotDDL)
			}
			hot := got["hot_interval_delta"].(map[string]any)
			if hot["delta_top_rows"].(float64) != 0 || hot["baseline_count"] != hot["current_count"] {
				t.Fatalf("spurious hot-interval delta: %#v", hot)
			}
			if !sameJSON(got, want) {
				gb, _ := json.Marshal(got)
				wb, _ := json.Marshal(want)
				t.Fatalf("diagnostics_delta differs from full-vs-full:\n got %s\nwant %s", gb, wb)
			}
		})
	}
}

// ADR 0011 §3: a document analyze marked as reduced is refused with exit 9
// and reason reduced_diagnostics, by compare (files and snapshots), trend,
// and snapshot save. It is never compared as if the lists were empty.
func TestReducedDiagnosticsReportIsRefused(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-dml-full.binlog")
	dir := t.TempDir()

	code, full, stderr := runRoot(t, "analyze", path, "--format", "json", "-q", "--snapshot-name", "base", "--snapshot-dir", dir)
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	fullPath := filepath.Join(dir, "full-report.json")
	writeTestFile(t, fullPath, full)

	for _, mode := range []string{"summary", "off"} {
		t.Run(mode, func(t *testing.T) {
			code, trimmed, stderr := runRoot(t, "analyze", path, "--format", "json", "-q", "--diagnostics", mode)
			if code != 0 {
				t.Fatalf("exit %d\n%s", code, stderr)
			}
			trimmedPath := filepath.Join(dir, "trimmed-"+mode+".json")
			writeTestFile(t, trimmedPath, trimmed)
			// Hand-copied into the snapshot dir, as an agent might do.
			snapName := "hand-" + mode
			writeTestFile(t, filepath.Join(dir, snapName+".json"), trimmed)

			cases := map[string][]string{
				"compare current file":  {"compare", trimmedPath, fullPath, "--format", "json"},
				"compare baseline file": {"compare", fullPath, trimmedPath, "--format", "json"},
				"compare snapshot":      {"compare", "--current-snapshot", snapName, "--baseline-snapshot", "base", "--snapshot-dir", dir, "--format", "json"},
				"trend snapshots":       {"trend", "base", snapName, "--snapshot-dir", dir, "--format", "json"},
				"snapshot save":         {"snapshot", "save", trimmedPath, "--name", "imported-" + mode, "--snapshot-dir", dir},
			}
			for label, args := range cases {
				code, stdout, stderr := runRoot(t, append([]string{"--error-format", "json"}, args...)...)
				if code != 9 {
					t.Fatalf("%s: exit %d, want 9\nstdout=%s\nstderr=%s", label, code, stdout, stderr)
				}
				line := strings.TrimSpace(stderr[strings.LastIndex(strings.TrimSpace(stderr), "\n")+1:])
				var body struct {
					Error struct {
						Kind    string         `json:"kind"`
						Message string         `json:"message"`
						Details map[string]any `json:"details"`
					} `json:"error"`
				}
				if err := json.Unmarshal([]byte(line), &body); err != nil {
					t.Fatalf("%s: stderr is not one JSON error: %v\n%s", label, err, stderr)
				}
				if body.Error.Kind != "unsupported" || body.Error.Details["reason"] != "reduced_diagnostics" || body.Error.Details["diagnostics_mode"] != mode {
					t.Fatalf("%s: error = %+v", label, body.Error)
				}
			}
		})
	}
}

func compareDiagnosticsDelta(t *testing.T, dir, current, baseline string) map[string]any {
	t.Helper()
	code, stdout, stderr := runRoot(t, "compare", "--current-snapshot", current, "--baseline-snapshot", baseline, "--snapshot-dir", dir, "--format", "json")
	if code != 0 {
		t.Fatalf("compare %s %s: exit %d\n%s", current, baseline, code, stderr)
	}
	return objectField(t, decodeObject(t, stdout), "diagnostics_delta")
}

func asList(v any) []any {
	list, _ := v.([]any)
	return list
}

// ADR 0011 §3 (#308): null is absent; a non-string is corrupt (exit 5), not reduced (exit 9).
func TestDiagnosticsModeMarkerEdges(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-dml-full.binlog")
	dir := t.TempDir()
	code, full, stderr := runRoot(t, "analyze", path, "--format", "json", "-q")
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, stderr)
	}
	fullPath := filepath.Join(dir, "full.json")
	writeTestFile(t, fullPath, full)

	nullPath := filepath.Join(dir, "null-mode.json")
	writeTestFile(t, nullPath, withDiagnosticsMode(t, full, nil))
	code, stdout, stderr := runRoot(t, "compare", nullPath, fullPath, "--format", "json")
	if code != 0 {
		t.Fatalf("null marker compare: exit %d\n%s\n%s", code, stdout, stderr)
	}
	if strings.Contains(stderr, "reduced_diagnostics") {
		t.Fatalf("null marker was treated as reduced: %s", stderr)
	}
	code, stdout, stderr = runRoot(t, "snapshot", "save", nullPath, "--name", "from-null", "--snapshot-dir", dir)
	if code != 0 {
		t.Fatalf("null marker snapshot save: exit %d\n%s", code, stderr)
	}
	if _, err := os.Stat(filepath.Join(dir, "from-null.json")); err != nil {
		t.Fatal(err)
	}

	badPath := filepath.Join(dir, "number-mode.json")
	writeTestFile(t, badPath, withDiagnosticsMode(t, full, 1))
	for _, args := range [][]string{
		{"compare", badPath, fullPath, "--format", "json"},
		{"snapshot", "save", badPath, "--name", "from-number", "--snapshot-dir", dir},
	} {
		code, stdout, stderr = runRoot(t, append([]string{"--error-format", "json"}, args...)...)
		if code != 5 {
			t.Fatalf("%v: exit %d, want 5\nstdout=%s\nstderr=%s", args, code, stdout, stderr)
		}
		if stdout != "" {
			t.Fatalf("%v: stdout = %q", args, stdout)
		}
		if strings.Contains(stderr, "reduced_diagnostics") || strings.Contains(stderr, `"kind":"unsupported"`) {
			t.Fatalf("%v: non-string marker must be corrupt, not reduced: %s", args, stderr)
		}
		if !strings.Contains(stderr, `"kind":"corrupt"`) {
			t.Fatalf("%v: stderr = %s", args, stderr)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "from-number.json")); !os.IsNotExist(err) {
		t.Fatal("corrupt snapshot save must not write the file")
	}
}

// #308: snapshot list marks a reduced document instead of waiting for show/compare.
func TestSnapshotListFlagsReducedDocument(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-dml-full.binlog")
	dir := t.TempDir()
	if code, _, stderr := runRoot(t, "analyze", path, "--format", "json", "-q", "--snapshot-name", "base", "--snapshot-dir", dir); code != 0 {
		t.Fatalf("save base: exit %d\n%s", code, stderr)
	}
	for _, mode := range []string{"summary", "off"} {
		code, trimmed, stderr := runRoot(t, "analyze", path, "--format", "json", "-q", "--diagnostics", mode, "--snapshot-name", "kept-"+mode, "--snapshot-dir", dir)
		if code != 0 {
			t.Fatalf("%s: exit %d\n%s", mode, code, stderr)
		}
		writeTestFile(t, filepath.Join(dir, "copied-"+mode+".json"), trimmed)
	}
	baseRaw, err := os.ReadFile(filepath.Join(dir, "base.json"))
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(dir, "copied-empty.json"), withDiagnosticsMode(t, string(baseRaw), ""))

	code, text, stderr := runRoot(t, "snapshot", "list", "--snapshot-dir", dir)
	if code != 0 {
		t.Fatalf("list: exit %d\n%s", code, stderr)
	}
	if !strings.Contains(text, "DIAGNOSTICS") {
		t.Fatalf("list text missing DIAGNOSTICS column:\n%s", text)
	}
	for _, mode := range []string{"summary", "off"} {
		line := snapshotListLine(t, text, "copied-"+mode)
		if !strings.HasSuffix(strings.TrimSpace(line), mode) {
			t.Fatalf("list line for copied-%s = %q", mode, line)
		}
		kept := snapshotListLine(t, text, "kept-"+mode)
		if strings.HasSuffix(strings.TrimSpace(kept), mode) {
			t.Fatalf("archive snapshot kept-%s must not be marked reduced: %q", mode, kept)
		}
	}
	emptyLine := snapshotListLine(t, text, "copied-empty")
	emptyFields := strings.Fields(emptyLine)
	if len(emptyFields) < 2 || emptyFields[1] == "-" || emptyFields[len(emptyFields)-1] != "(empty)" {
		t.Fatalf("empty marker line = %q", emptyLine)
	}

	code, raw, stderr := runRoot(t, "snapshot", "list", "--snapshot-dir", dir, "--format", "json")
	if code != 0 {
		t.Fatalf("list json: exit %d\n%s", code, stderr)
	}
	doc := decodeObject(t, raw)
	if doc["schema_version"] != float64(1) {
		t.Fatalf("schema_version = %#v, want 1", doc["schema_version"])
	}
	modes := map[string]string{}
	present := map[string]bool{}
	for _, item := range doc["snapshots"].([]any) {
		entry := item.(map[string]any)
		name := entry["name"].(string)
		mode, ok := entry["diagnostics_mode"]
		present[name] = ok
		if ok {
			modes[name], _ = mode.(string)
		}
	}
	if modes["copied-summary"] != "summary" || modes["copied-off"] != "off" {
		t.Fatalf("json modes = %#v", modes)
	}
	if present["base"] || present["kept-summary"] || present["kept-off"] {
		t.Fatalf("full snapshot marked reduced: present=%#v modes=%#v", present, modes)
	}
	if !present["copied-empty"] || modes["copied-empty"] != "" {
		t.Fatalf("empty marker JSON = present %v mode %q", present["copied-empty"], modes["copied-empty"])
	}

	code, stdout, stderr := runRoot(t, "--error-format", "json", "snapshot", "show", "copied-empty", "--snapshot-dir", dir)
	if code != 9 || stdout != "" || !strings.Contains(stderr, "reduced_diagnostics") || !strings.Contains(stderr, `"diagnostics_mode":""`) {
		t.Fatalf("empty marker show: exit %d\nstdout=%q\nstderr=%s", code, stdout, stderr)
	}
}

func snapshotListLine(t *testing.T, text, name string) string {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == name {
			return line
		}
	}
	t.Fatalf("no list line for %s:\n%s", name, text)
	return ""
}

func withDiagnosticsMode(t *testing.T, doc string, mode any) string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(doc), &payload); err != nil {
		t.Fatal(err)
	}
	payload["diagnostics_mode"] = mode
	out, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func writeTestFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
