package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #268: the remaining exit-code misclassifications after #263 and #264.
func TestExitCodesRemainingKinds(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	dir := t.TempDir()
	good := filepath.Join("testdata", "minimal.binlog")
	cases := []struct {
		name string
		args []string
		code int
	}{
		{"directory as binlog", []string{"analyze", dir}, 4},
		{"directory as flashback input", []string{"flashback", dir}, 4},
		{"compare missing report", []string{"compare", filepath.Join(dir, "nosuch.json"), filepath.Join(dir, "other.json")}, 4},
		{"compare directory", []string{"compare", dir, dir}, 4},
		{"snapshot unknown subcommand", []string{"snapshot", filepath.Join(dir, "x.binlog")}, 3},
		{"workflow unknown subcommand", []string{"workflow", filepath.Join(dir, "x.yaml")}, 3},
		{"unknown error format", []string{"--error-format", "xml", "analyze", good}, 3},
		{"negative top-transactions", []string{"analyze", good, "--top-transactions", "-1"}, 3},
		{"negative top", []string{"analyze", good, "--top", "-1"}, 3},
		{"negative top-minutes", []string{"analyze", good, "--top-minutes", "-1"}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runRoot(t, tc.args...)
			if code != tc.code {
				t.Fatalf("exit %d, want %d\n%s", code, tc.code, stderr)
			}
			if stdout != "" {
				t.Fatalf("a failed run prints nothing on stdout:\n%s", stdout)
			}
		})
	}
}

func TestUnreadableBinlogIsInput(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	body, err := os.ReadFile(filepath.Join("testdata", "minimal.binlog"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "noperm.binlog")
	if err := os.WriteFile(path, body, 0o000); err != nil {
		t.Fatal(err)
	}
	if f, err := os.Open(path); err == nil {
		_ = f.Close()
		t.Skip("running with permission to read a mode 000 file (root)")
	}
	code, _, stderr := runRoot(t, "analyze", path)
	if code != 4 {
		t.Fatalf("exit %d, want 4\n%s", code, stderr)
	}
}

// Group commands still print help and exit 0 when called with no arguments.
func TestGroupCommandsWithoutArgsPrintHelp(t *testing.T) {
	for _, group := range []string{"snapshot", "workflow"} {
		code, stdout, stderr := runRoot(t, group)
		if code != 0 || !strings.Contains(stdout, "Usage:") {
			t.Fatalf("%s: exit %d stdout=%q stderr=%q", group, code, stdout, stderr)
		}
	}
}

// With JSON errors the one JSON object is the whole of stderr: no usage
// text for an unknown command, no "Finalizing analysis..." before a failure.
func TestJSONErrorIsTheOnlyStderrLine(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Cleanup(func() { errorFormat = "" })
	dir := t.TempDir()
	cases := []struct {
		name string
		args []string
		kind string
	}{
		{"unknown command", []string{"--error-format", "json", "frobnicate"}, "usage"},
		{"empty window after parse", []string{"--error-format", "json", "analyze", filepath.Join("testdata", "minimal.binlog"), "--start", "2000-01-01T00:00:00Z", "--end", "2000-01-02T00:00:00Z"}, "no_data"},
		{"directory", []string{"--error-format", "json", "analyze", dir}, "input"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, stderr := runRoot(t, tc.args...)
			lines := strings.Split(strings.TrimSpace(stderr), "\n")
			if len(lines) != 1 {
				t.Fatalf("want one stderr line, got %d:\n%s", len(lines), stderr)
			}
			var v struct {
				Error struct {
					Kind string `json:"kind"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(lines[0]), &v); err != nil {
				t.Fatalf("not JSON: %q", lines[0])
			}
			if v.Error.Kind != tc.kind {
				t.Fatalf("kind %q, want %q", v.Error.Kind, tc.kind)
			}
		})
	}
}

// Text mode keeps the info line on success.
func TestTextModeKeepsFinalizingLine(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, _, stderr := runRoot(t, "analyze", filepath.Join("testdata", "minimal.binlog"))
	if code != 0 || !strings.Contains(stderr, "Finalizing analysis") {
		t.Fatalf("exit %d stderr=%q", code, stderr)
	}
}
