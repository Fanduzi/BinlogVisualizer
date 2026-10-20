// Package binlogviz checks the ADR 0010 contract: every JSON document a command prints on stdout carries a version key.
// input: the workflow, snapshot, compare, trend, analyze, flashback and version commands run on the minimal fixtures.
// output: a failure naming any JSON output whose stdout lacks report_version, plan_version or schema_version as its version key.
// pos: guard so a new JSON output cannot ship unversioned.
// note: if this file changes, update docs/adr/0010-every-json-output-is-versioned.md.
package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/guard"
)

func TestEveryJSONOutputIsVersioned(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")
	if code, _, stderr := runRoot(t, "workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir); code != 0 {
		t.Fatalf("workflow run exit %d: %s", code, stderr)
	}

	code, listOut, stderr := runRoot(t, "snapshot", "list", "--snapshot-dir", snapshotDir, "--format", "json")
	if code != 0 {
		t.Fatalf("snapshot list exit %d: %s", code, stderr)
	}
	var list struct {
		Snapshots []struct {
			Name string `json:"name"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal([]byte(listOut), &list); err != nil || len(list.Snapshots) < 2 {
		t.Fatalf("snapshot list: %v\n%s", err, listOut)
	}
	a, b := list.Snapshots[0].Name, list.Snapshots[1].Name
	binlog := filepath.Join("testdata", "minimal.binlog")
	verifyFile := filepath.Join(t.TempDir(), "flashback.sql")
	var sealed strings.Builder
	if err := guard.New(nil).Write(&sealed, func(emit func(guard.Piece) error) error {
		return emit(guard.Piece{Kind: guard.PieceStmt, Text: "DO 0"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(verifyFile, []byte(sealed.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		key  string
		want float64
		args []string
		exit int
	}{
		{"analyze", "report_version", 4, []string{"analyze", binlog, "--format", "json", "-q"}, 0},
		// The fixture holds a CREATE TABLE, so the real run refuses (exit 6); --plan still prints the plan.
		{"flashback --plan", "plan_version", 2, []string{"flashback", binlog, "--plan"}, 6},
		{"compare", "schema_version", 1, []string{"compare", "--snapshot-dir", snapshotDir, "--current-snapshot", b, "--baseline-snapshot", a, "--format", "json"}, 0},
		{"trend", "schema_version", 1, []string{"trend", a, b, "--snapshot-dir", snapshotDir, "--format", "json"}, 0},
		{"snapshot list", "schema_version", 1, []string{"snapshot", "list", "--snapshot-dir", snapshotDir, "--format", "json"}, 0},
		{"snapshot show", "schema_version", 1, []string{"snapshot", "show", a, "--snapshot-dir", snapshotDir, "--format", "json"}, 0},
		{"workflow validate", "schema_version", 1, []string{"workflow", "validate", planPath, "--format", "json"}, 0},
		{"workflow describe", "schema_version", 1, []string{"workflow", "describe", planPath, "--format", "json"}, 0},
		{"workflow status", "schema_version", 1, []string{"workflow", "status", outputDir, "--format", "json"}, 0},
		{"workflow clean", "schema_version", 1, []string{"workflow", "clean", outputDir, "--format", "json"}, 0},
		{"workflow export", "schema_version", 1, []string{"workflow", "export", outputDir, "--output", filepath.Join(t.TempDir(), "out.zip"), "--format", "json"}, 0},
		{"version", "schema_version", 1, []string{"version", "--format", "json"}, 0},
		{"flashback verify", "schema_version", 1, []string{"flashback", "verify", verifyFile, "--format", "json"}, 0},
	}
	for _, tc := range cases {
		code, stdout, stderr := runRoot(t, tc.args...)
		if code != tc.exit {
			t.Errorf("%s: exit %d, want %d: %s", tc.name, code, tc.exit, stderr)
			continue
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
			t.Errorf("%s: stdout is not one JSON object: %v\n%s", tc.name, err, stdout)
			continue
		}
		if got, ok := doc[tc.key].(float64); !ok || got != tc.want {
			t.Errorf("%s: %s = %v, want %v", tc.name, tc.key, doc[tc.key], tc.want)
		}
		if tc.key == "schema_version" && !strings.HasPrefix(strings.TrimSpace(stdout), "{\n  \"schema_version\":") {
			t.Errorf("%s: schema_version is not the first key:\n%.200s", tc.name, stdout)
		}
	}
}

func TestVersionRejectsUnknownFormat(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	if code, _, stderr := runRoot(t, "version", "--format", "yaml"); code != 3 || !strings.Contains(stderr, "unsupported version format") {
		t.Fatalf("version --format yaml: exit %d, stderr %q; want exit 3", code, stderr)
	}
}
