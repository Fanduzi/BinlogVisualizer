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

// jsonOutputRun is one command that prints a versioned JSON document.
// Names are the schema catalog (schemaOutputs), not a second list.
type jsonOutputRun struct {
	name string
	args []string
	exit int
}

func prepareJSONOutputRuns(t *testing.T) []jsonOutputRun {
	t.Helper()
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

	return []jsonOutputRun{
		{name: "analyze", args: []string{"analyze", binlog, "--format", "json", "-q"}, exit: 0},
		// The fixture holds a CREATE TABLE, so the real run refuses (exit 6); --plan still prints the plan.
		{name: "flashback-plan", args: []string{"flashback", binlog, "--plan"}, exit: 6},
		{name: "compare", args: []string{"compare", "--snapshot-dir", snapshotDir, "--current-snapshot", b, "--baseline-snapshot", a, "--format", "json"}, exit: 0},
		{name: "trend", args: []string{"trend", a, b, "--snapshot-dir", snapshotDir, "--format", "json"}, exit: 0},
		{name: "snapshot-list", args: []string{"snapshot", "list", "--snapshot-dir", snapshotDir, "--format", "json"}, exit: 0},
		{name: "snapshot-show", args: []string{"snapshot", "show", a, "--snapshot-dir", snapshotDir, "--format", "json"}, exit: 0},
		{name: "workflow-validate", args: []string{"workflow", "validate", planPath, "--format", "json"}, exit: 0},
		{name: "workflow-describe", args: []string{"workflow", "describe", planPath, "--format", "json"}, exit: 0},
		{name: "workflow-status", args: []string{"workflow", "status", outputDir, "--format", "json"}, exit: 0},
		{name: "workflow-clean", args: []string{"workflow", "clean", outputDir, "--format", "json"}, exit: 0},
		{name: "workflow-export", args: []string{"workflow", "export", outputDir, "--output", filepath.Join(t.TempDir(), "out.zip"), "--format", "json"}, exit: 0},
		{name: "version", args: []string{"version", "--format", "json"}, exit: 0},
		{name: "flashback-verify", args: []string{"flashback", "verify", verifyFile, "--format", "json"}, exit: 0},
	}
}

func schemaCatalogByName(t *testing.T) map[string]schemaOutput {
	t.Helper()
	byName := map[string]schemaOutput{}
	for _, out := range schemaOutputs() {
		if _, dup := byName[out.Name]; dup {
			t.Fatalf("duplicate schema name %q", out.Name)
		}
		byName[out.Name] = out
	}
	return byName
}

func TestEveryJSONOutputIsVersioned(t *testing.T) {
	runs := prepareJSONOutputRuns(t)
	byName := schemaCatalogByName(t)
	if len(runs) != len(byName) {
		t.Fatalf("JSON runners %d, schema catalog %d", len(runs), len(byName))
	}
	seen := map[string]bool{}
	for _, run := range runs {
		out, ok := byName[run.name]
		if !ok {
			t.Errorf("runner %s is not in schemaOutputs()", run.name)
			continue
		}
		seen[run.name] = true
		code, stdout, stderr := runRoot(t, run.args...)
		if code != run.exit {
			t.Errorf("%s: exit %d, want %d: %s", run.name, code, run.exit, stderr)
			continue
		}
		var doc map[string]any
		if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
			t.Errorf("%s: stdout is not one JSON object: %v\n%s", run.name, err, stdout)
			continue
		}
		got, ok := doc[out.VersionKey].(float64)
		if !ok || got != float64(out.Version) {
			t.Errorf("%s: %s = %v, want %d", run.name, out.VersionKey, doc[out.VersionKey], out.Version)
		}
		if out.VersionKey == "schema_version" && !strings.HasPrefix(strings.TrimSpace(stdout), "{\n  \"schema_version\":") {
			t.Errorf("%s: schema_version is not the first key:\n%.200s", run.name, stdout)
		}
	}
	for name := range byName {
		if !seen[name] {
			t.Errorf("schema %s has no JSON runner", name)
		}
	}
}

func TestVersionRejectsUnknownFormat(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	if code, _, stderr := runRoot(t, "version", "--format", "yaml"); code != 3 || !strings.Contains(stderr, "unsupported version format") {
		t.Fatalf("version --format yaml: exit %d, stderr %q; want exit 3", code, stderr)
	}
}
