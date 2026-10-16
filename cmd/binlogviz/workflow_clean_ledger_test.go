// input: real workflow runs (basic-plan.yaml) followed by renames, failed re-runs, manual files and ledger loss.
// output: #289/#288 guard: clean deletes only ledger-attributed files whose sha256 still matches, refuses after a failed run (6 run_not_successful) or without a ledger (6 no_ledger), and never deletes snapshots it did not create.
// pos: ADR 0009 §8 deletion model.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
)

func ledgerRun(t *testing.T, planPath, outputDir, snapshotDir string) error {
	t.Helper()
	_, _, err := captureStdoutStderrRun(t, func() error {
		return execRoot(t, "workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir)
	})
	return err
}

func renamedPlan(t *testing.T, planPath string) string {
	t.Helper()
	body, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	s := strings.ReplaceAll(string(body), "week2", "week2b")
	p := filepath.Join(t.TempDir(), "plan.yaml")
	if err := os.WriteFile(p, []byte(s), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func mustExist(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s must survive clean: %v", p, err)
		}
	}
}

func mustBeGone(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			t.Fatalf("%s should have been deleted", p)
		}
	}
}

func cleanQuiet(t *testing.T, args ...string) error {
	t.Helper()
	_, _, err := captureStdoutStderrRun(t, func() error {
		return execRoot(t, append([]string{"workflow", "clean"}, args...)...)
	})
	return err
}

// #289 r4: a failed re-run rewrites the manifest; clean must not delete the
// last good run's outputs or snapshots.
func TestCleanRefusesAfterFailedRerun289(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")
	if err := ledgerRun(t, planPath, outputDir, snapshotDir); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := ledgerRun(t, renamedPlan(t, planPath), outputDir, snapshotDir); err == nil {
		t.Fatal("re-run into the same snapshot dir should fail (snapshot week1 exists)")
	}
	for _, args := range [][]string{
		{outputDir, "--include-snapshots"},
		{outputDir, "--apply", "--include-snapshots"},
		{outputDir, "--apply", "--include-snapshots", "--force"},
	} {
		assertKind(t, cleanQuiet(t, args...), clierr.KindRefused, "run_not_successful")
	}
	mustExist(t,
		filepath.Join(outputDir, "analyze", "week1.json"), filepath.Join(outputDir, "analyze", "week2.json"),
		filepath.Join(outputDir, "compare", "week2_vs_week1.json"), filepath.Join(outputDir, "trend", "weekly_series.json"),
		filepath.Join(snapshotDir, "week1.json"), filepath.Join(snapshotDir, "week2.json"))
}

// A successful re-run supersedes outputs: clean deletes the ledger-attributed
// ones whose hash still matches and keeps everything else unless --force.
func TestCleanDeletesOnlyLedgerAttributedFiles(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")
	if err := ledgerRun(t, planPath, outputDir, snapshotDir); err != nil {
		t.Fatalf("first run: %v", err)
	}
	// Remove only week1's snapshot so the renamed re-run succeeds and owns week1 again.
	if err := os.Remove(filepath.Join(snapshotDir, "week1.json")); err != nil {
		t.Fatal(err)
	}
	if err := ledgerRun(t, renamedPlan(t, planPath), outputDir, snapshotDir); err != nil {
		t.Fatalf("renamed re-run: %v", err)
	}
	manual := filepath.Join(outputDir, "analyze", "manual.json")
	if err := os.WriteFile(manual, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A superseded compare output edited by hand no longer matches its hash.
	edited := filepath.Join(outputDir, "compare", "week2_vs_week1.html")
	if err := os.WriteFile(edited, []byte("<p>notes</p>"), 0o644); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(snapshotDir, "someone-else.json")
	if err := os.WriteFile(foreign, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := cleanQuiet(t, outputDir, "--apply", "--include-snapshots", "--snapshot-dir", snapshotDir); err != nil {
		t.Fatalf("clean: %v", err)
	}
	mustBeGone(t,
		filepath.Join(outputDir, "analyze", "week2.json"),
		filepath.Join(outputDir, "compare", "week2_vs_week1.json"),
		filepath.Join(snapshotDir, "week2.json"))
	mustExist(t, manual, edited, foreign,
		filepath.Join(outputDir, "analyze", "week1.json"), filepath.Join(outputDir, "analyze", "week2b.json"),
		filepath.Join(snapshotDir, "week1.json"), filepath.Join(snapshotDir, "week2b.json"))

	// --force removes unattributed artifacts, but never a snapshot the
	// workflow did not create.
	if err := cleanQuiet(t, outputDir, "--apply", "--include-snapshots", "--force", "--snapshot-dir", snapshotDir); err != nil {
		t.Fatalf("clean --force: %v", err)
	}
	mustBeGone(t, manual, edited)
	mustExist(t, foreign, filepath.Join(outputDir, "analyze", "week2b.json"))
}

// #288 with snapshots: an emptied step list is still refused, and the
// snapshots survive.
func TestCleanEmptyStepsKeepsSnapshots288(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")
	if err := ledgerRun(t, planPath, outputDir, snapshotDir); err != nil {
		t.Fatalf("run: %v", err)
	}
	mfPath := filepath.Join(outputDir, "manifest.json")
	var mf map[string]any
	body, _ := os.ReadFile(mfPath)
	if err := json.Unmarshal(body, &mf); err != nil {
		t.Fatal(err)
	}
	mf["steps"] = []any{}
	body, _ = json.Marshal(mf)
	if err := os.WriteFile(mfPath, body, 0o644); err != nil {
		t.Fatal(err)
	}
	assertKind(t, cleanQuiet(t, outputDir, "--apply", "--include-snapshots"), clierr.KindRefused, "untrusted_manifest")
	mustExist(t, filepath.Join(snapshotDir, "week1.json"), filepath.Join(snapshotDir, "week2.json"),
		filepath.Join(outputDir, "analyze", "week1.json"))
}

// Workflow dirs from before the ledger: refuse unless --force, and say how to
// rebuild it.
func TestCleanWithoutLedgerNeedsForce(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")
	if err := ledgerRun(t, planPath, outputDir, snapshotDir); err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := os.Remove(filepath.Join(outputDir, "ledger.jsonl")); err != nil {
		t.Fatalf("run must write ledger.jsonl: %v", err)
	}
	stale := filepath.Join(outputDir, "analyze", "stale.json")
	if err := os.WriteFile(stale, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := cleanQuiet(t, outputDir, "--apply", "--include-snapshots")
	assertKind(t, err, clierr.KindRefused, "no_ledger")
	if !strings.Contains(err.Error(), "workflow run") {
		t.Fatalf("no_ledger error must say how to rebuild the ledger: %v", err)
	}
	mustExist(t, stale)

	if err := cleanQuiet(t, outputDir, "--apply", "--include-snapshots", "--force", "--snapshot-dir", snapshotDir); err != nil {
		t.Fatalf("clean --force: %v", err)
	}
	mustBeGone(t, stale)
	// Without a ledger nothing proves a snapshot is ours: never delete one.
	mustExist(t, filepath.Join(snapshotDir, "week1.json"), filepath.Join(snapshotDir, "week2.json"),
		filepath.Join(outputDir, "analyze", "week1.json"))
}

func TestExportPlanWarningRespectsJSONErrors(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	t.Cleanup(func() { errorFormat = "" })
	outside := filepath.Join(t.TempDir(), "plan.yaml")
	if err := os.WriteFile(outside, []byte(validPlan277), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := makeArtifactDir(t, `{"manifest_version":2,"workflow_name":"w","status":"success","plan_path":"`+outside+`","plan_sha256":"abc","steps":[
		{"kind":"analyze","name":"week1","status":"success","artifacts":["analyze/week1.json"]}]}`)
	_, stderr, err := captureStdoutStderrRun(t, func() error {
		cmd := NewRootCommand()
		cmd.SetArgs([]string{"--error-format", "json", "workflow", "export", dir, "--output", filepath.Join(t.TempDir(), "x.zip")})
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	line := strings.TrimSpace(stderr)
	var v struct {
		Warning struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"warning"`
	}
	if json.Unmarshal([]byte(line), &v) != nil || v.Warning.Code != "plan_not_included" || v.Warning.Message == "" {
		t.Fatalf("stderr must be one JSON warning line, got %q", stderr)
	}
}
