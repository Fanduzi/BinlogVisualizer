// Package binlogviz verifies workflow command orchestration and operator-facing error output.
// input: workflow CLI args, plan fixtures, guarded compare/trend artifacts, and cobra command trees.
// output: regression coverage for workflow run/resume/validate/describe/status/clean/export contracts, including comparability-guard summary propagation and relative output-root resolution.
// pos: command-layer tests for workflow I/O, including Error-once failure output without Usage dumps.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"archive/zip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/workflow"
)

func executeWorkflowLikeMain(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	forceEnglishRuntimeOutput(t)
	cmd := NewRootCommand()
	cmd.SetArgs(append([]string{"workflow"}, args...))
	return captureStdoutStderrRun(t, func() error {
		err := cmd.Execute()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error:", err)
		}
		return err
	})
}

func assertErrorOnceWithoutUsage(t *testing.T, stdout, stderr string, err error, wantSub string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected non-zero workflow failure")
	}
	if strings.Contains(stdout, "Usage:") || strings.Contains(stderr, "Usage:") {
		t.Fatalf("failure path dumped Usage:\nstdout=%q\nstderr=%q", stdout, stderr)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected Error printed once, got %q", stderr)
	}
	if !strings.Contains(stderr, wantSub) {
		t.Fatalf("expected stderr to contain %q, got %q", wantSub, stderr)
	}
}

// TestWorkflowRunWritesAnalyzeArtifacts tests that a multi-window plan produces analyze JSON files.
func TestWorkflowRunWritesAnalyzeArtifacts(t *testing.T) {
	binlogDir, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	// Verify both analyze artifacts
	for _, name := range []string{"week1", "week2"} {
		analyzePath := filepath.Join(outputDir, "analyze", name+".json")
		if _, err := os.Stat(analyzePath); err != nil {
			t.Fatalf("expected analyze artifact at %s: %v", analyzePath, err)
		}
	}

	// Verify manifest exists
	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if mf.Status != "success" {
		t.Fatalf("expected manifest status success, got %q", mf.Status)
	}
	if mf.WorkflowName != "multi-window-test" {
		t.Fatalf("expected workflow_name multi-window-test, got %q", mf.WorkflowName)
	}

	// Verify analyze step records with snapshot names
	for _, window := range []string{"week1", "week2"} {
		found := false
		for _, step := range mf.Steps {
			if step.Kind == "analyze" && step.Name == window {
				found = true
				if step.Status != "success" {
					t.Fatalf("expected analyze step %q success, got %q", window, step.Status)
				}
				if len(step.Artifacts) == 0 || step.Artifacts[0] != "analyze/"+window+".json" {
					t.Fatalf("expected artifact analyze/%s.json, got %v", window, step.Artifacts)
				}
				if step.SnapshotName != window {
					t.Fatalf("expected snapshot_name %s, got %q", window, step.SnapshotName)
				}
			}
		}
		if !found {
			t.Fatalf("expected to find analyze step for %q in manifest", window)
		}
	}

	_ = binlogDir
}

func TestWorkflowRunCopiesPlanIntoWorkflowRoot(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	expectedPlanPath := filepath.Join(outputDir, "plan.yaml")
	if mf.PlanPath != expectedPlanPath {
		t.Fatalf("expected manifest plan_path %q, got %q", expectedPlanPath, mf.PlanPath)
	}

	wantPlanBody, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read source plan: %v", err)
	}
	gotPlanBody, err := os.ReadFile(expectedPlanPath)
	if err != nil {
		t.Fatalf("read copied plan: %v", err)
	}
	if string(gotPlanBody) != string(wantPlanBody) {
		t.Fatalf("copied plan body mismatch")
	}
}

func TestWorkflowRunMissingPlanPrintsErrorOnceWithoutUsage(t *testing.T) {
	stdout, stderr, err := executeWorkflowLikeMain(t, "run", filepath.Join(t.TempDir(), "missing.yaml"))
	assertErrorOnceWithoutUsage(t, stdout, stderr, err, "Error: open workflow plan:")
}

func TestWorkflowRunMissingFromDirPrintsErrorOnceWithoutUsage(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "artifacts")
	missingFromDir := filepath.Join(t.TempDir(), "does-not-exist")
	planPath := filepath.Join(t.TempDir(), "plan.yaml")
	plan := fmt.Sprintf(`version: 1
workflow:
  name: missing-from-dir
  output_dir: %s
defaults:
  input:
    from_dir: %s
    prefix: mysql-bin.
windows:
  - name: week1
    start: 2025-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
`, outputDir, missingFromDir)
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	stdout, stderr, err := executeWorkflowLikeMain(t, "run", planPath)
	assertErrorOnceWithoutUsage(t, stdout, stderr, err, "Error: discover binlog files:")
}

func TestWorkflowCommandTreeSilencesUsageAndErrors(t *testing.T) {
	root := NewRootCommand()
	for _, name := range []string{"run", "resume", "validate", "describe", "status", "clean", "export"} {
		cmd, _, err := root.Find([]string{"workflow", name})
		if err != nil {
			t.Fatalf("find workflow %s: %v", name, err)
		}
		if !cmd.SilenceUsage || !cmd.SilenceErrors {
			t.Fatalf("workflow %s must SilenceUsage and SilenceErrors so CheckErr prints Error once without Usage", name)
		}
	}
}

func TestWorkflowRunPersistsDefaultSnapshotDirInManifest(t *testing.T) {
	_, outputDir, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")
	home := t.TempDir()
	t.Setenv("HOME", home)

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	expectedDir := filepath.Join(home, ".binlogviz", "snapshots")
	if mf.SnapshotDir != expectedDir {
		t.Fatalf("expected manifest snapshot_dir %q, got %q", expectedDir, mf.SnapshotDir)
	}

	for _, name := range []string{"week1.json", "week2.json"} {
		if _, err := os.Stat(filepath.Join(expectedDir, name)); err != nil {
			t.Fatalf("expected snapshot %s in default snapshot dir: %v", name, err)
		}
	}
}

// TestWorkflowRunWritesCompareAndTrendArtifacts tests full workflow with multi-window compare and trend.
func TestWorkflowRunWritesCompareAndTrendArtifacts(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	// Verify compare artifacts (json + html)
	for _, format := range []string{"json", "html"} {
		comparePath := filepath.Join(outputDir, "compare", "week2_vs_week1."+format)
		if _, err := os.Stat(comparePath); err != nil {
			t.Fatalf("expected compare artifact at %s: %v", comparePath, err)
		}
	}

	// Verify trend artifacts (json + html)
	for _, format := range []string{"json", "html"} {
		trendPath := filepath.Join(outputDir, "trend", "weekly_series."+format)
		if _, err := os.Stat(trendPath); err != nil {
			t.Fatalf("expected trend artifact at %s: %v", trendPath, err)
		}
	}

	// Verify manifest status
	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if mf.Status != "success" {
		t.Fatalf("expected manifest status success, got %q: steps=%v", mf.Status, mf.Steps)
	}

	// Verify we have 2 analyze + 1 compare + 1 trend steps
	stepKinds := make(map[string]int)
	for _, step := range mf.Steps {
		stepKinds[step.Kind]++
	}
	if stepKinds["analyze"] != 2 {
		t.Fatalf("expected 2 analyze steps, got %d", stepKinds["analyze"])
	}
	if stepKinds["compare"] != 1 {
		t.Fatalf("expected 1 compare step, got %d", stepKinds["compare"])
	}
	if stepKinds["trend"] != 1 {
		t.Fatalf("expected 1 trend step, got %d", stepKinds["trend"])
	}
}

// TestWorkflowRunWritesFailedManifestOnStepError tests partial-failure manifest behavior.
func TestWorkflowRunWritesFailedManifestOnStepError(t *testing.T) {
	planDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "artifacts")

	// Create a plan with an invalid input directory
	plan := strings.ReplaceAll(`
version: 1
workflow:
  name: failing-test
  output_dir: PLACEHOLDER
defaults:
  input:
    from_dir: /nonexistent/path/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2025-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
`, "PLACEHOLDER", outputDir)

	planPath := filepath.Join(planDir, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected workflow run to fail")
	}

	// Manifest should still exist
	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, merr := workflowManifestFromJSON(manifestPath)
	if merr != nil {
		t.Fatalf("read manifest: %v", merr)
	}
	if mf.Status != "failed" {
		t.Fatalf("expected manifest status failed, got %q", mf.Status)
	}
}

// TestWorkflowRunRejectsInvalidPlan tests that invalid plans fail before writing artifacts.
func TestWorkflowRunRejectsInvalidPlan(t *testing.T) {
	planDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "artifacts")

	// Create a plan missing version
	plan := `
workflow:
  name: no-version
  output_dir: ./out
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: w1
    start: 2025-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
`
	planPath := filepath.Join(planDir, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected invalid plan to fail")
	}
	if !strings.Contains(err.Error(), "workflow plan version is required") {
		t.Fatalf("expected version error, got %v", err)
	}

	// No artifacts should be written
	if _, err := os.Stat(outputDir); !os.IsNotExist(err) {
		// outputDir may exist if --output-dir flag was passed, but manifest should not
		manifestPath := filepath.Join(outputDir, "manifest.json")
		if _, err := os.Stat(manifestPath); err == nil {
			t.Fatal("expected no manifest.json for invalid plan")
		}
	}
}

// TestWorkflowRunManifestWriteFailsReturnsError tests that manifest write failure on an otherwise
// successful run returns a hard error instead of silently succeeding.
func TestWorkflowRunManifestWriteFailsReturnsError(t *testing.T) {
	binlogDir, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	// Pre-create a directory at the manifest path to force WriteManifest to fail
	manifestBlockingDir := filepath.Join(outputDir, "manifest.json")
	if err := os.MkdirAll(manifestBlockingDir, 0o755); err != nil {
		t.Fatalf("create blocking dir: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected workflow to fail when manifest write fails")
	}
	if !strings.Contains(err.Error(), "write manifest") {
		t.Fatalf("expected manifest write error, got %v", err)
	}

	_ = binlogDir
}

// TestWorkflowRunSnapshotSaveFailsStep tests that snapshot save failure fails the workflow step.
func TestWorkflowRunSnapshotSaveFailsStep(t *testing.T) {
	binlogDir, outputDir, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	// Point snapshot-dir to a file (not a directory) to force SaveJSON failure
	filePath := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(filePath, []byte("blocker"), 0o644); err != nil {
		t.Fatalf("write blocker file: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", filePath})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected workflow to fail when snapshot save fails")
	}

	// Manifest should exist with status=failed
	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, merr := workflowManifestFromJSON(manifestPath)
	if merr != nil {
		t.Fatalf("read manifest: %v", merr)
	}
	if mf.Status != "failed" {
		t.Fatalf("expected manifest status failed, got %q", mf.Status)
	}

	// The first analyze step should be failed with snapshot-related error
	if len(mf.Steps) == 0 {
		t.Fatal("expected at least one step in manifest")
	}
	firstStep := mf.Steps[0]
	if firstStep.Kind != "analyze" {
		t.Fatalf("expected first step kind analyze, got %q", firstStep.Kind)
	}
	if firstStep.Status != "failed" {
		t.Fatalf("expected first step status failed, got %q", firstStep.Status)
	}

	_ = binlogDir
}

// setupWorkflowTestWithSnapshots creates a test environment with a binlog fixture directory, plan file, and isolated snapshot directory.
func setupWorkflowTestWithSnapshots(t *testing.T, planFile string) (binlogDir, outputDir, planPath, snapshotDir string) {
	t.Helper()

	// Create binlog directory with two numbered files for multi-file discovery
	binlogDir = t.TempDir()
	fixtureSrc := filepath.Join("testdata", "minimal.binlog")
	src, err := os.ReadFile(fixtureSrc)
	if err != nil {
		t.Fatalf("read binlog fixture: %v", err)
	}
	for _, suffix := range []string{"000001", "000002"} {
		dst := filepath.Join(binlogDir, "mysql-bin."+suffix)
		if err := os.WriteFile(dst, src, 0o644); err != nil {
			t.Fatalf("write binlog fixture %s: %v", dst, err)
		}
	}

	outputDir = filepath.Join(t.TempDir(), "artifacts")
	snapshotDir = t.TempDir()

	// Read and patch the plan template
	templatePath := filepath.Join("testdata", "workflow", planFile)
	template, err := os.ReadFile(templatePath)
	if err != nil {
		t.Fatalf("read plan template: %v", err)
	}
	planContent := strings.ReplaceAll(string(template), "PLACEHOLDER/artifacts", outputDir)
	planContent = strings.ReplaceAll(planContent, "PLACEHOLDER/binlog", binlogDir)

	planDir := t.TempDir()
	planPath = filepath.Join(planDir, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(planContent), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	return binlogDir, outputDir, planPath, snapshotDir
}

func writeWorkflowSummaryFixtureArtifacts(t *testing.T, outputDir string) {
	t.Helper()

	fixtures := []struct {
		source string
		target string
	}{
		{
			source: filepath.Join("testdata", "golden", "compare-key-findings.golden.json"),
			target: filepath.Join(outputDir, "compare", "week2_vs_week1.json"),
		},
		{
			source: filepath.Join("testdata", "golden", "trend-summary.golden.json"),
			target: filepath.Join(outputDir, "trend", "weekly_series.json"),
		},
	}

	for _, fixture := range fixtures {
		data, err := os.ReadFile(fixture.source)
		if err != nil {
			t.Fatalf("read fixture %s: %v", fixture.source, err)
		}
		if err := os.MkdirAll(filepath.Dir(fixture.target), 0o755); err != nil {
			t.Fatalf("mkdir for fixture %s: %v", fixture.target, err)
		}
		if err := os.WriteFile(fixture.target, data, 0o644); err != nil {
			t.Fatalf("write fixture %s: %v", fixture.target, err)
		}
	}

	htmlArtifacts := map[string]string{
		filepath.Join(outputDir, "compare", "week2_vs_week1.html"): "<html><body>compare</body></html>",
		filepath.Join(outputDir, "trend", "weekly_series.html"):    "<html><body>trend</body></html>",
	}
	for path, content := range htmlArtifacts {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write html artifact %s: %v", path, err)
		}
	}
}

// TestWorkflowCompareOutputIsValidJSON verifies the compare and trend outputs are valid JSON.
func TestWorkflowCompareOutputIsValidJSON(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	comparePath := filepath.Join(outputDir, "compare", "week2_vs_week1.json")
	data, err := os.ReadFile(comparePath)
	if err != nil {
		t.Fatalf("read compare output: %v", err)
	}
	if !json.Valid(data) {
		t.Fatalf("compare output is not valid JSON: %s", string(data[:min(len(data), 200)]))
	}

	trendPath := filepath.Join(outputDir, "trend", "weekly_series.json")
	data, err = os.ReadFile(trendPath)
	if err != nil {
		t.Fatalf("read trend output: %v", err)
	}
	if !json.Valid(data) {
		t.Fatalf("trend output is not valid JSON: %s", string(data[:min(len(data), 200)]))
	}
}

// TestWorkflowRunWritesIndexHTMLOnSuccess tests that a successful run writes index.html.
func TestWorkflowRunWritesIndexHTMLOnSuccess(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	indexPath := filepath.Join(outputDir, "index.html")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read index.html: %v", err)
	}
	html := string(data)
	for _, token := range []string{"multi-window-test", "./compare/week2_vs_week1.html", "./manifest.json"} {
		if !strings.Contains(html, token) {
			t.Fatalf("expected %q in workflow index", token)
		}
	}
}

// TestWorkflowRunWritesIndexHTMLOnFailure tests that a failed run still writes index.html.
func TestWorkflowRunWritesIndexHTMLOnFailure(t *testing.T) {
	planDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "artifacts")

	plan := strings.ReplaceAll(`
version: 1
workflow:
  name: failing-index-test
  output_dir: PLACEHOLDER
defaults:
  input:
    from_dir: /nonexistent/path/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2025-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
`, "PLACEHOLDER", outputDir)

	planPath := filepath.Join(planDir, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	_ = cmd.Execute()

	indexPath := filepath.Join(outputDir, "index.html")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read index.html on failure: %v", err)
	}
	html := string(data)
	if !strings.Contains(html, "failed") {
		t.Fatalf("expected 'failed' status in index.html")
	}
	if !strings.Contains(html, "failing-index-test") {
		t.Fatalf("expected workflow name in index.html")
	}
	if !strings.Contains(html, "step-error") {
		t.Fatalf("expected error block in index.html")
	}
	if !strings.Contains(html, "discover binlog files") {
		t.Fatalf("expected discovery error message in index.html")
	}
}

func TestWorkflowRunWritesWorkflowSummaryRecommendations(t *testing.T) {
	outputDir := t.TempDir()
	writeWorkflowSummaryFixtureArtifacts(t, outputDir)

	mf := workflow.Manifest{
		WorkflowName: "multi-window-test",
		Status:       "success",
		Steps: []workflow.StepRecord{
			{Kind: "compare", Name: "week2_vs_week1", Status: "success", Artifacts: []string{"compare/week2_vs_week1.json", "compare/week2_vs_week1.html"}},
			{Kind: "trend", Name: "weekly_series", Status: "success", Artifacts: []string{"trend/weekly_series.json", "trend/weekly_series.html"}},
		},
	}

	if err := finalizeWorkflow(outputDir, &mf, time.Time{}, io.Discard, nil); err != nil {
		t.Fatalf("finalize workflow: %v", err)
	}

	stored, err := workflowManifestFromJSON(filepath.Join(outputDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if stored.WorkflowSummary.Recommendations == nil {
		t.Fatal("workflow_summary.recommendations should be non-nil")
	}
	if len(stored.WorkflowSummary.Recommendations) == 0 {
		t.Fatal("expected workflow_summary.recommendations to be populated")
	}
	if len(stored.WorkflowSummary.Findings) == 0 {
		t.Fatal("expected workflow_summary.findings to be populated")
	}
}

func TestWorkflowRunFailedLateStepStillWritesWorkflowSummary(t *testing.T) {
	outputDir := t.TempDir()
	writeWorkflowSummaryFixtureArtifacts(t, outputDir)

	mf := workflow.Manifest{
		WorkflowName: "multi-window-test",
		Status:       "success",
		Steps: []workflow.StepRecord{
			{Kind: "compare", Name: "week2_vs_week1", Status: "success", Artifacts: []string{"compare/week2_vs_week1.json", "compare/week2_vs_week1.html"}},
			{Kind: "trend", Name: "weekly_series", Status: "failed", Error: "trend render failed"},
		},
	}

	err := finalizeWorkflow(outputDir, &mf, time.Time{}, io.Discard, errors.New("trend render failed"))
	if err == nil {
		t.Fatal("expected workflow finalization to return the step error")
	}

	stored, readErr := workflowManifestFromJSON(filepath.Join(outputDir, "manifest.json"))
	if readErr != nil {
		t.Fatalf("read manifest: %v", readErr)
	}
	if stored.Status != "failed" {
		t.Fatalf("expected failed manifest status, got %q", stored.Status)
	}
	if stored.WorkflowSummary.Findings == nil || stored.WorkflowSummary.Recommendations == nil || stored.WorkflowSummary.Warnings == nil {
		t.Fatalf("expected non-nil workflow_summary arrays, got %#v", stored.WorkflowSummary)
	}
	if len(stored.WorkflowSummary.Findings) == 0 && len(stored.WorkflowSummary.Recommendations) == 0 {
		t.Fatal("expected workflow_summary to keep earlier successful compare/trend rollups")
	}
}

func TestWorkflowFinalizeRecordsSummaryWarningsWithoutFailingWrites(t *testing.T) {
	outputDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(outputDir, "compare"), 0o755); err != nil {
		t.Fatalf("mkdir compare dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "compare", "week2_vs_week1.json"), []byte("not-json"), 0o644); err != nil {
		t.Fatalf("write malformed compare json: %v", err)
	}

	mf := workflow.Manifest{
		WorkflowName: "warning-test",
		Status:       "success",
		Steps: []workflow.StepRecord{
			{Kind: "compare", Name: "week2_vs_week1", Status: "success", Artifacts: []string{"compare/week2_vs_week1.json"}},
		},
	}

	if err := finalizeWorkflow(outputDir, &mf, time.Time{}, io.Discard, nil); err != nil {
		t.Fatalf("finalize workflow: %v", err)
	}

	stored, err := workflowManifestFromJSON(filepath.Join(outputDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if stored.Status != "success" {
		t.Fatalf("expected manifest status success, got %q", stored.Status)
	}
	wantWarning := `compare step "week2_vs_week1": invalid JSON artifact "compare/week2_vs_week1.json"`
	if len(stored.WorkflowSummary.Warnings) != 1 || stored.WorkflowSummary.Warnings[0] != wantWarning {
		t.Fatalf("warnings = %#v, want [%q]", stored.WorkflowSummary.Warnings, wantWarning)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "index.html")); err != nil {
		t.Fatalf("expected index.html to be written: %v", err)
	}
}

func TestWorkflowRunWithoutCompareOrTrendWritesEmptyWorkflowSummary(t *testing.T) {
	binlogDir := t.TempDir()
	fixtureSrc := filepath.Join("testdata", "minimal.binlog")
	src, err := os.ReadFile(fixtureSrc)
	if err != nil {
		t.Fatalf("read binlog fixture: %v", err)
	}
	for _, suffix := range []string{"000001", "000002"} {
		dst := filepath.Join(binlogDir, "mysql-bin."+suffix)
		if err := os.WriteFile(dst, src, 0o644); err != nil {
			t.Fatalf("write binlog fixture %s: %v", dst, err)
		}
	}

	rootDir := t.TempDir()
	outputDir := filepath.Join(rootDir, "artifacts")
	plan := strings.ReplaceAll(`
version: 1
workflow:
  name: analyze-only
  output_dir: OUTPUT_PLACEHOLDER
defaults:
  input:
    from_dir: BINLOG_PLACEHOLDER
    prefix: mysql-bin.
  analyze:
    format: json
  snapshot:
    save: true
windows:
  - name: week1
    start: 2025-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
`, "OUTPUT_PLACEHOLDER", outputDir)
	plan = strings.ReplaceAll(plan, "BINLOG_PLACEHOLDER", binlogDir)

	planPath := filepath.Join(t.TempDir(), "plan.yaml")
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", t.TempDir()})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	mf, err := workflowManifestFromJSON(filepath.Join(outputDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if len(mf.WorkflowSummary.Findings) != 0 {
		t.Fatalf("findings len = %d, want 0", len(mf.WorkflowSummary.Findings))
	}
	if len(mf.WorkflowSummary.Recommendations) != 0 {
		t.Fatalf("recommendations len = %d, want 0", len(mf.WorkflowSummary.Recommendations))
	}
	if len(mf.WorkflowSummary.Warnings) != 0 {
		t.Fatalf("warnings len = %d, want 0", len(mf.WorkflowSummary.Warnings))
	}
}

func TestWorkflowResumeRefreshesWorkflowSummary(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	runCmd := NewRootCommand()
	runCmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	runCmd.SilenceUsage = true
	runCmd.SilenceErrors = true
	if err := runCmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	manifestPath := filepath.Join(outputDir, "manifest.json")
	before, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		t.Fatalf("read manifest before resume: %v", err)
	}
	before.WorkflowSummary = workflow.WorkflowSummary{
		Findings: []workflow.WorkflowFinding{{
			Kind:              "stale_finding",
			Title:             "Stale finding",
			Summary:           "should be replaced on resume",
			SourceStepKind:    "compare",
			SourceStepName:    "week2_vs_week1",
			SourceReportPath:  "compare/week2_vs_week1.json",
			SourceReportLabel: "week2_vs_week1",
		}},
		Recommendations: []workflow.WorkflowRecommendation{{
			Kind:              "stale_recommendation",
			Priority:          "high",
			Title:             "Stale recommendation",
			Summary:           "should be replaced on resume",
			SourceStepKind:    "compare",
			SourceStepName:    "week2_vs_week1",
			SourceReportPath:  "compare/week2_vs_week1.json",
			SourceReportLabel: "week2_vs_week1",
		}},
		Warnings: []string{"stale warning"},
	}
	if err := workflow.WriteManifest(manifestPath, before); err != nil {
		t.Fatalf("write stale manifest: %v", err)
	}

	resumeCmd := NewRootCommand()
	resumeCmd.SetArgs([]string{"workflow", "resume", outputDir, "--snapshot-dir", snapshotDir, "--rerun", "compare:week2_vs_week1"})
	resumeCmd.SilenceUsage = true
	resumeCmd.SilenceErrors = true
	if err := resumeCmd.Execute(); err != nil {
		t.Fatalf("workflow resume: %v", err)
	}

	after, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		t.Fatalf("read manifest after resume: %v", err)
	}
	if after.Mode != "resume" {
		t.Fatalf("expected mode=resume, got %q", after.Mode)
	}
	if after.Attempt != 2 {
		t.Fatalf("expected attempt=2, got %d", after.Attempt)
	}
	if len(after.WorkflowSummary.Findings) != 1 ||
		after.WorkflowSummary.Findings[0].Kind != "comparability_guard" ||
		after.WorkflowSummary.Findings[0].SourceStepKind != "compare" ||
		after.WorkflowSummary.Findings[0].SourceStepName != "week2_vs_week1" {
		t.Fatalf("expected refreshed workflow_summary to propagate one compare guard, got %#v", after.WorkflowSummary.Findings)
	}
	if len(after.WorkflowSummary.Recommendations) != 0 {
		t.Fatalf("expected refreshed workflow_summary.recommendations to be empty, got %#v", after.WorkflowSummary.Recommendations)
	}
	if len(after.WorkflowSummary.Warnings) != 0 {
		t.Fatalf("expected refreshed workflow_summary.warnings to be empty, got %#v", after.WorkflowSummary.Warnings)
	}
}

// --- Workflow resume tests ---

// TestWorkflowResumeReusesSuccessfulSteps tests that resume reuses previously
// successful analyze steps and only reruns failed compare/trend.
func TestWorkflowResumeReusesSuccessfulSteps(t *testing.T) {
	binlogDir, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	// Step 1: Run the full workflow successfully
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("initial workflow run: %v", err)
	}

	// Step 2: Delete compare and trend artifacts to simulate a late-stage failure
	for _, path := range []string{
		filepath.Join(outputDir, "compare", "week2_vs_week1.json"),
		filepath.Join(outputDir, "compare", "week2_vs_week1.html"),
		filepath.Join(outputDir, "trend", "weekly_series.json"),
		filepath.Join(outputDir, "trend", "weekly_series.html"),
	} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove artifact %s: %v", path, err)
		}
	}

	// Step 3: Resume the workflow
	cmd2 := NewRootCommand()
	cmd2.SetArgs([]string{"workflow", "resume", outputDir, "--snapshot-dir", snapshotDir})
	cmd2.SilenceUsage = true
	cmd2.SilenceErrors = true

	if err := cmd2.Execute(); err != nil {
		t.Fatalf("workflow resume: %v", err)
	}

	// Verify manifest shows resume mode
	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if mf.Mode != "resume" {
		t.Fatalf("expected mode=resume, got %q", mf.Mode)
	}
	if mf.Attempt != 2 {
		t.Fatalf("expected attempt=2, got %d", mf.Attempt)
	}
	if mf.Status != "success" {
		t.Fatalf("expected status=success, got %q", mf.Status)
	}

	// Verify reused analyze steps
	for _, step := range mf.Steps {
		if step.Kind == "analyze" {
			if step.Execution != "reused" {
				t.Fatalf("expected analyze step %q to be reused, got %q", step.Name, step.Execution)
			}
		}
		if step.Kind == "compare" || step.Kind == "trend" {
			if step.Execution != "executed" {
				t.Fatalf("expected %s step %q to be executed, got %q", step.Kind, step.Name, step.Execution)
			}
		}
	}

	// Verify restored artifacts exist
	for _, path := range []string{
		filepath.Join(outputDir, "compare", "week2_vs_week1.json"),
		filepath.Join(outputDir, "trend", "weekly_series.json"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected restored artifact at %s: %v", path, err)
		}
	}

	_ = binlogDir
}

// TestWorkflowResumeRefusesLegacyManifest tests that resume refuses a manifest
// without manifest_version.
func TestWorkflowResumeRefusesLegacyManifest(t *testing.T) {
	outputDir := t.TempDir()

	// Write a legacy manifest without manifest_version
	legacyManifest := `{
  "workflow_name": "old-run",
  "status": "success",
  "steps": []
}`
	manifestPath := filepath.Join(outputDir, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte(legacyManifest), 0o644); err != nil {
		t.Fatalf("write legacy manifest: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "resume", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected resume to refuse legacy manifest")
	}
	if !strings.Contains(err.Error(), "legacy") {
		t.Fatalf("expected legacy format error, got: %v", err)
	}
}

// TestWorkflowResumeRefusesPlanHashMismatch tests that resume refuses when the
// plan file has changed since the original run.
func TestWorkflowResumeRefusesPlanHashMismatch(t *testing.T) {
	outputDir := t.TempDir()

	// Write a valid plan file inside the workflow root
	planPath := filepath.Join(outputDir, "plan.yaml")
	plan := `
version: 1
workflow:
  name: hash-test
  output_dir: /tmp/out
defaults:
  input:
    from_dir: /tmp/mysql
    prefix: mysql-bin.
windows:
  - name: w1
    start: 2025-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
`
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	// Write a v2 manifest with a wrong plan_sha256
	manifest := `{
  "manifest_version": 2,
  "mode": "run",
  "attempt": 1,
  "workflow_name": "hash-test",
  "plan_path": "` + strings.ReplaceAll(planPath, `\`, `\\`) + `",
  "plan_sha256": "deadbeef00",
  "resolved_input_files": ["/tmp/mysql-bin.000001"],
  "status": "success",
  "steps": []
}`
	if err := os.WriteFile(filepath.Join(outputDir, "manifest.json"), []byte(manifest), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "resume", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected resume to refuse plan hash mismatch")
	}
	if !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("expected hash mismatch error, got: %v", err)
	}
}

// TestWorkflowResumeRefusesUnknownRerunSelector tests that resume refuses an
// unknown --rerun selector.
func TestWorkflowResumeRefusesUnknownRerunSelector(t *testing.T) {
	binlogDir, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	// Run the full workflow first
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	if err := cmd.Execute(); err != nil {
		t.Fatalf("initial run: %v", err)
	}

	// Resume with unknown selector
	cmd2 := NewRootCommand()
	cmd2.SetArgs([]string{"workflow", "resume", outputDir, "--snapshot-dir", snapshotDir, "--rerun", "analyze:nonexistent"})
	cmd2.SilenceUsage = true
	cmd2.SilenceErrors = true

	err := cmd2.Execute()
	if err == nil {
		t.Fatal("expected resume to refuse unknown selector")
	}
	if !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("expected unknown selector error, got: %v", err)
	}

	_ = binlogDir
}

// TestWorkflowResumeNothingToDo tests that resume after a fully successful
// run exits 0 with one stderr line and no Usage dump.
func TestWorkflowResumeNothingToDo(t *testing.T) {
	binlogDir, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	if err := cmd.Execute(); err != nil {
		t.Fatalf("initial run: %v", err)
	}

	cmd2 := NewRootCommand()
	cmd2.SetArgs([]string{"workflow", "resume", outputDir, "--snapshot-dir", snapshotDir})

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd2.Execute() })
	if err != nil {
		t.Fatalf("expected nothing-to-resume to exit 0, got: %v", err)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if strings.TrimSpace(stderr) != "nothing to resume" {
		t.Fatalf("expected one stderr line %q, got %q", "nothing to resume", stderr)
	}
	if strings.Contains(stderr, "Usage:") || strings.Contains(stdout, "Usage:") {
		t.Fatalf("did not expect Usage dump, stdout=%q stderr=%q", stdout, stderr)
	}

	_ = binlogDir
}

// TestWorkflowResumeExplicitRerun tests that --rerun analyze:week2 invalidates
// dependent compare and trend jobs.
func TestWorkflowResumeExplicitRerun(t *testing.T) {
	binlogDir, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	// Run the full workflow first
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	if err := cmd.Execute(); err != nil {
		t.Fatalf("initial run: %v", err)
	}

	// Resume with explicit --rerun analyze:week2
	cmd2 := NewRootCommand()
	cmd2.SetArgs([]string{"workflow", "resume", outputDir, "--snapshot-dir", snapshotDir, "--rerun", "analyze:week2"})
	cmd2.SilenceUsage = true
	cmd2.SilenceErrors = true

	if err := cmd2.Execute(); err != nil {
		t.Fatalf("workflow resume with rerun: %v", err)
	}

	// Verify manifest
	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if mf.Mode != "resume" {
		t.Fatalf("expected mode=resume, got %q", mf.Mode)
	}
	if mf.Attempt != 2 {
		t.Fatalf("expected attempt=2, got %d", mf.Attempt)
	}

	// week1 analyze should be reused, week2 should be executed
	for _, step := range mf.Steps {
		if step.Kind == "analyze" && step.Name == "week1" {
			if step.Execution != "reused" {
				t.Fatalf("expected week1 to be reused, got %q", step.Execution)
			}
		}
		if step.Kind == "analyze" && step.Name == "week2" {
			if step.Execution != "executed" {
				t.Fatalf("expected week2 to be executed, got %q", step.Execution)
			}
		}
		if step.Kind == "compare" {
			if step.Execution != "executed" {
				t.Fatalf("expected compare to be executed (depends on week2), got %q", step.Execution)
			}
		}
		if step.Kind == "trend" {
			if step.Execution != "executed" {
				t.Fatalf("expected trend to be executed (depends on week2), got %q", step.Execution)
			}
		}
	}

	_ = binlogDir
}

func TestWorkflowValidateWarnsOnPlaceholderAndMissingFromDir(t *testing.T) {
	planDir := t.TempDir()
	planPath := filepath.Join(planDir, "placeholder-plan.yaml")
	plan := `
version: 1
workflow:
  name: placeholder-sample
  output_dir: ./artifacts
defaults:
  input:
    from_dir: PLACEHOLDER/binlog
    prefix: mysql-bin.
windows:
  - name: week1
    start: 2025-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
`
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "validate", planPath, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("expected validate to stay valid with warnings, got: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	var decoded struct {
		Valid    bool     `json:"valid"`
		Warnings []string `json:"warnings"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if !decoded.Valid {
		t.Fatalf("expected valid=true, got %#v", decoded)
	}
	joined := strings.Join(decoded.Warnings, "\n")
	if !strings.Contains(joined, "placeholder") || !strings.Contains(joined, "does not exist") {
		t.Fatalf("expected placeholder and missing from_dir warnings, got %v", decoded.Warnings)
	}
}

func TestWorkflowStatusFailedDiscoveryIsNotComplete(t *testing.T) {
	planDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "artifacts")
	plan := strings.ReplaceAll(`
version: 1
workflow:
  name: missing-input
  output_dir: PLACEHOLDER
defaults:
  input:
    from_dir: /nonexistent/path/mysql
    prefix: mysql-bin.
windows:
  - name: week1
    start: 2025-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
`, "PLACEHOLDER", outputDir)
	planPath := filepath.Join(planDir, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	runCmd := NewRootCommand()
	runCmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir})
	runCmd.SilenceUsage = true
	runCmd.SilenceErrors = true
	if err := runCmd.Execute(); err == nil {
		t.Fatal("expected workflow run to fail when from_dir is missing")
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", outputDir, "--format", "json"})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true
	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	var decoded struct {
		Status       string `json:"status"`
		RuntimeState string `json:"runtime_state"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if decoded.Status != "failed" {
		t.Fatalf("expected status failed, got %#v", decoded)
	}
	if decoded.RuntimeState == "complete" {
		t.Fatalf("runtime_state must not be complete after failed discovery, got %#v\n%s", decoded, stdout)
	}
}

func TestIncidentYAMLRunsFromRepoRoot(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	planPath := filepath.Join(repoRoot, "incident.yaml")
	if _, err := os.Stat(planPath); err != nil {
		t.Fatalf("expected shipped incident.yaml at repo root: %v", err)
	}
	t.Chdir(repoRoot)

	outputDir := t.TempDir()
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", "incident.yaml", "--output-dir", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run incident.yaml: %v", err)
	}

	for _, name := range []string{"week1", "week2"} {
		if _, err := os.Stat(filepath.Join(outputDir, "analyze", name+".json")); err != nil {
			t.Fatalf("expected analyze artifact %s: %v", name, err)
		}
	}

	resumeCmd := NewRootCommand()
	resumeCmd.SetArgs([]string{"workflow", "resume", outputDir})
	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return resumeCmd.Execute() })
	if err != nil {
		t.Fatalf("resume after successful incident.yaml run: %v", err)
	}
	if strings.TrimSpace(stderr) != "nothing to resume" {
		t.Fatalf("expected nothing to resume, stdout=%q stderr=%q", stdout, stderr)
	}
}

func TestWorkflowRelativeOutputRootRemainsResumable(t *testing.T) {
	_, originalOutputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")
	planContent, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read plan: %v", err)
	}

	workDir := t.TempDir()
	t.Chdir(workDir)
	relativeOutputDir := filepath.Join("artifacts", "relative")
	// Replace the generated absolute output root while keeping the input fixture absolute.
	updatedPlanContent := strings.Replace(string(planContent), originalOutputDir, relativeOutputDir, 1)
	if updatedPlanContent == string(planContent) {
		t.Fatal("relative-output plan did not replace the generated output root")
	}
	planContent = []byte(updatedPlanContent)
	relativePlanPath := filepath.Join(workDir, "plan.yaml")
	if err := os.WriteFile(relativePlanPath, planContent, 0o644); err != nil {
		t.Fatalf("write relative-output plan: %v", err)
	}

	runCmd := NewRootCommand()
	runCmd.SetArgs([]string{"workflow", "run", relativePlanPath, "--snapshot-dir", snapshotDir})
	runCmd.SilenceUsage = true
	runCmd.SilenceErrors = true
	if err := runCmd.Execute(); err != nil {
		t.Fatalf("workflow run with relative output root: %v", err)
	}

	outputDir := filepath.Join(workDir, relativeOutputDir)
	manifest, err := workflowManifestFromJSON(filepath.Join(outputDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if manifest.PlanPath != filepath.Join(outputDir, "plan.yaml") {
		t.Fatalf("manifest plan_path = %q, want canonical %q", manifest.PlanPath, filepath.Join(outputDir, "plan.yaml"))
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", relativeOutputDir, "--format", "json"})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true
	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status with relative output root: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty status stderr, got %q", stderr)
	}
	var status struct {
		Resumable   bool   `json:"resumable"`
		ResumeError string `json:"resume_error"`
	}
	if err := json.Unmarshal([]byte(stdout), &status); err != nil {
		t.Fatalf("decode status: %v\n%s", err, stdout)
	}
	if !status.Resumable {
		t.Fatalf("expected relative-output workflow to be resumable, got %q", status.ResumeError)
	}

	resumeCmd := NewRootCommand()
	resumeCmd.SetArgs([]string{"workflow", "resume", relativeOutputDir, "--snapshot-dir", snapshotDir, "--rerun", "analyze:week2"})
	resumeCmd.SilenceUsage = true
	resumeCmd.SilenceErrors = true
	if err := resumeCmd.Execute(); err != nil {
		t.Fatalf("workflow resume with relative output root: %v", err)
	}

	manifest, err = workflowManifestFromJSON(filepath.Join(outputDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read resumed manifest: %v", err)
	}
	if manifest.Attempt != 2 {
		t.Fatalf("resumed manifest attempt = %d, want 2", manifest.Attempt)
	}
	foundWeek2 := false
	for _, step := range manifest.Steps {
		if step.Kind == "analyze" && step.Name == "week2" {
			foundWeek2 = true
			if step.Execution != "executed" {
				t.Fatalf("week2 execution = %q, want executed", step.Execution)
			}
		}
	}
	if !foundWeek2 {
		t.Fatal("resumed manifest did not include analyze:week2")
	}
}

func TestWorkflowValidateTextOutputForValidPlan(t *testing.T) {
	_, _, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "validate", planPath})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow validate: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	for _, token := range []string{"Workflow plan valid", "multi-window-test", "windows: 2", "compare jobs: 1", "trend jobs: 1"} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("expected %q in stdout, got %q", token, stdout)
		}
	}
}

func TestWorkflowValidateJSONOutputForValidPlan(t *testing.T) {
	_, _, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "validate", planPath, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow validate: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	var decoded struct {
		Valid        bool   `json:"valid"`
		WorkflowName string `json:"workflow_name"`
		Windows      int    `json:"windows"`
		CompareJobs  int    `json:"compare_jobs"`
		TrendJobs    int    `json:"trend_jobs"`
		OutputDir    string `json:"output_dir"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if !decoded.Valid || decoded.WorkflowName != "multi-window-test" || decoded.Windows != 2 || decoded.CompareJobs != 1 || decoded.TrendJobs != 1 {
		t.Fatalf("unexpected validate json payload: %#v", decoded)
	}
}

func TestWorkflowValidateTextOutputForInvalidPlan(t *testing.T) {
	planDir := t.TempDir()
	planPath := filepath.Join(planDir, "invalid-plan.yaml")
	plan := `
version: 1
workflow:
  name: invalid
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
compare:
  - name: drift
    current: incident
    baseline: baseline
    formats: [json]
`
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "validate", planPath})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err == nil {
		t.Fatal("expected workflow validate to fail")
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	for _, token := range []string{"Workflow plan invalid", `compare "drift" references unknown current window "incident"`} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("expected %q in stdout, got %q", token, stdout)
		}
	}
}

func TestWorkflowValidateJSONOutputForInvalidPlan(t *testing.T) {
	planDir := t.TempDir()
	planPath := filepath.Join(planDir, "invalid-plan.yaml")
	plan := `
version: 1
workflow:
  name: invalid
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
compare:
  - name: drift
    current: incident
    baseline: baseline
    formats: [json]
`
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "validate", planPath, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err == nil {
		t.Fatal("expected workflow validate to fail")
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	var decoded struct {
		Valid bool   `json:"valid"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if decoded.Valid {
		t.Fatalf("expected valid=false, got %#v", decoded)
	}
	if !strings.Contains(decoded.Error, `compare "drift" references unknown current window "incident"`) {
		t.Fatalf("unexpected validate json error: %#v", decoded)
	}
}

func TestWorkflowValidateRejectsUnsupportedFormat(t *testing.T) {
	_, _, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "validate", planPath, "--format", "yaml"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected invalid format error")
	}
	if !strings.Contains(err.Error(), "unsupported workflow validate format") {
		t.Fatalf("expected invalid format error, got %v", err)
	}
}

func TestWorkflowDescribeTextOutputForValidPlan(t *testing.T) {
	_, _, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "describe", planPath})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow describe: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	for _, token := range []string{
		"Workflow: multi-window-test",
		"Output Root:",
		"Snapshot Save: true",
		"Analyze Windows",
		"Compare Jobs",
		"Trend Jobs",
		"analyze/week1.json",
		"compare/week2_vs_week1.json",
		"trend/weekly_series.html",
	} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("expected %q in stdout, got %q", token, stdout)
		}
	}
}

func TestWorkflowDescribeJSONOutputForValidPlan(t *testing.T) {
	_, _, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "describe", planPath, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow describe: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	var decoded struct {
		WorkflowName string `json:"workflow_name"`
		OutputDir    string `json:"output_dir"`
		SnapshotSave bool   `json:"snapshot_save"`
		Windows      []struct {
			Name         string   `json:"name"`
			Artifacts    []string `json:"artifacts"`
			SnapshotName string   `json:"snapshot_name"`
		} `json:"windows"`
		Compare []struct {
			Name      string   `json:"name"`
			Current   string   `json:"current"`
			Baseline  string   `json:"baseline"`
			Artifacts []string `json:"artifacts"`
		} `json:"compare"`
		Trend []struct {
			Name      string   `json:"name"`
			Snapshots []string `json:"snapshots"`
			Artifacts []string `json:"artifacts"`
		} `json:"trend"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if decoded.WorkflowName != "multi-window-test" || !decoded.SnapshotSave || len(decoded.Windows) != 2 || len(decoded.Compare) != 1 || len(decoded.Trend) != 1 {
		t.Fatalf("unexpected describe json payload: %#v", decoded)
	}
}

func TestWorkflowDescribeRejectsInvalidPlan(t *testing.T) {
	planDir := t.TempDir()
	planPath := filepath.Join(planDir, "invalid-plan.yaml")
	plan := `
version: 1
workflow:
  name: invalid
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
compare:
  - name: drift
    current: incident
    baseline: baseline
    formats: [json]
`
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "describe", planPath})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err == nil {
		t.Fatal("expected workflow describe to fail")
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, `compare "drift" references unknown current window "incident"`) {
		t.Fatalf("expected validation error in stdout, got %q", stdout)
	}
}

func TestWorkflowDescribeRejectsUnsupportedFormat(t *testing.T) {
	_, _, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "describe", planPath, "--format", "yaml"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected invalid format error")
	}
	if !strings.Contains(err.Error(), "unsupported workflow describe format") {
		t.Fatalf("expected invalid format error, got %v", err)
	}
}

func TestWorkflowStatusTextOutputForHealthyRoot(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", outputDir})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	for _, token := range []string{"Workflow Status:", "Runtime State: complete", "Resumable: yes", "Resume Preview", "reuse analyze:week1"} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("expected %q in stdout, got %q", token, stdout)
		}
	}
}

func TestWorkflowStatusJSONOutputForHealthyRoot(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", outputDir, "--format", "json"})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	var decoded struct {
		WorkflowName  string `json:"workflow_name"`
		RuntimeState  string `json:"runtime_state"`
		Resumable     bool   `json:"resumable"`
		ResumePreview []struct {
			Action string `json:"action"`
		} `json:"resume_preview"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if decoded.WorkflowName != "multi-window-test" || decoded.RuntimeState != "complete" || !decoded.Resumable || len(decoded.ResumePreview) != 4 {
		t.Fatalf("unexpected status json payload: %#v", decoded)
	}
}

func TestWorkflowStatusJSONOutputIncludesWorkflowSummary(t *testing.T) {
	outputDir := t.TempDir()
	manifest := workflow.Manifest{
		ManifestVersion: 2,
		WorkflowName:    "status-summary-json",
		Status:          "success",
		WorkflowSummary: workflow.WorkflowSummary{
			Findings: []workflow.WorkflowFinding{{
				Kind:              "pattern_driver",
				Title:             "Top pattern driver",
				Summary:           "refunds.create drove most row growth",
				SourceStepKind:    "compare",
				SourceStepName:    "week2_vs_week1",
				SourceReportPath:  "compare/week2_vs_week1.html",
				SourceReportLabel: "week2_vs_week1",
			}},
			Recommendations: []workflow.WorkflowRecommendation{{
				Kind:                "check_pattern_driver",
				Priority:            "high",
				Title:               "Check pattern driver",
				Summary:             "Review the source report.",
				RelatedFindingKinds: []string{"pattern_driver"},
				SourceStepKind:      "compare",
				SourceStepName:      "week2_vs_week1",
				SourceReportPath:    "compare/week2_vs_week1.html",
				SourceReportLabel:   "week2_vs_week1",
			}},
			Warnings: []string{"compare step \"week2_vs_week1\": missing JSON artifact"},
		},
		Steps: []workflow.StepRecord{},
	}
	if err := workflow.WriteManifest(filepath.Join(outputDir, "manifest.json"), manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", outputDir, "--format", "json"})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	var decoded struct {
		WorkflowSummary workflow.WorkflowSummary `json:"workflow_summary"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if len(decoded.WorkflowSummary.Findings) != 1 {
		t.Fatalf("expected 1 finding, got %#v", decoded.WorkflowSummary)
	}
	if len(decoded.WorkflowSummary.Recommendations) != 1 {
		t.Fatalf("expected 1 recommendation, got %#v", decoded.WorkflowSummary)
	}
	if len(decoded.WorkflowSummary.Warnings) != 1 {
		t.Fatalf("expected 1 warning, got %#v", decoded.WorkflowSummary)
	}
}

func TestWorkflowStatusShowsIncompleteWhenArtifactMissing(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}
	if err := os.Remove(filepath.Join(outputDir, "compare", "week2_vs_week1.json")); err != nil {
		t.Fatalf("remove compare artifact: %v", err)
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", outputDir})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	for _, token := range []string{"Runtime State: incomplete", "rerun compare:week2_vs_week1", "artifact file missing"} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("expected %q in stdout, got %q", token, stdout)
		}
	}
}

func TestWorkflowStatusLegacyManifestRemainsInspectable(t *testing.T) {
	outputDir := t.TempDir()
	legacyManifest := `{
  "workflow_name": "legacy-run",
  "status": "failed",
  "steps": []
}`
	if err := os.WriteFile(filepath.Join(outputDir, "manifest.json"), []byte(legacyManifest), 0o644); err != nil {
		t.Fatalf("write legacy manifest: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "status", outputDir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	var decoded struct {
		ManifestVersion int                      `json:"manifest_version"`
		Resumable       bool                     `json:"resumable"`
		ResumeError     string                   `json:"resume_error"`
		WorkflowSummary workflow.WorkflowSummary `json:"workflow_summary"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if decoded.ManifestVersion != 0 || decoded.Resumable {
		t.Fatalf("unexpected legacy status payload: %#v", decoded)
	}
	if !strings.Contains(decoded.ResumeError, "legacy format") {
		t.Fatalf("expected legacy format error, got %#v", decoded)
	}
	if decoded.WorkflowSummary.Findings == nil || decoded.WorkflowSummary.Recommendations == nil || decoded.WorkflowSummary.Warnings == nil {
		t.Fatalf("expected normalized workflow_summary arrays, got %#v", decoded.WorkflowSummary)
	}
}

func TestWorkflowStatusTextOutputIncludesWorkflowSummarySections(t *testing.T) {
	outputDir := t.TempDir()
	manifest := workflow.Manifest{
		ManifestVersion: 2,
		WorkflowName:    "status-summary-text",
		Status:          "success",
		WorkflowSummary: workflow.WorkflowSummary{
			Findings: []workflow.WorkflowFinding{{
				Kind:              "pattern_driver",
				Title:             "Top pattern driver",
				Summary:           "refunds.create drove most row growth",
				SourceStepKind:    "compare",
				SourceStepName:    "week2_vs_week1",
				SourceReportPath:  "compare/week2_vs_week1.html",
				SourceReportLabel: "week2_vs_week1",
			}},
			Recommendations: []workflow.WorkflowRecommendation{{
				Kind:                "check_pattern_driver",
				Priority:            "high",
				Title:               "Check pattern driver",
				Summary:             "Review the source report.",
				RelatedFindingKinds: []string{"pattern_driver"},
				SourceStepKind:      "compare",
				SourceStepName:      "week2_vs_week1",
				SourceReportPath:    "compare/week2_vs_week1.html",
				SourceReportLabel:   "week2_vs_week1",
			}},
			Warnings: []string{"compare step \"week2_vs_week1\": missing JSON artifact"},
		},
		Steps: []workflow.StepRecord{},
	}
	if err := workflow.WriteManifest(filepath.Join(outputDir, "manifest.json"), manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", outputDir})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	for _, token := range []string{
		"Workflow Recommendations",
		"Workflow Findings",
		"Workflow Summary Warnings",
		"Check pattern driver",
		"Top pattern driver",
		`compare step "week2_vs_week1": missing JSON artifact`,
	} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("expected %q in stdout, got %q", token, stdout)
		}
	}
}

func TestWorkflowStatusTextOutputOmitsEmptyWorkflowSummarySections(t *testing.T) {
	outputDir := t.TempDir()
	manifest := workflow.Manifest{
		ManifestVersion: 2,
		WorkflowName:    "status-summary-empty-sections",
		Status:          "success",
		WorkflowSummary: workflow.WorkflowSummary{
			Findings:        []workflow.WorkflowFinding{},
			Recommendations: []workflow.WorkflowRecommendation{},
			Warnings:        []string{"compare step \"week2_vs_week1\": missing JSON artifact"},
		},
		Steps: []workflow.StepRecord{},
	}
	if err := workflow.WriteManifest(filepath.Join(outputDir, "manifest.json"), manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", outputDir})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "Workflow Summary Warnings") {
		t.Fatalf("expected warnings section, got %q", stdout)
	}
	for _, token := range []string{"Workflow Recommendations", "Workflow Findings"} {
		if strings.Contains(stdout, token) {
			t.Fatalf("expected %q to be omitted, got %q", token, stdout)
		}
	}
}

func TestWorkflowStatusRejectsMissingManifest(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "status", t.TempDir()})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected missing manifest error")
	}
	if !strings.Contains(err.Error(), "manifest.json") {
		t.Fatalf("expected manifest error, got %v", err)
	}
}

func TestWorkflowStatusRejectsUnsupportedFormat(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "status", t.TempDir(), "--format", "yaml"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected invalid format error")
	}
	if !strings.Contains(err.Error(), "unsupported workflow status format") {
		t.Fatalf("expected invalid format error, got %v", err)
	}
}

func TestWorkflowStatusShowsPlanLoadFailureAsNonResumable(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	manifestPath := filepath.Join(outputDir, "manifest.json")
	manifest, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	// Overwrite the trusted plan.yaml with invalid YAML to trigger load failure
	brokenPlanPath := filepath.Join(outputDir, "plan.yaml")
	if err := os.WriteFile(brokenPlanPath, []byte("not: [valid"), 0o644); err != nil {
		t.Fatalf("write broken plan: %v", err)
	}
	manifest.PlanPath = brokenPlanPath
	manifest.PlanSHA256, err = computeFileSHA256(brokenPlanPath)
	if err != nil {
		t.Fatalf("hash broken plan: %v", err)
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(manifestPath, append(manifestBytes, '\n'), 0o644); err != nil {
		t.Fatalf("rewrite manifest: %v", err)
	}

	t.Run("text", func(t *testing.T) {
		statusCmd := NewRootCommand()
		statusCmd.SetArgs([]string{"workflow", "status", outputDir})
		statusCmd.SilenceUsage = true
		statusCmd.SilenceErrors = true

		stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
		if err != nil {
			t.Fatalf("workflow status: %v", err)
		}
		if strings.TrimSpace(stderr) != "" {
			t.Fatalf("expected empty stderr, got %q", stderr)
		}
		for _, token := range []string{"Resumable: no", "Reason:", "load plan"} {
			if !strings.Contains(stdout, token) {
				t.Fatalf("expected %q in stdout, got %q", token, stdout)
			}
		}
		if strings.Contains(stdout, "Resume Preview") {
			t.Fatalf("expected preview to be omitted when plan load fails, got %q", stdout)
		}
	})

	t.Run("json", func(t *testing.T) {
		statusCmd := NewRootCommand()
		statusCmd.SetArgs([]string{"workflow", "status", outputDir, "--format", "json"})
		statusCmd.SilenceUsage = true
		statusCmd.SilenceErrors = true

		stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
		if err != nil {
			t.Fatalf("workflow status: %v", err)
		}
		if strings.TrimSpace(stderr) != "" {
			t.Fatalf("expected empty stderr, got %q", stderr)
		}
		var decoded struct {
			Resumable     bool   `json:"resumable"`
			ResumeError   string `json:"resume_error"`
			ResumePreview []any  `json:"resume_preview"`
		}
		if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
			t.Fatalf("decode json output: %v\n%s", err, stdout)
		}
		if decoded.Resumable {
			t.Fatalf("expected resumable false, got %#v", decoded)
		}
		if !strings.Contains(decoded.ResumeError, "load plan") {
			t.Fatalf("expected load plan error, got %#v", decoded)
		}
		if len(decoded.ResumePreview) != 0 {
			t.Fatalf("expected preview to be omitted, got %#v", decoded)
		}
	})
}

func TestWorkflowCleanTextDryRun(t *testing.T) {
	outputDir := setupWorkflowCleanCommandFixture(t)

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "clean", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow clean: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	for _, token := range []string{"Workflow Clean:", "Mode: dry-run", "Include Snapshots: no", "compare/stale-report.json"} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("expected %q in stdout, got %q", token, stdout)
		}
	}
}

func TestWorkflowCleanJSONDryRun(t *testing.T) {
	outputDir := setupWorkflowCleanCommandFixture(t)

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "clean", outputDir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow clean: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	var decoded struct {
		WorkflowName     string   `json:"workflow_name"`
		Mode             string   `json:"mode"`
		IncludeSnapshots bool     `json:"include_snapshots"`
		ArtifactOrphans  []string `json:"artifact_orphans"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if decoded.WorkflowName != "cleanup-test" || decoded.Mode != "dry-run" || decoded.IncludeSnapshots {
		t.Fatalf("unexpected clean json payload: %#v", decoded)
	}
	assertStringSliceContains(t, decoded.ArtifactOrphans, "compare/stale-report.json")
}

func TestWorkflowCleanApplyDeletesCandidates(t *testing.T) {
	outputDir := setupWorkflowCleanCommandFixture(t)

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "clean", outputDir, "--apply"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow clean apply: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "Mode: apply") {
		t.Fatalf("expected apply mode in stdout, got %q", stdout)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "compare", "stale-report.json")); !os.IsNotExist(err) {
		t.Fatalf("expected stale compare report to be deleted, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "snapshots", "stale-snapshot.json")); err != nil {
		t.Fatalf("expected stale snapshot to remain without include flag: %v", err)
	}
}

func TestWorkflowCleanApplyIncludesSnapshotsFromDefaultSnapshotDir(t *testing.T) {
	_, outputDir, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")
	home := t.TempDir()
	t.Setenv("HOME", home)

	runCmd := NewRootCommand()
	runCmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir})
	runCmd.SilenceUsage = true
	runCmd.SilenceErrors = true
	if err := runCmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	manifestPath := filepath.Join(outputDir, "manifest.json")
	mf, err := workflowManifestFromJSON(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	staleSnapshotPath := filepath.Join(mf.SnapshotDir, "stale-snapshot.json")
	if err := os.WriteFile(staleSnapshotPath, []byte("{}"), 0o644); err != nil {
		t.Fatalf("write stale snapshot: %v", err)
	}

	cleanCmd := NewRootCommand()
	cleanCmd.SetArgs([]string{"workflow", "clean", outputDir, "--apply", "--include-snapshots"})
	cleanCmd.SilenceUsage = true
	cleanCmd.SilenceErrors = true

	_, stderr, err := captureStdoutStderrRun(t, func() error { return cleanCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow clean apply snapshots: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	if _, err := os.Stat(staleSnapshotPath); !os.IsNotExist(err) {
		t.Fatalf("expected stale snapshot to be deleted from default snapshot dir, stat err=%v", err)
	}
	for _, name := range []string{"week1.json", "week2.json"} {
		if _, err := os.Stat(filepath.Join(mf.SnapshotDir, name)); err != nil {
			t.Fatalf("expected live snapshot %s to remain: %v", name, err)
		}
	}
}

func TestWorkflowCleanApplyIncludesSnapshots(t *testing.T) {
	outputDir := setupWorkflowCleanCommandFixture(t)

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "clean", outputDir, "--apply", "--include-snapshots"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow clean apply snapshots: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "Include Snapshots: yes") {
		t.Fatalf("expected include snapshots in stdout, got %q", stdout)
	}
	if _, err := os.Stat(filepath.Join(outputDir, "snapshots", "stale-snapshot.json")); !os.IsNotExist(err) {
		t.Fatalf("expected stale snapshot to be deleted, stat err=%v", err)
	}
}

func TestWorkflowCleanRejectsUnsupportedFormat(t *testing.T) {
	outputDir := setupWorkflowCleanCommandFixture(t)

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "clean", outputDir, "--format", "yaml"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected invalid format error")
	}
	if !strings.Contains(err.Error(), "unsupported workflow clean format") {
		t.Fatalf("expected invalid format error, got %v", err)
	}
}

func TestWorkflowCleanRejectsMissingManifest(t *testing.T) {
	outputDir := t.TempDir()

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "clean", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected missing manifest error")
	}
	if !strings.Contains(err.Error(), "manifest.json") {
		t.Fatalf("expected manifest error, got %v", err)
	}
}

func TestWorkflowExportWritesDefaultArchiveNextToOutputDir(t *testing.T) {
	outputDir := setupWorkflowExportCommandFixture(t)
	archivePath := outputDir + ".zip"

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "export", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow export: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "Archive: "+archivePath) {
		t.Fatalf("expected archive path in stdout, got %q", stdout)
	}
	if !strings.Contains(stdout, "Format: zip") {
		t.Fatalf("expected zip format in stdout, got %q", stdout)
	}
	if _, err := os.Stat(archivePath); err != nil {
		t.Fatalf("expected archive at %s: %v", archivePath, err)
	}
}

func TestWorkflowExportUsesExplicitOutputPath(t *testing.T) {
	outputDir := setupWorkflowExportCommandFixture(t)
	cwd := t.TempDir()
	explicitPath := filepath.Join(cwd, "incident.zip")
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(cwd); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldwd)
	})

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "export", outputDir, "--output", "./incident.zip"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow export: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	resolvedExplicitPath, err := filepath.EvalSymlinks(explicitPath)
	if err != nil {
		t.Fatalf("resolve explicit path: %v", err)
	}
	if !strings.Contains(stdout, "Archive: "+resolvedExplicitPath) {
		t.Fatalf("expected explicit archive path in stdout, got %q", stdout)
	}
	if !strings.Contains(stdout, "Format: zip") {
		t.Fatalf("expected zip format in stdout, got %q", stdout)
	}
	if _, err := os.Stat(explicitPath); err != nil {
		t.Fatalf("expected archive at %s: %v", explicitPath, err)
	}
}

func TestWorkflowExportAcceptsOutputShorthand(t *testing.T) {
	outputDir := setupWorkflowExportCommandFixture(t)
	cwd := t.TempDir()
	explicitPath := filepath.Join(cwd, "incident-short.zip")
	oldwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	if err := os.Chdir(cwd); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(oldwd)
	})

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "export", outputDir, "-o", "./incident-short.zip"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow export -o: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	if _, err := os.Stat(explicitPath); err != nil {
		t.Fatalf("expected archive at %s: %v", explicitPath, err)
	}
	if !strings.Contains(stdout, "Format: zip") {
		t.Fatalf("expected zip format in stdout, got %q", stdout)
	}
}

func TestWorkflowExportJSONOutputIncludesArchiveFormatCountsAndNormalizedWarnings(t *testing.T) {
	outputDir := setupWorkflowExportCommandFixtureWithoutIndex(t)
	archivePath := outputDir + ".zip"

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "export", outputDir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow export: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	var decoded struct {
		OutputDir         string   `json:"output_dir"`
		ArchivePath       string   `json:"archive_path"`
		Format            string   `json:"format"`
		IncludedFiles     int      `json:"included_files"`
		IncludedSnapshots int      `json:"included_snapshots"`
		Warnings          []string `json:"warnings"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if decoded.OutputDir != outputDir {
		t.Fatalf("output_dir = %q, want %q", decoded.OutputDir, outputDir)
	}
	if decoded.ArchivePath != archivePath {
		t.Fatalf("archive_path = %q, want %q", decoded.ArchivePath, archivePath)
	}
	if decoded.Format != "zip" {
		t.Fatalf("format = %q, want zip", decoded.Format)
	}
	if decoded.IncludedFiles != 3 {
		t.Fatalf("included_files = %d, want 3", decoded.IncludedFiles)
	}
	if decoded.IncludedSnapshots != 0 {
		t.Fatalf("included_snapshots = %d, want 0", decoded.IncludedSnapshots)
	}
	if decoded.Warnings == nil {
		t.Fatal("warnings should be a normalized array, got nil")
	}
	assertStringSliceContains(t, decoded.Warnings, "missing index.html")
}

func TestWorkflowExportTextWarningsSectionRendersOnlyWhenPresent(t *testing.T) {
	t.Run("without warnings", func(t *testing.T) {
		outputDir := setupWorkflowExportCommandFixture(t)

		cmd := NewRootCommand()
		cmd.SetArgs([]string{"workflow", "export", outputDir})
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true

		stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
		if err != nil {
			t.Fatalf("workflow export: %v", err)
		}
		if strings.TrimSpace(stderr) != "" {
			t.Fatalf("expected empty stderr, got %q", stderr)
		}
		if strings.Contains(stdout, "\nWarnings\n") {
			t.Fatalf("expected warnings section omitted, got %q", stdout)
		}
	})

	t.Run("with warnings", func(t *testing.T) {
		outputDir := setupWorkflowExportCommandFixtureWithoutIndex(t)

		cmd := NewRootCommand()
		cmd.SetArgs([]string{"workflow", "export", outputDir})
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true

		stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
		if err != nil {
			t.Fatalf("workflow export: %v", err)
		}
		if strings.TrimSpace(stderr) != "" {
			t.Fatalf("expected empty stderr, got %q", stderr)
		}
		if !strings.Contains(stdout, "\nWarnings\n") {
			t.Fatalf("expected warnings section, got %q", stdout)
		}
		if !strings.Contains(stdout, "missing index.html") {
			t.Fatalf("expected warning in stdout, got %q", stdout)
		}
	})
}

func TestWorkflowExportIncludeSnapshotsChangesExportedSnapshotCount(t *testing.T) {
	outputDir := setupWorkflowExportCommandFixture(t)
	archivePath := outputDir + ".zip"

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "export", outputDir, "--include-snapshots", "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow export: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	var decoded struct {
		IncludedFiles     int `json:"included_files"`
		IncludedSnapshots int `json:"included_snapshots"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if decoded.IncludedSnapshots != 1 {
		t.Fatalf("included_snapshots = %d, want 1", decoded.IncludedSnapshots)
	}
	if decoded.IncludedFiles != 5 {
		t.Fatalf("included_files = %d, want 5", decoded.IncludedFiles)
	}

	zr, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	defer zr.Close()
	if len(zr.File) != decoded.IncludedFiles {
		t.Fatalf("zip entries = %d, want %d", len(zr.File), decoded.IncludedFiles)
	}
}

func TestWorkflowExportRejectsOutputPathInsideWorkflowRoot(t *testing.T) {
	outputDir := setupWorkflowExportCommandFixture(t)
	insidePath := filepath.Join(outputDir, "incident.zip")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "export", outputDir, "--output", insidePath})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err == nil {
		t.Fatal("expected workflow export to fail")
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	if !strings.Contains(err.Error(), "inside workflow root") {
		t.Fatalf("expected inside-workflow-root error, got %v", err)
	}
}

func TestWorkflowExportFailsWhenArchiveWriteFails(t *testing.T) {
	outputDir := setupWorkflowExportCommandFixture(t)
	archivePath := filepath.Join(t.TempDir(), "blocked.zip")
	if err := os.MkdirAll(archivePath, 0o755); err != nil {
		t.Fatalf("mkdir blocking archive path: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "export", outputDir, "--output", archivePath})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err == nil {
		t.Fatal("expected workflow export to fail when archive write fails")
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	if !strings.Contains(err.Error(), "create export archive") {
		t.Fatalf("expected archive creation error, got %v", err)
	}
}

func TestWorkflowExportFailsWhenManifestReadOrDecodeFails(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(t *testing.T, outputDir string)
		wantErrSubstr string
	}{
		{
			name: "missing manifest",
			setup: func(t *testing.T, outputDir string) {
				t.Helper()
			},
			wantErrSubstr: "read workflow manifest",
		},
		{
			name: "invalid manifest json",
			setup: func(t *testing.T, outputDir string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(outputDir, "manifest.json"), []byte("{invalid"), 0o644); err != nil {
					t.Fatalf("write invalid manifest: %v", err)
				}
			},
			wantErrSubstr: "read workflow manifest",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outputDir := t.TempDir()
			tt.setup(t, outputDir)

			cmd := NewRootCommand()
			cmd.SetArgs([]string{"workflow", "export", outputDir})
			cmd.SilenceUsage = true
			cmd.SilenceErrors = true

			stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
			if err == nil {
				t.Fatal("expected workflow export to fail")
			}
			if strings.TrimSpace(stdout) != "" {
				t.Fatalf("expected empty stdout, got %q", stdout)
			}
			if strings.TrimSpace(stderr) != "" {
				t.Fatalf("expected empty stderr, got %q", stderr)
			}
			if !strings.Contains(err.Error(), tt.wantErrSubstr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErrSubstr, err)
			}
		})
	}
}

func TestWorkflowExportJSONOutputIncludesWarningsForMissingOptionalInputs(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		mutate      func(t *testing.T, outputDir string, manifest workflow.Manifest)
		wantWarning string
	}{
		{
			name: "missing manifest artifact",
			args: []string{"--format", "json"},
			mutate: func(t *testing.T, outputDir string, manifest workflow.Manifest) {
				t.Helper()
				if err := os.Remove(filepath.Join(outputDir, "analyze", "week1.json")); err != nil {
					t.Fatalf("remove manifest artifact: %v", err)
				}
			},
			wantWarning: "missing manifest artifact: analyze/week1.json",
		},
		{
			name: "missing plan file",
			args: []string{"--format", "json"},
			mutate: func(t *testing.T, outputDir string, manifest workflow.Manifest) {
				t.Helper()
				if err := os.Remove(manifest.PlanPath); err != nil {
					t.Fatalf("remove plan file: %v", err)
				}
			},
			wantWarning: "missing plan.yaml from manifest plan path: ",
		},
		{
			name: "missing snapshot file",
			args: []string{"--include-snapshots", "--format", "json"},
			mutate: func(t *testing.T, outputDir string, manifest workflow.Manifest) {
				t.Helper()
				if err := os.Remove(filepath.Join(manifest.SnapshotDir, "week1.json")); err != nil {
					t.Fatalf("remove snapshot file: %v", err)
				}
			},
			wantWarning: "missing snapshot: week1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outputDir := setupWorkflowExportCommandFixture(t)
			manifest, err := workflowManifestFromJSON(filepath.Join(outputDir, "manifest.json"))
			if err != nil {
				t.Fatalf("read fixture manifest: %v", err)
			}
			tt.mutate(t, outputDir, manifest)

			archivePath := outputDir + ".zip"
			args := append([]string{"workflow", "export", outputDir}, tt.args...)
			cmd := NewRootCommand()
			cmd.SetArgs(args)
			cmd.SilenceUsage = true
			cmd.SilenceErrors = true

			stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
			if err != nil {
				t.Fatalf("workflow export: %v", err)
			}
			if strings.TrimSpace(stderr) != "" {
				t.Fatalf("expected empty stderr, got %q", stderr)
			}

			var decoded struct {
				ArchivePath string   `json:"archive_path"`
				Warnings    []string `json:"warnings"`
			}
			if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
				t.Fatalf("decode json output: %v\n%s", err, stdout)
			}
			if decoded.ArchivePath != archivePath {
				t.Fatalf("archive_path = %q, want %q", decoded.ArchivePath, archivePath)
			}
			if decoded.Warnings == nil {
				t.Fatal("warnings should be a normalized array, got nil")
			}
			if strings.HasSuffix(tt.wantWarning, ": ") {
				matched := false
				for _, warning := range decoded.Warnings {
					if strings.HasPrefix(warning, tt.wantWarning) {
						matched = true
						break
					}
				}
				if !matched {
					t.Fatalf("expected warning with prefix %q in %v", tt.wantWarning, decoded.Warnings)
				}
			} else {
				assertStringSliceContains(t, decoded.Warnings, tt.wantWarning)
			}
			if _, err := os.Stat(archivePath); err != nil {
				t.Fatalf("expected archive at %s: %v", archivePath, err)
			}
		})
	}
}

func setupWorkflowCleanCommandFixture(t *testing.T) string {
	t.Helper()

	outputDir := t.TempDir()
	snapshotDir := filepath.Join(outputDir, "snapshots")
	manifest := workflow.Manifest{
		WorkflowName: "cleanup-test",
		SnapshotDir:  snapshotDir,
		Steps: []workflow.StepRecord{
			{Kind: "analyze", Name: "week1", Status: "success", Artifacts: []string{"analyze/week1.json"}, SnapshotName: "week1"},
			{Kind: "analyze", Name: "week2", Status: "success", Artifacts: []string{"analyze/week2.json"}, SnapshotName: "week2"},
			{Kind: "compare", Name: "week2_vs_week1", Status: "success", Artifacts: []string{"compare/week2_vs_week1.json", "compare/week2_vs_week1.html"}},
			{Kind: "trend", Name: "series", Status: "success", Artifacts: []string{"trend/series.json", "trend/series.html"}},
		},
	}
	createWorkflowCleanCommandFiles(t, outputDir, snapshotDir, manifest)
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "manifest.json"), append(manifestBytes, '\n'), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return outputDir
}

func setupWorkflowExportCommandFixture(t *testing.T) string {
	t.Helper()
	return setupWorkflowExportCommandFixtureWithOptions(t, true)
}

func setupWorkflowExportCommandFixtureWithoutIndex(t *testing.T) string {
	t.Helper()
	return setupWorkflowExportCommandFixtureWithOptions(t, false)
}

func setupWorkflowExportCommandFixtureWithOptions(t *testing.T, includeIndex bool) string {
	t.Helper()

	outputDir := filepath.Join(t.TempDir(), "workflow-run")
	snapshotDir := filepath.Join(t.TempDir(), "snapshots")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		t.Fatalf("mkdir output dir: %v", err)
	}
	planPath := filepath.Join(outputDir, "plan.yaml")
	planBody := strings.Join([]string{
		"version: 1",
		"workflow:",
		"  name: export-test",
		"  output_dir: artifacts/export-test",
		"defaults:",
		"  input:",
		"    from_dir: /var/lib/mysql",
		"    prefix: mysql-bin.",
		"  analyze:",
		"    format: json",
		"windows:",
		"  - name: week1",
		"    start: 2026-03-01T10:00:00Z",
		"    end: 2026-03-01T10:30:00Z",
	}, "\n") + "\n"
	if err := os.WriteFile(planPath, []byte(planBody), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}
	planSHA256, err := computeFileSHA256(planPath)
	if err != nil {
		t.Fatalf("hash plan: %v", err)
	}

	manifest := workflow.Manifest{
		WorkflowName:        "export-test",
		WorkflowPlanVersion: 1,
		PlanPath:            planPath,
		PlanSHA256:          planSHA256,
		SnapshotDir:         snapshotDir,
		Steps: []workflow.StepRecord{
			{Kind: "analyze", Name: "week1", Status: "success", Artifacts: []string{"analyze/week1.json"}, SnapshotName: "week1"},
		},
	}

	if err := os.MkdirAll(filepath.Join(outputDir, "analyze"), 0o755); err != nil {
		t.Fatalf("mkdir analyze: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "analyze", "week1.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write analyze artifact: %v", err)
	}
	if includeIndex {
		if err := os.WriteFile(filepath.Join(outputDir, "index.html"), []byte("<html>index</html>\n"), 0o644); err != nil {
			t.Fatalf("write index: %v", err)
		}
	}
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatalf("mkdir snapshots: %v", err)
	}
	if err := os.WriteFile(filepath.Join(snapshotDir, "week1.json"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	if err := workflow.WriteManifest(filepath.Join(outputDir, "manifest.json"), manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	return outputDir
}

func createWorkflowCleanCommandFiles(t *testing.T, outputDir, snapshotDir string, manifest workflow.Manifest) {
	t.Helper()
	for _, step := range manifest.Steps {
		for _, art := range step.Artifacts {
			path := filepath.Join(outputDir, art)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", art, err)
			}
			if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
				t.Fatalf("write %s: %v", art, err)
			}
		}
		if step.SnapshotName != "" {
			if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
				t.Fatalf("mkdir snapshot dir: %v", err)
			}
			if err := os.WriteFile(filepath.Join(snapshotDir, step.SnapshotName+".json"), []byte("{}"), 0o644); err != nil {
				t.Fatalf("write snapshot: %v", err)
			}
		}
	}
	for path, content := range map[string]string{
		"analyze/stale-analyze.json": "{}",
		"compare/stale-report.json":  "{}",
		"compare/stale-report.html":  "<html></html>",
		"trend/stale-trend.json":     "{}",
		"trend/stale-trend.html":     "<html></html>",
		"compare/ignored.txt":        "ignored",
		"index.html":                 "<html></html>",
	} {
		fullPath := filepath.Join(outputDir, path)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatalf("mkdir orphan dir %s: %v", path, err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			t.Fatalf("write orphan %s: %v", path, err)
		}
	}
	if err := os.WriteFile(filepath.Join(snapshotDir, "stale-snapshot.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write stale snapshot: %v", err)
	}
}

// --- Trust-boundary CLI regression tests ---

func TestWorkflowStatusOutsidePlanPathDegradesToNonResumable(t *testing.T) {
	outputDir := t.TempDir()
	outsideDir := t.TempDir()
	outsidePlan := filepath.Join(outsideDir, "plan.yaml")
	if err := os.WriteFile(outsidePlan, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write outside plan: %v", err)
	}

	manifest := workflow.Manifest{
		ManifestVersion: 2,
		WorkflowName:    "trust-test",
		PlanPath:        outsidePlan,
		Status:          "success",
		Steps:           []workflow.StepRecord{},
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "manifest.json"), append(manifestBytes, '\n'), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	t.Run("text", func(t *testing.T) {
		cmd := NewRootCommand()
		cmd.SetArgs([]string{"workflow", "status", outputDir})
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true

		stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
		if err != nil {
			t.Fatalf("workflow status should succeed: %v", err)
		}
		if strings.TrimSpace(stderr) != "" {
			t.Fatalf("expected empty stderr, got %q", stderr)
		}
		if !strings.Contains(stdout, "Resumable: no") {
			t.Fatalf("expected Resumable: no, got %q", stdout)
		}
		if !strings.Contains(stdout, "Reason:") || !strings.Contains(stdout, "trust") {
			t.Fatalf("expected trust-boundary reason, got %q", stdout)
		}
	})

	t.Run("json", func(t *testing.T) {
		cmd := NewRootCommand()
		cmd.SetArgs([]string{"workflow", "status", outputDir, "--format", "json"})
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true

		stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
		if err != nil {
			t.Fatalf("workflow status should succeed: %v", err)
		}
		if strings.TrimSpace(stderr) != "" {
			t.Fatalf("expected empty stderr, got %q", stderr)
		}
		var decoded struct {
			Resumable   bool   `json:"resumable"`
			ResumeError string `json:"resume_error"`
		}
		if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
			t.Fatalf("decode json: %v\n%s", err, stdout)
		}
		if decoded.Resumable {
			t.Fatal("expected resumable false")
		}
		if !strings.Contains(decoded.ResumeError, "trust") {
			t.Fatalf("expected trust-boundary resume_error, got %q", decoded.ResumeError)
		}
	})
}

func TestWorkflowResumeOutsidePlanPathHardFails(t *testing.T) {
	outputDir := t.TempDir()
	outsideDir := t.TempDir()
	outsidePlan := filepath.Join(outsideDir, "plan.yaml")
	if err := os.WriteFile(outsidePlan, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write outside plan: %v", err)
	}

	manifest := workflow.Manifest{
		ManifestVersion:    2,
		WorkflowName:       "trust-test",
		PlanPath:           outsidePlan,
		PlanSHA256:         "abc",
		ResolvedInputFiles: []string{"/tmp/mysql-bin.000001"},
		Status:             "success",
		Steps:              []workflow.StepRecord{},
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "manifest.json"), append(manifestBytes, '\n'), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "resume", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err = cmd.Execute()
	if err == nil {
		t.Fatal("expected resume to hard-fail for outside-root plan_path")
	}
	if !strings.Contains(err.Error(), "trust") {
		t.Fatalf("expected trust-boundary error, got: %v", err)
	}
}

func TestWorkflowResumeSymlinkEscapedPlanPathFails(t *testing.T) {
	outputDir := t.TempDir()
	outsideDir := t.TempDir() + "-outside"
	if err := os.MkdirAll(outsideDir, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	outsidePlan := filepath.Join(outsideDir, "plan.yaml")
	if err := os.WriteFile(outsidePlan, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write outside plan: %v", err)
	}

	linkPath := filepath.Join(outputDir, "plan.yaml")
	if err := os.Symlink(outsidePlan, linkPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	manifest := workflow.Manifest{
		ManifestVersion:    2,
		WorkflowName:       "trust-symlink-test",
		PlanPath:           linkPath,
		PlanSHA256:         "abc",
		ResolvedInputFiles: []string{"/tmp/mysql-bin.000001"},
		Status:             "success",
		Steps:              []workflow.StepRecord{},
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outputDir, "manifest.json"), append(manifestBytes, '\n'), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "resume", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err = cmd.Execute()
	if err == nil {
		t.Fatal("expected resume to hard-fail for symlink-escaped plan_path")
	}
	if !strings.Contains(err.Error(), "trust") {
		t.Fatalf("expected trust-boundary error, got: %v", err)
	}
}

func assertStringSliceContains(t *testing.T, values []string, want string) {
	t.Helper()
	for _, value := range values {
		if value == want {
			return
		}
	}
	t.Fatalf("expected %q in %v", want, values)
}
