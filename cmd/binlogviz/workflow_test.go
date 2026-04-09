package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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

	// Create binlog directory with the test fixture
	binlogDir = t.TempDir()
	fixtureSrc := filepath.Join("testdata", "minimal.binlog")
	fixtureDst := filepath.Join(binlogDir, "mysql-bin.000001")
	src, err := os.ReadFile(fixtureSrc)
	if err != nil {
		t.Fatalf("read binlog fixture: %v", err)
	}
	if err := os.WriteFile(fixtureDst, src, 0o644); err != nil {
		t.Fatalf("write binlog fixture: %v", err)
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
