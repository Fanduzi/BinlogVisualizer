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

	// Write a valid plan file
	planDir := t.TempDir()
	planPath := filepath.Join(planDir, "plan.yaml")
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

// TestWorkflowResumeNothingToDo tests that resume fails with a clear message
// when the workflow is already fully successful.
func TestWorkflowResumeNothingToDo(t *testing.T) {
	binlogDir, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	// Run the full workflow successfully
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	if err := cmd.Execute(); err != nil {
		t.Fatalf("initial run: %v", err)
	}

	// Resume without rerun should fail with "nothing to do"
	cmd2 := NewRootCommand()
	cmd2.SetArgs([]string{"workflow", "resume", outputDir, "--snapshot-dir", snapshotDir})
	cmd2.SilenceUsage = true
	cmd2.SilenceErrors = true

	err := cmd2.Execute()
	if err == nil {
		t.Fatal("expected nothing-to-do error")
	}
	if !strings.Contains(err.Error(), "nothing to resume") {
		t.Fatalf("expected nothing-to-resume error, got: %v", err)
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
		ManifestVersion int    `json:"manifest_version"`
		Resumable       bool   `json:"resumable"`
		ResumeError     string `json:"resume_error"`
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
	planDir := t.TempDir()
	brokenPlanPath := filepath.Join(planDir, "broken-plan.yaml")
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
