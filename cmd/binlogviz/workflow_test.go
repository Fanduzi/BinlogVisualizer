package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWorkflowRunWritesAnalyzeArtifacts tests that a basic plan produces analyze JSON files.
func TestWorkflowRunWritesAnalyzeArtifacts(t *testing.T) {
	binlogDir, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	// Verify analyze artifact
	analyzePath := filepath.Join(outputDir, "analyze", "baseline.json")
	if _, err := os.Stat(analyzePath); err != nil {
		t.Fatalf("expected analyze artifact at %s: %v", analyzePath, err)
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
	if mf.WorkflowName != "basic-test" {
		t.Fatalf("expected workflow_name basic-test, got %q", mf.WorkflowName)
	}

	// Verify analyze step record
	found := false
	for _, step := range mf.Steps {
		if step.Kind == "analyze" && step.Name == "baseline" {
			found = true
			if step.Status != "success" {
				t.Fatalf("expected analyze step success, got %q", step.Status)
			}
			if len(step.Artifacts) == 0 || step.Artifacts[0] != "analyze/baseline.json" {
				t.Fatalf("expected artifact analyze/baseline.json, got %v", step.Artifacts)
			}
			if step.SnapshotName != "baseline" {
				t.Fatalf("expected snapshot_name baseline, got %q", step.SnapshotName)
			}
		}
	}
	if !found {
		t.Fatal("expected to find analyze step for baseline in manifest")
	}

	_ = binlogDir
}

// TestWorkflowRunWritesCompareAndTrendArtifacts tests full workflow with compare and trend.
func TestWorkflowRunWritesCompareAndTrendArtifacts(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	// Verify compare artifact
	comparePath := filepath.Join(outputDir, "compare", "baseline_vs_baseline.json")
	if _, err := os.Stat(comparePath); err != nil {
		t.Fatalf("expected compare artifact at %s: %v", comparePath, err)
	}

	// Verify trend artifact
	trendPath := filepath.Join(outputDir, "trend", "baseline_series.json")
	if _, err := os.Stat(trendPath); err != nil {
		t.Fatalf("expected trend artifact at %s: %v", trendPath, err)
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

	// Verify we have analyze + compare + trend steps
	stepKinds := make(map[string]int)
	for _, step := range mf.Steps {
		stepKinds[step.Kind]++
	}
	if stepKinds["analyze"] != 1 {
		t.Fatalf("expected 1 analyze step, got %d", stepKinds["analyze"])
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

// TestWorkflowCompareOutputIsValidJSON verifies the compare output is valid JSON.
func TestWorkflowCompareOutputIsValidJSON(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	comparePath := filepath.Join(outputDir, "compare", "baseline_vs_baseline.json")
	data, err := os.ReadFile(comparePath)
	if err != nil {
		t.Fatalf("read compare output: %v", err)
	}
	if !json.Valid(data) {
		t.Fatalf("compare output is not valid JSON: %s", string(data[:min(len(data), 200)]))
	}

	trendPath := filepath.Join(outputDir, "trend", "baseline_series.json")
	data, err = os.ReadFile(trendPath)
	if err != nil {
		t.Fatalf("read trend output: %v", err)
	}
	if !json.Valid(data) {
		t.Fatalf("trend output is not valid JSON: %s", string(data[:min(len(data), 200)]))
	}
}
