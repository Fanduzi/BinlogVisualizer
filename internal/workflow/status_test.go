package workflow

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildStatusCompleteResumable(t *testing.T) {
	root, plan, manifest := makeStatusFixture(t)
	createTestArtifacts(t, root, manifest.SnapshotDir, manifest.Steps)

	status, err := BuildStatus(root, manifest, &plan)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if status.RuntimeState != "complete" {
		t.Fatalf("expected runtime_state complete, got %q", status.RuntimeState)
	}
	if !status.Resumable {
		t.Fatalf("expected resumable true, got false with error %q", status.ResumeError)
	}
}

func TestBuildStatusMarksMissingArtifactIncomplete(t *testing.T) {
	root, plan, manifest := makeStatusFixture(t)
	createTestArtifacts(t, root, manifest.SnapshotDir, manifest.Steps)

	missing := filepath.Join(root, "compare", "week2_vs_week1.json")
	if err := os.Remove(missing); err != nil {
		t.Fatalf("remove artifact: %v", err)
	}

	status, err := BuildStatus(root, manifest, &plan)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if status.RuntimeState != "incomplete" {
		t.Fatalf("expected runtime_state incomplete, got %q", status.RuntimeState)
	}
	if status.Status != "success" {
		t.Fatalf("expected manifest status to remain success, got %q", status.Status)
	}
	if status.Steps[2].Artifacts[0].Exists {
		t.Fatalf("expected missing artifact to be reported as missing")
	}
}

func TestBuildStatusFailedDiscoveryIsNotComplete(t *testing.T) {
	root := t.TempDir()
	manifest := Manifest{
		ManifestVersion:    2,
		Mode:               "run",
		Attempt:            1,
		WorkflowName:       "failed-discovery",
		Status:             "failed",
		Error:              "discover binlog files: open PLACEHOLDER/binlog: no such file or directory",
		ResolvedInputFiles: nil,
		Steps:              []StepRecord{},
	}

	status, err := BuildStatus(root, manifest, nil)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if status.Status != "failed" {
		t.Fatalf("expected status failed, got %q", status.Status)
	}
	if status.RuntimeState == "complete" {
		t.Fatalf("runtime_state must not be complete with no successful step and no resolved_input_files, got %q", status.RuntimeState)
	}
	if status.RuntimeState != "incomplete" {
		t.Fatalf("expected runtime_state incomplete, got %q", status.RuntimeState)
	}
}

func TestBuildStatusFailedStepsWithoutSuccessAreNotComplete(t *testing.T) {
	root := t.TempDir()
	manifest := Manifest{
		ManifestVersion:    2,
		Mode:               "run",
		Attempt:            1,
		WorkflowName:       "failed-analyze",
		Status:             "failed",
		ResolvedInputFiles: []string{"/tmp/mysql-bin.000001"},
		Steps: []StepRecord{
			{Kind: "analyze", Name: "week1", Status: "failed", Error: "analyze failed"},
		},
	}

	status, err := BuildStatus(root, manifest, nil)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if status.RuntimeState == "complete" {
		t.Fatalf("runtime_state must not be complete with no successful step, got %q", status.RuntimeState)
	}
}

func TestBuildStatusPreservesFailedManifestStatus(t *testing.T) {
	root, plan, manifest := makeStatusFixture(t)
	createTestArtifacts(t, root, manifest.SnapshotDir, manifest.Steps)
	manifest.Status = "failed"
	manifest.Error = "compare failed"
	manifest.Steps[2].Status = "failed"
	manifest.Steps[2].Error = "compare failed"

	status, err := BuildStatus(root, manifest, &plan)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if status.Status != "failed" {
		t.Fatalf("expected top-level status failed, got %q", status.Status)
	}
}

func TestBuildStatusLegacyManifestIsNonResumable(t *testing.T) {
	root, _, manifest := makeStatusFixture(t)
	createTestArtifacts(t, root, manifest.SnapshotDir, manifest.Steps)
	manifest.ManifestVersion = 0

	status, err := BuildStatus(root, manifest, nil)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if status.Resumable {
		t.Fatal("expected legacy manifest to be non-resumable")
	}
	if !strings.Contains(status.ResumeError, "legacy format") {
		t.Fatalf("expected legacy resume error, got %q", status.ResumeError)
	}
	if status.WorkflowSummary.Findings == nil || status.WorkflowSummary.Recommendations == nil || status.WorkflowSummary.Warnings == nil {
		t.Fatalf("expected normalized workflow_summary arrays, got %#v", status.WorkflowSummary)
	}
}

func TestBuildStatusOutsidePlanPathNonResumable(t *testing.T) {
	root, _, manifest := makeStatusFixture(t)
	createTestArtifacts(t, root, manifest.SnapshotDir, manifest.Steps)

	outsideDir := t.TempDir()
	manifest.PlanPath = filepath.Join(outsideDir, "plan.yaml")

	status, err := BuildStatus(root, manifest, nil)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if status.Resumable {
		t.Fatal("expected outside-root plan_path to be non-resumable")
	}
	if !strings.Contains(status.ResumeError, "trust") {
		t.Fatalf("expected trust-boundary resume error, got %q", status.ResumeError)
	}
	if len(status.ResumePreview) != 0 {
		t.Fatalf("expected no resume preview for untrusted plan, got %d steps", len(status.ResumePreview))
	}
}

func TestBuildStatusSymlinkEscapedPlanPathNonResumable(t *testing.T) {
	root, _, manifest := makeStatusFixture(t)
	createTestArtifacts(t, root, manifest.SnapshotDir, manifest.Steps)

	outsideDir := t.TempDir() + "-outside"
	if err := os.MkdirAll(outsideDir, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	outsidePlan := filepath.Join(outsideDir, "plan.yaml")
	if err := os.WriteFile(outsidePlan, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write outside plan: %v", err)
	}

	linkPath := filepath.Join(root, "evil-plan.yaml")
	if err := os.Symlink(outsidePlan, linkPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	manifest.PlanPath = linkPath

	status, err := BuildStatus(root, manifest, nil)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if status.Resumable {
		t.Fatal("expected symlink-escaped plan_path to be non-resumable")
	}
	if !strings.Contains(status.ResumeError, "trust") {
		t.Fatalf("expected trust-boundary resume error, got %q", status.ResumeError)
	}
}

func TestBuildStatusMissingPlanStillBuilds(t *testing.T) {
	root, _, manifest := makeStatusFixture(t)
	createTestArtifacts(t, root, manifest.SnapshotDir, manifest.Steps)
	manifest.PlanPath = filepath.Join(root, "plan.yaml")
	// Delete the plan file so the trust check passes but hash read fails.
	os.Remove(manifest.PlanPath)

	status, err := BuildStatus(root, manifest, nil)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if status.Resumable {
		t.Fatal("expected missing plan to disable resumability")
	}
	if !strings.Contains(status.ResumeError, "not found") {
		t.Fatalf("expected plan-file resume error, got %q", status.ResumeError)
	}
}

func TestBuildStatusResumePreviewContainsReuseAndRerun(t *testing.T) {
	root, plan, manifest := makeStatusFixture(t)

	for _, step := range manifest.Steps {
		if step.Kind == "analyze" {
			for _, art := range step.Artifacts {
				path := filepath.Join(root, art)
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatalf("mkdir artifact dir: %v", err)
				}
				if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
					t.Fatalf("write artifact: %v", err)
				}
			}
			if step.SnapshotName != "" {
				snapshotPath := filepath.Join(manifest.SnapshotDir, step.SnapshotName+".json")
				if err := os.MkdirAll(filepath.Dir(snapshotPath), 0o755); err != nil {
					t.Fatalf("mkdir snapshot dir: %v", err)
				}
				if err := os.WriteFile(snapshotPath, []byte("{}"), 0o644); err != nil {
					t.Fatalf("write snapshot: %v", err)
				}
			}
		}
	}

	status, err := BuildStatus(root, manifest, &plan)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if len(status.ResumePreview) != 4 {
		t.Fatalf("expected 4 resume preview steps, got %d", len(status.ResumePreview))
	}
	if got := status.ResumePreview[0].Action; got != "reuse" {
		t.Fatalf("expected first preview action reuse, got %q", got)
	}
	if got := status.ResumePreview[2].Action; got != "rerun" {
		t.Fatalf("expected compare preview action rerun, got %q", got)
	}
	if got := status.ResumePreview[2].Reason; got != "artifact file missing" {
		t.Fatalf("expected compare preview reason artifact file missing, got %q", got)
	}
	if got := status.ResumePreview[3].Action; got != "rerun" {
		t.Fatalf("expected trend preview action rerun, got %q", got)
	}
	if got := status.ResumePreview[3].Reason; got != "artifact file missing" {
		t.Fatalf("expected trend preview reason artifact file missing, got %q", got)
	}
}

func TestBuildStatusResumeHealthyRootShowsReuse(t *testing.T) {
	root, plan, manifest := makeStatusFixture(t)
	createTestArtifacts(t, root, manifest.SnapshotDir, manifest.Steps)

	status, err := BuildStatus(root, manifest, &plan)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if len(status.ResumePreview) != 4 {
		t.Fatalf("expected 4 resume preview steps, got %d", len(status.ResumePreview))
	}
	for _, step := range status.ResumePreview {
		if step.Action != "reuse" {
			t.Fatalf("expected all preview actions to be reuse, got %s:%s=%q", step.Kind, step.Name, step.Action)
		}
	}
}

func TestBuildStatusResumeKeepsResumableWhenPlanNotLoaded(t *testing.T) {
	root, _, manifest := makeStatusFixture(t)
	createTestArtifacts(t, root, manifest.SnapshotDir, manifest.Steps)

	status, err := BuildStatus(root, manifest, nil)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if !status.Resumable {
		t.Fatalf("expected resumable true, got false with error %q", status.ResumeError)
	}
	if status.ResumeError != "" {
		t.Fatalf("expected empty resume_error, got %q", status.ResumeError)
	}
	if len(status.ResumePreview) != 0 {
		t.Fatalf("expected preview to be omitted when plan is not loaded, got %d steps", len(status.ResumePreview))
	}
}

func TestBuildStatusMarksMissingSnapshotIncomplete(t *testing.T) {
	root, plan, manifest := makeStatusFixture(t)
	createTestArtifacts(t, root, manifest.SnapshotDir, manifest.Steps)

	if err := os.Remove(filepath.Join(manifest.SnapshotDir, "week2.json")); err != nil {
		t.Fatalf("remove snapshot: %v", err)
	}

	status, err := BuildStatus(root, manifest, &plan)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if status.RuntimeState != "incomplete" {
		t.Fatalf("expected runtime_state incomplete, got %q", status.RuntimeState)
	}
	if !status.Resumable {
		t.Fatalf("expected resumable true, got false with error %q", status.ResumeError)
	}
	if got := status.ResumePreview[1].Action; got != "rerun" {
		t.Fatalf("expected week2 preview action rerun, got %q", got)
	}
	if got := status.ResumePreview[1].Reason; got != "snapshot file missing" {
		t.Fatalf("expected week2 preview reason snapshot file missing, got %q", got)
	}
}

func TestBuildStatusMarksMissingSnapshotIncompleteWithoutPreviewStringDependency(t *testing.T) {
	root, plan, manifest := makeStatusFixture(t)
	createTestArtifacts(t, root, manifest.SnapshotDir, manifest.Steps)

	if err := os.Remove(filepath.Join(manifest.SnapshotDir, "week2.json")); err != nil {
		t.Fatalf("remove snapshot: %v", err)
	}
	manifest.Steps[1].Status = "success"

	status, err := BuildStatus(root, manifest, &plan)
	if err != nil {
		t.Fatalf("build status: %v", err)
	}
	if status.RuntimeState != "incomplete" {
		t.Fatalf("expected runtime_state incomplete, got %q", status.RuntimeState)
	}
}

func makeStatusFixture(t *testing.T) (string, Plan, Manifest) {
	t.Helper()

	root := t.TempDir()
	planPath := filepath.Join(root, "plan.yaml")
	planYAML := strings.TrimSpace(`
version: 1
workflow:
  name: test-resume
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /tmp/mysql
    prefix: mysql-bin.
  analyze:
    format: json
  snapshot:
    save: true
windows:
  - name: week1
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
  - name: week2
    start: 2026-04-09T11:00:00Z
    end: 2026-04-09T11:30:00Z
compare:
  - name: week2_vs_week1
    current: week2
    baseline: week1
    formats: [json, html]
trend:
  - name: series
    snapshots: [week1, week2]
    formats: [json, html]
`) + "\n"
	if err := os.WriteFile(planPath, []byte(planYAML), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	f, err := os.Open(planPath)
	if err != nil {
		t.Fatalf("open plan: %v", err)
	}
	defer f.Close()

	plan, err := LoadPlan(f)
	if err != nil {
		t.Fatalf("load plan: %v", err)
	}

	snapshotDir := filepath.Join(root, "snapshots")
	manifest := Manifest{
		ManifestVersion:    2,
		Mode:               "run",
		Attempt:            1,
		WorkflowName:       plan.Workflow.Name,
		WorkflowPlanVersion: plan.Version,
		PlanPath:           planPath,
		PlanSHA256:         sha256Hex([]byte(planYAML)),
		ResolvedInputFiles: []string{"/tmp/mysql-bin.000001"},
		SnapshotDir:        snapshotDir,
		Status:             "success",
		Steps: []StepRecord{
			{Kind: "analyze", Name: "week1", Status: "success", Execution: "executed", Artifacts: []string{"analyze/week1.json"}, SnapshotName: "week1"},
			{Kind: "analyze", Name: "week2", Status: "success", Execution: "executed", Artifacts: []string{"analyze/week2.json"}, SnapshotName: "week2"},
			{Kind: "compare", Name: "week2_vs_week1", Status: "success", Execution: "executed", Artifacts: []string{"compare/week2_vs_week1.json", "compare/week2_vs_week1.html"}},
			{Kind: "trend", Name: "series", Status: "success", Execution: "executed", Artifacts: []string{"trend/series.json", "trend/series.html"}},
		},
	}

	return root, plan, manifest
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}
