package workflow

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- Selector parsing ---

func TestParseRerunSelectorsValid(t *testing.T) {
	plan := Plan{
		Windows: []Window{{Name: "week1"}, {Name: "week2"}},
		Compare: []CompareJob{{Name: "week2_vs_week1", Current: "week2", Baseline: "week1"}},
		Trend:   []TrendJob{{Name: "series", Snapshots: []string{"week1", "week2"}}},
	}

	sels, err := ParseRerunSelectors(plan, []string{"analyze:week2", "compare:week2_vs_week1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sels) != 2 {
		t.Fatalf("expected 2 selectors, got %d", len(sels))
	}
	if sels[0] != (RerunSelector{Kind: "analyze", Name: "week2"}) {
		t.Errorf("selector 0: got %+v", sels[0])
	}
	if sels[1] != (RerunSelector{Kind: "compare", Name: "week2_vs_week1"}) {
		t.Errorf("selector 1: got %+v", sels[1])
	}
}

func TestParseRerunSelectorsInvalidKind(t *testing.T) {
	plan := Plan{}
	_, err := ParseRerunSelectors(plan, []string{"bogus:name"})
	if err == nil {
		t.Fatal("expected error for invalid kind")
	}
}

func TestParseRerunSelectorsUnknownName(t *testing.T) {
	plan := Plan{
		Windows: []Window{{Name: "week1"}},
	}
	_, err := ParseRerunSelectors(plan, []string{"analyze:nonexistent"})
	if err == nil {
		t.Fatal("expected error for unknown name")
	}
}

func TestParseRerunSelectorsBadFormat(t *testing.T) {
	plan := Plan{}
	_, err := ParseRerunSelectors(plan, []string{"nocolon"})
	if err == nil {
		t.Fatal("expected error for bad format")
	}
}

// --- Resumability validation ---

func TestValidateResumableManifestRejectsOutsidePlanPath(t *testing.T) {
	outsideDir := t.TempDir()
	outsidePlan := filepath.Join(outsideDir, "plan.yaml")
	if err := os.WriteFile(outsidePlan, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	workflowRoot := t.TempDir()
	m := Manifest{
		ManifestVersion:    2,
		PlanPath:           outsidePlan,
		PlanSHA256:         "abc",
		ResolvedInputFiles: []string{"/tmp/mysql-bin.000001"},
	}
	err := ValidateResumableManifest(m, workflowRoot, outsidePlan, "abc")
	if err == nil {
		t.Fatal("expected rejection for outside-root plan_path")
	}
	if !strings.Contains(err.Error(), "trust") {
		t.Fatalf("expected trust-boundary error, got: %v", err)
	}
}

func TestValidateResumableManifestRejectsSymlinkEscapedPlanPath(t *testing.T) {
	workflowRoot := t.TempDir()
	outsideDir := t.TempDir() + "-outside"
	if err := os.MkdirAll(outsideDir, 0o755); err != nil {
		t.Fatalf("mkdir outside: %v", err)
	}
	outsidePlan := filepath.Join(outsideDir, "plan.yaml")
	if err := os.WriteFile(outsidePlan, []byte("version: 1\n"), 0o644); err != nil {
		t.Fatalf("write outside plan: %v", err)
	}

	linkPath := filepath.Join(workflowRoot, "plan.yaml")
	if err := os.Symlink(outsidePlan, linkPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	m := Manifest{
		ManifestVersion:    2,
		PlanPath:           linkPath,
		PlanSHA256:         "abc",
		ResolvedInputFiles: []string{"/tmp/mysql-bin.000001"},
	}
	err := ValidateResumableManifest(m, workflowRoot, linkPath, "abc")
	if err == nil {
		t.Fatal("expected rejection for symlink-escaped plan_path")
	}
	if !strings.Contains(err.Error(), "trust") {
		t.Fatalf("expected trust-boundary error, got: %v", err)
	}
}

func TestValidateResumableManifestRejectsLegacyVersion(t *testing.T) {
	m := Manifest{ManifestVersion: 0}
	err := ValidateResumableManifest(m, "", "", "")
	if err == nil {
		t.Fatal("expected rejection for missing manifest_version")
	}
}

func TestValidateResumableManifestRejectsUnsupportedVersion(t *testing.T) {
	m := Manifest{ManifestVersion: 99}
	err := ValidateResumableManifest(m, "", "", "")
	if err == nil {
		t.Fatal("expected rejection for unsupported manifest_version")
	}
}

func TestValidateResumableManifestRejectsEmptyResolvedFiles(t *testing.T) {
	m := Manifest{
		ManifestVersion:    2,
		PlanPath:           "/tmp/plan.yaml",
		PlanSHA256:         "abc",
		ResolvedInputFiles: []string{},
	}
	err := ValidateResumableManifest(m, "", "/tmp/plan.yaml", "abc")
	if err == nil {
		t.Fatal("expected rejection for empty resolved_input_files")
	}
}

func TestValidateResumableManifestRejectsPlanHashMismatch(t *testing.T) {
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.yaml")
	os.WriteFile(planPath, []byte("version: 1\n"), 0o644)

	m := Manifest{
		ManifestVersion:    2,
		PlanPath:           planPath,
		PlanSHA256:         "abc",
		ResolvedInputFiles: []string{"/tmp/mysql-bin.000001"},
	}
	err := ValidateResumableManifest(m, "", planPath, "different_hash")
	if err == nil {
		t.Fatal("expected rejection for plan hash mismatch")
	}
}

func TestValidateResumableManifestRejectsMissingPlanSHA256(t *testing.T) {
	dir := t.TempDir()
	planPath := filepath.Join(dir, "plan.yaml")
	os.WriteFile(planPath, []byte("version: 1\n"), 0o644)

	m := Manifest{
		ManifestVersion:    2,
		PlanPath:           planPath,
		PlanSHA256:         "",
		ResolvedInputFiles: []string{"/tmp/mysql-bin.000001"},
	}
	err := ValidateResumableManifest(m, "", planPath, "some_hash")
	if err == nil {
		t.Fatal("expected rejection for missing plan_sha256")
	}
	if !strings.Contains(err.Error(), "plan_sha256") {
		t.Fatalf("expected plan_sha256 error, got: %v", err)
	}
}

// --- Resume plan builder ---

func makeResumePlanAndManifest() (Plan, Manifest) {
	plan := Plan{
		Version: 1,
		Workflow: WorkflowMeta{Name: "test-resume"},
		Defaults: Defaults{
			Input:   InputDefaults{FromDir: "/tmp/mysql", Prefix: "mysql-bin."},
			Snapshot: SnapshotConfig{Save: true},
		},
		Windows: []Window{{Name: "week1"}, {Name: "week2"}},
		Compare: []CompareJob{
			{Name: "week2_vs_week1", Current: "week2", Baseline: "week1", Formats: []string{"json", "html"}},
		},
		Trend: []TrendJob{
			{Name: "series", Snapshots: []string{"week1", "week2"}, Formats: []string{"json", "html"}},
		},
	}
	m := Manifest{
		ManifestVersion:    2,
		Mode:               "run",
		Attempt:            1,
		WorkflowName:       "test-resume",
		PlanSHA256:         "deadbeef",
		ResolvedInputFiles: []string{"/tmp/mysql-bin.000001"},
		SnapshotDir:        "/tmp/snapshots",
		Status:             "success",
		Steps: []StepRecord{
			{Kind: "analyze", Name: "week1", Status: "success", Execution: "executed", Artifacts: []string{"analyze/week1.json"}, SnapshotName: "week1"},
			{Kind: "analyze", Name: "week2", Status: "success", Execution: "executed", Artifacts: []string{"analyze/week2.json"}, SnapshotName: "week2"},
			{Kind: "compare", Name: "week2_vs_week1", Status: "success", Execution: "executed", Artifacts: []string{"compare/week2_vs_week1.json", "compare/week2_vs_week1.html"}},
			{Kind: "trend", Name: "series", Status: "success", Execution: "executed", Artifacts: []string{"trend/series.json", "trend/series.html"}},
		},
	}
	return plan, m
}

func TestBuildResumePlanRerunAnalyzelnvalidatesDownstream(t *testing.T) {
	plan, m := makeResumePlanAndManifest()
	dir := t.TempDir()
	snapDir := t.TempDir()

	createTestArtifacts(t, dir, snapDir, m.Steps)

	rp, err := BuildResumePlan(plan, m, []string{"analyze:week2"}, dir, snapDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var executed, reused []string
	for _, s := range rp.Steps {
		if s.Execute {
			executed = append(executed, s.Kind+":"+s.Name)
		} else {
			reused = append(reused, s.Kind+":"+s.Name)
		}
	}

	// week1 analyze is reusable; week2 analyze is explicitly rerun;
	// compare depends on week2 so it must rerun; trend depends on week2 so it must rerun
	assertContains(t, executed, "analyze:week2", "week2 should be rerun")
	assertContains(t, executed, "compare:week2_vs_week1", "compare should rerun (depends on week2)")
	assertContains(t, executed, "trend:series", "trend should rerun (depends on week2)")
	assertContains(t, reused, "analyze:week1", "week1 should be reused")
	assertNotContains(t, executed, "analyze:week1", "week1 should NOT be rerun")
}

func TestBuildResumePlanRerunCompareOnly(t *testing.T) {
	plan, m := makeResumePlanAndManifest()
	dir := t.TempDir()
	snapDir := t.TempDir()

	createTestArtifacts(t, dir, snapDir, m.Steps)

	rp, err := BuildResumePlan(plan, m, []string{"compare:week2_vs_week1"}, dir, snapDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var executed, reused []string
	for _, s := range rp.Steps {
		if s.Execute {
			executed = append(executed, s.Kind+":"+s.Name)
		} else {
			reused = append(reused, s.Kind+":"+s.Name)
		}
	}

	// Rerunning compare does NOT invalidate analyze
	assertContains(t, executed, "compare:week2_vs_week1", "compare should be rerun")
	assertContains(t, reused, "analyze:week1", "week1 should be reused")
	assertContains(t, reused, "analyze:week2", "week2 should be reused")
	assertNotContains(t, executed, "analyze:week1", "week1 should NOT be rerun")
	assertNotContains(t, executed, "analyze:week2", "week2 should NOT be rerun")
}

func TestBuildResumePlanFailedStepRerunsAutomatically(t *testing.T) {
	plan, m := makeResumePlanAndManifest()
	dir := t.TempDir()

	// Mark the compare step as failed
	for i := range m.Steps {
		if m.Steps[i].Kind == "compare" {
			m.Steps[i].Status = "failed"
			m.Steps[i].Execution = "executed"
			m.Steps[i].Error = "some error"
		}
	}
	// Create artifact files for successful steps only
	snapDir := t.TempDir()
	for _, step := range m.Steps {
		if step.Status != "success" {
			continue
		}
		for _, art := range step.Artifacts {
			p := filepath.Join(dir, art)
			os.MkdirAll(filepath.Dir(p), 0o755)
			os.WriteFile(p, []byte("{}"), 0o644)
		}
		if step.SnapshotName != "" {
			os.MkdirAll(snapDir, 0o755)
			os.WriteFile(filepath.Join(snapDir, step.SnapshotName+".json"), []byte("{}"), 0o644)
		}
	}

	// No explicit --rerun selectors: should auto-rerun the failed compare step
	rp, err := BuildResumePlan(plan, m, nil, dir, snapDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var executed, reused []string
	for _, s := range rp.Steps {
		if s.Execute {
			executed = append(executed, s.Kind+":"+s.Name)
		} else {
			reused = append(reused, s.Kind+":"+s.Name)
		}
	}

	assertContains(t, executed, "compare:week2_vs_week1", "failed compare should auto-rerun")
	assertContains(t, reused, "analyze:week1", "week1 should be reused")
	assertContains(t, reused, "analyze:week2", "week2 should be reused")
}

func TestBuildResumePlanMissingArtifactReruns(t *testing.T) {
	plan, m := makeResumePlanAndManifest()
	dir := t.TempDir()

	// Only create artifacts for analyze steps, not compare/trend
	snapDir := t.TempDir()
	for _, step := range m.Steps {
		if step.Kind != "analyze" {
			continue
		}
		for _, art := range step.Artifacts {
			p := filepath.Join(dir, art)
			os.MkdirAll(filepath.Dir(p), 0o755)
			os.WriteFile(p, []byte("{}"), 0o644)
		}
		if step.SnapshotName != "" {
			os.MkdirAll(snapDir, 0o755)
			os.WriteFile(filepath.Join(snapDir, step.SnapshotName+".json"), []byte("{}"), 0o644)
		}
	}

	// No selectors: missing artifacts should trigger rerun of compare and trend
	rp, err := BuildResumePlan(plan, m, nil, dir, snapDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var executed []string
	for _, s := range rp.Steps {
		if s.Execute {
			executed = append(executed, s.Kind+":"+s.Name)
		}
	}

	assertContains(t, executed, "compare:week2_vs_week1", "missing compare artifact should trigger rerun")
	assertContains(t, executed, "trend:series", "missing trend artifact should trigger rerun")
}

func TestBuildResumePlanMissingSnapshotTriggersAnalyzeRerun(t *testing.T) {
	plan, m := makeResumePlanAndManifest()
	dir := t.TempDir()
	snapDir := t.TempDir()

	// Create all artifact files
	for _, step := range m.Steps {
		for _, art := range step.Artifacts {
			p := filepath.Join(dir, art)
			os.MkdirAll(filepath.Dir(p), 0o755)
			os.WriteFile(p, []byte("{}"), 0o644)
		}
	}

	// Create snapshot for week1 but NOT week2
	os.WriteFile(filepath.Join(snapDir, "week1.json"), []byte("{}"), 0o644)
	// week2.json intentionally missing

	// No explicit rerun: missing snapshot should still trigger week2 rerun
	rp, err := BuildResumePlan(plan, m, nil, dir, snapDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var executed []string
	for _, s := range rp.Steps {
		if s.Execute {
			executed = append(executed, s.Kind+":"+s.Name)
		}
	}

	assertContains(t, executed, "analyze:week2", "missing snapshot should trigger week2 analyze rerun")
	assertContains(t, executed, "compare:week2_vs_week1", "downstream compare should rerun")
	assertContains(t, executed, "trend:series", "downstream trend should rerun")
}

func TestBuildResumePlanNothingToDoReturnsError(t *testing.T) {
	plan, m := makeResumePlanAndManifest()
	dir := t.TempDir()
	snapDir := t.TempDir()

	createTestArtifacts(t, dir, snapDir, m.Steps)

	// All steps succeeded, no missing artifacts, no explicit reruns
	_, err := BuildResumePlan(plan, m, nil, dir, snapDir)
	if !errors.Is(err, ErrNothingToResume) {
		t.Fatalf("expected ErrNothingToResume, got %v", err)
	}
}

func TestBuildResumePlanSetsAttemptAndMode(t *testing.T) {
	plan, m := makeResumePlanAndManifest()
	dir := t.TempDir()
	snapDir := t.TempDir()

	createTestArtifacts(t, dir, snapDir, m.Steps)

	rp, err := BuildResumePlan(plan, m, []string{"analyze:week2"}, dir, snapDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if rp.UpdatedManifest.Mode != "resume" {
		t.Errorf("expected mode=resume, got %q", rp.UpdatedManifest.Mode)
	}
	if rp.UpdatedManifest.Attempt != 2 {
		t.Errorf("expected attempt=2, got %d", rp.UpdatedManifest.Attempt)
	}
}

// createTestArtifacts writes artifact files for all steps in the manifest
// and optionally snapshot files if snapDir is non-empty.
func createTestArtifacts(t *testing.T, dir, snapDir string, steps []StepRecord) {
	t.Helper()
	for _, step := range steps {
		for _, art := range step.Artifacts {
			p := filepath.Join(dir, art)
			os.MkdirAll(filepath.Dir(p), 0o755)
			os.WriteFile(p, []byte("{}"), 0o644)
		}
		if snapDir != "" && step.SnapshotName != "" {
			os.MkdirAll(snapDir, 0o755)
			os.WriteFile(filepath.Join(snapDir, step.SnapshotName+".json"), []byte("{}"), 0o644)
		}
	}
}

func assertContains(t *testing.T, list []string, item, msg string) {
	t.Helper()
	for _, v := range list {
		if v == item {
			return
		}
	}
	t.Fatalf("%s: %q not found in %v", msg, item, list)
}

func assertNotContains(t *testing.T, list []string, item, msg string) {
	t.Helper()
	for _, v := range list {
		if v == item {
			t.Fatalf("%s: %q unexpectedly found in %v", msg, item, list)
		}
	}
}
