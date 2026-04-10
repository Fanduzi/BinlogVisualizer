package workflow

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestDiscoverCleanupFindsArtifactOrphansByDefault(t *testing.T) {
	outputDir, manifest := makeCleanFixture(t)
	createTestArtifacts(t, outputDir, manifest.SnapshotDir, manifest.Steps)
	createCleanOrphans(t, outputDir, manifest.SnapshotDir)

	result, err := DiscoverCleanCandidates(outputDir, manifest, false)
	if err != nil {
		t.Fatalf("discover cleanup: %v", err)
	}

	wantArtifacts := []string{
		"analyze/stale-analyze.json",
		"compare/stale-report.html",
		"compare/stale-report.json",
		"trend/stale-trend.html",
		"trend/stale-trend.json",
	}
	if !reflect.DeepEqual(result.ArtifactOrphans, wantArtifacts) {
		t.Fatalf("artifact orphans mismatch: got %v want %v", result.ArtifactOrphans, wantArtifacts)
	}
	if len(result.SnapshotOrphans) != 0 {
		t.Fatalf("expected no snapshot orphans by default, got %v", result.SnapshotOrphans)
	}
	if result.Mode != "dry-run" {
		t.Fatalf("expected mode dry-run, got %q", result.Mode)
	}
	if result.IncludeSnapshots {
		t.Fatal("expected include_snapshots false")
	}
}

func TestDiscoverCleanupIncludesSnapshotsWhenRequested(t *testing.T) {
	outputDir, manifest := makeCleanFixture(t)
	createTestArtifacts(t, outputDir, manifest.SnapshotDir, manifest.Steps)
	createCleanOrphans(t, outputDir, manifest.SnapshotDir)

	result, err := DiscoverCleanCandidates(outputDir, manifest, true)
	if err != nil {
		t.Fatalf("discover cleanup: %v", err)
	}

	wantSnapshots := []string{"stale-snapshot.json"}
	if !reflect.DeepEqual(result.SnapshotOrphans, wantSnapshots) {
		t.Fatalf("snapshot orphans mismatch: got %v want %v", result.SnapshotOrphans, wantSnapshots)
	}
	assertNotContains(t, result.SnapshotOrphans, "week1.json", "live snapshot must not be orphaned")
	assertNotContains(t, result.SnapshotOrphans, "week2.json", "live snapshot must not be orphaned")
}

func TestDiscoverCleanupIgnoresMissingSnapshotDirectory(t *testing.T) {
	outputDir, manifest := makeCleanFixture(t)
	createTestArtifacts(t, outputDir, manifest.SnapshotDir, manifest.Steps)
	if err := os.RemoveAll(manifest.SnapshotDir); err != nil {
		t.Fatalf("remove snapshot dir: %v", err)
	}
	createCleanArtifactOrphans(t, outputDir)

	result, err := DiscoverCleanCandidates(outputDir, manifest, true)
	if err != nil {
		t.Fatalf("discover cleanup: %v", err)
	}

	if len(result.SnapshotOrphans) != 0 {
		t.Fatalf("expected no snapshot orphans when snapshot dir is missing, got %v", result.SnapshotOrphans)
	}
}

func TestApplyCleanupDeletesArtifactOrphans(t *testing.T) {
	outputDir, manifest := makeCleanFixture(t)
	createTestArtifacts(t, outputDir, manifest.SnapshotDir, manifest.Steps)
	createCleanOrphans(t, outputDir, manifest.SnapshotDir)

	result, err := DiscoverCleanCandidates(outputDir, manifest, false)
	if err != nil {
		t.Fatalf("discover cleanup: %v", err)
	}

	applied := ApplyClean(result)

	if applied.Mode != "apply" {
		t.Fatalf("expected mode apply, got %q", applied.Mode)
	}
	if len(applied.Skipped) != 0 {
		t.Fatalf("expected no skipped deletes, got %v", applied.Skipped)
	}
	if !reflect.DeepEqual(applied.Deleted, applied.ArtifactOrphans) {
		t.Fatalf("deleted mismatch: got %v want %v", applied.Deleted, applied.ArtifactOrphans)
	}
	for _, relPath := range applied.ArtifactOrphans {
		if _, err := os.Stat(filepath.Join(outputDir, relPath)); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be deleted, stat err=%v", relPath, err)
		}
	}
	for _, relPath := range []string{
		"analyze/week1.json",
		"analyze/week2.json",
		"compare/week2_vs_week1.json",
		"compare/week2_vs_week1.html",
		"trend/series.json",
		"trend/series.html",
	} {
		if _, err := os.Stat(filepath.Join(outputDir, relPath)); err != nil {
			t.Fatalf("expected live artifact %s to remain: %v", relPath, err)
		}
	}
	if _, err := os.Stat(filepath.Join(manifest.SnapshotDir, "stale-snapshot.json")); err != nil {
		t.Fatalf("expected snapshot to remain when includeSnapshots is false: %v", err)
	}
}

func TestApplyCleanupDeletesSnapshotOrphansWhenIncluded(t *testing.T) {
	outputDir, manifest := makeCleanFixture(t)
	createTestArtifacts(t, outputDir, manifest.SnapshotDir, manifest.Steps)
	createCleanOrphans(t, outputDir, manifest.SnapshotDir)

	result, err := DiscoverCleanCandidates(outputDir, manifest, true)
	if err != nil {
		t.Fatalf("discover cleanup: %v", err)
	}

	applied := ApplyClean(result)

	assertContains(t, applied.Deleted, "stale-snapshot.json", "snapshot orphan should be deleted")
	if _, err := os.Stat(filepath.Join(manifest.SnapshotDir, "stale-snapshot.json")); !os.IsNotExist(err) {
		t.Fatalf("expected stale snapshot to be deleted, stat err=%v", err)
	}
	for _, name := range []string{"week1.json", "week2.json"} {
		if _, err := os.Stat(filepath.Join(manifest.SnapshotDir, name)); err != nil {
			t.Fatalf("expected live snapshot %s to remain: %v", name, err)
		}
	}
}

func TestApplyCleanupRecordsSkippedDeletionFailures(t *testing.T) {
	outputDir, manifest := makeCleanFixture(t)
	createTestArtifacts(t, outputDir, manifest.SnapshotDir, manifest.Steps)
	createCleanOrphans(t, outputDir, manifest.SnapshotDir)

	result, err := DiscoverCleanCandidates(outputDir, manifest, false)
	if err != nil {
		t.Fatalf("discover cleanup: %v", err)
	}

	blocked := filepath.Join(outputDir, "compare", "stale-report.json")
	if err := os.Remove(blocked); err != nil {
		t.Fatalf("remove stale file: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(blocked, "nested"), 0o755); err != nil {
		t.Fatalf("create blocking directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(blocked, "nested", "keep.txt"), []byte("blocked"), 0o644); err != nil {
		t.Fatalf("write blocking file: %v", err)
	}

	applied := ApplyClean(result)

	assertContains(t, applied.Skipped, "compare/stale-report.json", "failed deletion should be recorded in skipped")
	assertNotContains(t, applied.Deleted, "compare/stale-report.json", "failed deletion must not be marked deleted")
	if _, err := os.Stat(blocked); err != nil {
		t.Fatalf("expected blocked path to remain after failed delete: %v", err)
	}
}

func makeCleanFixture(t *testing.T) (string, Manifest) {
	t.Helper()

	outputDir := t.TempDir()
	snapshotDir := filepath.Join(t.TempDir(), "snapshots")
	manifest := Manifest{
		WorkflowName: "cleanup-test",
		SnapshotDir:  snapshotDir,
		Steps: []StepRecord{
			{Kind: "analyze", Name: "week1", Status: "success", Artifacts: []string{"analyze/week1.json"}, SnapshotName: "week1"},
			{Kind: "analyze", Name: "week2", Status: "success", Artifacts: []string{"analyze/week2.json"}, SnapshotName: "week2"},
			{Kind: "compare", Name: "week2_vs_week1", Status: "success", Artifacts: []string{"compare/week2_vs_week1.json", "compare/week2_vs_week1.html"}},
			{Kind: "trend", Name: "series", Status: "success", Artifacts: []string{"trend/series.json", "trend/series.html"}},
		},
	}
	return outputDir, manifest
}

func createCleanOrphans(t *testing.T, outputDir, snapshotDir string) {
	t.Helper()
	createCleanArtifactOrphans(t, outputDir)
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		t.Fatalf("mkdir snapshot dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(snapshotDir, "stale-snapshot.json"), []byte("{}"), 0o644); err != nil {
		t.Fatalf("write stale snapshot: %v", err)
	}
	if err := os.WriteFile(filepath.Join(snapshotDir, "ignored.txt"), []byte("ignored"), 0o644); err != nil {
		t.Fatalf("write ignored snapshot file: %v", err)
	}
}

func createCleanArtifactOrphans(t *testing.T, outputDir string) {
	t.Helper()
	for path, content := range map[string]string{
		"compare/stale-report.json": "{}",
		"compare/stale-report.html": "<html></html>",
		"trend/stale-trend.json":   "{}",
		"trend/stale-trend.html":   "<html></html>",
		"analyze/stale-analyze.json": "{}",
		"compare/ignored.txt":      "ignored",
		"misc/unknown.json":        "{}",
		"index.html":               "<html></html>",
		"manifest.json":            "{}",
	} {
		fullPath := filepath.Join(outputDir, path)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
}
