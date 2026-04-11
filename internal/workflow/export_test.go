package workflow

import (
	"archive/zip"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBuildExportIncludesManifestAndExistingArtifacts(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{\"workflow_name\":\"export-test\"}\n")
	writeExportFile(t, filepath.Join(outputDir, "index.html"), "<html>index</html>")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{\"ok\":true}\n")
	writeExportFile(t, filepath.Join(outputDir, "compare", "delta.html"), "<html>delta</html>")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	if got, want := result.Root, filepath.Base(outputDir); got != want {
		t.Fatalf("root mismatch: got %q want %q", got, want)
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", result.Warnings)
	}

	gotPaths := exportArchivePaths(result.Entries)
	wantPaths := []string{
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "analyze", "week1.json")),
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "compare", "delta.html")),
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "index.html")),
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "manifest.json")),
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "plan.yaml")),
	}
	if !reflect.DeepEqual(gotPaths, wantPaths) {
		t.Fatalf("archive paths mismatch: got %v want %v", gotPaths, wantPaths)
	}
}

func TestBuildExportMissingManifestArtifactBecomesWarning(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, `missing manifest artifact: analyze/week1.json`, "missing artifact should warn")
	assertNotContainsString(t, exportArchivePaths(result.Entries), filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "analyze", "week1.json")), "missing artifact must not be archived")
}

func TestBuildExportMissingIndexBecomesWarning(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, "missing index.html", "missing index should warn")
}

func TestBuildExportUnreadablePlanBecomesWarning(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	planDir := filepath.Join(outputDir, "missing", "plan-parent")
	manifest.PlanPath = filepath.Join(planDir, "plan.yaml")
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, `missing plan.yaml from manifest plan path: `+manifest.PlanPath, "missing plan should warn")
	assertNotContainsString(t, exportArchivePaths(result.Entries), filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "plan.yaml")), "missing plan must not be archived")
}

func TestBuildExportSkipsPlanPathOutsideWorkflowRoot(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	outsidePlan := filepath.Join(t.TempDir(), "outside-plan.yaml")
	writeExportFile(t, outsidePlan, "secret: true\n")
	manifest.PlanPath = outsidePlan
	manifest.PlanSHA256 = sha256HexString(t, "secret: true\n")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, "skipping plan.yaml because manifest plan path is outside workflow root: "+manifest.PlanPath, "outside plan should warn")
	assertNotContainsString(t, exportArchivePaths(result.Entries), filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "plan.yaml")), "outside plan must not be archived")
}

func TestBuildExportSkipsValidPlanPathOutsideWorkflowRoot(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	outsidePlan := filepath.Join(t.TempDir(), "outside-plan.yaml")
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
	writeExportFile(t, outsidePlan, planBody)
	manifest.PlanPath = outsidePlan
	manifest.PlanSHA256 = sha256HexString(t, planBody)

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, "skipping plan.yaml because manifest plan path is outside workflow root: "+manifest.PlanPath, "outside valid plan should warn")
	assertNotContainsString(t, exportArchivePaths(result.Entries), filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "plan.yaml")), "outside valid plan must not be archived")
}

func TestBuildExportSkipsPlanPathSymlinkEscapingWorkflowRoot(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	outsidePlan := filepath.Join(t.TempDir(), "outside", "secret-plan.yaml")
	writeExportFile(t, outsidePlan, "secret: true\n")
	linkedPlan := filepath.Join(outputDir, "linked-plan.yaml")
	createSymlinkOrSkip(t, outsidePlan, linkedPlan)
	manifest.PlanPath = linkedPlan
	manifest.PlanSHA256 = sha256HexString(t, "secret: true\n")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, "skipping plan.yaml because manifest plan path is outside workflow root: "+manifest.PlanPath, "symlinked outside plan should warn")
	assertNotContainsString(t, exportArchivePaths(result.Entries), filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "plan.yaml")), "symlinked outside plan must not be archived")
}

func TestBuildExportExcludesSnapshotsByDefault(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	writeExportFile(t, filepath.Join(manifest.SnapshotDir, "week1.json"), "{\"snapshot\":1}")

	result, err := BuildExport(outputDir, manifest, ExportOptions{IncludeSnapshots: false})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	for _, archivePath := range exportArchivePaths(result.Entries) {
		if strings.Contains(archivePath, "/snapshots/") {
			t.Fatalf("expected no snapshots in archive, got %v", exportArchivePaths(result.Entries))
		}
	}
}

func TestBuildExportIncludesOnlyReferencedSnapshotsWhenRequested(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	manifest.Steps = append(manifest.Steps,
		StepRecord{Kind: "analyze", Name: "week2", Status: "success", SnapshotName: "week2"},
		StepRecord{Kind: "analyze", Name: "week3", Status: "failed", SnapshotName: "week3"},
	)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	writeExportFile(t, filepath.Join(manifest.SnapshotDir, "week1.json"), "{\"snapshot\":1}")
	writeExportFile(t, filepath.Join(manifest.SnapshotDir, "week2.json"), "{\"snapshot\":2}")
	writeExportFile(t, filepath.Join(manifest.SnapshotDir, "orphan.json"), "{\"snapshot\":999}")

	result, err := BuildExport(outputDir, manifest, ExportOptions{IncludeSnapshots: true})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	gotPaths := exportArchivePaths(result.Entries)
	assertContainsString(t, gotPaths, filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "snapshots", "week1.json")), "week1 snapshot should be included")
	assertContainsString(t, gotPaths, filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "snapshots", "week2.json")), "week2 snapshot should be included")
	assertNotContainsString(t, gotPaths, filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "snapshots", "week3.json")), "missing referenced snapshot must not be archived")
	assertNotContainsString(t, gotPaths, filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "snapshots", "orphan.json")), "unreferenced snapshot must not be archived")
	assertContainsString(t, result.Warnings, `missing snapshot: week3`, "missing referenced snapshot should warn")
}

func TestBuildExportSkipsSnapshotsWhenSnapshotDirMissing(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	manifest.SnapshotDir = ""

	result, err := BuildExport(outputDir, manifest, ExportOptions{IncludeSnapshots: true})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, "skipping snapshots: manifest snapshot dir is empty", "missing snapshot dir should warn")
	for _, archivePath := range exportArchivePaths(result.Entries) {
		if strings.Contains(archivePath, "/snapshots/") {
			t.Fatalf("expected no snapshots in archive, got %v", exportArchivePaths(result.Entries))
		}
	}
}

func TestBuildExportDedupesDuplicateManifestArchivePathsDeterministically(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "index.html"), "<html>index</html>")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "first\n")
	writeExportFile(t, filepath.Join(outputDir, "compare", "delta.html"), "delta\n")

	manifest.Steps = append([]StepRecord{
		{Kind: "analyze", Name: "first", Status: "success", Artifacts: []string{"shared/report.txt"}},
		{Kind: "analyze", Name: "second", Status: "success", Artifacts: []string{"shared/report.txt"}},
	}, manifest.Steps...)
	writeExportFile(t, filepath.Join(outputDir, "shared", "report.txt"), "first occurrence wins\n")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	duplicatePath := filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "shared", "report.txt"))
	if got := countArchivePath(result.Entries, duplicatePath); got != 1 {
		t.Fatalf("duplicate path count mismatch: got %d want 1; entries=%v", got, exportArchivePaths(result.Entries))
	}
	entry := exportEntryByArchivePath(t, result.Entries, duplicatePath)
	if got, want := string(entry.Body), "first occurrence wins\n"; got != want {
		t.Fatalf("duplicate path body mismatch: got %q want %q", got, want)
	}
}

func TestBuildExportSkipsManifestArtifactEscapingWorkflowRoot(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "compare", "delta.html"), "{}")
	outsideArtifact := filepath.Join(t.TempDir(), "secret.txt")
	writeExportFile(t, outsideArtifact, "secret")
	manifest.Steps[0].Artifacts = append(manifest.Steps[0].Artifacts, filepath.ToSlash(filepath.Join("..", filepath.Base(filepath.Dir(outsideArtifact)), filepath.Base(outsideArtifact))))

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, "skipping manifest artifact with non-normalized archive path: ../"+filepath.Base(filepath.Dir(outsideArtifact))+"/"+filepath.Base(outsideArtifact), "escaping artifact should warn")
	assertNotContainsString(t, exportArchivePaths(result.Entries), filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "..", filepath.Base(filepath.Dir(outsideArtifact)), filepath.Base(outsideArtifact))), "escaping artifact must not be archived")
}

func TestBuildExportSkipsSnapshotNameEscapingSnapshotDir(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	manifest.Steps[0].SnapshotName = filepath.ToSlash(filepath.Join("..", "escaped"))
	writeExportFile(t, filepath.Join(filepath.Dir(manifest.SnapshotDir), "escaped.json"), "{\"snapshot\":999}")

	result, err := BuildExport(outputDir, manifest, ExportOptions{IncludeSnapshots: true})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, "skipping snapshot with non-normalized archive path: ../escaped", "escaping snapshot should warn")
	assertNotContainsString(t, exportArchivePaths(result.Entries), filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "snapshots", "..", "escaped.json")), "escaping snapshot must not be archived")
}

func TestBuildExportSkipsManifestArtifactSymlinkEscapingWorkflowRoot(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")

	outsideArtifact := filepath.Join(t.TempDir(), "outside", "secret.json")
	writeExportFile(t, outsideArtifact, "{\"secret\":true}\n")
	linkPath := filepath.Join(outputDir, "analyze", "linked.json")
	createSymlinkOrSkip(t, outsideArtifact, linkPath)
	manifest.Steps[0].Artifacts = append(manifest.Steps[0].Artifacts, "analyze/linked.json")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, "skipping manifest artifact outside workflow root: analyze/linked.json", "symlinked artifact outside root should warn")
	assertNotContainsString(t, exportArchivePaths(result.Entries), filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "analyze", "linked.json")), "symlinked artifact outside root must not be archived")
}

func TestBuildExportSkipsManifestArtifactWithDotDotArchivePath(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "shared", "report.json"), "{\"ok\":true}\n")
	manifest.Steps[0].Artifacts = append(manifest.Steps[0].Artifacts, "analyze/../shared/report.json")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, "skipping manifest artifact with non-normalized archive path: analyze/../shared/report.json", "dot-dot archive path should warn")
	assertNotContainsString(t, exportArchivePaths(result.Entries), filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "shared", "report.json")), "dot-dot archive path must not be archived")
}

func TestBuildExportSkipsIndexSymlinkEscapingWorkflowRoot(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")

	outsideIndex := filepath.Join(t.TempDir(), "outside", "index.html")
	writeExportFile(t, outsideIndex, "<html>outside</html>\n")
	createSymlinkOrSkip(t, outsideIndex, filepath.Join(outputDir, "index.html"))

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, "skipping index.html because it resolves outside workflow root", "index symlink outside root should warn")
	assertNotContainsString(t, exportArchivePaths(result.Entries), filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "index.html")), "index symlink outside root must not be archived")
}

func TestBuildExportRejectsManifestSymlinkEscapingWorkflowRoot(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")

	outsideManifest := filepath.Join(t.TempDir(), "outside", "manifest.json")
	writeExportFile(t, outsideManifest, "{\"secret\":true}\n")
	createSymlinkOrSkip(t, outsideManifest, filepath.Join(outputDir, "manifest.json"))

	_, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err == nil {
		t.Fatal("expected manifest symlink escaping workflow root to be rejected")
	}
	if !strings.Contains(err.Error(), "manifest.json resolves outside workflow root") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildExportSkipsSnapshotWithDotDotArchivePath(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	manifest.Steps[0].SnapshotName = "nested/../escape"
	writeExportFile(t, filepath.Join(manifest.SnapshotDir, "escape.json"), "{\"snapshot\":1}")

	result, err := BuildExport(outputDir, manifest, ExportOptions{IncludeSnapshots: true})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, "skipping snapshot with non-normalized archive path: nested/../escape", "dot-dot snapshot path should warn")
	assertNotContainsString(t, exportArchivePaths(result.Entries), filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "snapshots", "escape.json")), "dot-dot snapshot path must not be archived")
}

func TestBuildExportSkipsManifestArtifactWithBackslashTraversalPath(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	manifest.Steps[0].Artifacts = append(manifest.Steps[0].Artifacts, `..\outside.json`)

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, `skipping manifest artifact with non-normalized archive path: ..\outside.json`, "backslash traversal artifact should warn")
}

func TestBuildExportSkipsSnapshotWithBackslashTraversalPath(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")
	manifest.Steps[0].SnapshotName = `nested\..\escape`

	result, err := BuildExport(outputDir, manifest, ExportOptions{IncludeSnapshots: true})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	assertContainsString(t, result.Warnings, `skipping snapshot with non-normalized archive path: nested\..\escape`, "backslash traversal snapshot should warn")
}

func TestWriteExportArchiveRejectsSymlinkOutputResolvingInsideWorkflowRoot(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	insideArchivePath := filepath.Join(outputDir, "existing.zip")
	writeExportFile(t, insideArchivePath, "do not overwrite")
	archivePath := filepath.Join(t.TempDir(), "bundle.zip")
	createSymlinkOrSkip(t, insideArchivePath, archivePath)

	err = WriteExportArchive(archivePath, result)
	if err == nil {
		t.Fatal("expected symlink output resolving inside workflow root to be rejected")
	}
	if !strings.Contains(err.Error(), "inside workflow root") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWriteExportArchiveUsesStableRootedNamesAndMetadata(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "manifest\n")
	writeExportFile(t, filepath.Join(outputDir, "index.html"), "index\n")
	writeExportFile(t, filepath.Join(outputDir, "compare", "delta.html"), "delta\n")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "week1\n")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	archivePath := filepath.Join(t.TempDir(), "bundle.zip")
	if err := WriteExportArchive(archivePath, result); err != nil {
		t.Fatalf("write export archive: %v", err)
	}

	zr, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	defer zr.Close()

	var gotNames []string
	for _, file := range zr.File {
		gotNames = append(gotNames, file.Name)
		if !file.Modified.Equal(exportZipTimestamp()) {
			t.Fatalf("unexpected modified time for %s: got %v want %v", file.Name, file.Modified, exportZipTimestamp())
		}
		if file.Mode().Perm() != 0o644 {
			t.Fatalf("unexpected permissions for %s: got %v want %v", file.Name, file.Mode().Perm(), os.FileMode(0o644))
		}
	}

	wantNames := []string{
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "analyze", "week1.json")),
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "compare", "delta.html")),
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "index.html")),
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "manifest.json")),
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "plan.yaml")),
	}
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("zip entry names mismatch: got %v want %v", gotNames, wantNames)
	}
}

func TestWriteExportArchiveSortsEntriesByArchivePath(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "workflow-run")
	result := ExportResult{
		OutputDir: outputDir,
		Root:      filepath.Base(outputDir),
		Entries: []ExportEntry{
			{ArchivePath: filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "z-last.txt")), Body: []byte("last\n")},
			{ArchivePath: filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "a-first.txt")), Body: []byte("first\n")},
			{ArchivePath: filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "m-middle.txt")), Body: []byte("middle\n")},
		},
	}

	archivePath := filepath.Join(t.TempDir(), "bundle.zip")
	if err := WriteExportArchive(archivePath, result); err != nil {
		t.Fatalf("write export archive: %v", err)
	}

	zr, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	defer zr.Close()

	gotNames := make([]string, 0, len(zr.File))
	for _, file := range zr.File {
		gotNames = append(gotNames, file.Name)
	}

	wantNames := []string{
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "a-first.txt")),
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "m-middle.txt")),
		filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "z-last.txt")),
	}
	if !reflect.DeepEqual(gotNames, wantNames) {
		t.Fatalf("zip entry names mismatch: got %v want %v", gotNames, wantNames)
	}
}

func TestWriteExportArchiveDedupesDuplicateArchivePaths(t *testing.T) {
	outputDir := filepath.Join(t.TempDir(), "workflow-run")
	duplicatePath := filepath.ToSlash(filepath.Join(filepath.Base(outputDir), "same.txt"))
	result := ExportResult{
		OutputDir: outputDir,
		Root:      filepath.Base(outputDir),
		Entries: []ExportEntry{
			{ArchivePath: duplicatePath, Body: []byte("first\n")},
			{ArchivePath: duplicatePath, Body: []byte("second\n")},
		},
	}

	archivePath := filepath.Join(t.TempDir(), "bundle.zip")
	if err := WriteExportArchive(archivePath, result); err != nil {
		t.Fatalf("write export archive: %v", err)
	}

	zr, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	defer zr.Close()

	if got, want := len(zr.File), 1; got != want {
		t.Fatalf("zip entry count mismatch: got %d want %d", got, want)
	}
	if got, want := zr.File[0].Name, duplicatePath; got != want {
		t.Fatalf("zip entry name mismatch: got %q want %q", got, want)
	}
	body := readZipFileBody(t, zr.File[0])
	if got, want := body, "first\n"; got != want {
		t.Fatalf("zip entry body mismatch: got %q want %q", got, want)
	}
}

func TestWriteExportArchiveRejectsOutputInsideWorkflowRoot(t *testing.T) {
	outputDir, manifest := makeExportFixture(t)
	writeExportFile(t, filepath.Join(outputDir, "manifest.json"), "{}")
	writeExportFile(t, filepath.Join(outputDir, "analyze", "week1.json"), "{}")

	result, err := BuildExport(outputDir, manifest, ExportOptions{})
	if err != nil {
		t.Fatalf("build export: %v", err)
	}

	archivePath := filepath.Join(outputDir, "bundle.zip")
	err = WriteExportArchive(archivePath, result)
	if err == nil {
		t.Fatal("expected output path inside workflow root to be rejected")
	}
	if !strings.Contains(err.Error(), "inside workflow root") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func makeExportFixture(t *testing.T) (string, Manifest) {
	t.Helper()

	outputDir := filepath.Join(t.TempDir(), "workflow-run")
	snapshotDir := filepath.Join(t.TempDir(), "snapshots")
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
		"  snapshot:",
		"    save: true",
		"windows:",
		"  - name: week1",
		"    start: 2026-03-01T10:00:00Z",
		"    end: 2026-03-01T10:30:00Z",
	}, "\n") + "\n"
	writeExportFile(t, planPath, planBody)

	manifest := Manifest{
		WorkflowName:        "export-test",
		WorkflowPlanVersion: 1,
		PlanPath:            planPath,
		PlanSHA256:          sha256HexString(t, planBody),
		SnapshotDir:         snapshotDir,
		Steps: []StepRecord{
			{Kind: "analyze", Name: "week1", Status: "success", Artifacts: []string{"analyze/week1.json"}, SnapshotName: "week1"},
			{Kind: "compare", Name: "delta", Status: "success", Artifacts: []string{"compare/delta.html"}},
		},
	}
	return outputDir, manifest
}

func writeExportFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func exportArchivePaths(entries []ExportEntry) []string {
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		paths = append(paths, entry.ArchivePath)
	}
	return paths
}

func countArchivePath(entries []ExportEntry, want string) int {
	count := 0
	for _, entry := range entries {
		if entry.ArchivePath == want {
			count++
		}
	}
	return count
}

func exportEntryByArchivePath(t *testing.T, entries []ExportEntry, want string) ExportEntry {
	t.Helper()
	for _, entry := range entries {
		if entry.ArchivePath == want {
			return entry
		}
	}
	t.Fatalf("missing export entry %q in %v", want, exportArchivePaths(entries))
	return ExportEntry{}
}

func readZipFileBody(t *testing.T, file *zip.File) string {
	t.Helper()
	reader, err := file.Open()
	if err != nil {
		t.Fatalf("open zip file %s: %v", file.Name, err)
	}
	defer reader.Close()

	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read zip file %s: %v", file.Name, err)
	}
	return string(body)
}

func assertContainsString(t *testing.T, values []string, want string, msg string) {
	t.Helper()
	for _, value := range values {
		if value == want {
			return
		}
	}
	t.Fatalf("%s: missing %q in %v", msg, want, values)
}

func assertNotContainsString(t *testing.T, values []string, unwanted string, msg string) {
	t.Helper()
	for _, value := range values {
		if value == unwanted {
			t.Fatalf("%s: found %q in %v", msg, unwanted, values)
		}
	}
}

func createSymlinkOrSkip(t *testing.T, target string, link string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		t.Fatalf("mkdir symlink parent %s: %v", link, err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink unsupported in test environment: %v", err)
	}
}

func sha256HexString(t *testing.T, content string) string {
	t.Helper()
	sum := sha256.Sum256([]byte(content))
	return fmt.Sprintf("%x", sum[:])
}

func exportZipTimestamp() time.Time {
	return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
}
