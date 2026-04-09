package workflow

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArtifactPathPlacesFilesUnderKindDirectory(t *testing.T) {
	root := t.TempDir()
	got := ArtifactPath(root, "compare", "incident_vs_baseline", "html")
	want := filepath.Join(root, "compare", "incident_vs_baseline.html")
	if got != want {
		t.Fatalf("artifact path mismatch: got %q want %q", got, want)
	}
}

func TestArtifactPathAnalyzeJSON(t *testing.T) {
	root := t.TempDir()
	got := ArtifactPath(root, "analyze", "baseline_10_00", "json")
	want := filepath.Join(root, "analyze", "baseline_10_00.json")
	if got != want {
		t.Fatalf("artifact path mismatch: got %q want %q", got, want)
	}
}

func TestEnsureLayoutCreatesDirectories(t *testing.T) {
	root := t.TempDir()
	artifactRoot := filepath.Join(root, "artifacts")

	if err := EnsureLayout(artifactRoot); err != nil {
		t.Fatalf("ensure layout: %v", err)
	}

	for _, dir := range []string{"analyze", "compare", "trend"} {
		info, err := os.Stat(filepath.Join(artifactRoot, dir))
		if err != nil {
			t.Fatalf("stat %s: %v", dir, err)
		}
		if !info.IsDir() {
			t.Fatalf("expected %s to be a directory", dir)
		}
	}
}

func TestManifestJSONIncludesFailedStepError(t *testing.T) {
	manifest := Manifest{
		WorkflowName: "incident",
		Status:       "failed",
		Steps: []StepRecord{
			{
				Kind:   "compare",
				Name:   "incident_vs_baseline",
				Status: "failed",
				Error:  "load baseline report: file missing",
			},
		},
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if !strings.Contains(string(data), `"error": "load baseline report: file missing"`) {
		t.Fatalf("expected error field in manifest json, got %s", data)
	}
}

func TestManifestJSONIncludesArtifacts(t *testing.T) {
	manifest := Manifest{
		WorkflowName: "incident",
		Status:       "success",
		Steps: []StepRecord{
			{
				Kind:      "analyze",
				Name:      "baseline_10_00",
				Status:    "success",
				Artifacts: []string{"analyze/baseline_10_00.json"},
			},
		},
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if !strings.Contains(string(data), `"artifacts"`) || !strings.Contains(string(data), "analyze/baseline_10_00.json") {
		t.Fatalf("expected artifacts field in manifest json, got %s", data)
	}
}

func TestManifestJSONOmitsEmptySnapshotName(t *testing.T) {
	manifest := Manifest{
		WorkflowName: "incident",
		Status:       "success",
		Steps: []StepRecord{
			{
				Kind:   "analyze",
				Name:   "baseline",
				Status: "success",
			},
		},
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if strings.Contains(string(data), "snapshot_name") {
		t.Fatalf("expected snapshot_name to be omitted when empty, got %s", data)
	}
}

func TestWriteManifestCreatesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.json")

	manifest := Manifest{
		WorkflowName:        "test",
		WorkflowPlanVersion: 1,
		BinlogvizVersion:    "dev",
		Status:              "success",
		Steps:               []StepRecord{},
	}

	if err := WriteManifest(path, manifest); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	var decoded Manifest
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if decoded.WorkflowName != "test" {
		t.Fatalf("expected workflow_name test, got %s", decoded.WorkflowName)
	}
}

func TestManifestJSONIncludesResumeMetadata(t *testing.T) {
	manifest := Manifest{
		ManifestVersion:    2,
		Mode:               "resume",
		Attempt:            2,
		PlanSHA256:         "abc123",
		ResolvedInputFiles: []string{"/tmp/mysql-bin.000001", "/tmp/mysql-bin.000002"},
		SnapshotDir:        "/tmp/snapshots",
		Status:             "success",
		Steps: []StepRecord{
			{
				Kind:      "analyze",
				Name:      "week2",
				Status:    "success",
				Execution: "reused",
			},
		},
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	for _, token := range []string{
		`"manifest_version": 2`,
		`"mode": "resume"`,
		`"attempt": 2`,
		`"plan_sha256": "abc123"`,
		`"resolved_input_files"`,
		`"snapshot_dir": "/tmp/snapshots"`,
		`"execution": "reused"`,
	} {
		if !strings.Contains(string(data), token) {
			t.Fatalf("expected %q in manifest json: %s", token, data)
		}
	}
}

func TestManifestJSONOmitsEmptyResumeFields(t *testing.T) {
	manifest := Manifest{
		WorkflowName: "test",
		Status:       "success",
		Steps:        []StepRecord{},
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	for _, token := range []string{
		"plan_sha256",
		"resolved_input_files",
		"snapshot_dir",
		"execution",
	} {
		if strings.Contains(string(data), token) {
			t.Fatalf("expected %q to be omitted when empty, got %s", token, data)
		}
	}
}
