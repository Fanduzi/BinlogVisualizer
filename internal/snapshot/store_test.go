// Package snapshot verifies filesystem-backed snapshot helpers.
// input: snapshot names, home directories, and JSON report payloads.
// output: path resolution, name validation, and overwrite-safe file persistence regression coverage.
// pos: snapshot store foundation tests for CLI-facing persistence helpers.
// note: if this file changes, update this header and module README.md.
package snapshot

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultSnapshotDir(t *testing.T) {
	got, err := DefaultSnapshotDir("/tmp/test-home")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := filepath.Join("/tmp/test-home", ".binlogviz", "snapshots")
	if got != want {
		t.Fatalf("unexpected default snapshot dir: got %q want %q", got, want)
	}
}

func TestResolveSnapshotDirUsesExplicitDir(t *testing.T) {
	got, err := ResolveSnapshotDir("relative-snapshots")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want, err := filepath.Abs("relative-snapshots")
	if err != nil {
		t.Fatalf("unexpected abs error: %v", err)
	}
	if got != want {
		t.Fatalf("unexpected resolved dir: got %q want %q", got, want)
	}
}

func TestResolveSnapshotDirUsesHomeDefault(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	got, err := ResolveSnapshotDir("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := filepath.Join(home, ".binlogviz", "snapshots")
	if got != want {
		t.Fatalf("unexpected resolved dir: got %q want %q", got, want)
	}
}

func TestValidateName(t *testing.T) {
	valid := []string{"snapshot-1", "snapshot_1", "snapshot1"}
	for _, name := range valid {
		if err := ValidateName(name); err != nil {
			t.Fatalf("expected %q to be valid: %v", name, err)
		}
	}

	invalid := []string{"", "snapshot/1", "snapshot\\1", "snapshot.1", " snapshot", "snapshot ", ".."}
	for _, name := range invalid {
		if err := ValidateName(name); err == nil {
			t.Fatalf("expected %q to be invalid", name)
		}
	}
}

func TestSaveJSONRejectsOverwrite(t *testing.T) {
	dir := t.TempDir()
	report := []byte(`{"hello":"world"}`)

	path, err := SaveJSON(dir, "daily-snapshot", report)
	if err != nil {
		t.Fatalf("unexpected error on first save: %v", err)
	}

	wantPath := filepath.Join(dir, "daily-snapshot.json")
	if path != wantPath {
		t.Fatalf("unexpected save path: got %q want %q", path, wantPath)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if string(got) != string(report) {
		t.Fatalf("unexpected file contents: got %q want %q", string(got), string(report))
	}

	_, err = SaveJSON(dir, "daily-snapshot", report)
	if err == nil {
		t.Fatal("expected overwrite rejection")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected overwrite error, got %v", err)
	}
}

func TestListSnapshotsReturnsSortedJSONEntries(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "zeta.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatalf("write zeta: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "alpha.json"), []byte(`{}`), 0o644); err != nil {
		t.Fatalf("write alpha: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "ignore.txt"), []byte(`noop`), 0o644); err != nil {
		t.Fatalf("write ignore: %v", err)
	}

	entries, err := ListSnapshots(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 snapshot entries, got %d", len(entries))
	}
	if entries[0].Name != "alpha" || entries[1].Name != "zeta" {
		t.Fatalf("expected sorted entries, got %+v", entries)
	}
}

func TestLoadSnapshotReadsNamedSnapshot(t *testing.T) {
	dir := t.TempDir()
	path, err := SaveJSON(dir, "daily", []byte(`{"summary":{"total_rows":1},"tables":[],"alerts":[],"warnings":0}`))
	if err != nil {
		t.Fatalf("save json: %v", err)
	}

	loadedPath, payload, err := LoadSnapshot(dir, "daily")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loadedPath != path {
		t.Fatalf("unexpected path: got %q want %q", loadedPath, path)
	}
	if string(payload) != `{"summary":{"total_rows":1},"tables":[],"alerts":[],"warnings":0}` {
		t.Fatalf("unexpected payload: %s", payload)
	}
}

func TestLoadSnapshotReportsMissingSnapshotClearly(t *testing.T) {
	_, _, err := LoadSnapshot(t.TempDir(), "missing")
	if err == nil {
		t.Fatal("expected missing snapshot error")
	}
	if !strings.Contains(err.Error(), `snapshot "missing" not found`) {
		t.Fatalf("expected missing snapshot error, got %v", err)
	}
}
