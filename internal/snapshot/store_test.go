// Package snapshot verifies filesystem-backed snapshot helpers.
// input: snapshot names, home directories, and JSON report payloads.
// output: path resolution, name validation, and overwrite-safe file persistence regression coverage.
// pos: snapshot store foundation tests for CLI-facing persistence helpers.
// note: if this file changes, update this header and module README.md.
package snapshot

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
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

func TestDescribeSnapshotReturnsNormalizedMetadata(t *testing.T) {
	dir := t.TempDir()
	_, err := SaveJSON(dir, "incident", []byte(minimalSnapshotStoreReportJSON("incident", "incident", 2400, 2)))
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	desc, err := DescribeSnapshot(dir, "incident")
	if err != nil {
		t.Fatalf("describe snapshot: %v", err)
	}

	if desc.Name != "incident" {
		t.Fatalf("unexpected name: %q", desc.Name)
	}
	if desc.Path != filepath.Join(dir, "incident.json") {
		t.Fatalf("unexpected path: %q", desc.Path)
	}
	if desc.InputMode != "files" {
		t.Fatalf("unexpected input mode: %q", desc.InputMode)
	}
	if got, want := desc.Summary.TotalRows, 2400; got != want {
		t.Fatalf("unexpected total rows: got %d want %d", got, want)
	}
	if got, want := desc.Warnings, 2; got != want {
		t.Fatalf("unexpected warnings: got %d want %d", got, want)
	}
	if got, want := len(desc.Input.Files), 1; got != want {
		t.Fatalf("unexpected file count: got %d want %d", got, want)
	}
	if got, want := desc.Filters.IncludeSchemas, []string{"shop"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("unexpected include schemas: %+v", got)
	}
}

func TestDescribeSnapshotRejectsUnsupportedReportVersion(t *testing.T) {
	dir := t.TempDir()
	_, err := SaveJSON(dir, "incident", []byte(minimalSnapshotStoreReportJSONWithVersion("incident", "incident", 2400, 2, 99)))
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	_, err = DescribeSnapshot(dir, "incident")
	if err == nil || !strings.Contains(err.Error(), "unsupported report_version") {
		t.Fatalf("expected report_version compatibility error, got %v", err)
	}
}

func TestRenameSnapshotUpdatesIdentityAndFilename(t *testing.T) {
	dir := t.TempDir()
	_, err := SaveJSON(dir, "old_name", []byte(minimalSnapshotStoreReportJSON("old_name", "old_name", 2400, 2)))
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	newPath, err := RenameSnapshot(dir, "old_name", "new_name")
	if err != nil {
		t.Fatalf("rename snapshot: %v", err)
	}
	if filepath.Base(newPath) != "new_name.json" {
		t.Fatalf("unexpected new path: %q", newPath)
	}
	if _, err := os.Stat(filepath.Join(dir, "old_name.json")); !os.IsNotExist(err) {
		t.Fatalf("expected old file removed, got err=%v", err)
	}

	_, payload, err := LoadSnapshot(dir, "new_name")
	if err != nil {
		t.Fatalf("load renamed snapshot: %v", err)
	}
	var decoded struct {
		Snapshot struct {
			Name  string `json:"name"`
			Label string `json:"label"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal renamed snapshot: %v", err)
	}
	if decoded.Snapshot.Name != "new_name" {
		t.Fatalf("expected renamed snapshot.name, got %q", decoded.Snapshot.Name)
	}
	if decoded.Snapshot.Label != "new_name" {
		t.Fatalf("expected renamed snapshot.label, got %q", decoded.Snapshot.Label)
	}
}

func TestRenameSnapshotPreservesDistinctLabel(t *testing.T) {
	dir := t.TempDir()
	_, err := SaveJSON(dir, "old_name", []byte(minimalSnapshotStoreReportJSON("old_name", "custom_label", 2400, 2)))
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	_, err = RenameSnapshot(dir, "old_name", "new_name")
	if err != nil {
		t.Fatalf("rename snapshot: %v", err)
	}

	_, payload, err := LoadSnapshot(dir, "new_name")
	if err != nil {
		t.Fatalf("load renamed snapshot: %v", err)
	}
	var decoded struct {
		Snapshot struct {
			Name  string `json:"name"`
			Label string `json:"label"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal renamed snapshot: %v", err)
	}
	if decoded.Snapshot.Name != "new_name" {
		t.Fatalf("expected renamed snapshot.name, got %q", decoded.Snapshot.Name)
	}
	if decoded.Snapshot.Label != "custom_label" {
		t.Fatalf("expected distinct label preserved, got %q", decoded.Snapshot.Label)
	}
}

func TestRenameSnapshotReportsMissingSourceClearly(t *testing.T) {
	_, err := RenameSnapshot(t.TempDir(), "missing", "new_name")
	if err == nil {
		t.Fatal("expected missing snapshot error")
	}
	if !strings.Contains(err.Error(), `snapshot "missing" not found`) {
		t.Fatalf("expected clear missing error, got %v", err)
	}
}

func TestRenameSnapshotRejectsExistingDestination(t *testing.T) {
	dir := t.TempDir()
	_, err := SaveJSON(dir, "old_name", []byte(minimalSnapshotStoreReportJSON("old_name", "old_name", 2400, 2)))
	if err != nil {
		t.Fatalf("save old snapshot: %v", err)
	}
	_, err = SaveJSON(dir, "new_name", []byte(minimalSnapshotStoreReportJSON("new_name", "new_name", 2400, 2)))
	if err != nil {
		t.Fatalf("save new snapshot: %v", err)
	}

	_, err = RenameSnapshot(dir, "old_name", "new_name")
	if err == nil {
		t.Fatal("expected destination conflict")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected destination conflict error, got %v", err)
	}
}

func TestDeleteSnapshotRemovesStoredFile(t *testing.T) {
	dir := t.TempDir()
	_, err := SaveJSON(dir, "incident", []byte(minimalSnapshotStoreReportJSON("incident", "incident", 2400, 2)))
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	deletedPath, err := DeleteSnapshot(dir, "incident")
	if err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}
	if deletedPath != filepath.Join(dir, "incident.json") {
		t.Fatalf("unexpected deleted path: %q", deletedPath)
	}
	if _, err := os.Stat(deletedPath); !os.IsNotExist(err) {
		t.Fatalf("expected snapshot file removed, got err=%v", err)
	}
}

func TestDeleteSnapshotReportsMissingSnapshotClearly(t *testing.T) {
	_, err := DeleteSnapshot(t.TempDir(), "missing")
	if err == nil {
		t.Fatal("expected missing snapshot error")
	}
	if !strings.Contains(err.Error(), `snapshot "missing" not found`) {
		t.Fatalf("expected clear missing error, got %v", err)
	}
}

func minimalSnapshotStoreReportJSON(name, label string, totalRows, warnings int) string {
	return minimalSnapshotStoreReportJSONWithVersion(name, label, totalRows, warnings, -1)
}

func minimalSnapshotStoreReportJSONWithVersion(name, label string, totalRows, warnings, version int) string {
	reportVersion := ""
	if version >= 0 {
		reportVersion = `  "report_version": ` + strconv.Itoa(version) + `,
`
	}
	return `{
` + reportVersion + `  "summary": {
    "total_transactions": 120,
    "total_rows": ` + strconv.Itoa(totalRows) + `,
    "total_events": 3000,
    "start_time": "2026-03-20T10:00:00Z",
    "end_time": "2026-03-20T10:30:00Z",
    "duration": "30m0s"
  },
  "tables": [
    {
      "schema": "shop",
      "table": "orders",
      "total_rows": ` + strconv.Itoa(totalRows) + `,
      "insert_rows": 700,
      "update_rows": 400,
      "delete_rows": 100,
      "txn_count": 80
    }
  ],
  "alerts": [],
  "warnings": ` + strconv.Itoa(warnings) + `,
  "snapshot": {
    "name": "` + name + `",
    "label": "` + label + `",
    "created_at": "2026-03-20T10:31:00Z",
    "binlogviz_version": "dev",
    "input_mode": "files",
    "input": {
      "files": ["mysql-bin.000123"],
      "from_dir": "",
      "prefix": ""
    },
    "window": {
      "start_time": "2026-03-20T10:00:00Z",
      "end_time": "2026-03-20T10:30:00Z"
    },
    "filters": {
      "include_schema": ["shop"],
      "exclude_schema": [],
      "include_table": [],
      "exclude_table": []
    }
  }
}`
}
