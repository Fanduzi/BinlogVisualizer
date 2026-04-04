package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	snapshotpkg "binlogviz/internal/snapshot"
)

func TestSnapshotCommandRegistrationAndSubtreePresence(t *testing.T) {
	cmd := NewRootCommand()

	for _, args := range [][]string{
		{"snapshot"},
		{"snapshot", "save"},
		{"snapshot", "list"},
		{"snapshot", "show"},
		{"snapshot", "rename"},
		{"snapshot", "delete"},
	} {
		found, _, err := cmd.Find(args)
		if err != nil {
			t.Fatalf("find %v: %v", args, err)
		}
		if found == nil {
			t.Fatalf("expected command for %v", args)
		}
	}
}

func TestSnapshotSaveCommandSuccessPath(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	reportPath := writeSnapshotSourceReport(t, false)
	snapshotDir := t.TempDir()

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "save", reportPath, "--name", "incident_window", "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}

	savedPath := filepath.Join(snapshotDir, "incident_window.json")
	savedBytes, err := os.ReadFile(savedPath)
	if err != nil {
		t.Fatalf("read saved snapshot: %v", err)
	}

	var saved struct {
		Snapshot *struct {
			Name  string `json:"name"`
			Label string `json:"label"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal(savedBytes, &saved); err != nil {
		t.Fatalf("decode saved snapshot: %v", err)
	}
	if saved.Snapshot == nil {
		t.Fatalf("expected saved snapshot metadata, got %s", savedBytes)
	}
	if saved.Snapshot.Name != "incident_window" {
		t.Fatalf("expected injected snapshot name, got %q", saved.Snapshot.Name)
	}
	if saved.Snapshot.Label != "incident_window" {
		t.Fatalf("expected injected snapshot label, got %q", saved.Snapshot.Label)
	}
	if !strings.Contains(stderr, `Saved snapshot "incident_window" to `) || !strings.Contains(stderr, savedPath) {
		t.Fatalf("expected save path on stderr, got %q", stderr)
	}
}

func TestSnapshotListCommandShowsSavedSnapshots(t *testing.T) {
	dir := t.TempDir()
	first := []byte(minimalSnapshotReportJSON("first", "baseline", 1200, 3))
	second := []byte(minimalSnapshotReportJSON("second", "candidate", 2400, 1))

	if _, err := snapshotpkg.SaveJSON(dir, "zeta", first); err != nil {
		t.Fatalf("save zeta: %v", err)
	}
	if _, err := snapshotpkg.SaveJSON(dir, "alpha", second); err != nil {
		t.Fatalf("save alpha: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "list", "--snapshot-dir", dir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "alpha") || !strings.Contains(stdout, "zeta") {
		t.Fatalf("expected snapshot names in stdout, got %q", stdout)
	}
	if strings.Index(stdout, "alpha") > strings.Index(stdout, "zeta") {
		t.Fatalf("expected sorted snapshot list, got %q", stdout)
	}
	for _, token := range []string{
		"NAME",
		"LABEL",
		"CREATED_AT",
		"INPUT_MODE",
		"WINDOW",
		"candidate",
		"baseline",
		"files",
		"2026-03-20T10:00:00Z -> 2026-03-20T10:30:00Z",
	} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("expected snapshot list text output to contain %q, got %q", token, stdout)
		}
	}
}

func TestSnapshotShowCommandShowsMetadataAndSummary(t *testing.T) {
	dir := t.TempDir()
	if _, err := snapshotpkg.SaveJSON(dir, "incident_window", []byte(minimalSnapshotReportJSON("incident_window", "current", 2400, 2))); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "show", "incident_window", "--snapshot-dir", dir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	for _, token := range []string{"incident_window", "current", "total_rows: 2400", "warnings: 2"} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("expected %q in stdout, got %q", token, stdout)
		}
	}
}

func TestSnapshotShowCommandMissingSnapshotErrorIsClear(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "show", "missing_snapshot", "--snapshot-dir", t.TempDir()})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	_, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err == nil {
		t.Fatal("expected missing snapshot error")
	}
	if !strings.Contains(err.Error(), `snapshot "missing_snapshot" not found`) {
		t.Fatalf("expected clear missing snapshot error, got %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr on execution failure, got %q", stderr)
	}
}

func TestSnapshotListCommandJSONOutput(t *testing.T) {
	dir := t.TempDir()
	if _, err := snapshotpkg.SaveJSON(dir, "zeta", []byte(minimalSnapshotReportJSON("zeta", "zeta", 1200, 3))); err != nil {
		t.Fatalf("save zeta: %v", err)
	}
	if _, err := snapshotpkg.SaveJSON(dir, "alpha", []byte(minimalSnapshotReportJSON("alpha", "alpha", 2400, 1))); err != nil {
		t.Fatalf("save alpha: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "list", "--snapshot-dir", dir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	var decoded struct {
		SnapshotDir string `json:"snapshot_dir"`
		Snapshots   []struct {
			Name      string `json:"name"`
			Path      string `json:"path"`
			InputMode string `json:"input_mode"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if decoded.SnapshotDir != dir {
		t.Fatalf("unexpected snapshot dir: got %q want %q", decoded.SnapshotDir, dir)
	}
	if len(decoded.Snapshots) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(decoded.Snapshots))
	}
	if decoded.Snapshots[0].Name != "alpha" || decoded.Snapshots[1].Name != "zeta" {
		t.Fatalf("expected sorted snapshots, got %+v", decoded.Snapshots)
	}
	if decoded.Snapshots[0].InputMode != "files" {
		t.Fatalf("expected input_mode in list output, got %+v", decoded.Snapshots[0])
	}
}

func TestSnapshotShowCommandJSONOutput(t *testing.T) {
	dir := t.TempDir()
	if _, err := snapshotpkg.SaveJSON(dir, "incident_window", []byte(minimalSnapshotReportJSON("incident_window", "current", 2400, 2))); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "show", "incident_window", "--snapshot-dir", dir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	var decoded struct {
		Snapshot struct {
			Name      string `json:"name"`
			Label     string `json:"label"`
			Path      string `json:"path"`
			InputMode string `json:"input_mode"`
			Summary   struct {
				TotalRows int `json:"total_rows"`
			} `json:"summary"`
			Warnings int `json:"warnings"`
		} `json:"snapshot"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode json output: %v\n%s", err, stdout)
	}
	if decoded.Snapshot.Name != "incident_window" {
		t.Fatalf("unexpected snapshot name: %q", decoded.Snapshot.Name)
	}
	if decoded.Snapshot.Label != "current" {
		t.Fatalf("unexpected snapshot label: %q", decoded.Snapshot.Label)
	}
	if decoded.Snapshot.Path != filepath.Join(dir, "incident_window.json") {
		t.Fatalf("unexpected snapshot path: %q", decoded.Snapshot.Path)
	}
	if decoded.Snapshot.InputMode != "files" {
		t.Fatalf("unexpected input mode: %q", decoded.Snapshot.InputMode)
	}
	if decoded.Snapshot.Summary.TotalRows != 2400 {
		t.Fatalf("unexpected total rows: %d", decoded.Snapshot.Summary.TotalRows)
	}
	if decoded.Snapshot.Warnings != 2 {
		t.Fatalf("unexpected warnings: %d", decoded.Snapshot.Warnings)
	}
}

func TestSnapshotListCommandRejectsInvalidFormat(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "list", "--snapshot-dir", t.TempDir(), "--format", "yaml"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected invalid format error")
	}
	if !strings.Contains(err.Error(), "unsupported snapshot list format") {
		t.Fatalf("expected invalid format error, got %v", err)
	}
}

func TestSnapshotShowCommandRejectsInvalidFormat(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "show", "incident_window", "--snapshot-dir", t.TempDir(), "--format", "yaml"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected invalid format error")
	}
	if !strings.Contains(err.Error(), "unsupported snapshot show format") {
		t.Fatalf("expected invalid format error, got %v", err)
	}
}

func TestSnapshotRenameCommandRenamesStoredSnapshot(t *testing.T) {
	dir := t.TempDir()
	if _, err := snapshotpkg.SaveJSON(dir, "old_name", []byte(minimalSnapshotReportJSON("old_name", "old_name", 2400, 2))); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "rename", "old_name", "new_name", "--snapshot-dir", dir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if !strings.Contains(stderr, filepath.Join(dir, "new_name.json")) {
		t.Fatalf("expected renamed path in stderr, got %q", stderr)
	}

	showCmd := NewRootCommand()
	showCmd.SetArgs([]string{"snapshot", "show", "new_name", "--snapshot-dir", dir, "--format", "json"})
	showCmd.SilenceUsage = true
	showCmd.SilenceErrors = true

	showStdout, _, err := captureStdoutStderrRun(t, func() error {
		return showCmd.Execute()
	})
	if err != nil {
		t.Fatalf("show renamed snapshot: %v", err)
	}
	if !strings.Contains(showStdout, `"name": "new_name"`) {
		t.Fatalf("expected renamed snapshot in show output, got %q", showStdout)
	}
}

func TestSnapshotDeleteCommandRemovesStoredSnapshot(t *testing.T) {
	dir := t.TempDir()
	if _, err := snapshotpkg.SaveJSON(dir, "incident_window", []byte(minimalSnapshotReportJSON("incident_window", "incident_window", 2400, 2))); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "delete", "incident_window", "--snapshot-dir", dir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if !strings.Contains(stderr, filepath.Join(dir, "incident_window.json")) {
		t.Fatalf("expected deleted path in stderr, got %q", stderr)
	}

	listCmd := NewRootCommand()
	listCmd.SetArgs([]string{"snapshot", "list", "--snapshot-dir", dir})
	listCmd.SilenceUsage = true
	listCmd.SilenceErrors = true

	listStdout, _, err := captureStdoutStderrRun(t, func() error {
		return listCmd.Execute()
	})
	if err != nil {
		t.Fatalf("list snapshots after delete: %v", err)
	}
	if strings.Contains(listStdout, "incident_window") {
		t.Fatalf("expected deleted snapshot absent from list, got %q", listStdout)
	}
}

func TestSnapshotSaveCommandRejectsExistingName(t *testing.T) {
	reportPath := writeSnapshotSourceReport(t, false)
	snapshotDir := t.TempDir()
	if _, err := snapshotpkg.SaveJSON(snapshotDir, "incident_window", []byte(minimalSnapshotReportJSON("incident_window", "incident_window", 2400, 2))); err != nil {
		t.Fatalf("seed snapshot: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "save", reportPath, "--name", "incident_window", "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected name conflict error")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected name conflict error, got %v", err)
	}
}

func TestSnapshotSaveCommandRejectsInvalidName(t *testing.T) {
	reportPath := writeSnapshotSourceReport(t, false)

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "save", reportPath, "--name", "bad.name", "--snapshot-dir", t.TempDir()})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected invalid name error")
	}
	if !strings.Contains(err.Error(), `invalid snapshot name "bad.name"`) {
		t.Fatalf("expected invalid name error, got %v", err)
	}
}

func TestSnapshotRenameCommandMissingSnapshotErrorIsClear(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "rename", "missing_snapshot", "new_name", "--snapshot-dir", t.TempDir()})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected missing snapshot error")
	}
	if !strings.Contains(err.Error(), `snapshot "missing_snapshot" not found`) {
		t.Fatalf("expected clear missing snapshot error, got %v", err)
	}
}

func TestSnapshotDeleteCommandMissingSnapshotErrorIsClear(t *testing.T) {
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "delete", "missing_snapshot", "--snapshot-dir", t.TempDir()})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected missing snapshot error")
	}
	if !strings.Contains(err.Error(), `snapshot "missing_snapshot" not found`) {
		t.Fatalf("expected clear missing snapshot error, got %v", err)
	}
}

func writeSnapshotSourceReport(t *testing.T, withMetadata bool) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "report.json")
	payload := minimalSnapshotReportJSON("source", "source", 2400, 1)
	if !withMetadata {
		payload = minimalAnalyzeReportJSON(2400, 1)
	}
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		t.Fatalf("write source report: %v", err)
	}
	return path
}

func minimalAnalyzeReportJSON(totalRows, warnings int) string {
	return `{
  "summary": {
    "total_transactions": 120,
    "total_rows": ` + intString(totalRows) + `,
    "total_events": 3000,
    "start_time": "2026-03-20T10:00:00Z",
    "end_time": "2026-03-20T10:30:00Z",
    "duration": "30m0s"
  },
  "tables": [
    {
      "schema": "shop",
      "table": "orders",
      "total_rows": ` + intString(totalRows) + `,
      "insert_rows": 700,
      "update_rows": 400,
      "delete_rows": 100,
      "txn_count": 80
    }
  ],
  "alerts": [],
  "warnings": ` + intString(warnings) + `
}`
}

func minimalSnapshotReportJSON(name, label string, totalRows, warnings int) string {
	return `{
  "summary": {
    "total_transactions": 120,
    "total_rows": ` + intString(totalRows) + `,
    "total_events": 3000,
    "start_time": "2026-03-20T10:00:00Z",
    "end_time": "2026-03-20T10:30:00Z",
    "duration": "30m0s"
  },
  "tables": [
    {
      "schema": "shop",
      "table": "orders",
      "total_rows": ` + intString(totalRows) + `,
      "insert_rows": 700,
      "update_rows": 400,
      "delete_rows": 100,
      "txn_count": 80
    }
  ],
  "alerts": [],
  "warnings": ` + intString(warnings) + `,
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

func intString(v int) string {
	return strconv.Itoa(v)
}
