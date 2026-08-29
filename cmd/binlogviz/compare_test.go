// Package binlogviz verifies compare command modes and report rendering integration.
// input: file and snapshot report-v3 fixtures with explicit workload identity/scope plus invalid and legacy command arguments.
// output: regression coverage for compare CLI validation, loading, structured comparability output, and file/snapshot behavior.
// pos: command-layer compare suite across Cobra parsing, snapshot storage, compare building, and render dispatch.
// note: if this file changes, update this header and cmd/binlogviz/README.md.
package binlogviz

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompareCommandRejectsRawBinlogsWithOneActionableError(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixture := mustFixturePath(t, "minimal.binlog")
	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		cmd := NewRootCommand()
		cmd.SetArgs([]string{"compare", fixture, fixture})
		err := cmd.Execute()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error:", err)
		}
		return err
	})

	if err == nil {
		t.Fatal("expected raw binlogs to be rejected")
	}
	if stdout != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected one Error line, got %q", stderr)
	}
	if strings.Contains(stderr, "Usage:") {
		t.Fatalf("expected no Usage dump, got %q", stderr)
	}
	if strings.Contains(stderr, "invalid character") {
		t.Fatalf("expected no raw decoder error, got %q", stderr)
	}
	if !strings.Contains(stderr, "compare wants two JSON reports or --current-snapshot/--baseline-snapshot") {
		t.Fatalf("expected actionable input guidance, got %q", stderr)
	}
}

func TestRootCommandRegistersCompareCommand(t *testing.T) {
	cmd := NewRootCommand()
	found := false
	for _, child := range cmd.Commands() {
		if child.Name() == "compare" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected compare command to be registered")
	}
}

func TestCompareCommandRequiresTwoFiles(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{"only-one.json"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "accepts 2 arg(s), received 1") {
		t.Fatalf("expected exact-args error, got %v", err)
	}
}

func TestCompareCommandRejectsOnlyOneSnapshotFlag(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "current only",
			args: []string{"--current-snapshot", "current"},
		},
		{
			name: "baseline only",
			args: []string{"--baseline-snapshot", "baseline"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newCompareCommand()
			cmd.SetArgs(tt.args)
			cmd.SilenceUsage = true
			cmd.SilenceErrors = true

			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), "must be provided together") {
				t.Fatalf("expected paired snapshot flag error, got %v", err)
			}
		})
	}
}

func TestCompareCommandRejectsMixedFileAndSnapshotModes(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		"current.json",
		"baseline.json",
		"--current-snapshot", "current",
		"--baseline-snapshot", "baseline",
	})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "cannot combine file and snapshot compare modes") {
		t.Fatalf("expected mixed-mode error, got %v", err)
	}
}

func TestCompareCommandRejectsUnknownFormat(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{"a.json", "b.json", "--format", "markdown"})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "unsupported compare format") {
		t.Fatalf("expected format error, got %v", err)
	}
}

func TestCompareCommandTextOutputContainsCompareSummary(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "baseline.json"),
		"--format", "text",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{"Compare Summary", "Top Table Changes", "Key Findings", "orders.refunds"} {
		if !strings.Contains(output.String(), token) {
			t.Fatalf("expected text output to contain %q, got %s", token, output.String())
		}
	}
}

func TestCompareCommandJSONOutputIsValidJSON(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "baseline.json"),
		"--format", "json",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid json: %v", err)
	}
}

func TestCompareCommandHTMLOutputContainsHTMLDocument(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "baseline.json"),
		"--format", "html",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{"<html", "compare-top-tables", "compare-alerts"} {
		if !strings.Contains(output.String(), token) {
			t.Fatalf("expected html output to contain %q, got %s", token, output.String())
		}
	}
}

func TestCompareCommandTextOutputContainsPatternChanges(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current_patterns.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "baseline_patterns.json"),
		"--format", "text",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{"Top Pattern Changes", "payments.update_status", "refunds.create"} {
		if !strings.Contains(output.String(), token) {
			t.Fatalf("expected text output to contain %q, got %s", token, output.String())
		}
	}
}

func TestCompareCommandJSONOutputIncludesPatternChanges(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current_patterns.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "baseline_patterns.json"),
		"--format", "json",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid json: %v", err)
	}
	if _, ok := decoded["pattern_changes"]; !ok {
		t.Fatalf("expected pattern_changes in compare output, got %#v", decoded)
	}
}

func TestCompareCommandAcceptsLegacyBaselineWithoutPatterns(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current_patterns.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "legacy_no_patterns.json"),
		"--format", "json",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(output.String(), `"baseline_rows": 0`) {
		t.Fatalf("expected legacy baseline pattern rows to stay at 0, got %s", output.String())
	}
}

func TestCompareCommandHTMLOutputContainsPatternSection(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current_patterns.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "baseline_patterns.json"),
		"--format", "html",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{"compare-pattern-changes", "Pattern Changes", "window.comparePatternChanges"} {
		if !strings.Contains(output.String(), token) {
			t.Fatalf("expected html output to contain %q, got %s", token, output.String())
		}
	}
}

func TestCompareCommandReportsMissingInputFile(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "missing.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "baseline.json"),
		"--format", "json",
	})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "load current report") {
		t.Fatalf("expected missing-file error, got %v", err)
	}
}

func TestCompareCommandAcceptsSnapshotFlags(t *testing.T) {
	dir := t.TempDir()
	writeSnapshotFixture(t, dir, "current-snap", compareSnapshotFixtureJSON("current-snap", "Current Snapshot", "2026-03-20T10:31:00Z"))
	writeSnapshotFixture(t, dir, "baseline-snap", compareSnapshotFixtureJSON("baseline-snap", "Baseline Snapshot", "2026-03-13T10:31:00Z"))

	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		"--current-snapshot", "current-snap",
		"--baseline-snapshot", "baseline-snap",
		"--snapshot-dir", dir,
		"--format", "json",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatalf("expected valid json: %v", err)
	}
	if decoded["current_label"] != "Current Snapshot (current-snap)" {
		t.Fatalf("expected snapshot-aware current label, got %#v", decoded["current_label"])
	}
	if decoded["baseline_label"] != "Baseline Snapshot (baseline-snap)" {
		t.Fatalf("expected snapshot-aware baseline label, got %#v", decoded["baseline_label"])
	}
}

func TestCompareCommandSnapshotModeWorksEndToEnd(t *testing.T) {
	dir := t.TempDir()
	writeSnapshotFixture(t, dir, "current-snap", compareSnapshotFixtureJSON("current-snap", "Current Snapshot", "2026-03-20T10:31:00Z"))
	writeSnapshotFixture(t, dir, "baseline-snap", compareSnapshotFixtureJSON("baseline-snap", "Baseline Snapshot", "2026-03-13T10:31:00Z"))

	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		"--current-snapshot", "current-snap",
		"--baseline-snapshot", "baseline-snap",
		"--snapshot-dir", dir,
		"--format", "text",
	})

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{
		"Compare Summary",
		"Current Snapshot (current-snap)",
		"Baseline Snapshot (baseline-snap)",
		"orders.refunds",
	} {
		if !strings.Contains(output.String(), token) {
			t.Fatalf("expected snapshot compare output to contain %q, got %s", token, output.String())
		}
	}
}

func TestCompareCommandReportsMissingSnapshotClearly(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		"--current-snapshot", "missing-current",
		"--baseline-snapshot", "baseline",
		"--snapshot-dir", t.TempDir(),
		"--format", "text",
	})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected missing snapshot error")
	}
	if !strings.Contains(err.Error(), `load current snapshot "missing-current": snapshot "missing-current" not found`) {
		t.Fatalf("expected clear missing snapshot error, got %v", err)
	}
}

func TestCompareCommandRejectsInvalidReportShape(t *testing.T) {
	cmd := newCompareCommand()
	cmd.SetArgs([]string{
		filepath.Join("..", "..", "internal", "compare", "testdata", "current.json"),
		filepath.Join("..", "..", "internal", "compare", "testdata", "foreign.json"),
		"--format", "json",
	})

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "load baseline report") || !strings.Contains(err.Error(), "unsupported BinlogViz report shape") {
		t.Fatalf("expected invalid-shape error, got %v", err)
	}
}

func writeSnapshotFixture(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name+".json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write snapshot fixture %s: %v", path, err)
	}
}

func compareSnapshotFixtureJSON(name, label, createdAt string) string {
	return `{
  "report_version": 3,
  "workload_id": "orders-production",
  "scope": {"include_schema": ["orders"]},
  "provenance": {"server_ids": [7], "server_versions": ["8.4.0"], "server_flavors": ["mysql"], "mixed_producers": false},
  "summary": {
    "total_transactions": 120,
    "partial_transactions": 0,
    "unknown_transactions": 0,
    "total_rows": 2400,
    "total_events": 3000,
    "start_time": "2026-03-20T10:00:00Z",
    "end_time": "2026-03-20T10:30:00Z",
    "duration": "30m0s"
  },
  "tables": [
    {
      "schema": "orders",
      "table": "payments",
      "total_rows": 1200,
      "insert_rows": 700,
      "update_rows": 400,
      "delete_rows": 100,
      "txn_count": 80
    },
    {
      "schema": "orders",
      "table": "refunds",
      "total_rows": 900,
      "insert_rows": 300,
      "update_rows": 500,
      "delete_rows": 100,
      "txn_count": 25
    }
  ],
  "transactions": [],
  "minutes": [],
  "alerts": [
    {
      "type": "spike",
      "severity": "warning",
      "message": "rows spiked at 10:12",
      "minute": "2026-03-20T10:12:00Z",
      "details": {"rows": 900}
    }
  ],
  "warnings": 0,
  "snapshot": {
    "name": "` + name + `",
    "label": "` + label + `",
    "created_at": "` + createdAt + `",
    "binlogviz_version": "1.2.3",
    "input_mode": "files",
    "input": {
      "files": ["mysql-bin.000001"],
      "from_dir": "",
      "prefix": ""
    },
    "window": {
      "start_time": "2026-03-20T10:00:00Z",
      "end_time": "2026-03-20T10:30:00Z"
    },
    "filters": {
      "include_schema": [],
      "exclude_schema": [],
      "include_table": [],
      "exclude_table": []
    }
  }
}`
}
