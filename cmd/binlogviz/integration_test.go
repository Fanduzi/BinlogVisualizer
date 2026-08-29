// Package binlogviz validates end-to-end analyze command behavior and DuckDB temp-store lifecycle.
// input: mock parsers (including MariaDB XA/Annotate sequences), fixture binlogs, CLI-derived analyzer options, and temporary command resources.
// output: regression coverage for XA/LOAD_DATA reports, selected-file/count-event byte coverage, rendered output, temp DuckDB cleanup, and command/analyzer integration semantics.
// pos: command-layer integration test suite covering parse-normalize-analyze-render execution paths.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
	snapshotpkg "binlogviz/internal/snapshot"
)

type fakeStreamingAnalyzer struct {
	consumed     []model.NormalizedEvent
	consumeErr   error
	finalizeErr  error
	finalResult  *model.AnalysisResult
	finalized    bool
	consumeCalls int
}

func (f *fakeStreamingAnalyzer) Consume(ev model.NormalizedEvent) error {
	f.consumeCalls++
	if f.consumeErr != nil {
		return f.consumeErr
	}
	f.consumed = append(f.consumed, ev)
	return nil
}

func (f *fakeStreamingAnalyzer) Finalize() (*model.AnalysisResult, error) {
	f.finalized = true
	if f.finalizeErr != nil {
		return nil, f.finalizeErr
	}
	if f.finalResult != nil {
		return f.finalResult, nil
	}
	return &model.AnalysisResult{}, nil
}

// mockParser implements binlog.Parser for testing.
type mockParser struct {
	events                 []binlog.RawEvent
	err                    error
	parseFiles             func(paths []string, handler func(binlog.RawEvent) error) error
	parseFilesWithProgress func(paths []string, onProgress func(binlog.ParseProgress), handler func(binlog.RawEvent) error) error
}

func (m *mockParser) ParseFiles(paths []string, handler func(binlog.RawEvent) error) error {
	if m.parseFiles != nil {
		return m.parseFiles(paths, handler)
	}
	if m.err != nil {
		return m.err
	}
	for _, ev := range m.events {
		if err := handler(ev); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockParser) ParseFilesWithProgress(paths []string, onProgress func(binlog.ParseProgress), handler func(binlog.RawEvent) error) error {
	if m.parseFilesWithProgress != nil {
		return m.parseFilesWithProgress(paths, onProgress, handler)
	}
	if m.parseFiles != nil {
		return m.parseFiles(paths, handler)
	}
	if m.err != nil {
		return m.err
	}
	for index, path := range paths {
		for _, ev := range m.events {
			if onProgress != nil {
				onProgress(binlog.ParseProgress{Path: path, Index: index, Offset: int64(ev.Position)})
			}
			if err := handler(ev); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestAnalyzeCommandRejectsMissingFiles(t *testing.T) {
	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{"missing-binlog.000001"})

	// Silence usage output during test
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestResolveAnalyzePathsRejectsMixedModes(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	opts := &analyzeOptions{fromDir: "/tmp/binlogs", prefix: "mysql-bin."}

	_, _, _, err := resolveAnalyzePaths([]string{"mysql-bin.000123"}, opts)
	if err == nil || !strings.Contains(err.Error(), "cannot combine") {
		t.Fatalf("expected mixed-mode error, got %v", err)
	}
}

func TestResolveAnalyzePathsRejectsIncompleteDiscoveryFlags(t *testing.T) {
	tests := []struct {
		name string
		opts *analyzeOptions
	}{
		{name: "missing prefix", opts: &analyzeOptions{fromDir: "/tmp/binlogs"}},
		{name: "missing from-dir", opts: &analyzeOptions{prefix: "mysql-bin."}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, err := resolveAnalyzePaths(nil, tt.opts)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestResolveAnalyzePathsExplicitArgsRemainUnchanged(t *testing.T) {
	opts := &analyzeOptions{}
	want := []string{"a", "b"}

	got, discovered, _, err := resolveAnalyzePaths(want, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if discovered {
		t.Fatal("expected explicit mode")
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected paths: %#v", got)
	}
}

func TestRunAnalysisParsesMultipleFilesConcurrentlyAndConsumesInOrder(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	paths := []string{"mysql-bin.000044", "mysql-bin.000045", "mysql-bin.000046"}
	delays := map[string]time.Duration{
		paths[0]: 60 * time.Millisecond,
		paths[1]: 5 * time.Millisecond,
		paths[2]: 5 * time.Millisecond,
	}

	var mu sync.Mutex
	activeParses := 0
	peakParses := 0
	parser := &mockParser{
		parseFilesWithProgress: func(parsePaths []string, onProgress func(binlog.ParseProgress), handler func(binlog.RawEvent) error) error {
			for _, path := range parsePaths {
				index := slices.Index(paths, path)
				if index < 0 {
					t.Fatalf("unexpected path %q", path)
				}
				mu.Lock()
				activeParses++
				if activeParses > peakParses {
					peakParses = activeParses
				}
				mu.Unlock()

				time.Sleep(delays[path])
				if onProgress != nil {
					onProgress(binlog.ParseProgress{Path: path, Index: index, Offset: 1})
				}
				if err := handler(binlog.RawEvent{
					Timestamp:  time.Date(2026, 4, 18, 10, index, 0, 0, time.UTC),
					BinlogPath: path,
					EventType:  "WRITE_ROWS_EVENT",
					Schema:     "shop",
					Table:      path,
					RowCount:   1,
				}); err != nil {
					return err
				}

				mu.Lock()
				activeParses--
				mu.Unlock()
			}
			return nil
		},
	}

	fakeAnalyzer := &fakeStreamingAnalyzer{finalResult: &model.AnalysisResult{}}
	_, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisStreamingFastWithSnapshot(
			paths,
			analyzer.DefaultOptions(),
			report.DefaultOptions(),
			"json",
			parser,
			func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer {
				return fakeAnalyzer
			},
			createDuckDBTempStore,
			t.TempDir(),
			nil,
			model.FileCoverage{},
			"",
			"",
		)
	})
	if err != nil {
		t.Fatalf("runAnalysisStreamingFastWithSnapshot returned error: %v", err)
	}

	mu.Lock()
	gotPeak := peakParses
	mu.Unlock()
	if gotPeak < 2 {
		t.Fatalf("expected multiple files to be parsed concurrently, peak concurrency was %d", gotPeak)
	}

	if len(fakeAnalyzer.consumed) != len(paths) {
		t.Fatalf("expected %d consumed events, got %d", len(paths), len(fakeAnalyzer.consumed))
	}
	for index, path := range paths {
		if fakeAnalyzer.consumed[index].BinlogPath != path {
			t.Fatalf("expected analyzer consumption to preserve file order at %d: want %s got %s", index, path, fakeAnalyzer.consumed[index].BinlogPath)
		}
	}
}

func TestDiscoverBinlogPathsFindsAndSortsNumericSuffixes(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, dir+"/mysql-bin.000124")
	mustWriteFile(t, dir+"/mysql-bin.000123")
	mustWriteFile(t, dir+"/mysql-bin.index")
	mustWriteFile(t, dir+"/other.log")

	got, err := discoverBinlogPaths(dir, "mysql-bin.")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{dir + "/mysql-bin.000123", dir + "/mysql-bin.000124"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected paths: want=%v got=%v", want, got)
	}
}

func TestDiscoverBinlogPathsSortsByNumericSuffixNotLexicalOrder(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, dir+"/mysql-bin.10")
	mustWriteFile(t, dir+"/mysql-bin.9")

	got, err := discoverBinlogPaths(dir, "mysql-bin.")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{dir + "/mysql-bin.9", dir + "/mysql-bin.10"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected numeric order: want=%v got=%v", want, got)
	}
}

func TestDiscoverBinlogPathsSortsNumericSuffixesWithoutDotSeparator(t *testing.T) {
	dir := t.TempDir()
	mustWriteFile(t, dir+"/mysql-bin10")
	mustWriteFile(t, dir+"/mysql-bin9")

	got, err := discoverBinlogPaths(dir, "mysql-bin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{dir + "/mysql-bin9", dir + "/mysql-bin10"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected numeric order without dot separator: want=%v got=%v", want, got)
	}
}

func TestDiscoverBinlogPathsFailsWhenNoMatches(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	dir := t.TempDir()
	mustWriteFile(t, dir+"/mysql-bin.index")

	_, err := discoverBinlogPaths(dir, "mysql-bin.")
	if err == nil || !strings.Contains(err.Error(), "no matching binlog files") {
		t.Fatalf("expected no-match error, got %v", err)
	}
}

func TestAnalyzeCommandRejectsFromDirWithoutPrefix(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{"--from-dir", t.TempDir()})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "must be provided together") {
		t.Fatalf("expected paired-flag error, got %v", err)
	}
}

func TestAnalyzeCommandRejectsPrefixWithoutFromDir(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{"--prefix", "mysql-bin."})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "must be provided together") {
		t.Fatalf("expected paired-flag error, got %v", err)
	}
}

func TestAnalyzeCommandRejectsMixedPositionalAndDiscoveryModes(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	file, err := os.CreateTemp(t.TempDir(), "binlog.*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}

	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{file.Name(), "--from-dir", t.TempDir(), "--prefix", "mysql-bin."})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	err = cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "cannot combine") {
		t.Fatalf("expected mixed-mode error, got %v", err)
	}
}

func TestAnalyzeCommandDiscoveryModePrintsResolvedFilesToStderr(t *testing.T) {
	dir := t.TempDir()
	fixture := mustFixturePath(t, "minimal.binlog")
	copyFile(t, fixture, dir+"/mysql-bin.000123")
	copyFile(t, fixture, dir+"/mysql-bin.000124")

	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{"--from-dir", dir, "--prefix", "mysql-bin.", "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !json.Valid([]byte(stdout)) {
		t.Fatalf("stdout must be valid json: %s", stdout)
	}
	if !strings.Contains(stderr, "mysql-bin.000123") || !strings.Contains(stderr, "mysql-bin.000124") {
		t.Fatalf("stderr must list resolved files, got: %s", stderr)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(stdout), &parsed); err != nil {
		t.Fatalf("unmarshal stdout json: %v", err)
	}
	diagnostics, ok := parsed["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected diagnostics object, got %v", parsed["diagnostics"])
	}
	fileCoverage, ok := diagnostics["file_coverage"].(map[string]any)
	if !ok {
		t.Fatalf("expected diagnostics.file_coverage object, got %v", diagnostics["file_coverage"])
	}
	selected, ok := fileCoverage["selected"].([]any)
	if !ok || len(selected) != 2 {
		t.Fatalf("expected two selected file coverage entries, got %v", fileCoverage["selected"])
	}
}

func TestResolveAnalyzePathsDiscoveryModeNarrowsFilesByTimeWindow(t *testing.T) {
	dir := t.TempDir()
	path44 := filepath.Join(dir, "mysql-bin.000044")
	path45 := filepath.Join(dir, "mysql-bin.000045")
	path46 := filepath.Join(dir, "mysql-bin.000046")
	mustWriteFile(t, path44)
	mustWriteFile(t, path45)
	mustWriteFile(t, path46)

	mustSetFileModTime(t, path44, time.Date(2026, 4, 5, 9, 30, 0, 0, time.UTC))
	mustSetFileModTime(t, path45, time.Date(2026, 4, 5, 10, 30, 0, 0, time.UTC))
	mustSetFileModTime(t, path46, time.Date(2026, 4, 5, 11, 30, 0, 0, time.UTC))

	originalProbeAnalyzePaths := probeAnalyzePaths
	probeAnalyzePaths = func(paths []string, workerCount int) ([]binlog.FileProbe, error) {
		return []binlog.FileProbe{
			{
				BinlogPath:   path44,
				FirstEventAt: time.Date(2026, 4, 5, 8, 45, 0, 0, time.UTC),
				LastEventAt:  time.Date(2026, 4, 5, 9, 59, 59, 0, time.UTC),
			},
			{
				BinlogPath:   path45,
				FirstEventAt: time.Date(2026, 4, 5, 10, 30, 1, 0, time.UTC),
				LastEventAt:  time.Date(2026, 4, 5, 11, 30, 0, 0, time.UTC),
			},
			{
				BinlogPath:   path46,
				FirstEventAt: time.Date(2026, 4, 5, 11, 30, 1, 0, time.UTC),
				LastEventAt:  time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC),
			},
		}, nil
	}
	t.Cleanup(func() {
		probeAnalyzePaths = originalProbeAnalyzePaths
	})

	opts := &analyzeOptions{
		fromDir:   dir,
		prefix:    "mysql-bin.",
		startTime: "2026-04-05T09:00:00Z",
		endTime:   "2026-04-05T10:00:00Z",
	}

	got, discovered, _, err := resolveAnalyzePaths(nil, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !discovered {
		t.Fatal("expected discovery mode")
	}

	want := []string{path44}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected time-window paths: want=%v got=%v", want, got)
	}
}

func TestAnalyzeCommandJSONSnapshotFlowPersistsReportAndPrintsSavePath(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixture := mustFixturePath(t, "minimal.binlog")
	snapshotDir := t.TempDir()
	snapshotName := "incident_window"

	cmd := NewRootCommand()
	cmd.SetArgs([]string{
		"analyze",
		fixture,
		"--format", "json",
		"--snapshot-name", snapshotName,
		"--snapshot-dir", snapshotDir,
		"--start", "2026-03-15T14:00:00Z",
		"--end", "2026-03-15T15:00:00Z",
		"--include-schema", "testdb",
	})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !json.Valid([]byte(stdout)) {
		t.Fatalf("stdout must be valid json, got: %s", stdout)
	}

	savedPath := filepath.Join(snapshotDir, snapshotName+".json")
	savedBytes, err := os.ReadFile(savedPath)
	if err != nil {
		t.Fatalf("read saved snapshot: %v", err)
	}
	if strings.TrimSpace(string(savedBytes)) != strings.TrimSpace(stdout) {
		t.Fatalf("saved snapshot content must match stdout\nsaved=%s\nstdout=%s", savedBytes, stdout)
	}
	if !strings.Contains(stderr, savedPath) {
		t.Fatalf("stderr must contain saved snapshot path %q, got: %s", savedPath, stderr)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(stdout), &parsed); err != nil {
		t.Fatalf("unmarshal stdout json: %v", err)
	}
	snapshot, ok := parsed["snapshot"].(map[string]any)
	if !ok {
		t.Fatalf("expected snapshot metadata object, got %v", parsed["snapshot"])
	}
	if snapshot["name"] != snapshotName {
		t.Fatalf("expected snapshot.name %q, got %v", snapshotName, snapshot["name"])
	}
	if snapshot["label"] != snapshotName {
		t.Fatalf("expected snapshot.label %q, got %v", snapshotName, snapshot["label"])
	}
	if snapshot["input_mode"] != "files" {
		t.Fatalf("expected snapshot.input_mode files, got %v", snapshot["input_mode"])
	}
	if !strings.Contains(stderr, `Saved snapshot "incident_window" to `) {
		t.Fatalf("stderr must contain save message, got: %s", stderr)
	}
	input, ok := snapshot["input"].(map[string]any)
	if !ok {
		t.Fatalf("expected snapshot.input object, got %v", snapshot["input"])
	}
	files, ok := input["files"].([]any)
	if !ok || len(files) != 1 || files[0] != fixture {
		t.Fatalf("expected snapshot.input.files to contain fixture path, got %v", input["files"])
	}
	window, ok := snapshot["window"].(map[string]any)
	if !ok {
		t.Fatalf("expected snapshot.window object, got %v", snapshot["window"])
	}
	if window["start_time"] != "2026-03-15T14:00:00Z" || window["end_time"] != "2026-03-15T15:00:00Z" {
		t.Fatalf("unexpected snapshot.window payload: %v", window)
	}
	filters, ok := snapshot["filters"].(map[string]any)
	if !ok {
		t.Fatalf("expected snapshot.filters object, got %v", snapshot["filters"])
	}
	includeSchemas, ok := filters["include_schema"].([]any)
	if !ok || len(includeSchemas) != 1 || includeSchemas[0] != "testdb" {
		t.Fatalf("expected snapshot.filters.include_schema to contain testdb, got %v", filters["include_schema"])
	}
}

func TestSnapshotImportOldAnalyzeJSONAndCompareByName(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	reportDir := t.TempDir()
	snapshotDir := t.TempDir()
	baselinePath := filepath.Join(reportDir, "baseline.json")
	currentPath := filepath.Join(reportDir, "current.json")
	if err := os.WriteFile(baselinePath, []byte(minimalAnalyzeReportJSONForIntegration(1200, 1)), 0o644); err != nil {
		t.Fatalf("write baseline report: %v", err)
	}
	if err := os.WriteFile(currentPath, []byte(minimalAnalyzeReportJSONForIntegration(2400, 2)), 0o644); err != nil {
		t.Fatalf("write current report: %v", err)
	}

	saveBaseline := NewRootCommand()
	saveBaseline.SetArgs([]string{"snapshot", "save", baselinePath, "--name", "baseline", "--snapshot-dir", snapshotDir})
	saveBaseline.SilenceUsage = true
	saveBaseline.SilenceErrors = true
	if err := saveBaseline.Execute(); err != nil {
		t.Fatalf("save baseline snapshot: %v", err)
	}

	saveCurrent := NewRootCommand()
	saveCurrent.SetArgs([]string{"snapshot", "save", currentPath, "--name", "current", "--snapshot-dir", snapshotDir})
	saveCurrent.SilenceUsage = true
	saveCurrent.SilenceErrors = true
	if err := saveCurrent.Execute(); err != nil {
		t.Fatalf("save current snapshot: %v", err)
	}

	compareCmd := NewRootCommand()
	compareCmd.SetArgs([]string{
		"compare",
		"--current-snapshot", "current",
		"--baseline-snapshot", "baseline",
		"--snapshot-dir", snapshotDir,
		"--format", "text",
	})
	compareCmd.SilenceUsage = true
	compareCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return compareCmd.Execute()
	})
	if err != nil {
		t.Fatalf("compare imported snapshots: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}
	for _, token := range []string{
		"Current Label: current",
		"Baseline Label: baseline",
		"Current Input Mode: imported",
		"Baseline Input Mode: imported",
	} {
		if !strings.Contains(stdout, token) {
			t.Fatalf("expected compare output to contain %q, got %q", token, stdout)
		}
	}
}

func TestSnapshotCommandsUseDefaultHomeSnapshotDir(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	home := t.TempDir()
	t.Setenv("HOME", home)
	reportPath := filepath.Join(t.TempDir(), "import.json")
	if err := os.WriteFile(reportPath, []byte(minimalAnalyzeReportJSONForIntegration(2400, 1)), 0o644); err != nil {
		t.Fatalf("write import report: %v", err)
	}

	saveCmd := NewRootCommand()
	saveCmd.SetArgs([]string{"snapshot", "save", reportPath, "--name", "default_home_snapshot"})
	saveCmd.SilenceUsage = true
	saveCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return saveCmd.Execute()
	})
	if err != nil {
		t.Fatalf("save default-home snapshot: %v", err)
	}
	if strings.TrimSpace(stdout) != "" {
		t.Fatalf("expected empty stdout, got %q", stdout)
	}

	expectedDir := filepath.Join(home, ".binlogviz", "snapshots")
	expectedPath := filepath.Join(expectedDir, "default_home_snapshot.json")
	if _, err := os.Stat(expectedPath); err != nil {
		t.Fatalf("expected snapshot at default path %q: %v", expectedPath, err)
	}
	if !strings.Contains(stderr, expectedPath) {
		t.Fatalf("expected stderr to contain default snapshot path %q, got %q", expectedPath, stderr)
	}

	listCmd := NewRootCommand()
	listCmd.SetArgs([]string{"snapshot", "list", "--format", "json"})
	listCmd.SilenceUsage = true
	listCmd.SilenceErrors = true

	listStdout, _, err := captureStdoutStderrRun(t, func() error {
		return listCmd.Execute()
	})
	if err != nil {
		t.Fatalf("list default-home snapshots: %v", err)
	}

	var decoded struct {
		SnapshotDir string `json:"snapshot_dir"`
		Snapshots   []struct {
			Name string `json:"name"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal([]byte(listStdout), &decoded); err != nil {
		t.Fatalf("decode list json: %v\n%s", err, listStdout)
	}
	if decoded.SnapshotDir != expectedDir {
		t.Fatalf("unexpected default snapshot dir: got %q want %q", decoded.SnapshotDir, expectedDir)
	}
	if len(decoded.Snapshots) != 1 || decoded.Snapshots[0].Name != "default_home_snapshot" {
		t.Fatalf("unexpected default-home snapshots: %+v", decoded.Snapshots)
	}
}

func forceEnglishRuntimeOutput(t *testing.T) {
	t.Helper()

	t.Setenv("LANG", "en_US.UTF-8")
	t.Setenv("LC_ALL", "en_US.UTF-8")
	i18n.ResetForTesting()
	if err := i18n.Init("en"); err != nil {
		t.Fatalf("init english i18n: %v", err)
	}
	t.Cleanup(i18n.ResetForTesting)
}

func minimalAnalyzeReportJSONForIntegration(totalRows, warnings int) string {
	return `{
  "summary": {
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
  "warnings": ` + strconv.Itoa(warnings) + `
}`
}

func TestAnalyzeToCompareWorkflowWithGeneratedReports(t *testing.T) {
	fixture := mustFixturePath(t, "minimal.binlog")
	tempDir := t.TempDir()
	currentPath := filepath.Join(tempDir, "current.json")
	baselinePath := filepath.Join(tempDir, "baseline.json")

	runAnalyze := func(args ...string) string {
		t.Helper()

		cmd := NewRootCommand()
		cmd.SetArgs(append([]string{"analyze"}, args...))
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true

		stdout, _, err := captureStdoutStderrRun(t, func() error {
			return cmd.Execute()
		})
		if err != nil {
			t.Fatalf("analyze failed for args %v: %v", args, err)
		}
		if !json.Valid([]byte(stdout)) {
			t.Fatalf("analyze stdout must be valid json for args %v, got: %s", args, stdout)
		}
		return stdout
	}

	currentJSON := runAnalyze(fixture, fixture, "--format", "json")
	if err := os.WriteFile(currentPath, []byte(currentJSON), 0o644); err != nil {
		t.Fatalf("write current report: %v", err)
	}

	baselineJSON := runAnalyze(fixture, "--format", "json")
	if err := os.WriteFile(baselinePath, []byte(baselineJSON), 0o644); err != nil {
		t.Fatalf("write baseline report: %v", err)
	}

	runCompare := func(format string) string {
		t.Helper()

		cmd := NewRootCommand()
		cmd.SetArgs([]string{"compare", currentPath, baselinePath, "--format", format})
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		output := &bytes.Buffer{}
		cmd.SetOut(output)
		cmd.SetErr(io.Discard)

		if err := cmd.Execute(); err != nil {
			t.Fatalf("compare failed for format %s: %v", format, err)
		}
		return output.String()
	}

	textOut := runCompare("text")
	if !strings.Contains(textOut, "Compare Summary") {
		t.Fatalf("expected text compare output to contain Compare Summary, got: %s", textOut)
	}

	jsonOut := runCompare("json")
	var decoded map[string]any
	if err := json.Unmarshal([]byte(jsonOut), &decoded); err != nil {
		t.Fatalf("expected compare json output to be valid json: %v", err)
	}
	if _, ok := decoded["summary"]; !ok {
		t.Fatalf("expected compare json output to contain summary, got: %v", decoded)
	}
	if _, ok := decoded["table_changes"]; !ok {
		t.Fatalf("expected compare json output to contain table_changes, got: %v", decoded)
	}

	htmlOut := runCompare("html")
	for _, token := range []string{"<html", "compare-summary-chart", "echarts.init(document.getElementById('compare-summary-chart'))"} {
		if !strings.Contains(htmlOut, token) {
			t.Fatalf("expected compare html output to contain %q, got: %s", token, htmlOut)
		}
	}
}

func TestRunAnalysisHappyPath(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	// Create mock parser with sample events
	mock := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 1, 0, time.UTC), EventType: "TABLE_MAP_EVENT", Schema: "shop", Table: "orders"},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 1, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "shop", Table: "orders", RowCount: 5},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 2, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	opts := analyzer.Options{
		TopTables:       10,
		TopTransactions: 10,
	}

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := runAnalysisWithParser([]string{"dummy.binlog"}, opts, "text", mock)

	// Restore stdout
	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify output contains expected sections
	if !bytes.Contains([]byte(output), []byte("=== Summary ===")) {
		t.Error("expected output to contain Summary section")
	}
	if !bytes.Contains([]byte(output), []byte("=== Top Tables ===")) {
		t.Error("expected output to contain Top Tables section")
	}
	if !bytes.Contains([]byte(output), []byte("shop.orders")) {
		t.Error("expected output to contain shop.orders table")
	}
	if !bytes.Contains([]byte(output), []byte("Total Transactions: 1")) {
		t.Error("expected output to show 1 transaction")
	}
	if !bytes.Contains([]byte(output), []byte("=== Next Actions ===")) {
		t.Error("expected output to contain Next Actions section")
	}
}

func TestRunAnalysisStreamsEventsDirectlyIntoAnalyzer(t *testing.T) {
	fakeAnalyzer := &fakeStreamingAnalyzer{
		finalResult: &model.AnalysisResult{
			Summary: model.WorkloadSummary{TotalTransactions: 1, TotalRows: 5, TotalEvents: 3},
			Tables:  []model.TableStats{{Schema: "shop", Table: "orders", TotalRows: 5}},
		},
	}
	parserSawConsume := false
	mock := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 1, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "shop", Table: "orders", RowCount: 5},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 2, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	mock.parseFiles = func(paths []string, handler func(binlog.RawEvent) error) error {
		for idx, ev := range mock.events {
			if err := handler(ev); err != nil {
				return err
			}
			if idx == 1 && fakeAnalyzer.consumeCalls > 0 {
				parserSawConsume = true
			}
		}
		return nil
	}

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := runAnalysisStreamingWithDeps([]string{"dummy.binlog"}, analyzer.Options{}, report.DefaultOptions(), "text", mock, binlog.NormalizeRawEvent, func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer {
		return fakeAnalyzer
	}, createDuckDBTempStore, "")

	w.Close()
	os.Stdout = old
	_, _ = io.Copy(&bytes.Buffer{}, r)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !parserSawConsume {
		t.Fatal("expected analyzer.Consume to run during parser callback, not after full collection")
	}
	if fakeAnalyzer.consumeCalls != 3 {
		t.Fatalf("expected 3 consume calls, got %d", fakeAnalyzer.consumeCalls)
	}
	if !fakeAnalyzer.finalized {
		t.Fatal("expected Finalize to be called after parsing")
	}
}

func TestRunAnalysisJSONOutput(t *testing.T) {
	mock := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 1, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "test", Table: "users", RowCount: 3},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 2, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	opts := analyzer.Options{TopTables: 10, TopTransactions: 10}

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := runAnalysisWithParser([]string{"dummy.binlog"}, opts, "json", mock)

	// Restore stdout
	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify JSON output starts with { and contains expected fields
	if output[0] != '{' {
		t.Error("expected JSON output to start with '{'")
	}
	if !bytes.Contains([]byte(output), []byte(`"total_transactions": 1`)) {
		t.Error("expected JSON to contain total_transactions")
	}
	if !bytes.Contains([]byte(output), []byte(`"schema": "test"`)) {
		t.Error("expected JSON to contain test schema")
	}
}

func TestRunAnalysisJSONPreservesMariaDBXAAndLoadDataBoundaries(t *testing.T) {
	ts := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	xid := "X'6276742d3537',X'',1"
	mock := &mockParser{events: []binlog.RawEvent{
		{Timestamp: ts, EventType: "MariadbGTIDEvent"},
		{Timestamp: ts, EventType: "QueryEvent", Query: "XA START " + xid, PositionStart: 3802, PositionEnd: 3900, BinlogPath: "mariadb-bin.000001"},
		{Timestamp: ts.Add(time.Second), EventType: "WriteRowsEventV2", Schema: "dogfood_cut", Table: "xa_a", RowCount: 2, PositionStart: 3900, PositionEnd: 4100, BinlogPath: "mariadb-bin.000001"},
		{Timestamp: ts.Add(2 * time.Second), EventType: "UpdateRowsEventV2", Schema: "dogfood_cut", Table: "xa_b", RowCount: 1, PositionStart: 4100, PositionEnd: 4300, BinlogPath: "mariadb-bin.000001"},
		{Timestamp: ts.Add(3 * time.Second), EventType: "QueryEvent", Query: "XA PREPARE " + xid, PositionStart: 4300, PositionEnd: 4444, BinlogPath: "mariadb-bin.000001"},
		{Timestamp: ts.Add(4 * time.Second), EventType: "MariadbGTIDEvent"},
		{Timestamp: ts.Add(4 * time.Second), EventType: "QueryEvent", Query: "XA COMMIT " + xid},
		{Timestamp: ts.Add(5 * time.Second), EventType: "MariadbGTIDEvent"},
		{Timestamp: ts.Add(5 * time.Second), EventType: "WriteRowsEventV2", Schema: "dogfood_cut", Table: "next_gtid", RowCount: 4},
		{Timestamp: ts.Add(6 * time.Second), EventType: "XIDEvent"},
		{Timestamp: ts.Add(7 * time.Second), EventType: "MariadbAnnotateRowsEvent", QuerySQL: "LOAD DATA INFILE '/tmp/slow.csv' INTO TABLE dogfood_cut.slow"},
		{Timestamp: ts.Add(8 * time.Second), EventType: "WriteRowsEventV2", Schema: "dogfood_cut", Table: "slow", RowCount: 2},
		{Timestamp: ts.Add(9 * time.Second), EventType: "XIDEvent"},
	}}

	stdout, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParser([]string{"dummy.binlog"}, analyzer.DefaultOptions(), "json", mock)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var parsed struct {
		Transactions []struct {
			XAXID      string         `json:"xa_xid"`
			TotalRows  int            `json:"total_rows"`
			PosStart   int64          `json:"pos_start"`
			PosEnd     int64          `json:"pos_end"`
			Tables     map[string]int `json:"tables"`
			Operations map[string]int `json:"operations"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(stdout), &parsed); err != nil {
		t.Fatalf("decode analyze JSON: %v\n%s", err, stdout)
	}
	if len(parsed.Transactions) != 3 {
		t.Fatalf("expected XA, following GTID, and LOAD DATA transactions, got %+v", parsed.Transactions)
	}
	var xa, next, load = parsed.Transactions[0], parsed.Transactions[0], parsed.Transactions[0]
	for _, txn := range parsed.Transactions {
		switch {
		case txn.XAXID != "":
			xa = txn
		case txn.Tables["dogfood_cut.next_gtid"] > 0:
			next = txn
		case txn.Tables["dogfood_cut.slow"] > 0:
			load = txn
		}
	}
	if xa.XAXID != xid || xa.TotalRows != 3 || xa.PosStart != 3802 || xa.PosEnd != 4444 || xa.Tables["dogfood_cut.next_gtid"] != 0 {
		t.Fatalf("unexpected prepared XA transaction: %+v", xa)
	}
	if next.XAXID != "" || next.TotalRows != 4 || next.Operations["INSERT"] != 4 {
		t.Fatalf("unexpected following GTID transaction: %+v", next)
	}
	if load.TotalRows != 2 || load.Operations["LOAD_DATA"] != 2 || load.Operations["INSERT"] != 0 {
		t.Fatalf("unexpected LOAD DATA transaction: %+v", load)
	}
}

func TestRunAnalysisTextSQLContextModes(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	result := &model.AnalysisResult{
		Transactions: []model.Transaction{
			{
				TxnKey:       "txn-1",
				TotalRows:    3,
				EventCount:   1,
				Duration:     time.Second,
				QuerySummary: "UPDATE users SET name = ? WHERE id = ?",
				QueryContext: model.NewQueryContext("UPDATE users SET name = 'alice' WHERE id = 7"),
			},
		},
	}
	tests := []struct {
		name string
		mode report.SQLContextMode
	}{
		{name: "summary", mode: report.SQLContextSummary},
		{name: "off", mode: report.SQLContextOff},
		{name: "full", mode: report.SQLContextFull},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			err := runAnalysisStreamingWithDeps([]string{"dummy.binlog"}, analyzer.Options{}, report.Options{SQLContextMode: tt.mode}, "text", &mockParser{}, binlog.NormalizeRawEvent, func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer {
				return &fakeStreamingAnalyzer{finalResult: result}
			}, createDuckDBTempStore, "")

			w.Close()
			os.Stdout = old

			var buf bytes.Buffer
			_, _ = io.Copy(&buf, r)
			out := buf.String()

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !strings.Contains(out, "=== Summary ===") {
				t.Fatalf("expected diagnostic summary output, got: %s", out)
			}
			if strings.Contains(out, "Query:") {
				t.Fatalf("default diagnostic text output should omit query lines regardless of SQL context mode, got: %s", out)
			}
		})
	}
}

func TestRunAnalysisJSONSQLContextModes(t *testing.T) {
	result := &model.AnalysisResult{
		Transactions: []model.Transaction{
			{
				TxnKey:       "txn-1",
				TotalRows:    3,
				EventCount:   1,
				Duration:     time.Second,
				QuerySummary: "UPDATE users SET name = ? WHERE id = ?",
				QueryContext: model.NewQueryContext("UPDATE users SET name = 'alice' WHERE id = 7"),
			},
		},
	}
	tests := []struct {
		name         string
		mode         report.SQLContextMode
		wantFields   []string
		omitFields   []string
		wantQuerySQL string
	}{
		{name: "summary", mode: report.SQLContextSummary, wantFields: []string{"query_summary", "query_truncated", "query_original_bytes"}, omitFields: []string{"query_sql"}},
		{name: "off", mode: report.SQLContextOff, omitFields: []string{"query_summary", "query_truncated", "query_original_bytes", "query_sql"}},
		{name: "full", mode: report.SQLContextFull, wantFields: []string{"query_summary", "query_truncated", "query_original_bytes", "query_sql"}, wantQuerySQL: "UPDATE users SET name = 'alice' WHERE id = 7"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			err := runAnalysisStreamingWithDeps([]string{"dummy.binlog"}, analyzer.Options{}, report.Options{SQLContextMode: tt.mode}, "json", &mockParser{}, binlog.NormalizeRawEvent, func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer {
				return &fakeStreamingAnalyzer{finalResult: result}
			}, createDuckDBTempStore, "")

			w.Close()
			os.Stdout = old

			var buf bytes.Buffer
			_, _ = io.Copy(&buf, r)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var parsed map[string]any
			if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
				t.Fatalf("invalid JSON output: %v", err)
			}
			txn := parsed["transactions"].([]any)[0].(map[string]any)
			for _, field := range tt.wantFields {
				if _, ok := txn[field]; !ok {
					t.Fatalf("expected field %q in output", field)
				}
			}
			for _, field := range tt.omitFields {
				if _, ok := txn[field]; ok {
					t.Fatalf("expected field %q to be omitted", field)
				}
			}
			if tt.wantQuerySQL != "" && txn["query_sql"] != tt.wantQuerySQL {
				t.Fatalf("unexpected query_sql: %v", txn["query_sql"])
			}
		})
	}
}

func TestRunAnalysisJSONReportsWarningsForTruncatedQueryContext(t *testing.T) {
	longSQL := "UPDATE users SET bio = '" + strings.Repeat("x", model.MaxStoredSQLBytes+256) + "' WHERE id = 7"

	mock := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 1, 0, time.UTC), EventType: "ROWS_QUERY_EVENT", QuerySQL: longSQL},
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 2, 0, time.UTC), EventType: "UPDATE_ROWS_EVENT", Schema: "shop", Table: "users", RowCount: 2},
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 3, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParser([]string{"dummy.binlog"}, analyzer.DefaultOptions(), "json", mock)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stderr) == "" {
		t.Fatal("expected progress/finalizing output on stderr")
	}

	var parsed struct {
		Warnings int `json:"warnings"`
	}
	if err := json.Unmarshal([]byte(stdout), &parsed); err != nil {
		t.Fatalf("decode analyze json: %v\n%s", err, stdout)
	}
	if parsed.Warnings != 1 {
		t.Fatalf("expected warnings=1 for truncated query context, got %d", parsed.Warnings)
	}
}

func TestRunAnalysisJSONWarningsPersistThroughSnapshotRoundTrip(t *testing.T) {
	longSQL := "UPDATE users SET bio = '" + strings.Repeat("x", model.MaxStoredSQLBytes+256) + "' WHERE id = 7"

	mock := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 1, 0, time.UTC), EventType: "ROWS_QUERY_EVENT", QuerySQL: longSQL},
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 2, 0, time.UTC), EventType: "UPDATE_ROWS_EVENT", Schema: "shop", Table: "users", RowCount: 2},
			{Timestamp: time.Date(2026, 3, 17, 10, 0, 3, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	snapshotDir := t.TempDir()
	snapshotName := "warning_snapshot"
	opts := &analyzeOptions{snapshotName: snapshotName}
	snapshotMeta := buildSnapshotMetadata([]string{"dummy.binlog"}, opts, time.Time{}, time.Time{}, false)

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParserAndTempDirAndReportAndSnapshotOptions(
			[]string{"dummy.binlog"},
			analyzer.DefaultOptions(),
			report.DefaultOptions(),
			"json",
			snapshotMeta,
			model.FileCoverage{},
			snapshotName,
			snapshotDir,
			mock,
			"",
			nil,
		)
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !json.Valid([]byte(stdout)) {
		t.Fatalf("stdout must remain valid json, got: %s", stdout)
	}
	if !strings.Contains(stderr, "Finalizing analysis") {
		t.Fatalf("expected finalizing status on stderr, got: %s", stderr)
	}
	if !strings.Contains(stderr, `Saved snapshot "warning_snapshot" to `) {
		t.Fatalf("expected snapshot save message on stderr, got: %s", stderr)
	}

	requireWarningsPersistInSavedSnapshot(t, snapshotDir, snapshotName, stdout)
}

func requireWarningsPersistInSavedSnapshot(t *testing.T, snapshotDir, snapshotName, stdout string) {
	t.Helper()

	var rendered struct {
		Warnings int `json:"warnings"`
	}
	if err := json.Unmarshal([]byte(stdout), &rendered); err != nil {
		t.Fatalf("decode rendered json: %v", err)
	}

	desc, err := snapshotpkg.DescribeSnapshot(snapshotDir, snapshotName)
	if err != nil {
		t.Fatalf("DescribeSnapshot returned error: %v", err)
	}
	if desc.Warnings != rendered.Warnings {
		t.Fatalf("expected snapshot warnings %d to match rendered warnings %d", desc.Warnings, rendered.Warnings)
	}
}

func TestCreateDuckDBTempStoreCreatesAndCleansFiles(t *testing.T) {
	root := t.TempDir()

	store, cleanup, path, err := createDuckDBTempStore(root)
	if err != nil {
		t.Fatalf("createDuckDBTempStore returned error: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected DuckDB file at %s: %v", path, err)
	}
	if _, err := os.Stat(path + ".querysql.jsonl"); !os.IsNotExist(err) {
		t.Fatalf("expected no SQL context sidecar file, got err=%v", err)
	}

	if err := cleanup(); err != nil {
		t.Fatalf("cleanup returned error: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected DuckDB file to be removed, got err=%v", err)
	}
}

func TestRunAnalysisWithParserCleansDuckDBTempStoreOnFailure(t *testing.T) {
	root := t.TempDir()
	var createdPath string
	opts := analyzer.DefaultOptions()
	opts.DetailStoreMode = analyzer.DetailStoreDuckDB

	err := runAnalysisWithParserAndTempDir([]string{"dummy.binlog"}, opts, "text", &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 1, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "shop", Table: "orders", RowCount: 5},
			{Timestamp: time.Date(2026, 3, 14, 10, 0, 2, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
		},
	}, root, func(path string) {
		createdPath = path
	})
	if err == nil {
		t.Fatal("expected analysis error")
	}
	if createdPath == "" {
		t.Fatal("expected created DuckDB path to be captured")
	}
	if _, statErr := os.Stat(createdPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected cleanup to remove DuckDB path on failure, got err=%v", statErr)
	}
}

func TestRunAnalysisPropagatesNormalizeError(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	wantErr := errors.New("normalize boom")
	err := runAnalysisStreamingWithDeps([]string{"dummy.binlog"}, analyzer.Options{}, report.DefaultOptions(), "text", &mockParser{
		events: []binlog.RawEvent{{Timestamp: time.Now(), EventType: "WRITE_ROWS_EVENT", Position: 42}},
	}, func(raw binlog.RawEvent) (*model.NormalizedEvent, error) {
		return nil, wantErr
	}, func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer {
		return &fakeStreamingAnalyzer{}
	}, createDuckDBTempStore, "")
	if err == nil {
		t.Fatal("expected normalize error")
	}
	if got := err.Error(); got != "parse error: normalize error at position 42: normalize boom" {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestRunAnalysisPropagatesAnalyzerConsumeError(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	wantErr := errors.New("consume boom")
	err := runAnalysisStreamingWithDeps([]string{"dummy.binlog"}, analyzer.Options{}, report.DefaultOptions(), "text", &mockParser{
		events: []binlog.RawEvent{{Timestamp: time.Now(), EventType: "WRITE_ROWS_EVENT", Schema: "shop", Table: "orders", RowCount: 1}},
	}, binlog.NormalizeRawEvent, func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer {
		return &fakeStreamingAnalyzer{consumeErr: wantErr}
	}, createDuckDBTempStore, "")
	if err == nil {
		t.Fatal("expected analyzer consume error")
	}
	if got := err.Error(); got != "parse error: analysis consume error: consume boom" {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestRunAnalysisPropagatesAnalyzerFinalizeError(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	wantErr := errors.New("finalize boom")
	err := runAnalysisStreamingWithDeps([]string{"dummy.binlog"}, analyzer.Options{}, report.DefaultOptions(), "text", &mockParser{
		events: []binlog.RawEvent{{Timestamp: time.Now(), EventType: "WRITE_ROWS_EVENT", Schema: "shop", Table: "orders", RowCount: 1}},
	}, binlog.NormalizeRawEvent, func(opts analyzer.Options, store *analyzer.DuckDBStore) commandAnalyzer {
		return &fakeStreamingAnalyzer{finalizeErr: wantErr}
	}, createDuckDBTempStore, "")
	if err == nil {
		t.Fatal("expected analyzer finalize error")
	}
	if got := err.Error(); got != "analysis finalize error: finalize boom" {
		t.Fatalf("unexpected error: %s", got)
	}
}

func TestBuildAnalyzerOptionsIncludesSpikeDefaults(t *testing.T) {
	// When detectSpikes is enabled, spike detection parameters should have defaults
	cliOpts := &analyzeOptions{
		detectSpikes: true,
	}

	result := buildAnalyzerOptions(cliOpts, time.Time{}, time.Time{})

	// Verify spike detection is enabled
	if !result.DetectSpikes {
		t.Error("expected DetectSpikes to be true")
	}

	// Verify spike detection has valid defaults (not zero values)
	if result.SpikeWindow <= 0 {
		t.Errorf("expected SpikeWindow > 0, got %d", result.SpikeWindow)
	}
	if result.SpikeFactor <= 0 {
		t.Errorf("expected SpikeFactor > 0, got %f", result.SpikeFactor)
	}
	if result.SpikeMinRows <= 0 {
		t.Errorf("expected SpikeMinRows > 0, got %d", result.SpikeMinRows)
	}
}

func TestBuildAnalyzerOptionsUsesDefaultOptions(t *testing.T) {
	// Verify that buildAnalyzerOptions uses DefaultOptions as base
	cliOpts := &analyzeOptions{
		topTables:        10,
		topTransactions:  10,
		largeTrxRows:     1000,
		largeTrxDuration: 30 * time.Second,
	}

	result := buildAnalyzerOptions(cliOpts, time.Time{}, time.Time{})

	// Check that we got the defaults from analyzer.DefaultOptions()
	defaults := analyzer.DefaultOptions()

	// These should match defaults when not overridden by CLI
	if result.SpikeWindow != defaults.SpikeWindow {
		t.Errorf("SpikeWindow: expected %d, got %d", defaults.SpikeWindow, result.SpikeWindow)
	}
	if result.SpikeFactor != defaults.SpikeFactor {
		t.Errorf("SpikeFactor: expected %f, got %f", defaults.SpikeFactor, result.SpikeFactor)
	}
	if result.SpikeMinRows != defaults.SpikeMinRows {
		t.Errorf("SpikeMinRows: expected %d, got %d", defaults.SpikeMinRows, result.SpikeMinRows)
	}
}

func TestSpikeDetectionWithDefaultsProducesAlert(t *testing.T) {
	// Create events spanning 10 minutes with a spike at minute 7
	base := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	mock := &mockParser{}

	// Generate events: 6 minutes of baseline, then a spike
	for minute := 0; minute < 10; minute++ {
		rowCount := 100 // baseline
		if minute >= 7 {
			rowCount = 600 // spike (6x baseline)
		}
		for i := 0; i < rowCount; i++ {
			mock.events = append(mock.events, binlog.RawEvent{
				Timestamp: base.Add(time.Duration(minute)*time.Minute + time.Duration(i)*time.Millisecond),
				EventType: "WRITE_ROWS_EVENT",
				Schema:    "shop",
				Table:     "orders",
				RowCount:  1,
			})
		}
	}

	// Use DefaultOptions which includes spike detection defaults
	opts := analyzer.DefaultOptions()
	opts.DetectSpikes = true

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := runAnalysisWithParser([]string{"dummy.binlog"}, opts, "json", mock)

	// Restore stdout
	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify spike alert is produced
	if !bytes.Contains([]byte(output), []byte(`"type": "spike"`)) {
		t.Error("expected spike alert in output")
	}
}

// TestRealBinlogFixtureEndToEnd verifies the complete parsing pipeline with a real MySQL binlog file.
// This test uses internal/binlog/testdata/minimal.binlog which was generated from MySQL 5.7 with ROW binlog format.
// See internal/binlog/testdata/README.md for fixture generation instructions.
func TestRealBinlogFixtureEndToEnd(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixturePath := mustFixturePath(t, "minimal.binlog")

	// Run the full pipeline with real parser
	opts := analyzer.DefaultOptions()

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	err := runAnalysis([]string{fixturePath}, opts, "text")

	w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify output contains expected sections
	if !bytes.Contains([]byte(output), []byte("=== Summary ===")) {
		t.Error("expected output to contain Summary section")
	}
	if !bytes.Contains([]byte(output), []byte("=== Top Tables ===")) {
		t.Error("expected output to contain Top Tables section")
	}
	// The fixture contains INSERT/UPDATE/DELETE on testdb.users
	if !bytes.Contains([]byte(output), []byte("testdb.users")) {
		t.Error("expected output to contain testdb.users table")
	}
	// Verify we have total row activity in the summary (2 INSERT + 1 UPDATE + 1 DELETE)
	if !bytes.Contains([]byte(output), []byte("Total Rows: 4")) {
		t.Error("expected output to contain 'Total Rows: 4'")
	}
	if !bytes.Contains([]byte(output), []byte("Input File Size: 1.5KB")) {
		t.Error("expected output to contain selected input file size")
	}
	if !bytes.Contains([]byte(output), []byte("Counted Event Bytes:")) {
		t.Error("expected output to distinguish counted event bytes")
	}
}

func TestRealBinlogFixtureJSONIncludesNonZeroTransactionCounts(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixturePath := mustFixturePath(t, "minimal.binlog")

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return runAnalysis([]string{fixturePath}, analyzer.DefaultOptions(), "json")
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(stderr) == "" {
		t.Fatal("expected progress output on stderr for real fixture run")
	}

	var decoded struct {
		Summary struct {
			TotalTransactions int `json:"total_transactions"`
		} `json:"summary"`
		Tables []struct {
			Schema   string `json:"schema"`
			Table    string `json:"table"`
			TxnCount int    `json:"txn_count"`
		} `json:"tables"`
		Minutes []struct {
			Minute   string `json:"minute"`
			TxnCount int    `json:"txn_count"`
		} `json:"minutes"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode analyze json: %v\n%s", err, stdout)
	}
	if decoded.Summary.TotalTransactions == 0 {
		t.Fatalf("expected fixture to contain transactions, got %+v", decoded.Summary)
	}

	tableTxnCounts := make(map[string]int, len(decoded.Tables))
	hasTableTxnCount := false
	for _, table := range decoded.Tables {
		tableTxnCounts[table.Schema+"."+table.Table] = table.TxnCount
		if table.TxnCount > 0 {
			hasTableTxnCount = true
		}
	}
	if !hasTableTxnCount {
		t.Fatalf("expected at least one table txn_count > 0, got %+v", tableTxnCounts)
	}

	minuteTxnCounts := make(map[string]int, len(decoded.Minutes))
	hasMinuteTxnCount := false
	for _, minute := range decoded.Minutes {
		minuteTxnCounts[minute.Minute] = minute.TxnCount
		if minute.TxnCount > 0 {
			hasMinuteTxnCount = true
		}
	}
	if !hasMinuteTxnCount {
		t.Fatalf("expected at least one minute txn_count > 0, got %+v", minuteTxnCounts)
	}
}

func TestAnalyzeGeneratedSnapshotsWorkWithTrendWithoutExplicitWindow(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixture := mustFixturePath(t, "minimal.binlog")
	snapshotDir := t.TempDir()

	firstAnalyze := NewRootCommand()
	firstAnalyze.SetArgs([]string{
		"analyze",
		fixture,
		"--format", "json",
		"--snapshot-name", "fixture_single",
		"--snapshot-dir", snapshotDir,
	})
	firstAnalyze.SilenceUsage = true
	firstAnalyze.SilenceErrors = true
	if _, _, err := captureStdoutStderrRun(t, func() error { return firstAnalyze.Execute() }); err != nil {
		t.Fatalf("save single-fixture snapshot: %v", err)
	}

	secondAnalyze := NewRootCommand()
	secondAnalyze.SetArgs([]string{
		"analyze",
		fixture,
		fixture,
		"--format", "json",
		"--snapshot-name", "fixture_double",
		"--snapshot-dir", snapshotDir,
	})
	secondAnalyze.SilenceUsage = true
	secondAnalyze.SilenceErrors = true
	if _, _, err := captureStdoutStderrRun(t, func() error { return secondAnalyze.Execute() }); err != nil {
		t.Fatalf("save double-fixture snapshot: %v", err)
	}

	trendCmd := NewRootCommand()
	trendCmd.SetArgs([]string{
		"trend",
		"fixture_single",
		"fixture_double",
		"--snapshot-dir", snapshotDir,
		"--format", "json",
	})
	trendCmd.SilenceUsage = true
	trendCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return trendCmd.Execute()
	})
	if err != nil {
		t.Fatalf("trend analyze-generated snapshots: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	var decoded struct {
		Points []struct {
			Snapshot struct {
				Name string `json:"name"`
			} `json:"snapshot"`
		} `json:"points"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("decode trend json: %v\n%s", err, stdout)
	}
	if len(decoded.Points) != 2 {
		t.Fatalf("expected 2 trend points, got %d", len(decoded.Points))
	}
}

func TestAnalyzeTextOutputHidesMinutesUntilRequested(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixture := mustFixturePath(t, "minimal.binlog")
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"analyze", fixture, "--format", "text"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("analyze text failed: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}
	if strings.Contains(stdout, "Minute Details") {
		t.Fatalf("default output should hide minute details\nstdout=%s\nstderr=%s", stdout, stderr)
	}
}

func TestAnalyzeShowMinutesFlagEnablesMinuteDetails(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fixture := mustFixturePath(t, "minimal.binlog")
	cmd := NewRootCommand()
	cmd.SetArgs([]string{"analyze", fixture, "--format", "text", "--show-minutes"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return cmd.Execute()
	})
	if err != nil {
		t.Fatalf("analyze text failed: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}
	if !strings.Contains(stdout, "Minute Details") {
		t.Fatalf("expected minute details with --show-minutes\nstdout=%s\nstderr=%s", stdout, stderr)
	}
}

// Helper functions to create test data

func mustWriteFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("test"), 0o644); err != nil {
		t.Fatalf("write file %s: %v", path, err)
	}
}

func mustSetFileModTime(t *testing.T, path string, modTime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("set mod time for %s: %v", path, err)
	}
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read file %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatalf("write file %s: %v", dst, err)
	}
}

func createTestTableStats(count int) []model.TableStats {
	stats := make([]model.TableStats, count)
	for i := 0; i < count; i++ {
		stats[i] = model.TableStats{
			Schema:    "test",
			Table:     string(rune('a' + i)),
			TotalRows: i + 1,
		}
	}
	return stats
}

func createTestTransactions(count int) []model.Transaction {
	txns := make([]model.Transaction, count)
	for i := 0; i < count; i++ {
		txns[i] = model.Transaction{
			TxnKey:    string(rune('a' + i)),
			TotalRows: i + 1,
		}
	}
	return txns
}

func TestAnalyzeAcceptsDetailStoreFlag(t *testing.T) {
	cmd := newAnalyzeCommand()
	flags := cmd.Flags()
	if flags.Lookup("detail-store") == nil {
		t.Fatal("expected --detail-store flag")
	}
}

func TestBuildAnalyzerOptionsMapsDetailStoreMode(t *testing.T) {
	cliOpts := &analyzeOptions{detailStore: "duckdb"}
	got := buildAnalyzerOptions(cliOpts, time.Time{}, time.Time{})
	if got.DetailStoreMode != analyzer.DetailStoreDuckDB {
		t.Fatalf("DetailStoreMode = %q, want %q", got.DetailStoreMode, analyzer.DetailStoreDuckDB)
	}
}

func TestBuildAnalyzerOptionsDefaultsToNoDetailStore(t *testing.T) {
	cliOpts := &analyzeOptions{}
	got := buildAnalyzerOptions(cliOpts, time.Time{}, time.Time{})
	if got.DetailStoreMode != analyzer.DetailStoreNone {
		t.Fatalf("DetailStoreMode = %q, want %q", got.DetailStoreMode, analyzer.DetailStoreNone)
	}
}

func TestValidateAnalyzeOptionsRejectsInvalidDetailStore(t *testing.T) {
	err := validateAnalyzeOptions(&analyzeOptions{detailStore: "invalid"})
	if err == nil {
		t.Fatal("expected error for invalid detail-store mode")
	}
	if !strings.Contains(err.Error(), "invalid --detail-store") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunAnalysisDetailStoreNoneDoesNotCreateDuckDBTempStore(t *testing.T) {
	opts := analyzer.DefaultOptions()
	var createdPath string
	mock := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 4, 19, 10, 0, 1, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "shop", Table: "orders", RowCount: 1},
			{Timestamp: time.Date(2026, 4, 19, 10, 0, 2, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	stdout, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParserAndTempDir([]string{"dummy.binlog"}, opts, "json", mock, t.TempDir(), func(path string) {
			createdPath = path
		})
	})
	if err != nil {
		t.Fatalf("runAnalysisWithParserAndTempDir returned error: %v", err)
	}
	if createdPath != "" {
		t.Fatalf("detail-store=none created DuckDB temp store at %s", createdPath)
	}
	if !json.Valid([]byte(stdout)) {
		t.Fatalf("stdout must be valid json, got %s", stdout)
	}
}

func TestRunAnalysisDetailStoreDuckDBCreatesTempStore(t *testing.T) {
	opts := analyzer.DefaultOptions()
	opts.DetailStoreMode = analyzer.DetailStoreDuckDB
	var createdPath string
	mock := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: time.Date(2026, 4, 19, 10, 0, 0, 0, time.UTC), EventType: "QUERY_EVENT", Query: "BEGIN"},
			{Timestamp: time.Date(2026, 4, 19, 10, 0, 1, 0, time.UTC), EventType: "WRITE_ROWS_EVENT", Schema: "shop", Table: "orders", RowCount: 1},
			{Timestamp: time.Date(2026, 4, 19, 10, 0, 2, 0, time.UTC), EventType: "XID_EVENT"},
		},
	}

	_, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParserAndTempDir([]string{"dummy.binlog"}, opts, "json", mock, t.TempDir(), func(path string) {
			createdPath = path
		})
	})
	if err != nil {
		t.Fatalf("runAnalysisWithParserAndTempDir returned error: %v", err)
	}
	if createdPath == "" {
		t.Fatal("detail-store=duckdb did not create DuckDB temp store")
	}
}
