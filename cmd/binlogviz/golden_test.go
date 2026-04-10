package binlogviz

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var (
	timestampPattern      = regexp.MustCompile(`"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z"`)
	manifestErrorPattern  = regexp.MustCompile(`"error": "discover binlog files: .*"`)
	planSHA256Pattern     = regexp.MustCompile(`"plan_sha256": "[0-9a-f]+"`)
	resolvedFilesPattern  = regexp.MustCompile(`(?s)"resolved_input_files": \[.*?\]`)
)

func TestSnapshotShowJSONGoldenMinimalWorkflow(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "incident_window.json"), []byte(minimalSnapshotReportJSON("incident_window", "current", 2400, 2)), 0o644); err != nil {
		t.Fatalf("write snapshot fixture: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"snapshot", "show", "incident_window", "--snapshot-dir", dir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := mustReadGolden(t, "snapshot-show-minimal.golden.json")
	got := normalizeGoldenOutput(output.String(), dir)
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("snapshot show golden mismatch\n%s", diff)
	}
}

func TestCompareJSONGoldenLegacySnapshotWorkflow(t *testing.T) {
	dir := t.TempDir()

	current, err := os.ReadFile(filepath.Join("..", "..", "internal", "compare", "testdata", "current.json"))
	if err != nil {
		t.Fatalf("read current compare fixture: %v", err)
	}
	baseline, err := os.ReadFile(filepath.Join("..", "..", "internal", "compare", "testdata", "baseline.json"))
	if err != nil {
		t.Fatalf("read baseline compare fixture: %v", err)
	}

	writeSnapshotFixture(t, dir, "current-snap", string(current))
	writeSnapshotFixture(t, dir, "baseline-snap", string(baseline))

	cmd := NewRootCommand()
	cmd.SetArgs([]string{
		"compare",
		"--current-snapshot", "current-snap",
		"--baseline-snapshot", "baseline-snap",
		"--snapshot-dir", dir,
		"--format", "json",
	})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := mustReadGolden(t, "compare-legacy-snapshots.golden.json")
	got := normalizeGoldenOutput(output.String(), dir)
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("compare golden mismatch\n%s", diff)
	}
}

func TestCompareJSONGoldenPatternSnapshotWorkflow(t *testing.T) {
	dir := t.TempDir()

	current, err := os.ReadFile(filepath.Join("..", "..", "internal", "compare", "testdata", "current_patterns.json"))
	if err != nil {
		t.Fatalf("read current compare fixture: %v", err)
	}
	baseline, err := os.ReadFile(filepath.Join("..", "..", "internal", "compare", "testdata", "baseline_patterns.json"))
	if err != nil {
		t.Fatalf("read baseline compare fixture: %v", err)
	}

	writeSnapshotFixture(t, dir, "current-patterns", string(current))
	writeSnapshotFixture(t, dir, "baseline-patterns", string(baseline))

	cmd := NewRootCommand()
	cmd.SetArgs([]string{
		"compare",
		"--current-snapshot", "current-patterns",
		"--baseline-snapshot", "baseline-patterns",
		"--snapshot-dir", dir,
		"--format", "json",
	})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := mustReadGolden(t, "compare-patterns-snapshots.golden.json")
	got := normalizeGoldenOutput(output.String(), dir)
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("compare pattern golden mismatch\n%s", diff)
	}
}

func TestTrendJSONGoldenMinimalWorkflow(t *testing.T) {
	dir := t.TempDir()
	writeSnapshotFixture(t, dir, "later", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "later",
		Label:     "Later",
		StartTime: "2026-03-21T10:00:00Z",
		EndTime:   "2026-03-21T10:30:00Z",
		Rows:      3000,
		Txns:      150,
		Events:    3600,
		Inserts:   1600,
		Updates:   900,
		Deletes:   500,
		Alerts:    3,
		Patterns: []trendPatternFixture{
			{
				PatternKey:         "orders.payments|UPDATE|medium",
				Label:              "payments.update_status",
				TotalRows:          1300,
				TxnCount:           18,
				EventCount:         90,
				ShareOfRows:        0.4333333333,
				ShareOfTxns:        0.12,
				AvgRowsPerTxn:      72.2,
				Tables:             map[string]int{"orders.payments": 1300},
				Operations:         map[string]int{"UPDATE": 1300},
				SampleQuerySummary: "update payments set status = ?",
			},
		},
	}))
	writeSnapshotFixture(t, dir, "earlier", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "earlier",
		Label:     "Earlier",
		StartTime: "2026-03-19T10:00:00Z",
		EndTime:   "2026-03-19T10:30:00Z",
		Rows:      1000,
		Txns:      50,
		Events:    1200,
		Inserts:   500,
		Updates:   350,
		Deletes:   150,
		Alerts:    0,
		Patterns: []trendPatternFixture{
			{
				PatternKey:         "orders.payments|UPDATE|medium",
				Label:              "payments.update_status",
				TotalRows:          600,
				TxnCount:           9,
				EventCount:         45,
				ShareOfRows:        0.6,
				ShareOfTxns:        0.18,
				AvgRowsPerTxn:      66.7,
				Tables:             map[string]int{"orders.payments": 600},
				Operations:         map[string]int{"UPDATE": 600},
				SampleQuerySummary: "update payments set status = ?",
			},
		},
	}))
	writeSnapshotFixture(t, dir, "middle", trendSnapshotFixtureJSON(trendSnapshotFixture{
		Name:      "middle",
		Label:     "Middle",
		StartTime: "2026-03-20T10:00:00Z",
		EndTime:   "2026-03-20T10:30:00Z",
		Rows:      1800,
		Txns:      90,
		Events:    2200,
		Inserts:   900,
		Updates:   600,
		Deletes:   300,
		Alerts:    1,
		Patterns: []trendPatternFixture{
			{
				PatternKey:         "orders.payments|UPDATE|medium",
				Label:              "payments.update_status",
				TotalRows:          1000,
				TxnCount:           13,
				EventCount:         65,
				ShareOfRows:        0.5555555556,
				ShareOfTxns:        0.1444444444,
				AvgRowsPerTxn:      76.9,
				Tables:             map[string]int{"orders.payments": 1000},
				Operations:         map[string]int{"UPDATE": 1000},
				SampleQuerySummary: "update payments set status = ?",
			},
		},
	}))

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"trend", "later", "earlier", "middle", "--snapshot-dir", dir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := mustReadGolden(t, "trend-minimal.golden.json")
	got := normalizeGoldenOutput(output.String(), dir)
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("trend golden mismatch\n%s", diff)
	}
}

func TestTrendJSONGoldenLegacyFallbackWorkflow(t *testing.T) {
	dir := t.TempDir()
	writeSnapshotFixture(t, dir, "legacy-alpha", trendSnapshotFixtureJSONWithWindowOverride(trendSnapshotFixture{
		Name:      "legacy-alpha",
		Label:     "Legacy Alpha",
		StartTime: "2026-03-20T10:00:00Z",
		EndTime:   "2026-03-20T10:30:00Z",
		Rows:      2400,
		Txns:      120,
		Events:    3000,
		Inserts:   1200,
		Updates:   800,
		Deletes:   400,
		Alerts:    2,
	}, "", ""))
	writeSnapshotFixture(t, dir, "legacy-beta", trendSnapshotFixtureJSONWithWindowOverride(trendSnapshotFixture{
		Name:      "legacy-beta",
		Label:     "Legacy Beta",
		StartTime: "2026-03-21T10:00:00Z",
		EndTime:   "2026-03-21T10:30:00Z",
		Rows:      3200,
		Txns:      160,
		Events:    3800,
		Inserts:   1800,
		Updates:   1000,
		Deletes:   400,
		Alerts:    3,
	}, "", ""))

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"trend", "legacy-beta", "legacy-alpha", "--snapshot-dir", dir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	output := &bytes.Buffer{}
	cmd.SetOut(output)
	cmd.SetErr(io.Discard)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := mustReadGolden(t, "trend-legacy-fallback.golden.json")
	got := normalizeGoldenOutput(output.String(), dir)
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("legacy trend golden mismatch\n%s", diff)
	}
}

func mustReadGolden(t *testing.T, name string) string {
	t.Helper()

	path := filepath.Join("testdata", "golden", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v", path, err)
	}
	return string(data)
}

func diffGolden(want, got string) string {
	if strings.TrimSpace(want) == strings.TrimSpace(got) {
		return ""
	}
	return "expected:\n" + strings.TrimSpace(want) + "\n\ngot:\n" + strings.TrimSpace(got)
}

func normalizeGoldenOutput(out, snapshotDir string) string {
	return strings.ReplaceAll(out, snapshotDir, "<snapshot-dir>")
}

func TestWorkflowRunGoldenManifest(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	manifestPath := filepath.Join(outputDir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	got := normalizeWorkflowManifest(string(data), outputDir, snapshotDir)
	want := mustReadGolden(t, "workflow-basic-manifest.golden.json")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow manifest golden mismatch\n%s", diff)
	}
}

func TestWorkflowRunGoldenArtifactTree(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	tree := listArtifactTree(t, outputDir)
	got := normalizeWorkflowTree(tree, outputDir)
	want := mustReadGolden(t, "workflow-basic-tree.golden.txt")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow artifact tree golden mismatch\n%s", diff)
	}
}

func TestWorkflowRunGoldenFailedManifest(t *testing.T) {
	planDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "artifacts")

	plan := strings.ReplaceAll(`
version: 1
workflow:
  name: failing-test
  output_dir: PLACEHOLDER
defaults:
  input:
    from_dir: /nonexistent/path/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2025-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
`, "PLACEHOLDER", outputDir)

	planPath := filepath.Join(planDir, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	_ = cmd.Execute()

	manifestPath := filepath.Join(outputDir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	got := normalizeWorkflowManifest(string(data), outputDir, "")
	want := mustReadGolden(t, "workflow-failed-manifest.golden.json")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow failed manifest golden mismatch\n%s", diff)
	}
}

func TestWorkflowValidateGoldenTextValid(t *testing.T) {
	_, outputDir, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "validate", planPath})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow validate: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	got := strings.ReplaceAll(stdout, outputDir, "<output-dir>")
	want := mustReadGolden(t, "workflow-validate-valid.golden.txt")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow validate valid text golden mismatch\n%s", diff)
	}
}

func TestWorkflowValidateGoldenTextInvalid(t *testing.T) {
	planDir := t.TempDir()
	planPath := filepath.Join(planDir, "invalid-plan.yaml")
	plan := `
version: 1
workflow:
  name: invalid
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
compare:
  - name: drift
    current: incident
    baseline: baseline
    formats: [json]
`
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "validate", planPath})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err == nil {
		t.Fatal("expected workflow validate to fail")
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	want := mustReadGolden(t, "workflow-validate-invalid.golden.txt")
	if diff := diffGolden(want, stdout); diff != "" {
		t.Fatalf("workflow validate invalid text golden mismatch\n%s", diff)
	}
}

func TestWorkflowValidateGoldenJSONValid(t *testing.T) {
	_, outputDir, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "validate", planPath, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow validate: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	got := strings.ReplaceAll(stdout, outputDir, "<output-dir>")
	want := mustReadGolden(t, "workflow-validate-valid.golden.json")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow validate valid json golden mismatch\n%s", diff)
	}
}

func TestWorkflowValidateGoldenJSONInvalid(t *testing.T) {
	planDir := t.TempDir()
	planPath := filepath.Join(planDir, "invalid-plan.yaml")
	plan := `
version: 1
workflow:
  name: invalid
  output_dir: ./artifacts
defaults:
  input:
    from_dir: /var/lib/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2026-04-09T10:00:00Z
    end: 2026-04-09T10:30:00Z
compare:
  - name: drift
    current: incident
    baseline: baseline
    formats: [json]
`
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "validate", planPath, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err == nil {
		t.Fatal("expected workflow validate to fail")
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	want := mustReadGolden(t, "workflow-validate-invalid.golden.json")
	if diff := diffGolden(want, stdout); diff != "" {
		t.Fatalf("workflow validate invalid json golden mismatch\n%s", diff)
	}
}

func TestWorkflowDescribeGoldenText(t *testing.T) {
	_, outputDir, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "describe", planPath})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow describe: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	got := strings.ReplaceAll(stdout, outputDir, "<output-dir>")
	want := mustReadGolden(t, "workflow-describe.golden.txt")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow describe text golden mismatch\n%s", diff)
	}
}

func TestWorkflowDescribeGoldenJSON(t *testing.T) {
	_, outputDir, planPath, _ := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "describe", planPath, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow describe: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	got := strings.ReplaceAll(stdout, outputDir, "<output-dir>")
	want := mustReadGolden(t, "workflow-describe.golden.json")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow describe json golden mismatch\n%s", diff)
	}
}

func normalizeWorkflowManifest(raw, outputDir, snapshotDir string) string {
	out := strings.ReplaceAll(raw, outputDir, "<output-dir>")
	if snapshotDir != "" {
		out = strings.ReplaceAll(out, snapshotDir, "<snapshot-dir>")
	}
	// Normalize dynamic timestamp fields
	out = timestampPattern.ReplaceAllString(out, `"<timestamp>"`)
	// Normalize plan_path — it lives under a sibling temp dir sharing the
	// same test root, so after output-dir normalization the remaining temp
	// prefix is the test root itself.
	testRoot := filepath.Dir(filepath.Dir(outputDir))
	if testRoot != "" && testRoot != "." && testRoot != "/" {
		out = strings.ReplaceAll(out, testRoot+"/", "<test-root>/")
	}
	// Normalize top-level error (locale-dependent OS messages)
	out = manifestErrorPattern.ReplaceAllString(out, `"error": "<discovery-error>"`)
	// Normalize plan_sha256 (changes with plan content)
	out = planSHA256Pattern.ReplaceAllString(out, `"plan_sha256": "<plan-sha>"`)
	// Normalize resolved_input_files (contains temp paths)
	out = resolvedFilesPattern.ReplaceAllString(out, `"resolved_input_files": ["<resolved-files>"]`)
	return out
}

func normalizeWorkflowTree(lines []string, outputDir string) string {
	normalized := make([]string, len(lines))
	for i, l := range lines {
		normalized[i] = strings.ReplaceAll(l, outputDir, "<output-dir>")
	}
	return strings.Join(normalized, "\n")
}

func listArtifactTree(t *testing.T, root string) []string {
	t.Helper()
	var lines []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		lines = append(lines, rel)
		return nil
	})
	if err != nil {
		t.Fatalf("walk artifact tree: %v", err)
	}
	return lines
}

func TestWorkflowIndexGoldenSuccess(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	if err := cmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	indexPath := filepath.Join(outputDir, "index.html")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read index.html: %v", err)
	}

	got := normalizeWorkflowIndex(string(data), outputDir, snapshotDir)
	want := mustReadGolden(t, "workflow-basic-index.golden.html")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow index success golden mismatch\n%s", diff)
	}
}

func TestWorkflowIndexGoldenFailure(t *testing.T) {
	planDir := t.TempDir()
	outputDir := filepath.Join(t.TempDir(), "artifacts")

	plan := strings.ReplaceAll(`
version: 1
workflow:
  name: failing-index-golden
  output_dir: PLACEHOLDER
defaults:
  input:
    from_dir: /nonexistent/path/mysql
    prefix: mysql-bin.
windows:
  - name: baseline
    start: 2025-01-01T00:00:00Z
    end: 2099-12-31T23:59:59Z
`, "PLACEHOLDER", outputDir)

	planPath := filepath.Join(planDir, "plan.yaml")
	if err := os.WriteFile(planPath, []byte(plan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	_ = cmd.Execute()

	indexPath := filepath.Join(outputDir, "index.html")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read index.html: %v", err)
	}

	got := normalizeWorkflowIndex(string(data), outputDir, "")
	want := mustReadGolden(t, "workflow-failed-index.golden.html")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow index failure golden mismatch\n%s", diff)
	}
}

func TestWorkflowStatusTextGoldenSuccess(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	runCmd := NewRootCommand()
	runCmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	runCmd.SilenceUsage = true
	runCmd.SilenceErrors = true
	if err := runCmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", outputDir})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	got := normalizeWorkflowStatusOutput(stdout, outputDir)
	want := mustReadGolden(t, "workflow-status-success.golden.txt")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow status success text golden mismatch\n%s", diff)
	}
}

func TestWorkflowStatusJSONGoldenSuccess(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	runCmd := NewRootCommand()
	runCmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	runCmd.SilenceUsage = true
	runCmd.SilenceErrors = true
	if err := runCmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", outputDir, "--format", "json"})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	got := normalizeWorkflowStatusOutput(stdout, outputDir)
	want := mustReadGolden(t, "workflow-status-success.golden.json")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow status success json golden mismatch\n%s", diff)
	}
}

func TestWorkflowStatusTextGoldenIncomplete(t *testing.T) {
	_, outputDir, planPath, snapshotDir := setupWorkflowTestWithSnapshots(t, "basic-plan.yaml")

	runCmd := NewRootCommand()
	runCmd.SetArgs([]string{"workflow", "run", planPath, "--output-dir", outputDir, "--snapshot-dir", snapshotDir})
	runCmd.SilenceUsage = true
	runCmd.SilenceErrors = true
	if err := runCmd.Execute(); err != nil {
		t.Fatalf("workflow run: %v", err)
	}
	if err := os.Remove(filepath.Join(outputDir, "compare", "week2_vs_week1.json")); err != nil {
		t.Fatalf("remove compare artifact: %v", err)
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", outputDir})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	got := normalizeWorkflowStatusOutput(stdout, outputDir)
	want := mustReadGolden(t, "workflow-status-incomplete.golden.txt")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow status incomplete text golden mismatch\n%s", diff)
	}
}

func TestWorkflowStatusJSONGoldenLegacy(t *testing.T) {
	outputDir := t.TempDir()
	legacyManifest := `{
  "workflow_name": "legacy-run",
  "status": "failed",
  "steps": []
}`
	if err := os.WriteFile(filepath.Join(outputDir, "manifest.json"), []byte(legacyManifest), 0o644); err != nil {
		t.Fatalf("write legacy manifest: %v", err)
	}

	statusCmd := NewRootCommand()
	statusCmd.SetArgs([]string{"workflow", "status", outputDir, "--format", "json"})
	statusCmd.SilenceUsage = true
	statusCmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return statusCmd.Execute() })
	if err != nil {
		t.Fatalf("workflow status: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	got := normalizeWorkflowStatusOutput(stdout, outputDir)
	want := mustReadGolden(t, "workflow-status-legacy.golden.json")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow status legacy json golden mismatch\n%s", diff)
	}
}

func TestWorkflowCleanTextGoldenDryRun(t *testing.T) {
	outputDir := setupWorkflowCleanCommandFixture(t)

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "clean", outputDir})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow clean: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	got := normalizeWorkflowCleanOutput(stdout, outputDir)
	want := mustReadGolden(t, "workflow-clean-dry-run.golden.txt")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow clean dry-run text golden mismatch\n%s", diff)
	}
}

func TestWorkflowCleanJSONGoldenDryRun(t *testing.T) {
	outputDir := setupWorkflowCleanCommandFixture(t)

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "clean", outputDir, "--format", "json"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow clean: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	got := normalizeWorkflowCleanOutput(stdout, outputDir)
	want := mustReadGolden(t, "workflow-clean-dry-run.golden.json")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow clean dry-run json golden mismatch\n%s", diff)
	}
}

func TestWorkflowCleanTextGoldenApply(t *testing.T) {
	outputDir := setupWorkflowCleanCommandFixture(t)

	cmd := NewRootCommand()
	cmd.SetArgs([]string{"workflow", "clean", outputDir, "--apply", "--include-snapshots"})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true

	stdout, stderr, err := captureStdoutStderrRun(t, func() error { return cmd.Execute() })
	if err != nil {
		t.Fatalf("workflow clean apply: %v", err)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	got := normalizeWorkflowCleanOutput(stdout, outputDir)
	want := mustReadGolden(t, "workflow-clean-apply.golden.txt")
	if diff := diffGolden(want, got); diff != "" {
		t.Fatalf("workflow clean apply text golden mismatch\n%s", diff)
	}
}

func normalizeWorkflowIndex(raw, outputDir, snapshotDir string) string {
	out := strings.ReplaceAll(raw, outputDir, "<output-dir>")
	if snapshotDir != "" {
		out = strings.ReplaceAll(out, snapshotDir, "<snapshot-dir>")
	}
	out = timestampPattern.ReplaceAllString(out, `<timestamp>`)
	out = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z`).ReplaceAllString(out, `<timestamp>`)
	testRoot := filepath.Dir(filepath.Dir(outputDir))
	if testRoot != "" && testRoot != "." && testRoot != "/" {
		out = strings.ReplaceAll(out, testRoot+"/", "<test-root>/")
	}
	// Normalize workflow error text (locale-dependent)
	out = regexp.MustCompile(`discover binlog files: [^<]+`).ReplaceAllString(out, `<discovery-error>`)
	return out
}

func normalizeWorkflowStatusOutput(raw, outputDir string) string {
	out := strings.ReplaceAll(raw, outputDir, "<output-dir>")
	testRoot := filepath.Dir(filepath.Dir(outputDir))
	if testRoot != "" && testRoot != "." && testRoot != "/" {
		out = strings.ReplaceAll(out, testRoot+"/", "<test-root>/")
	}
	out = planSHA256Pattern.ReplaceAllString(out, `"plan_sha256": "<plan-sha>"`)
	return out
}

func normalizeWorkflowCleanOutput(raw, outputDir string) string {
	out := strings.ReplaceAll(raw, outputDir, "<output-dir>")
	testRoot := filepath.Dir(filepath.Dir(outputDir))
	if testRoot != "" && testRoot != "." && testRoot != "/" {
		out = strings.ReplaceAll(out, testRoot+"/", "<test-root>/")
	}
	return out
}
