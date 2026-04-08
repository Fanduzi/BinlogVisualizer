package binlogviz

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
