package binlogviz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// splitFlashback runs the flashback command like main and returns output
// and the typed error, for flag-validation cases that fail before input.
func splitFlashback(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := NewRootCommand()
	cmd.SetArgs(append([]string{"flashback"}, args...))
	return captureStdoutStderrRun(t, func() error {
		err := cmd.Execute()
		if err != nil {
			PrintCommandError(os.Stderr, err)
		}
		return err
	})
}

func splitRedo(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := NewRootCommand()
	cmd.SetArgs(append([]string{"redo"}, args...))
	return captureStdoutStderrRun(t, func() error {
		err := cmd.Execute()
		if err != nil {
			PrintCommandError(os.Stderr, err)
		}
		return err
	})
}

func splitFixture(t *testing.T) string {
	t.Helper()
	return mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog")
}

func TestSplitFlagWithoutOutputDirIsUsage(t *testing.T) {
	_, _, err := splitFlashback(t, splitFixture(t), "--file-per-table")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("--file-per-table alone = %v, want usage", err)
	}
	_, _, err = splitFlashback(t, splitFixture(t), "--max-statements-per-file", "4")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("--max-statements-per-file alone = %v, want usage", err)
	}
	_, _, err = splitRedo(t, splitFixture(t), "--max-statements-per-file", "4")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("redo --max-statements-per-file alone = %v, want usage", err)
	}
}

func TestSplitOutputDirWithoutSplitFlagIsUsage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	_, _, err := splitFlashback(t, splitFixture(t), "--output-dir", dir)
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("--output-dir alone = %v, want usage", err)
	}
	_, _, err = splitRedo(t, splitFixture(t), "--output-dir", dir)
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("redo --output-dir alone = %v, want usage", err)
	}
}

func TestSplitInvalidMaxIsUsage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	for _, n := range []string{"0", "-3"} {
		_, _, err := splitFlashback(t, splitFixture(t), "--output-dir", dir, "--max-statements-per-file", n)
		if err == nil || ExitCode(err) != 3 {
			t.Fatalf("--max-statements-per-file %s = %v, want usage", n, err)
		}
	}
}

func TestSplitPlanCombinationIsUsage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	_, _, err := splitFlashback(t, splitFixture(t), "--plan", "--output-dir", dir, "--file-per-table")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("--plan + split = %v, want usage", err)
	}
}

func TestSplitRedoHasNoFilePerTable(t *testing.T) {
	// --file-per-table is deliberately unregistered on redo (ADR 0025):
	// per-table partition scatters one transaction's replay across files.
	dir := filepath.Join(t.TempDir(), "out")
	_, _, err := splitRedo(t, splitFixture(t), "--output-dir", dir, "--file-per-table")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("redo --file-per-table = %v, want usage", err)
	}
}

func TestSplitNonEmptyDirIsUsage(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "old.sql"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err := splitFlashback(t, splitFixture(t), "--output-dir", dir, "--file-per-table")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("non-empty dir = %v, want usage", err)
	}
}

func TestSplitDirAsFileIsUsage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err := splitFlashback(t, splitFixture(t), "--output-dir", path, "--file-per-table")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("file as dir = %v, want usage", err)
	}
}

func TestSplitFlashbackPerTableWritesSealedFiles(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	dir := filepath.Join(t.TempDir(), "out")
	stdout, stderr, err := splitFlashback(t, splitFixture(t), "--output-dir", dir, "--file-per-table")
	if err != nil {
		t.Fatalf("split flashback: %v\n%s", err, stderr)
	}
	if stdout != "" {
		t.Fatalf("split output went to stdout:\n%s", stdout)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	manifest, err := os.ReadFile(filepath.Join(dir, "MANIFEST"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names {
		if name == "MANIFEST" {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		sql := string(body)
		if !strings.Contains(sql, "binlogviz-trailer-v1") || !strings.Contains(sql, "@binlogviz_sealed") {
			t.Fatalf("%s is not a sealed script", name)
		}
		if !strings.Contains(sql, "-- binlogviz split file: "+name) {
			t.Fatalf("%s missing split label", name)
		}
		if !strings.Contains(string(manifest), name) {
			t.Fatalf("%s missing from manifest:\n%s", name, manifest)
		}
	}
}

func TestSplitRedoCountParts(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	dir := filepath.Join(t.TempDir(), "out")
	stdout, stderr, err := splitRedo(t, splitFixture(t), "--output-dir", dir, "--max-statements-per-file", "3")
	if err != nil {
		t.Fatalf("split redo: %v\n%s", err, stderr)
	}
	if stdout != "" {
		t.Fatalf("split output went to stdout:\n%s", stdout)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var parts int
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "part-") {
			parts++
		}
	}
	if parts < 2 {
		t.Fatalf("parts = %d, want >= 2; dir has %v", parts, entries)
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), "part-") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(string(body), "SET SESSION sql_mode") {
			t.Fatalf("%s missing session preamble", e.Name())
		}
	}
}

func TestSplitRefusalLeavesNoDir(t *testing.T) {
	// A refused run must leave nothing behind: no output dir, no staging
	// dir — the staged-publication contract at directory granularity.
	forceEnglishRuntimeOutput(t)
	dir := filepath.Join(t.TempDir(), "out")
	// --where with an unresolvable column refuses at Prepare (exit 6).
	_, _, err := splitFlashback(t, splitFixture(t), "--output-dir", dir, "--file-per-table", "--where", "nosuchcol = 1")
	if err == nil {
		t.Fatal("expected refusal, got success")
	}
	if _, statErr := os.Stat(dir); !os.IsNotExist(statErr) {
		t.Fatalf("output dir exists after refusal: %v", statErr)
	}
	leftovers, _ := filepath.Glob(filepath.Join(filepath.Dir(dir), ".binlogviz-split-*"))
	if len(leftovers) != 0 {
		t.Fatalf("staging dirs left behind: %v", leftovers)
	}
}
