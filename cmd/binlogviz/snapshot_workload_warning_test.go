package binlogviz

import (
	"path/filepath"
	"strings"
	"testing"
)

// A snapshot saved without --workload-id cannot be compared causally, so the
// save says so once. The report itself does not change.
func TestSnapshotWithoutWorkloadIDWarnsOnStderr(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")

	dir := t.TempDir()
	code, stdout, stderr := runRoot(t, "analyze", fixture, "--format", "json", "--snapshot-name", "no-wid", "--snapshot-dir", dir)
	if code != 0 {
		t.Fatalf("analyze exit %d: %s", code, stderr)
	}
	if got := strings.Count(stderr, "warning: "); got != 1 || !strings.Contains(stderr, "--workload-id") {
		t.Fatalf("want exactly one workload-id warning, got %d:\n%s", got, stderr)
	}
	if !strings.Contains(stdout, `"report_version": 4`) {
		t.Fatalf("the report must be unchanged:\n%s", stdout)
	}

	code, _, stderr = runRoot(t, "analyze", fixture, "--format", "json", "--snapshot-name", "with-wid", "--workload-id", "demo", "--snapshot-dir", dir)
	if code != 0 {
		t.Fatalf("analyze with --workload-id exit %d: %s", code, stderr)
	}
	if strings.Contains(stderr, "warning: ") {
		t.Fatalf("no warning expected with --workload-id:\n%s", stderr)
	}
	_ = filepath.Join
}
