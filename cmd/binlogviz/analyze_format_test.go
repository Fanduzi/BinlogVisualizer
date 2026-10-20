package binlogviz

import (
	"strings"
	"testing"
)

func TestAnalyzeUnknownFormatExitsUsageBeforeInput(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, stdout, stderr := runRoot(t, "analyze", "--format", "yaml", "does-not-exist.binlog")
	if code != 3 {
		t.Fatalf("exit %d, want 3 (usage)\nstderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, `unknown format "yaml"`) {
		t.Fatalf("stderr %q", stderr)
	}
	if strings.Contains(stderr, "does-not-exist") || stdout != "" {
		t.Fatalf("format check read input or wrote a report\nstdout=%q\nstderr=%s", stdout, stderr)
	}
}
