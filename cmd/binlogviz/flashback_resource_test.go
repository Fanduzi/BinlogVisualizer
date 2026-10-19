package binlogviz

import (
	"bytes"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
	"binlogviz/internal/flashback"
)

func TestFlashbackResourceExit(t *testing.T) {
	err := flashbackExit(&flashback.ResourceError{Reason: "memory_limit", Limit: 100, Used: 95})
	if ExitCode(err) != 10 {
		t.Fatalf("exit %d", ExitCode(err))
	}
	kind, details := ErrorKind(err)
	if kind != clierr.KindResource || details["reason"] != "memory_limit" {
		t.Fatalf("kind %s details %v", kind, details)
	}
	errorFormat = "json"
	t.Cleanup(func() { errorFormat = "" })
	var stderr bytes.Buffer
	PrintCommandError(&stderr, err)
	got := stderr.String()
	if !strings.Contains(got, `"kind":"resource"`) || !strings.Contains(got, `"code":10`) || !strings.Contains(got, `"reason":"memory_limit"`) {
		t.Fatalf("stderr %s", got)
	}
	if ExitCode(err) == 2 {
		t.Fatal("resource must not exit 2")
	}
}

func TestFlashbackSpillLimitOnFixture(t *testing.T) {
	t.Setenv("BINLOGVIZ_SPILL_BYTES", "1")
	path := mustFixturePath(t, "mysql-8.0.46-flashback-full.binlog")
	code, stdout, stderr := runRoot(t, "--error-format", "json", "flashback", path)
	if code != 10 {
		t.Fatalf("exit %d, want 10\nstderr %s", code, stderr)
	}
	if stdout != "" {
		t.Fatalf("script was written:\n%s", stdout)
	}
	if !strings.Contains(stderr, `"kind":"resource"`) || !strings.Contains(stderr, "spill_limit") {
		t.Fatalf("stderr %s", stderr)
	}
}
