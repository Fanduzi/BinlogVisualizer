package binlogviz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/guard"
)

func writeVerifyFixture(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	g := guard.New(nil)
	if err := g.Write(&b, func(emit func(guard.Piece) error) error {
		return emit(guard.Piece{Kind: guard.PieceStmt, Text: "DO 0"})
	}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "flashback.sql")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFlashbackVerifyJSONAndRefusals(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := writeVerifyFixture(t)
	code, stdout, stderr := runRoot(t, "--error-format", "json", "flashback", "verify", path, "--format", "json")
	if code != 0 || !strings.HasPrefix(strings.TrimSpace(stdout), "{\n  \"schema_version\": 1") || !strings.Contains(stdout, `"command": "flashback verify"`) || !strings.Contains(stdout, `"ok": true`) {
		t.Fatalf("json exit %d\nstdout %s\nstderr %s", code, stdout, stderr)
	}
	code, stdout, stderr = runRoot(t, "flashback", "verify", path)
	if code != 0 || !strings.Contains(stdout, "SET @binlogviz_commit = IF(@binlogviz_sealed <=> 1, 'COMMIT', 'ROLLBACK');") || strings.Contains(stdout, "has no commit trailer") {
		t.Fatalf("sql exit %d\n%s\n%s", code, stdout, stderr)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	edited := strings.Replace(string(body), "DO 0", "DO 1", 1)
	bad := filepath.Join(t.TempDir(), "edited.sql")
	if err := os.WriteFile(bad, []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr = runRoot(t, "--error-format", "json", "flashback", "verify", bad)
	if code != 6 || stdout != "" || !strings.Contains(stderr, `"reason":"digest_mismatch"`) {
		t.Fatalf("edited exit %d\nstdout %q\nstderr %s", code, stdout, stderr)
	}
	cut := filepath.Join(t.TempDir(), "cut.sql")
	if err := os.WriteFile(cut, body[:len(body)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr = runRoot(t, "--error-format", "json", "flashback", "verify", cut)
	if code != 6 || stdout != "" || !strings.Contains(stderr, `"reason":"truncated"`) && !strings.Contains(stderr, `"reason":"missing_trailer"`) {
		t.Fatalf("cut exit %d\nstdout %q\nstderr %s", code, stdout, stderr)
	}
	empty := filepath.Join(t.TempDir(), "empty.sql")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	code, stdout, _ = runRoot(t, "--error-format", "json", "flashback", "verify", empty)
	if code != 5 || stdout != "" {
		t.Fatalf("empty exit %d stdout %q", code, stdout)
	}
	code, stdout, _ = runRoot(t, "flashback", "verify", filepath.Join(t.TempDir(), "missing.sql"))
	if code != 4 || stdout != "" {
		t.Fatalf("missing exit %d stdout %q", code, stdout)
	}
	code, stdout, _ = runRoot(t, "flashback", "verify", path, "--format", "yaml")
	if code != 3 || stdout != "" {
		t.Fatalf("format exit %d stdout %q", code, stdout)
	}
}

func TestFlashbackByteEnvExit(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "minimal.binlog")
	for _, name := range []string{"BINLOGVIZ_SPILL_BYTES", "BINLOGVIZ_MEMORY_BYTES"} {
		for _, v := range []string{"abc", "0", "-1", "1MB", ""} {
			t.Run(name+"="+v, func(t *testing.T) {
				t.Setenv(name, v)
				code, stdout, stderr := runRoot(t, "--error-format", "json", "flashback", path)
				if code != 3 || stdout != "" || !strings.Contains(stderr, `"kind":"usage"`) || !strings.Contains(stderr, name) {
					t.Fatalf("exit %d\nstdout %q\nstderr %s", code, stdout, stderr)
				}
			})
		}
	}
}
