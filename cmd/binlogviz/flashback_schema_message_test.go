package binlogviz

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A definition that describes a different number of columns is a dump that does
// not match this table, not a missing dump: the refusal has to say which
// (ADR 0030). The fixture is a MySQL 5.7 binlog, which carries no row metadata,
// so the definition is the only source here.
func TestFlashbackSchemaWidthRefusalNamesBothCounts(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := filepath.Join(t.TempDir(), "wrong.sql")
	if err := os.WriteFile(path, []byte("CREATE TABLE shop.wide (id INT NOT NULL, note VARCHAR(8)) DEFAULT CHARSET=utf8mb4;\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	stdout, _, err := executeFlashbackLikeMain(t, mustFixturePath(t, "mysql-5.7.44-wide-full.binlog"), "--schema-file", path)
	if err == nil || stdout != "" {
		t.Fatalf("a width mismatch must refuse, stdout=%q err=%v", stdout, err)
	}
	if got := ExitCode(err); got != 6 {
		t.Fatalf("exit %d, want 6: %v", got, err)
	}
	for _, want := range []string{"describes 2 columns", "row has 13", "incident time", "--exclude-table"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("message %q is missing %q", err.Error(), want)
		}
	}
	if strings.Contains(err.Error(), "no --schema-file definition covers") {
		t.Fatalf("a definition was found; blaming a missing one is wrong: %v", err)
	}
}
