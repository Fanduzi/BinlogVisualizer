package binlogviz

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestFlashbackRoundTripMySQL80Integrity is the #318/#321 apply check.
// A tampered or truncated script must not commit, and the intact script
// restores the pre-change checksum only when it goes through verify.
func TestFlashbackRoundTripMySQL80Integrity(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL 8.0 server")
	}
	forceEnglishRuntimeOutput(t)
	if _, err := exec.LookPath("sudo"); err != nil && os.Getenv("BINLOGVIZ_MYSQL") == "" {
		t.Fatal("BINLOGVIZ_FLASHBACK_E2E requires sudo mysql or BINLOGVIZ_MYSQL")
	}

	e2eMySQL(t, `
DROP DATABASE IF EXISTS bvint;
CREATE DATABASE bvint;
CREATE TABLE bvint.t (
  id INT NOT NULL,
  v INT NOT NULL,
  PRIMARY KEY (id)
);
INSERT INTO bvint.t VALUES (1, 1), (2, 2);`)
	t.Cleanup(func() { _, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS bvint") })
	const sumSQL = "CHECKSUM TABLE bvint.t"
	before := e2eMySQL(t, sumSQL)
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, `
START TRANSACTION;
UPDATE bvint.t SET v = 11 WHERE id = 1;
UPDATE bvint.t SET v = 22 WHERE id = 2;
COMMIT;`)
	after := e2eMySQL(t, sumSQL)
	if after == before {
		t.Fatal("incident did not change the checksum")
	}
	path := e2eIncidentBinlog(t)
	sql, stderr, err := executeFlashbackLikeMain(t, path, "--include-schema", "bvint")
	if err != nil {
		t.Fatalf("flashback: %v\n%s", err, stderr)
	}
	if !strings.Contains(sql, "-- binlogviz-trailer") || strings.Contains(sql, "\nCOMMIT;\n") {
		t.Fatal("published script commits")
	}

	// The file on disk, intact, does not commit.
	if _, err := e2eMySQLResult(t, sql); err == nil {
		t.Fatal("mysql accepted the no-commit trailer")
	}
	if got := e2eMySQL(t, sumSQL); got != after {
		t.Fatalf("intact file changed rows\nbefore incident end:\n%s\ngot:\n%s", after, got)
	}

	refused := []struct {
		name   string
		sql    string
		reason string
	}{
		{"edited", editFlashbackValue(t, sql), "digest_mismatch"},
		{"deleted", deleteFlashbackStatement(t, sql), "digest_mismatch"},
		{"reordered", reorderFlashbackStatements(t, sql), "digest_mismatch"},
		{"appended", sql + "INSERT INTO bvint.t VALUES (9, 9);\n", "appended"},
		{"truncated", sql[:len(sql)/2], "truncated"},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			file := filepath.Join(t.TempDir(), tc.name+".sql")
			if err := os.WriteFile(file, []byte(tc.sql), 0o644); err != nil {
				t.Fatal(err)
			}
			code, stdout, stderr := runRoot(t, "--error-format", "json", "flashback", "verify", file)
			reason := tc.reason
			if tc.name == "truncated" && strings.Contains(stderr, `"reason":"missing_trailer"`) {
				reason = "missing_trailer"
			}
			if code != 6 || stdout != "" || !strings.Contains(stderr, `"reason":"`+reason+`"`) {
				t.Fatalf("verify exit %d stdout %q stderr %s", code, stdout, stderr)
			}
			_, _ = e2eMySQLResult(t, tc.sql)
			if got := e2eMySQL(t, sumSQL); got != after {
				t.Fatalf("%s committed\nwant:\n%s\ngot:\n%s", tc.name, after, got)
			}
		})
	}

	e2eMySQL(t, committingFlashback(t, sql))
	if got := e2eMySQL(t, sumSQL); got != before {
		t.Fatalf("verified script\nwant:\n%s\ngot:\n%s", before, got)
	}
	rows := strings.TrimSpace(e2eMySQL(t, "SELECT id, v FROM bvint.t ORDER BY id"))
	if rows != "1\t1\n2\t2" {
		t.Fatalf("rows %q", rows)
	}
}

func editFlashbackValue(t *testing.T, sql string) string {
	t.Helper()
	const needle = "`v` = 1 WHERE"
	if !strings.Contains(sql, needle) {
		t.Fatalf("no undo value:\n%s", sql)
	}
	return strings.Replace(sql, needle, "`v` = 9 WHERE", 1)
}

func deleteFlashbackStatement(t *testing.T, sql string) string {
	t.Helper()
	start := strings.Index(sql, "-- binlogviz-exec\n")
	if start < 0 {
		t.Fatal("no exec piece")
	}
	next := strings.Index(sql[start+len("-- binlogviz-exec\n"):], "-- binlogviz-")
	if next < 0 {
		t.Fatal("no following piece")
	}
	return sql[:start] + sql[start+len("-- binlogviz-exec\n")+next:]
}

func reorderFlashbackStatements(t *testing.T, sql string) string {
	t.Helper()
	first := strings.Index(sql, "-- binlogviz-exec\n")
	if first < 0 {
		t.Fatal("no exec piece")
	}
	secondRel := strings.Index(sql[first+len("-- binlogviz-exec\n"):], "-- binlogviz-exec\n")
	if secondRel < 0 {
		t.Fatal("need two undo statements")
	}
	second := first + len("-- binlogviz-exec\n") + secondRel
	thirdRel := strings.Index(sql[second+len("-- binlogviz-exec\n"):], "-- binlogviz-")
	if thirdRel < 0 {
		t.Fatal("no piece after the second statement")
	}
	third := second + len("-- binlogviz-exec\n") + thirdRel
	return sql[:first] + sql[second:third] + sql[first:second] + sql[third:]
}
