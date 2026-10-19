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

// TestFlashbackRoundTripMySQL80PartialCommit is #328 and #329.
// verify | mysql --force must not commit a prefix when a statement fails,
// and a statement that implicitly commits must not commit an open undo.
func TestFlashbackRoundTripMySQL80PartialCommit(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL 8.0 server")
	}
	forceEnglishRuntimeOutput(t)
	if _, err := exec.LookPath("sudo"); err != nil && os.Getenv("BINLOGVIZ_MYSQL") == "" {
		t.Fatal("BINLOGVIZ_FLASHBACK_E2E requires sudo mysql or BINLOGVIZ_MYSQL")
	}

	e2eMySQL(t, `
DROP DATABASE IF EXISTS bv328;
CREATE DATABASE bv328;
CREATE TABLE bv328.t (id INT NOT NULL, v INT NOT NULL, PRIMARY KEY (id));
CREATE TABLE bv328.u (id INT NOT NULL, v INT NOT NULL, PRIMARY KEY (id));
INSERT INTO bv328.t VALUES (1, 1), (2, 2);
INSERT INTO bv328.u VALUES (1, 1);`)
	t.Cleanup(func() { _, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS bv328") })
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, `
START TRANSACTION;
UPDATE bv328.t SET v = 11 WHERE id = 1;
UPDATE bv328.t SET v = 22 WHERE id = 2;
UPDATE bv328.u SET v = 22 WHERE id = 1;
COMMIT;`)
	const incident = "t\t1\t11\nt\t2\t22\nu\t1\t22"
	if got := bv328Rows(t); got != incident {
		t.Fatalf("incident rows\n%s", got)
	}
	path := e2eIncidentBinlog(t)
	sql, stderr, err := executeFlashbackLikeMain(t, path, "--include-schema", "bv328")
	if err != nil {
		t.Fatalf("flashback: %v\n%s", err, stderr)
	}
	verified := committingFlashback(t, sql)
	reset := func(t *testing.T) {
		t.Helper()
		e2eMySQL(t, `
DROP TABLE IF EXISTS bv328.u;
CREATE TABLE bv328.u (id INT NOT NULL, v INT NOT NULL, PRIMARY KEY (id));
INSERT INTO bv328.u VALUES (1, 22);
UPDATE bv328.t SET v = 11 WHERE id = 1;
UPDATE bv328.t SET v = 22 WHERE id = 2;
DROP TABLE IF EXISTS bv328.noise;`)
		if got := bv328Rows(t); got != incident {
			t.Fatalf("reset\n%s", got)
		}
	}
	assertIncident := func(t *testing.T, out string) {
		t.Helper()
		if got := bv328Rows(t); got != incident {
			t.Fatalf("undo committed\nwant:\n%s\ngot:\n%s\nmysql:\n%s", incident, got, out)
		}
	}

	t.Run("force_missing_table", func(t *testing.T) {
		reset(t)
		e2eMySQL(t, "DROP TABLE bv328.u")
		out, _ := e2eMySQLForce(t, verified)
		if !strings.Contains(out, "ERROR 1146") {
			t.Fatalf("missing table was not a statement error:\n%s", out)
		}
		got := strings.TrimSpace(e2eMySQL(t, "SELECT id, v FROM bv328.t ORDER BY id"))
		if got != "1\t11\n2\t22" {
			t.Fatalf("prefix committed after a missing table\n%s\nmysql:\n%s", got, out)
		}
	})
	t.Run("force_missing_column", func(t *testing.T) {
		reset(t)
		e2eMySQL(t, "ALTER TABLE bv328.u DROP COLUMN v")
		out, _ := e2eMySQLForce(t, verified)
		if !strings.Contains(out, "ERROR 1054") {
			t.Fatalf("missing column was not a statement error:\n%s", out)
		}
		got := strings.TrimSpace(e2eMySQL(t, "SELECT id, v FROM bv328.t ORDER BY id"))
		if got != "1\t11\n2\t22" {
			t.Fatalf("prefix committed after a missing column\n%s\nmysql:\n%s", got, out)
		}
	})

	cutTrailer := cutFlashbackBefore(t, sql, "-- binlogviz-trailer")
	cutMid := cutFlashbackMidUndo(t, sql)
	for _, tc := range []struct {
		name string
		body string
		loud string
	}{
		{"cut_then_script", cutTrailer + "START TRANSACTION;\nSELECT 1;\nROLLBACK;\n", "ERROR 1399"},
		{"cut_then_begin", cutTrailer + "BEGIN;\n", "ERROR 1399"},
		{"cut_then_ddl", cutTrailer + "CREATE TABLE bv328.noise (id INT);\n", "ERROR 1399"},
		{"cut_mid_then_begin", cutMid + "BEGIN;\n", "ERROR 1399"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset(t)
			out, _ := e2eMySQLResult(t, tc.body)
			if !strings.Contains(out, tc.loud) {
				t.Fatalf("implicit commit was not refused:\n%s", out)
			}
			assertIncident(t, out)
		})
	}
	for _, tc := range []struct {
		name string
		tail string
	}{
		{"force_then_script", "START TRANSACTION;\nSELECT 1;\nROLLBACK;\n"},
		{"force_then_begin", "BEGIN;\n"},
		{"force_then_start", "START TRANSACTION;\n"},
		{"force_then_ddl", "CREATE TABLE bv328.noise (id INT);\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reset(t)
			out, _ := e2eMySQLForce(t, sql+"\n"+tc.tail)
			assertIncident(t, out)
		})
	}
}

func bv328Rows(t *testing.T) string {
	t.Helper()
	return strings.TrimSpace(e2eMySQL(t, `
SELECT 't', id, v FROM bv328.t
UNION ALL
SELECT 'u', id, v FROM bv328.u
ORDER BY 1, 2`))
}

func cutFlashbackBefore(t *testing.T, sql, marker string) string {
	t.Helper()
	i := strings.Index(sql, marker)
	if i < 0 {
		t.Fatalf("no %s", marker)
	}
	return sql[:i]
}

// cutFlashbackMidUndo keeps a prefix of the undo and drops the rest on a
// line boundary, the shape BinlogQA concatenated with a following statement.
func cutFlashbackMidUndo(t *testing.T, sql string) string {
	t.Helper()
	trailer := strings.Index(sql, "-- binlogviz-trailer")
	if trailer < 0 {
		t.Fatal("no trailer")
	}
	cut := strings.LastIndex(sql[:trailer], "-- binlogviz-exec\n")
	if cut <= 0 {
		t.Fatal("no undo piece to cut before")
	}
	return sql[:cut]
}
