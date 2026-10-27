package binlogviz

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// End-to-end differential check of the flashback planner and the apply guard
// on MySQL 8.0 and 8.4 (ADR 0020). BINLOGVIZ_DIFFTEST=1,
// BINLOGVIZ_DIFFTEST_MYSQL is the mysql client command, and
// BINLOGVIZ_DIFFTEST_DOCKER is the container name when the datadir is not on
// this machine. The test applies DML, builds the script from the closed
// binlog, applies it, and requires CHECKSUM TABLE plus an ordered row dump
// to match the baseline. Refusal cases leave that dump unchanged.

func TestFlashbackDiffMySQL(t *testing.T) {
	if os.Getenv("BINLOGVIZ_DIFFTEST") != "1" {
		t.Skip("set BINLOGVIZ_DIFFTEST=1 and BINLOGVIZ_DIFFTEST_MYSQL to round-trip flashback on MySQL")
	}
	if strings.TrimSpace(os.Getenv("BINLOGVIZ_DIFFTEST_MYSQL")) == "" {
		t.Fatal("BINLOGVIZ_DIFFTEST_MYSQL is required")
	}
	forceEnglishRuntimeOutput(t)
	line := strings.TrimSpace(diffMust(t, "SELECT @@version, @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata, @@gtid_mode"))
	fields := strings.Fields(line)
	if len(fields) < 6 || (!strings.HasPrefix(fields[0], "8.0") && !strings.HasPrefix(fields[0], "8.4")) || fields[1] != "1" || fields[2] != "ROW" || fields[3] != "FULL" || fields[4] != "FULL" || fields[5] != "ON" {
		t.Fatalf("server is not MySQL 8.0/8.4 ROW/GTID/FULL: %s", line)
	}
	t.Cleanup(func() {
		_, _ = diffQuery(t, "DROP DATABASE IF EXISTS bvdiff")
	})

	t.Run("restore", func(t *testing.T) {
		diffMust(t, `
DROP DATABASE IF EXISTS bvdiff;
CREATE DATABASE bvdiff;
CREATE TABLE bvdiff.t (
  id INT NOT NULL,
  s VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  n INT,
  d DECIMAL(10,2),
  dt DATETIME(3),
  j JSON,
  g INT AS (n + 1) STORED,
  PRIMARY KEY (id)
);
INSERT INTO bvdiff.t (id, s, n, d, dt, j) VALUES
  (1, 'AbC', 10, 1.50, '2020-01-02 03:04:05.120', JSON_OBJECT('a', 1)),
  (2, NULL, NULL, NULL, NULL, NULL);`)
		beforeSum := diffMust(t, "CHECKSUM TABLE bvdiff.t")
		beforeRows := diffMust(t, diffRowSQL)
		diffMust(t, "FLUSH LOGS")
		diffMust(t, `
UPDATE bvdiff.t SET s = 'changed', n = 20, d = 9.99, dt = '2024-05-06 07:08:09.010', j = JSON_OBJECT('a', 2) WHERE id = 1;
DELETE FROM bvdiff.t WHERE id = 2;
INSERT INTO bvdiff.t (id, s, n, d, dt, j) VALUES (3, 'new', 3, 2.00, '2021-02-03 04:05:06.000', JSON_OBJECT('b', 'x'));`)
		afterSum := diffMust(t, "CHECKSUM TABLE bvdiff.t")
		if afterSum == beforeSum {
			t.Fatal("incident did not change the checksum")
		}
		path := diffIncidentBinlog(t)
		schema := filepath.Join(t.TempDir(), "t.sql")
		if err := os.WriteFile(schema, []byte(diffCreate(t, "bvdiff.t")), 0o644); err != nil {
			t.Fatal(err)
		}
		sql, stderr, err := executeFlashbackLikeMain(t, path, "--schema-file", schema, "--include-table", "bvdiff.t")
		if err != nil {
			t.Fatalf("flashback: %v\n%s", err, stderr)
		}
		if !strings.Contains(stderr, "flashback is experimental") {
			t.Fatalf("missing experimental warning:\n%s", stderr)
		}
		diffMust(t, committingFlashback(t, sql))
		if got := diffMust(t, "CHECKSUM TABLE bvdiff.t"); got != beforeSum {
			t.Fatalf("checksum\nbefore:\n%s\nrestored:\n%s", beforeSum, got)
		}
		if got := diffMust(t, diffRowSQL); got != beforeRows {
			t.Fatalf("rows\nbefore:\n%s\nrestored:\n%s", beforeRows, got)
		}
	})

	t.Run("float refused", func(t *testing.T) {
		diffMust(t, `
DROP TABLE IF EXISTS bvdiff.flt;
CREATE TABLE bvdiff.flt (id INT NOT NULL PRIMARY KEY, x FLOAT);
INSERT INTO bvdiff.flt VALUES (1, 1.25);`)
		diffMust(t, "FLUSH LOGS")
		diffMust(t, "UPDATE bvdiff.flt SET x = 2.5 WHERE id = 1")
		held := diffMust(t, "CHECKSUM TABLE bvdiff.flt")
		path := diffIncidentBinlog(t)
		stdout, stderr, err := executeFlashbackLikeMain(t, path, "--include-table", "bvdiff.flt")
		if err == nil || ExitCode(err) != 6 || stdout != "" {
			t.Fatalf("float exit %v stdout %q\n%s", err, stdout, stderr)
		}
		if got := diffMust(t, "CHECKSUM TABLE bvdiff.flt"); got != held {
			t.Fatalf("float refusal changed the table\nwas:\n%s\nnow:\n%s", held, got)
		}
	})

	t.Run("conflict refused", func(t *testing.T) {
		// A selected transaction moves key 1, and a later transaction reuses
		// that key. The default policy refuses (exit 7) and writes no script.
		// Two updates of a non-key column in the selected range are one undo,
		// not a conflict.
		const gtid = "aaaaaaaa-0270-4000-8000-000000000270:1"
		diffMust(t, `
DROP TABLE IF EXISTS bvdiff.pk;
CREATE TABLE bvdiff.pk (id INT NOT NULL PRIMARY KEY, v INT);
INSERT INTO bvdiff.pk VALUES (1, 1);`)
		diffMust(t, "FLUSH LOGS")
		diffMust(t, "SET GTID_NEXT = '"+gtid+"';\nUPDATE bvdiff.pk SET id = 2, v = 2 WHERE id = 1;\nSET GTID_NEXT = 'AUTOMATIC';\nINSERT INTO bvdiff.pk VALUES (1, 9);")
		held := diffMust(t, "SELECT id, v FROM bvdiff.pk ORDER BY id")
		path := diffIncidentBinlog(t)
		stdout, stderr, err := executeFlashbackLikeMain(t, path, "--include-gtids", gtid)
		if err == nil || ExitCode(err) != 7 || stdout != "" {
			t.Fatalf("conflict exit %v stdout %q\n%s", err, stdout, stderr)
		}
		if got := diffMust(t, "SELECT id, v FROM bvdiff.pk ORDER BY id"); got != held {
			t.Fatalf("conflict refusal changed rows\nwas:\n%s\nnow:\n%s", held, got)
		}
	})

	t.Run("guard rejects", func(t *testing.T) {
		diffMust(t, `
DROP TABLE IF EXISTS bvdiff.gen;
CREATE TABLE bvdiff.gen (
  id INT NOT NULL PRIMARY KEY,
  n INT,
  g INT AS (n + 1) STORED
);
INSERT INTO bvdiff.gen (id, n) VALUES (1, 10);`)
		diffMust(t, "FLUSH LOGS")
		diffMust(t, "UPDATE bvdiff.gen SET n = 11 WHERE id = 1")
		path := diffIncidentBinlog(t)
		schema := filepath.Join(t.TempDir(), "gen.sql")
		if err := os.WriteFile(schema, []byte(diffCreate(t, "bvdiff.gen")), 0o644); err != nil {
			t.Fatal(err)
		}
		sql, stderr, err := executeFlashbackLikeMain(t, path, "--schema-file", schema, "--include-table", "bvdiff.gen")
		if err != nil {
			t.Fatalf("guard script: %v\n%s", err, stderr)
		}
		if !strings.Contains(sql, "@binlogviz_mismatch") {
			t.Fatalf("script has no apply guard:\n%s", sql)
		}
		diffMust(t, "ALTER TABLE bvdiff.gen MODIFY g INT NULL")
		held := diffMust(t, "SELECT id, n, g FROM bvdiff.gen")
		out, err := diffQuery(t, committingFlashback(t, sql))
		if err == nil || !strings.Contains(out, "schema file does not match the target") {
			t.Fatalf("guard did not reject: %v\n%s", err, out)
		}
		if got := diffMust(t, "SELECT id, n, g FROM bvdiff.gen"); got != held {
			t.Fatalf("rejected apply changed rows\nwas:\n%s\nnow:\n%s", held, got)
		}
	})
}

const diffRowSQL = "SELECT id, IFNULL(s, '<NULL>'), IFNULL(n, '<NULL>'), IFNULL(d, '<NULL>'), IFNULL(DATE_FORMAT(dt, '%Y-%m-%d %H:%i:%s.%f'), '<NULL>'), IFNULL(CAST(j AS CHAR), '<NULL>'), IFNULL(g, '<NULL>') FROM bvdiff.t ORDER BY id"

func diffCreate(t *testing.T, table string) string {
	t.Helper()
	out := diffMust(t, "SHOW CREATE TABLE "+table)
	i := strings.Index(out, "\t")
	if i < 0 {
		t.Fatalf("SHOW CREATE TABLE %s:\n%s", table, out)
	}
	db := strings.SplitN(table, ".", 2)[0]
	return "USE `" + db + "`;\n" + out[i+1:]
}

func diffIncidentBinlog(t *testing.T) string {
	t.Helper()
	diffMust(t, "FLUSH LOGS")
	datadir := strings.TrimSpace(diffMust(t, "SELECT @@datadir"))
	// The first column is the file name. MySQL 8 also returns a size and an
	// Encrypted flag, so the row is split on tabs rather than on spaces.
	var files []string
	for _, line := range strings.Split(diffMust(t, "SHOW BINARY LOGS"), "\n") {
		line = strings.TrimRight(line, "\r")
		name, _, _ := strings.Cut(strings.TrimSpace(line), "\t")
		if name != "" {
			files = append(files, name)
		}
	}
	if len(files) < 2 {
		t.Fatalf("binary logs: %q", files)
	}
	name := files[len(files)-2]
	dst := filepath.Join(t.TempDir(), name)
	src := strings.TrimRight(datadir, "/") + "/" + name
	if c := os.Getenv("BINLOGVIZ_DIFFTEST_DOCKER"); c != "" {
		cmd := exec.Command("docker", "cp", c+":"+src, dst)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("docker cp %s: %v\n%s", src, err, out)
		}
		return dst
	}
	if out, err := exec.Command("sudo", "cp", src, dst).CombinedOutput(); err != nil {
		t.Fatalf("cp %s: %v\n%s", src, err, out)
	}
	if out, err := exec.Command("sudo", "chmod", "a+r", dst).CombinedOutput(); err != nil {
		t.Fatalf("chmod %s: %v\n%s", dst, err, out)
	}
	return dst
}

func diffMust(t *testing.T, sql string) string {
	t.Helper()
	out, err := diffQuery(t, sql)
	if err != nil {
		t.Fatalf("mysql: %v\n%s\nSQL:\n%s", err, out, sql)
	}
	return out
}

func diffQuery(t *testing.T, sql string) (string, error) {
	t.Helper()
	base := strings.Fields(os.Getenv("BINLOGVIZ_DIFFTEST_MYSQL"))
	if len(base) == 0 {
		return "", fmt.Errorf("BINLOGVIZ_DIFFTEST_MYSQL is empty")
	}
	args := append(append([]string{}, base[1:]...), "--default-character-set=utf8mb4", "--batch", "--raw", "--skip-column-names", "--connect-timeout=15")
	cmd := exec.Command(base[0], args...)
	cmd.Stdin = strings.NewReader(sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	out := stdout.String() + stderr.String()
	if err != nil {
		return out, err
	}
	return stdout.String(), nil
}
