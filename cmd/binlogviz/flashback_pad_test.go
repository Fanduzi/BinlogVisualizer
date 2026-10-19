package binlogviz

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"binlogviz/internal/guard"
)

// TestFlashbackPadCharModes is #347. A session that has PAD_CHAR_TO_FULL_LENGTH
// must still restore CHAR keys under NO PAD collations, and a keyed undo that
// matches anything but one row must roll back. MySQL 5.7 has no utf8mb4_0900
// collation, so that server uses utf8mb4_general_ci and utf8mb4_bin.
func TestFlashbackPadCharModes(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to apply on a local MySQL server")
	}
	forceEnglishRuntimeOutput(t)
	ver := strings.TrimSpace(e2eMySQL(t, "SELECT @@version"))
	switch {
	case strings.HasPrefix(ver, "8.0"):
		flashbackPadMySQL80(t)
	case strings.HasPrefix(ver, "5.7"):
		flashbackPadMySQL57(t)
	default:
		t.Fatalf("MySQL 8.0 or 5.7 required, got %s", ver)
	}
}

func flashbackPadMySQL80(t *testing.T) {
	t.Helper()
	e2eMySQL(t, `
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';
SET GLOBAL binlog_row_metadata = 'FULL';`)
	vars := e2eMySQL(t, "SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata, @@gtid_mode")
	fields := strings.Fields(vars)
	if len(fields) < 5 || fields[0] != "1" || fields[1] != "ROW" || fields[2] != "FULL" || fields[3] != "FULL" || fields[4] != "ON" {
		t.Fatalf("server is not MySQL 8.0 ROW/GTID/FULL: %s", vars)
	}
	e2eMySQL(t, `
DROP DATABASE IF EXISTS pad347;
DROP DATABASE IF EXISTS binlogviz;
CREATE DATABASE pad347;
CREATE TABLE pad347.ai (
  k CHAR(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL PRIMARY KEY,
  v INT NOT NULL
);
CREATE TABLE pad347.bn (
  k CHAR(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL PRIMARY KEY,
  v INT NOT NULL
);
INSERT INTO pad347.ai VALUES ('k4', 4), ('k5', 5);
INSERT INTO pad347.bn VALUES ('k4', 4), ('k5', 5);`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS pad347; DROP DATABASE IF EXISTS binlogviz;")
	})
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, `
UPDATE pad347.ai SET v = 104 WHERE k = 'k4';
DELETE FROM pad347.ai WHERE k = 'k5';
INSERT INTO pad347.ai VALUES ('k6', 6);
UPDATE pad347.bn SET v = 104 WHERE k = 'k4';
DELETE FROM pad347.bn WHERE k = 'k5';
INSERT INTO pad347.bn VALUES ('k6', 6);`)
	path := e2eIncidentBinlog(t)
	sql, stderr, err := executeFlashbackLikeMain(t, path, "--include-schema", "pad347")
	if err != nil {
		t.Fatalf("flashback: %v\n%s", err, stderr)
	}
	applyPadMatrix(t, sql, []string{"pad347.ai", "pad347.bn"}, "6")
}

func flashbackPadMySQL57(t *testing.T) {
	t.Helper()
	e2eMySQL(t, `
DROP DATABASE IF EXISTS pad347;
DROP DATABASE IF EXISTS binlogviz;
CREATE DATABASE pad347;
CREATE TABLE pad347.g (
  k CHAR(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL PRIMARY KEY,
  v INT NOT NULL
);
CREATE TABLE pad347.b (
  k CHAR(4) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
  v INT NOT NULL
);`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS pad347; DROP DATABASE IF EXISTS binlogviz;")
	})
	applyPadMatrix(t, writePadUndoScript(t, []string{"`pad347`.`g`", "`pad347`.`b`"}), []string{"pad347.g", "pad347.b"}, "6")
}

func applyPadMatrix(t *testing.T, script string, tables []string, statements string) {
	t.Helper()
	apply := committingFlashback(t, script)
	modes := []string{
		"",
		"PAD_CHAR_TO_FULL_LENGTH",
		"STRICT_TRANS_TABLES,PAD_CHAR_TO_FULL_LENGTH",
		"ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,PAD_CHAR_TO_FULL_LENGTH",
		"ANSI",
		"TRADITIONAL",
		"NO_BACKSLASH_ESCAPES",
		"NO_AUTO_VALUE_ON_ZERO",
		"PAD_CHAR_TO_FULL_LENGTH,NO_BACKSLASH_ESCAPES",
		"ANSI_QUOTES",
	}
	want := padRows(tables, [][2]string{{"k4", "4"}, {"k5", "5"}})
	marker := "binlogviz: undo committed, " + statements + " statements"
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			resetPadIncident(t, tables)
			canon := canonicalSQLMode(t, mode)
			out, err := e2eMySQLResult(t, "SET SESSION sql_mode='"+mode+"';\n"+apply+"\nSELECT @@SESSION.sql_mode")
			if err != nil {
				t.Fatalf("apply: %v\n%s", err, out)
			}
			if !strings.Contains(out, marker) {
				t.Fatalf("missing %q\n%s", marker, out)
			}
			if strings.Contains(out, "undo rolled back") {
				t.Fatalf("commit also rolled back\n%s", out)
			}
			if got := lastDataLine(out); got != canon {
				t.Fatalf("sql_mode %q want %q\n%s", got, canon, out)
			}
			if got := strings.TrimSpace(e2eMySQL(t, padSelect(tables))); got != want {
				t.Fatalf("rows\n%s\nwant\n%s", got, want)
			}
		})
	}
	t.Run("zero rows roll back", func(t *testing.T) {
		resetPadIncident(t, tables)
		for _, table := range tables {
			e2eMySQL(t, "DELETE FROM "+table+" WHERE k = 'k4'")
		}
		out, err := e2eMySQLResult(t, "SET SESSION sql_mode='PAD_CHAR_TO_FULL_LENGTH';\n"+apply)
		if err == nil {
			t.Fatalf("client treated a 0-row keyed undo as success\n%s", out)
		}
		if strings.Contains(out, "binlogviz: undo committed") || !strings.Contains(out, "undo rolled back") || !strings.Contains(out, "keyed undo affected 0 rows, expected 1") {
			t.Fatalf("poison output:\n%s", out)
		}
		want := padRows(tables, [][2]string{{"k6", "6"}})
		if got := strings.TrimSpace(e2eMySQL(t, padSelect(tables))); got != want {
			t.Fatalf("rows\n%s\nwant\n%s", got, want)
		}
	})
}

func resetPadIncident(t *testing.T, tables []string) {
	t.Helper()
	var b strings.Builder
	for _, table := range tables {
		fmt.Fprintf(&b, "DELETE FROM %s;\nINSERT INTO %s VALUES ('k4', 104), ('k6', 6);\n", table, table)
	}
	e2eMySQL(t, b.String())
}

func padSelect(tables []string) string {
	var b strings.Builder
	for i, table := range tables {
		if i > 0 {
			b.WriteString(" UNION ALL ")
		}
		fmt.Fprintf(&b, "SELECT '%s', k, v FROM %s", table, table)
	}
	b.WriteString(" ORDER BY 1, 2")
	return b.String()
}

func padRows(tables []string, rows [][2]string) string {
	names := append([]string(nil), tables...)
	sort.Strings(names)
	var lines []string
	for _, table := range names {
		for _, row := range rows {
			lines = append(lines, table+"\t"+row[0]+"\t"+row[1])
		}
	}
	return strings.Join(lines, "\n")
}

func canonicalSQLMode(t *testing.T, mode string) string {
	t.Helper()
	out := e2eMySQL(t, "SET SESSION sql_mode='"+mode+"';\nSELECT @@SESSION.sql_mode")
	return lastDataLine(out)
}

func lastDataLine(out string) string {
	lines := dataLines(out)
	if len(lines) == 0 {
		return ""
	}
	return lines[len(lines)-1]
}

func dataLines(out string) []string {
	var lines []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "mysql: [Warning]") {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

// writePadUndoScript is the 5.7 apply. 5.7 cannot generate a flashback from
// its own binlog (no binlog_row_metadata). The header and the E/T marks are
// the ones the 8.0 generator writes.
func writePadUndoScript(t *testing.T, tables []string) string {
	t.Helper()
	var b strings.Builder
	g := guard.New(nil)
	err := g.Write(&b, func(emit func(guard.Piece) error) error {
		for i, stmt := range []string{
			"SET @binlogviz_saved_mode = @@SESSION.sql_mode, SESSION sql_mode = REPLACE(REPLACE(@@SESSION.sql_mode, 'PAD_CHAR_TO_FULL_LENGTH', ''), 'NO_BACKSLASH_ESCAPES', '')",
			"SET NAMES utf8mb4",
			"SET time_zone = '+00:00'",
			"SET @binlogviz_bad = NULL",
			"SET @binlogviz_txn = 0",
			"SET @binlogviz_next_mode = NULL",
		} {
			text := stmt + ";\n"
			if i == 0 {
				text = stmt + ";"
			}
			if err := emit(guard.Piece{Kind: guard.PieceRaw, Text: text}); err != nil {
				return err
			}
		}
		if err := emit(guard.Piece{Kind: guard.PieceRaw, Text: guard.ProcedureSQL()}); err != nil {
			return err
		}
		if err := emit(guard.Piece{Kind: guard.PieceRaw, Text: guard.UndoOpenSQL()}); err != nil {
			return err
		}
		if err := emit(guard.Piece{Kind: guard.PieceStmt, Text: "SET @binlogviz_txn = 1"}); err != nil {
			return err
		}
		for _, table := range tables {
			for _, stmt := range []string{
				"E\nUPDATE " + table + " SET `v` = 4 WHERE `k` <=> 'k4'",
				"T\nINSERT INTO " + table + " (`k`, `v`) VALUES ('k5', 5)",
				"E\nDELETE FROM " + table + " WHERE `k` <=> 'k6'",
			} {
				if err := emit(guard.Piece{Kind: guard.PieceStmt, Text: stmt}); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.String()
}
