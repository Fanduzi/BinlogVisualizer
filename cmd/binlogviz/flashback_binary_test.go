package binlogviz

import (
	"os"
	"strings"
	"testing"

	"binlogviz/internal/guard"
)

// TestFlashbackRoundTripMySQL80BinaryPad is the v0.32.0 BINARY(n) padding
// bug. A short value is stored padded with 0x00. The name matches the 8.0
// e2e filter; 5.7 applies the same literals from a hand script.
func TestFlashbackRoundTripMySQL80BinaryPad(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to apply on a local MySQL server")
	}
	forceEnglishRuntimeOutput(t)
	ver := mysqlVersion(e2eMySQL(t, "SELECT @@version"))
	switch {
	case strings.HasPrefix(ver, "8.0"):
		flashbackBinaryMySQL80(t)
	case strings.HasPrefix(ver, "5.7"):
		flashbackBinaryMySQL57(t)
	default:
		t.Fatalf("MySQL 8.0 or 5.7 required, got %s", ver)
	}
}

const (
	binShortK = "X'6B310000000000000000000000000000'"
	binFullK  = "X'11111111111111111111111111111111'"
	binUUIDK  = "X'AABBCCDDEEFF00112233445566778800'"
	binShortB = "X'61000000'"
	binFullB  = "X'11111111'"
	binUUIDB  = "X'62000000'"
)

func flashbackBinaryMySQL80(t *testing.T) {
	t.Helper()
	e2eMySQL(t, `
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';
SET GLOBAL binlog_row_metadata = 'FULL';`)
	setupBinaryTable(t)
	e2eMySQL(t, "FLUSH LOGS")
	e2eMySQL(t, `
UPDATE binpad.t SET v = v + 100 WHERE k = X'AABBCCDDEEFF00112233445566778800';
UPDATE binpad.t SET b = X'62' WHERE k = X'6B310000000000000000000000000000';
DELETE FROM binpad.t WHERE k = X'11111111111111111111111111111111';`)
	path := e2eIncidentBinlog(t)
	refuse, stderr, err := executeFlashbackLikeMain(t, path, "--include-schema", "binpad")
	if err != nil {
		t.Fatalf("flashback: %v\n%s", err, stderr)
	}
	guarded, stderr, err := executeFlashbackLikeMain(t, path, "--include-schema", "binpad", "--conflict", "guard")
	if err != nil {
		t.Fatalf("guard flashback: %v\n%s", err, stderr)
	}
	for _, sql := range []string{refuse, guarded} {
		assertBinaryPadding(t, sql)
	}
	if !flashbackScriptHas(refuse, "E\nUPDATE") || !flashbackScriptHas(refuse, "T\nINSERT") {
		t.Fatal("refuse script did not tag the undos")
	}
	if !flashbackScriptHas(guarded, "G\nUPDATE") || !flashbackScriptHas(guarded, "G\nINSERT") || flashbackScriptHas(guarded, "E\nUPDATE") {
		t.Fatal("guard script did not tag the undos")
	}
	applyBinaryModes(t, committingFlashback(t, refuse), committingFlashback(t, guarded))
}

func flashbackBinaryMySQL57(t *testing.T) {
	t.Helper()
	setupBinaryTable(t)
	e2eMySQL(t, binaryIncidentSQL())
	applyBinaryModes(t, committingFlashback(t, writeBinaryUndoScript(t, false)), committingFlashback(t, writeBinaryUndoScript(t, true)))
}

func setupBinaryTable(t *testing.T) {
	t.Helper()
	e2eMySQL(t, `
DROP DATABASE IF EXISTS binpad;
DROP DATABASE IF EXISTS binlogviz;
CREATE DATABASE binpad;
CREATE TABLE binpad.t (
  k BINARY(16) NOT NULL PRIMARY KEY,
  b BINARY(4) NOT NULL,
  v INT NOT NULL
);
INSERT INTO binpad.t VALUES
  (X'6B31', X'61', 1),
  (X'11111111111111111111111111111111', X'11111111', 2),
  (X'AABBCCDDEEFF00112233445566778800', X'6200', 3);`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS binpad; DROP DATABASE IF EXISTS binlogviz;")
	})
}

func binaryIncidentSQL() string {
	return `
DELETE FROM binpad.t;
INSERT INTO binpad.t VALUES
  (X'6B31', X'62', 1),
  (X'AABBCCDDEEFF00112233445566778800', X'6200', 103);`
}

func applyBinaryModes(t *testing.T, refuse, guarded string) {
	t.Helper()
	restored := binaryRows([][3]string{
		{trimX(binShortK), trimX(binShortB), "1"},
		{trimX(binFullK), trimX(binFullB), "2"},
		{trimX(binUUIDK), trimX(binUUIDB), "3"},
	})
	drifted := binaryRows([][3]string{
		{trimX(binShortK), "63000000", "1"},
		{trimX(binFullK), trimX(binFullB), "2"},
		{trimX(binUUIDK), trimX(binUUIDB), "9"},
	})
	const marker = "binlogviz: undo committed, 3 statements"
	t.Run("refuse", func(t *testing.T) {
		e2eMySQL(t, binaryIncidentSQL())
		out, err := e2eMySQLResult(t, refuse)
		if err != nil {
			t.Fatalf("apply: %v\n%s", err, out)
		}
		if !strings.Contains(out, marker) || strings.Contains(out, "undo rolled back") {
			t.Fatalf("output:\n%s", out)
		}
		if got := mysqlBatchValue(e2eMySQL(t, binarySelect())); got != restored {
			t.Fatalf("rows\n%s\nwant\n%s", got, restored)
		}
	})
	t.Run("guard", func(t *testing.T) {
		e2eMySQL(t, binaryIncidentSQL())
		out, err := e2eMySQLResult(t, guarded)
		if err != nil {
			t.Fatalf("apply: %v\n%s", err, out)
		}
		if !strings.Contains(out, marker) || strings.Contains(out, "undo rolled back") {
			t.Fatalf("output:\n%s", out)
		}
		if got := mysqlBatchValue(e2eMySQL(t, binarySelect())); got != restored {
			t.Fatalf("rows\n%s\nwant\n%s", got, restored)
		}
	})
	t.Run("refuse drift", func(t *testing.T) {
		e2eMySQL(t, binaryDriftSQL())
		out, err := e2eMySQLResult(t, refuse)
		if err == nil || strings.Contains(out, "binlogviz: undo committed") || !strings.Contains(out, "undo rolled back") {
			t.Fatalf("err %v\n%s", err, out)
		}
		if got := mysqlBatchValue(e2eMySQL(t, binarySelect())); got != drifted {
			t.Fatalf("rows\n%s\nwant\n%s", got, drifted)
		}
	})
	t.Run("guard drift", func(t *testing.T) {
		e2eMySQL(t, binaryDriftSQL())
		out, err := e2eMySQLResult(t, guarded)
		if err != nil || strings.Contains(out, "binlogviz: undo committed") || strings.Contains(out, "undo rolled back") {
			t.Fatalf("err %v\n%s", err, out)
		}
		if !strings.Contains(out, "binlogviz: undo skipped, 3 rows") {
			t.Fatalf("output:\n%s", out)
		}
		if got := mysqlBatchValue(e2eMySQL(t, binarySelect())); got != drifted {
			t.Fatalf("rows\n%s\nwant\n%s", got, drifted)
		}
	})
}

func binaryDriftSQL() string {
	return binaryIncidentSQL() + `
INSERT INTO binpad.t VALUES (X'11111111111111111111111111111111', X'11111111', 2);
UPDATE binpad.t SET b = X'63' WHERE k = X'6B310000000000000000000000000000';
UPDATE binpad.t SET v = 9 WHERE k = X'AABBCCDDEEFF00112233445566778800';`
}

func binarySelect() string {
	return "SELECT HEX(k), HEX(b), v FROM binpad.t ORDER BY v"
}

func binaryRows(rows [][3]string) string {
	lines := make([]string, len(rows))
	for i, row := range rows {
		lines[i] = row[0] + "\t" + row[1] + "\t" + row[2]
	}
	return strings.Join(lines, "\n")
}

func trimX(lit string) string {
	return strings.TrimSuffix(strings.TrimPrefix(lit, "X'"), "'")
}

func assertBinaryPadding(t *testing.T, sql string) {
	t.Helper()
	for _, lit := range []string{binShortK, binFullK, binUUIDK, binShortB, binFullB, binUUIDB} {
		if !flashbackScriptHas(sql, lit) {
			t.Fatalf("missing %s\n%s", lit, sql)
		}
	}
	for _, lit := range []string{"X'6B31'", "X'61'", "X'62'", "X'6200'", "X'AABBCCDDEEFF001122334455667788'"} {
		if flashbackScriptHas(sql, lit) {
			t.Fatalf("unpadded %s\n%s", lit, sql)
		}
	}
	if strings.Contains(sql, "binlogviz: undo committed") {
		t.Fatal("commit marker literal is in the SQL")
	}
}

func writeBinaryUndoScript(t *testing.T, postImage bool) string {
	t.Helper()
	tag := "E"
	insert := "T\nINSERT INTO `binpad`.`t` (`k`, `b`, `v`) VALUES (" + binFullK + ", " + binFullB + ", 2)"
	short := tag + "\nUPDATE `binpad`.`t` SET `b` = " + binShortB + " WHERE `k` <=> " + binShortK
	uuid := tag + "\nUPDATE `binpad`.`t` SET `v` = 3 WHERE `k` <=> " + binUUIDK
	if postImage {
		insert = "G\nINSERT INTO `binpad`.`t` (`k`, `b`, `v`) SELECT " + binFullK + ", " + binFullB + ", 2 FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM `binpad`.`t` WHERE `k` <=> " + binFullK + ")"
		short = "G\nUPDATE `binpad`.`t` SET `b` = " + binShortB + " WHERE `k` <=> " + binShortK + " AND `b` <=> " + binUUIDB + " AND `v` <=> 1"
		uuid = "G\nUPDATE `binpad`.`t` SET `v` = 3 WHERE `k` <=> " + binUUIDK + " AND `b` <=> " + binUUIDB + " AND `v` <=> 103"
	}
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
		for _, stmt := range []string{insert, short, uuid} {
			if err := emit(guard.Piece{Kind: guard.PieceStmt, Text: stmt}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.String()
}
