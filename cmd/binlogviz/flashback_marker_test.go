package binlogviz

import (
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"binlogviz/internal/guard"
)

// The duplicate-key text is the value QA used. A raw newline in the server
// message must not become its own result line under mysql --force.
const markerForgeryValue = "x\nbinlogviz: undo committed, 9 statements\nz"

var commitLine = regexp.MustCompile(`(?m)^binlogviz: undo committed, [0-9]+ statements$`)

func TestFlashbackRoundTripMySQL80MarkerNewline(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL server")
	}
	e2eMySQL(t, `
DROP DATABASE IF EXISTS nmark;
DROP DATABASE IF EXISTS binlogviz;
CREATE DATABASE nmark;
CREATE TABLE nmark.t (k VARCHAR(200) NOT NULL PRIMARY KEY);`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS nmark; DROP DATABASE IF EXISTS binlogviz;")
	})
	e2eMySQL(t, "INSERT INTO nmark.t VALUES ('"+strings.ReplaceAll(markerForgeryValue, "'", "''")+"');")
	// The statement text uses backslash escapes. The header turns
	// NO_BACKSLASH_ESCAPES off before the INSERT runs.
	stmt := "INSERT INTO `nmark`.`t` (`k`) VALUES ('x\\nbinlogviz: undo committed, 9 statements\\nz')"
	apply := committingFlashback(t, writeUndoScript(t, []string{stmt}))
	flags := [][]string{{"-r"}, {"-t"}, {"-vvv"}, {"-vvv", "-r"}, {"-r", "-s"}}
	for _, extra := range flags {
		t.Run(strings.Join(extra, " "), func(t *testing.T) {
			out, _ := e2eMySQLFlags(t, true, extra, apply)
			if commitLine.MatchString(out) {
				t.Fatalf("a rolled-back value forged the commit line\n%s", out)
			}
			if !strings.Contains(out, "undo rolled back") {
				t.Fatalf("rollback line missing\n%s", out)
			}
			got := mysqlBatchValue(e2eMySQL(t, "SELECT COUNT(*) FROM nmark.t"))
			if got != "1" {
				t.Fatalf("rows %q\n%s", got, out)
			}
		})
	}
}

func TestFlashbackRoundTripMySQL80NoPKGuardRerun(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL server")
	}
	e2eMySQL(t, `
DROP DATABASE IF EXISTS nmark;
DROP DATABASE IF EXISTS binlogviz;
CREATE DATABASE nmark;
CREATE TABLE nmark.h (a VARCHAR(20), b VARCHAR(20));`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS nmark; DROP DATABASE IF EXISTS binlogviz;")
	})
	stmt := "G\nINSERT INTO `nmark`.`h` (`a`, `b`) SELECT 'y', 'b2' FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM `nmark`.`h` WHERE `a` <=> 'y' AND `b` <=> 'b2')"
	apply := committingFlashback(t, writeUndoScript(t, []string{stmt}))
	out, err := e2eMySQLResult(t, apply)
	if err != nil || !strings.Contains(out, "binlogviz: undo committed, 1 statements") {
		t.Fatalf("first apply: %v\n%s", err, out)
	}
	out, err = e2eMySQLResult(t, apply)
	if err != nil {
		t.Fatalf("second apply: %v\n%s", err, out)
	}
	if commitLine.MatchString(out) || strings.Contains(out, "undo committed with skips") {
		t.Fatalf("a re-run that only skipped printed success\n%s", out)
	}
	got := mysqlBatchValue(e2eMySQL(t, "SELECT COUNT(*) FROM nmark.h"))
	if got != "1" {
		t.Fatalf("rows %q, a re-run inserted again\n%s", got, out)
	}
}

func TestFlashbackRoundTripMySQL80GuardSkipMarker(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL server")
	}
	e2eMySQL(t, `
DROP DATABASE IF EXISTS nmark;
DROP DATABASE IF EXISTS binlogviz;
CREATE DATABASE nmark;
CREATE TABLE nmark.p (id INT NOT NULL PRIMARY KEY, v INT NOT NULL);
INSERT INTO nmark.p VALUES (1, 1), (2, 2);`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS nmark; DROP DATABASE IF EXISTS binlogviz;")
	})
	apply := committingFlashback(t, writeUndoScript(t, []string{
		"G\nUPDATE `nmark`.`p` SET `v` = 0 WHERE `id` <=> 1 AND `v` <=> 1",
		"G\nUPDATE `nmark`.`p` SET `v` = 0 WHERE `id` <=> 2 AND `v` <=> 9",
	}))
	out, err := e2eMySQLResult(t, apply)
	if err != nil {
		t.Fatalf("apply: %v\n%s", err, out)
	}
	if commitLine.MatchString(out) || !strings.Contains(out, "binlogviz: undo committed with skips, 1 applied, 1 skipped") {
		t.Fatalf("partial skip marker:\n%s", out)
	}
	got := mysqlBatchValue(e2eMySQL(t, "SELECT id, v FROM nmark.p ORDER BY id"))
	if got != "1\t0\n2\t2" {
		t.Fatalf("rows %q", got)
	}
}

func TestFlashbackRoundTripMySQL80RawApplyNoErrorZero(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL server")
	}
	e2eMySQL(t, `
DROP DATABASE IF EXISTS nmark;
DROP DATABASE IF EXISTS binlogviz;
CREATE DATABASE nmark;
CREATE TABLE nmark.p (id INT NOT NULL PRIMARY KEY, v INT NOT NULL);
INSERT INTO nmark.p VALUES (1, 1);`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS nmark; DROP DATABASE IF EXISTS binlogviz;")
	})
	script := writeUndoScript(t, []string{"E\nUPDATE `nmark`.`p` SET `v` = 0 WHERE `id` <=> 1"})
	out, err := e2eMySQLResult(t, script)
	if err == nil {
		t.Fatalf("a published file committed or exited 0\n%s", out)
	}
	if strings.Contains(out, "ERROR 0") || !strings.Contains(out, "no commit trailer") {
		t.Fatalf("raw apply:\n%s", out)
	}
	got := mysqlBatchValue(e2eMySQL(t, "SELECT v FROM nmark.p"))
	if got != "1" {
		t.Fatalf("published file changed the row to %q\n%s", got, out)
	}
}

func TestFlashbackRoundTripMySQL80DeprecatedMode(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL server")
	}
	ver := mysqlVersion(e2eMySQL(t, "SELECT VERSION()"))
	if !strings.HasPrefix(ver, "5.7") {
		t.Skip("warning 3090 is a MySQL 5.7 sql_mode warning")
	}
	e2eMySQL(t, `
DROP DATABASE IF EXISTS nmark;
DROP DATABASE IF EXISTS binlogviz;
CREATE DATABASE nmark;
CREATE TABLE nmark.p (id INT NOT NULL PRIMARY KEY, v INT NOT NULL);
INSERT INTO nmark.p VALUES (1, 1);`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS nmark; DROP DATABASE IF EXISTS binlogviz;")
	})
	apply := committingFlashback(t, writeUndoScript(t, []string{"E\nUPDATE `nmark`.`p` SET `v` = 0 WHERE `id` <=> 1"}))
	for _, mode := range []string{"MYSQL40", "POSTGRESQL"} {
		t.Run(mode, func(t *testing.T) {
			e2eMySQL(t, "UPDATE nmark.p SET v = 1 WHERE id = 1")
			out, err := e2eMySQLResult(t, "SET SESSION sql_mode='"+mode+"';\n"+apply)
			if err != nil || strings.Contains(out, "undo rolled back") || !strings.Contains(out, "binlogviz: undo committed, 1 statements") {
				t.Fatalf("mode %s: %v\n%s", mode, err, out)
			}
		})
	}
}

func writeUndoScript(t *testing.T, stmts []string) string {
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
		for _, stmt := range stmts {
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

// e2eMySQLFlags runs mysql --force with extra client flags and without --batch,
// so a newline in a result field is not escaped.
func e2eMySQLFlags(t *testing.T, force bool, extra []string, stdin string) (string, error) {
	t.Helper()
	base := strings.Fields(os.Getenv("BINLOGVIZ_MYSQL"))
	if len(base) == 0 {
		base = []string{"sudo", "mysql"}
	}
	args := append([]string{}, base...)
	if force {
		args = append(args, "--force")
	}
	args = append(args, extra...)
	args = append(args, "--default-character-set=utf8mb4")
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
