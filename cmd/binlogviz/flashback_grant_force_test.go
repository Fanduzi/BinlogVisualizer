package binlogviz

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/guard"
)

// TestFlashbackRoundTripMySQL80PreXAGrantForce is the pre-XA --force invariant.
// A statement that fails before XA START, or a CALL that cannot run, must not
// commit the undo. mysql --force continues, and a later success must not clear
// the earlier failure. Each denial is applied with and without --force.
func TestFlashbackRoundTripMySQL80PreXAGrantForce(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to round-trip on a local MySQL server")
	}
	forceEnglishRuntimeOutput(t)
	if _, err := exec.LookPath("sudo"); err != nil && os.Getenv("BINLOGVIZ_MYSQL") == "" {
		t.Fatal("BINLOGVIZ_FLASHBACK_E2E requires sudo mysql or BINLOGVIZ_MYSQL")
	}

	const user = "bv337"
	const pass = "bv337pw"
	e2eMySQL(t, `
DROP DATABASE IF EXISTS n316;
DROP DATABASE IF EXISTS binlogviz;
CREATE DATABASE n316;
CREATE TABLE n316.t (id INT NOT NULL PRIMARY KEY, v INT NOT NULL);
INSERT INTO n316.t VALUES (1, 1);`)
	origSP := mysqlBatchValue(e2eMySQL(t, "SELECT @@GLOBAL.automatic_sp_privileges"))
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, `
SET GLOBAL automatic_sp_privileges = `+origSP+`;
DROP DATABASE IF EXISTS n316;
DROP DATABASE IF EXISTS binlogviz;
DROP USER IF EXISTS 'bv337'@'localhost';
DROP USER IF EXISTS 'bv337'@'%';`)
	})
	e2eMySQL(t, `
DROP USER IF EXISTS 'bv337'@'localhost';
DROP USER IF EXISTS 'bv337'@'%';
CREATE USER 'bv337'@'localhost' IDENTIFIED BY 'bv337pw';
CREATE USER 'bv337'@'%' IDENTIFIED BY 'bv337pw';`)

	script := writePreXAGrantScript(t)
	file := filepath.Join(t.TempDir(), "flashback.sql")
	if err := os.WriteFile(file, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		grants string
		schema bool
		autoSP string
		force  bool
		wantV  string
		errHas string
	}{
		{"full", "CREATE, CREATE ROUTINE, ALTER ROUTINE", false, "1", false, "0", ""},
		{"full_force", "CREATE, CREATE ROUTINE, ALTER ROUTINE", false, "1", true, "0", ""},
		{"missing_alter", "CREATE, CREATE ROUTINE", false, "1", false, "1", "1370"},
		{"missing_alter_force", "CREATE, CREATE ROUTINE", false, "1", true, "1", "1370"},
		{"missing_create", "CREATE ROUTINE, ALTER ROUTINE", false, "1", false, "1", "ERROR"},
		{"missing_create_force", "CREATE ROUTINE, ALTER ROUTINE", false, "1", true, "1", "ERROR"},
		{"missing_create_routine", "CREATE, ALTER ROUTINE", false, "1", false, "1", "ERROR"},
		{"missing_create_routine_force", "CREATE, ALTER ROUTINE", false, "1", true, "1", "ERROR"},
		{"missing_execute", "CREATE, CREATE ROUTINE, ALTER ROUTINE", false, "0", false, "1", "1370"},
		{"missing_execute_force", "CREATE, CREATE ROUTINE, ALTER ROUTINE", false, "0", true, "1", "1370"},
		{"schema_no_rights", "", true, "1", false, "1", "ERROR"},
		{"schema_no_rights_force", "", true, "1", true, "1", "ERROR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e2eMySQL(t, "SET GLOBAL automatic_sp_privileges = "+tc.autoSP)
			resetPreXAGrantTarget(t, tc.schema)
			grantPreXAAccount(t, user, tc.grants)
			code, apply, stderr := runRoot(t, "flashback", "verify", file)
			if code != 0 || !strings.Contains(apply, "SET @binlogviz_want = 1;") {
				t.Fatalf("verify exit %d\n%s\n%s", code, stderr, apply)
			}
			out, err := e2eMySQLAs(t, user, pass, tc.force, apply)
			if tc.wantV == "0" && err != nil {
				t.Fatalf("apply: %v\n%s", err, out)
			}
			if tc.wantV == "1" && !tc.force && err == nil {
				t.Fatalf("client did not stop\n%s", out)
			}
			if tc.errHas != "" && !strings.Contains(out, tc.errHas) {
				t.Fatalf("output missing %s\n%s", tc.errHas, out)
			}
			got := mysqlBatchValue(e2eMySQL(t, "SELECT CONCAT(COUNT(*), ':', IFNULL(MIN(v), '')) FROM n316.t"))
			want := "1:" + tc.wantV
			if got != want {
				t.Fatalf("rows %q want %q\nmysql:\n%s", got, want, out)
			}
		})
	}
}

func writePreXAGrantScript(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	g := guard.New(nil)
	err := g.Write(&b, func(emit func(guard.Piece) error) error {
		for _, stmt := range []string{
			"SET SESSION sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_BACKSLASH_ESCAPES', '')",
			"SET NAMES utf8mb4",
			"SET time_zone = '+00:00'",
			"SET @binlogviz_bad = NULL",
			"SET @binlogviz_txn = 0",
		} {
			if err := emit(guard.Piece{Kind: guard.PieceStmt, Text: stmt}); err != nil {
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
		return emit(guard.Piece{Kind: guard.PieceStmt, Text: "UPDATE `n316`.`t` SET `v` = 0 WHERE `id` = 1"})
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func resetPreXAGrantTarget(t *testing.T, keepSchema bool) {
	t.Helper()
	body := "DROP DATABASE IF EXISTS binlogviz;\nUPDATE n316.t SET v = 1 WHERE id = 1;\n"
	if keepSchema {
		body += "CREATE DATABASE binlogviz;\n"
	}
	e2eMySQL(t, body)
}

func grantPreXAAccount(t *testing.T, user, grants string) {
	t.Helper()
	var b strings.Builder
	for _, host := range []string{"localhost", "%"} {
		acct := "'" + user + "'@'" + host + "'"
		b.WriteString("REVOKE ALL PRIVILEGES, GRANT OPTION FROM " + acct + ";\n")
		b.WriteString("GRANT SELECT, INSERT, UPDATE, DELETE ON n316.* TO " + acct + ";\n")
		if grants != "" {
			b.WriteString("GRANT " + grants + " ON binlogviz.* TO " + acct + ";\n")
		}
	}
	e2eMySQL(t, b.String())
}

// e2eMySQLAs runs stdin as a password account. force is mysql --force.
func e2eMySQLAs(t *testing.T, user, pass string, force bool, stdin string) (string, error) {
	t.Helper()
	args := grantClientArgs(user, pass, force)
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func mysqlBatchValue(out string) string {
	var lines []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "mysql: [Warning]") {
			continue
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func grantClientArgs(user, pass string, force bool) []string {
	base := strings.Fields(os.Getenv("BINLOGVIZ_MYSQL"))
	if len(base) == 0 {
		base = []string{"sudo", "mysql"}
	}
	args := make([]string, 0, len(base)+6)
	for i := 0; i < len(base); i++ {
		a := base[i]
		switch {
		case a == "-u" || a == "--user" || a == "-p" || a == "--password":
			i++
			continue
		case strings.HasPrefix(a, "--user=") || strings.HasPrefix(a, "--password="):
			continue
		case strings.HasPrefix(a, "-u") && len(a) > 2:
			continue
		case strings.HasPrefix(a, "-p") && len(a) > 2 && !strings.HasPrefix(a, "-P"):
			continue
		default:
			args = append(args, a)
		}
	}
	args = append(args, "--user="+user, "--password="+pass)
	if force {
		args = append(args, "--force")
	}
	args = append(args, "--default-character-set=utf8mb4", "-N", "--batch")
	return args
}
