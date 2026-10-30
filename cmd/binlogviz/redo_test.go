package binlogviz

import (
	"os"
	"strings"
	"testing"
)

// redoUsageError runs the command like main and returns the typed error.
// Args/flag validation fails before any input is read, so no binlog or
// server is needed (ADR 0022's input rules mirror analyze's).
func redoUsageError(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	cmd := NewRootCommand()
	cmd.SetArgs(append([]string{"redo"}, args...))
	return captureStdoutStderrRun(t, func() error {
		err := cmd.Execute()
		if err != nil {
			PrintCommandError(os.Stderr, err)
		}
		return err
	})
}

func TestRedoRequiresInput(t *testing.T) {
	_, _, err := redoUsageError(t)
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("redo without input = %v, want usage error", err)
	}
}

func TestRedoSchemaFileDBRequiresFile(t *testing.T) {
	_, _, err := redoUsageError(t, "internal/binlog/testdata/mysql-8.0.46-dml-full.binlog", "--schema-file-db", "shop")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("--schema-file-db alone = %v, want usage error", err)
	}
}

func TestRedoRejectsMixedInputModes(t *testing.T) {
	// Positional files cannot combine with discovery (ADR 0022 reuses
	// analyze's input modes verbatim).
	_, _, err := redoUsageError(t, "mysql-bin.000001", "--from-dir", "/tmp", "--prefix", "mysql-bin.")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("positional + discovery = %v, want usage error", err)
	}
}

func TestRedoRejectsRemoteWithPositional(t *testing.T) {
	_, _, err := redoUsageError(t, "mysql-bin.000001", "--host", "db.internal", "--user", "replica")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("remote + positional = %v, want usage error", err)
	}
}

func TestRedoRejectsStartPosWithoutStartFile(t *testing.T) {
	_, _, err := redoUsageError(t, "--host", "db.internal", "--user", "replica", "--start-pos", "100")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("--start-pos alone = %v, want usage error", err)
	}
}

func TestRedoRejectsStartGTIDWithStartFile(t *testing.T) {
	_, _, err := redoUsageError(t, "--host", "db.internal", "--user", "replica",
		"--start-file", "binlog.000001", "--start-gtid", "uuid:1-5")
	if err == nil || ExitCode(err) != 3 {
		t.Fatalf("--start-gtid + --start-file = %v, want usage error", err)
	}
}

func TestRedoEmitsForwardSQL(t *testing.T) {
	// The full command path on the real FULL-metadata fixture: session
	// preamble, mirrored group boundaries, after-image statements (ADR
	// 0022's golden output).
	stdout, stderr, err := executeRedoLikeMain(t, mustFixturePath(t, "mysql-8.0.46-dml-full.binlog"))
	if err != nil {
		t.Fatalf("redo: %v\n%s", err, stderr)
	}
	for _, want := range []string{
		"SET NAMES utf8mb4;",
		"INSERT INTO `shop`.`orders`",
		"UPDATE `shop`.`orders` SET",
		"DELETE FROM `shop`.`orders`",
		"BEGIN;",
		"COMMIT;",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("script missing %q:\n%s", want, stdout)
		}
	}
	// A DDL group carries no fabricated transaction.
	if strings.Contains(stdout, "BEGIN;\nUSE `shop`;\nDROP DATABASE") {
		t.Fatalf("DDL wrapped in a fabricated transaction:\n%s", stdout)
	}
}

func TestRedoMinimalRefusesNamesWithoutSchema(t *testing.T) {
	// MINIMAL metadata: names cannot be guessed, so without --schema-file
	// the command refuses before a byte of SQL (exit 6, reason names).
	stdout, stderr, err := executeRedoLikeMain(t, mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog"))
	if err == nil {
		t.Fatalf("MINIMAL without schema emitted:\n%s", stdout)
	}
	if ExitCode(err) != 6 {
		t.Fatalf("exit %d, want 6; err=%v\nstderr=%s", ExitCode(err), err, stderr)
	}
	if stdout != "" {
		t.Fatalf("refusal printed SQL:\n%s", stdout)
	}
}
