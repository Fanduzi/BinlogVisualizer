package binlogviz

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func assertTargetGuardFailureModes(t *testing.T, sql, matchSum, matchAfter string) {
	t.Helper()
	assertGuardForceKeepsRows(t, "FORCE", sql, matchSum, matchAfter, func(out string) {
		msg := "binlogviz: schema file does not match the target: p183.t.c, p183.heap.note"
		errLine := mysqlErrorLine(out, "1231")
		msgAt := strings.Index(out, msg)
		errAt := strings.Index(out, errLine)
		if msgAt < 0 || errLine == "" || errAt < 0 || msgAt > errAt {
			t.Fatalf("guard message was not printed before the error\n%s", out)
		}
		if strings.Contains(errLine, ",") || strings.Contains(errLine, "(2 columns)") || !strings.Contains(errLine, "p183.t.c") || !strings.Contains(errLine, "p183.heap.note") || !strings.Contains(errLine, " | ") {
			t.Fatalf("error line: %s", errLine)
		}
		if mysqlErrorLine(out, "1792") == "" {
			t.Fatalf("missing read-only refusal\n%s", out)
		}
	})
	assertGuardForceKeepsRows(t, "AUTOCOMMIT0", "SET SESSION autocommit=0;\n"+sql+"COMMIT;\n", matchSum, matchAfter, func(out string) {
		errLine := mysqlErrorLine(out, "1231")
		if errLine == "" || !strings.Contains(errLine, "p183.t.c") || !strings.Contains(errLine, "p183.heap.note") || mysqlErrorLine(out, "1792") == "" {
			t.Fatalf("autocommit=0 guard:\n%s", out)
		}
	})
	// This server evaluates the rendered version check for other servers' version strings.
	assertGuardVersionChoice(t, sql)
	// A block cut off from the header stays read-only.
	if cut := strings.Index(sql, "-- gtid:"); cut > 0 {
		// The fragment has no procedure. The gate sets the session read-only,
		// and a following write is ERROR 1792.
		probe := "INSERT INTO p183.t (id, x) VALUES (999, 'x');\n"
		assertGuardForceKeepsRows(t, "HEADERLESS", sql[cut:]+probe, matchSum, matchAfter, func(out string) {
			if mysqlErrorLine(out, "1792") == "" {
				t.Fatalf("header-less block was not read-only\n%s", out)
			}
		})
		// A stale unlock and another script's token do not unlock this block.
		stale := "SET @binlogviz_unlock_sql = 'SET SESSION TRANSACTION READ WRITE';\nSET @binlogviz_ok = '0123456789abcdef';\n"
		assertGuardForceKeepsRows(t, "STALE_TOKEN", stale+sql[cut:]+probe, matchSum, matchAfter, func(out string) {
			if mysqlErrorLine(out, "1792") == "" {
				t.Fatalf("stale token unlocked a block\n%s", out)
			}
		})
		// A block's statements in a reconnected writable session change no row.
		var dml strings.Builder
		for _, stmt := range undoStatements(sql[cut:]) {
			if strings.HasPrefix(stmt, "INSERT ") || strings.HasPrefix(stmt, "UPDATE ") || strings.HasPrefix(stmt, "DELETE ") {
				dml.WriteString(stmt + ";\n")
			}
		}
		if dml.Len() == 0 {
			t.Fatalf("no undo statements in:\n%s", sql)
		}
		assertGuardForceKeepsRows(t, "RECONNECT_DML", dml.String(), matchSum, matchAfter, func(out string) {
			if mysqlErrorLine(out, "1242") == "" {
				t.Fatalf("token mismatch was not an error\n%s", out)
			}
		})
	}
	// A server that refuses new prepared statements still cannot write.
	prevPrep := strings.TrimSpace(e2eMySQL(t, "SELECT @@GLOBAL.max_prepared_stmt_count"))
	e2eMySQL(t, "SET GLOBAL max_prepared_stmt_count = 0")
	restorePrep := func() { e2eMySQL(t, "SET GLOBAL max_prepared_stmt_count = "+prevPrep) }
	t.Cleanup(func() { _, _ = e2eMySQLResult(t, "SET GLOBAL max_prepared_stmt_count = "+prevPrep) })
	assertGuardForceKeepsRows(t, "NOPREPARE", sql, matchSum, matchAfter, func(out string) {
		if mysqlErrorLine(out, "1461") == "" {
			t.Fatalf("max_prepared_stmt_count=0 guard:\n%s", out)
		}
	})
	restorePrep()
}

func assertWideTargetGuard(t *testing.T, asciiPath string) {
	t.Helper()
	const wideSum = "CHECKSUM TABLE p186.guardwide"
	wideAfter := e2eMySQL(t, wideSum)
	var wideSchema strings.Builder
	wideSchema.WriteString("USE `p186`;\nCREATE TABLE `guardwide` (\n  `id` int NOT NULL,\n")
	for i := 1; i <= 12; i++ {
		fmt.Fprintf(&wideSchema, "  `col_%02d` varchar(40) GENERATED ALWAYS AS (md5(`id`)) STORED,\n", i)
	}
	wideSchema.WriteString("  PRIMARY KEY (`id`)\n);\n")
	wideFile := filepath.Join(t.TempDir(), "p186.sql")
	if err := os.WriteFile(wideFile, []byte(wideSchema.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	wideSQL, stderr, err := executeFlashbackLikeMain(t, asciiPath, "--schema-file", wideFile, "--include-table", "p186.guardwide")
	if err != nil {
		t.Fatalf("wide guard: %v\n%s", err, stderr)
	}
	assertGuardForceKeepsRows(t, "WIDE_FORCE", wideSQL, wideSum, wideAfter, func(out string) {
		errLine := mysqlErrorLine(out, "1231")
		if errLine == "" || !strings.Contains(errLine, "(12 columns)") || !strings.Contains(errLine, "p186.guardwide.col_01") || !strings.Contains(errLine, "p186.guardwide.col_05") || strings.Contains(errLine, "p186.guardwide.col_06") || strings.Contains(errLine, ",") {
			t.Fatalf("wide error line: %s\n%s", errLine, out)
		}
		if !strings.Contains(out, "p186.guardwide.col_12") || mysqlErrorLine(out, "1792") == "" {
			t.Fatalf("wide guard output:\n%s", out)
		}
	})
	assertGuardForceKeepsRows(t, "WIDE_AUTOCOMMIT0", "SET SESSION autocommit=0;\n"+wideSQL+"COMMIT;\n", wideSum, wideAfter, nil)
}
