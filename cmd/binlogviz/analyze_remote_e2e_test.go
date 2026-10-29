// Package binlogviz runs the remote analyze golden path against a live MySQL
// server, gated on BINLOGVIZ_FLASHBACK_E2E like the other server tests.
// input: a local MySQL 8.0 with a throwaway replication account.
// output: the full flags-to-report path verified, plus a --start-file bound and an auth-failure exit.
// pos: live-server e2e for analyze_remote.go; unit tests own the rest.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"binlogviz/internal/clierr"
)

// TestAnalyzeRemoteMySQL80 exercises the remote input end to end: a
// replication account dumps the live server's binlog history through
// --host/--user plus BINLOGVIZ_MYSQL_PASSWORD, bounded at the manifest end.
func TestAnalyzeRemoteMySQL80(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to run against a local MySQL server")
	}
	forceEnglishRuntimeOutput(t)
	if _, err := exec.LookPath("sudo"); err != nil && os.Getenv("BINLOGVIZ_MYSQL") == "" {
		t.Fatal("BINLOGVIZ_FLASHBACK_E2E requires sudo mysql or BINLOGVIZ_MYSQL")
	}
	vars := e2eMySQL(t, "SELECT @@log_bin, @@binlog_format, @@version")
	fields := strings.Fields(vars)
	if len(fields) < 3 || fields[0] != "1" || fields[1] != "ROW" || !strings.HasPrefix(fields[2], "8.0") {
		t.Fatalf("server is not MySQL 8.0 with ROW binlogging: %s", vars)
	}

	e2eMySQL(t, `
DROP DATABASE IF EXISTS remsrc;
CREATE DATABASE remsrc;
CREATE TABLE remsrc.t (id INT PRIMARY KEY, v INT NOT NULL);
INSERT INTO remsrc.t VALUES (1, 10), (2, 20), (3, 30);`)
	e2eMySQL(t, `
DROP USER IF EXISTS 'binlogviz_remote'@'127.0.0.1';
CREATE USER 'binlogviz_remote'@'127.0.0.1' IDENTIFIED WITH mysql_native_password BY 'remsrc-pw-123';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'binlogviz_remote'@'127.0.0.1';
FLUSH PRIVILEGES;`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP USER IF EXISTS 'binlogviz_remote'@'127.0.0.1'; DROP DATABASE IF EXISTS remsrc;")
	})
	t.Setenv("BINLOGVIZ_MYSQL_PASSWORD", "remsrc-pw-123")

	out, stderr, err := executeAnalyzeLikeMain(t,
		"--host", "127.0.0.1", "--user", "binlogviz_remote",
		"--include-table", "remsrc.t", "--format", "json")
	if err != nil {
		t.Fatalf("remote analyze failed: %v\nstderr=%s", err, stderr)
	}
	var report struct {
		Summary struct {
			TotalRows   int `json:"total_rows"`
			TotalEvents int `json:"total_events"`
		} `json:"summary"`
	}
	if err := json.Unmarshal([]byte(out), &report); err != nil {
		t.Fatalf("json report did not parse: %v\n%.400s", err, out)
	}
	if report.Summary.TotalRows < 3 || report.Summary.TotalEvents == 0 {
		t.Fatalf("remote analyze saw no workload: rows=%d events=%d", report.Summary.TotalRows, report.Summary.TotalEvents)
	}

	// --start-file bounds the dump: the last manifest row is the server's
	// newest binlog. Parsed locally — binlogNames is a flashback helper
	// that requires two logs, and CI keeps exactly one.
	logs := e2eMySQL(t, "SHOW BINARY LOGS")
	last := ""
	for _, line := range strings.Split(logs, "\n") {
		if fields := strings.Fields(line); len(fields) > 0 {
			last = fields[0]
		}
	}
	if last == "" {
		t.Fatalf("SHOW BINARY LOGS returned no rows:\n%s", logs)
	}
	if _, stderr, err = executeAnalyzeLikeMain(t,
		"--host", "127.0.0.1", "--user", "binlogviz_remote",
		"--start-file", last, "--format", "json"); err != nil {
		t.Fatalf("--start-file %s failed: %v\nstderr=%s", last, err, stderr)
	}
	// A purged name fails input, not a server error the operator decodes.
	_, _, err = executeAnalyzeLikeMain(t,
		"--host", "127.0.0.1", "--user", "binlogviz_remote", "--start-file", "binlog.999999")
	if err == nil {
		t.Fatal("a start-file absent from the manifest must fail")
	}
	var cerr *clierr.Error
	if !errors.As(err, &cerr) || cerr.Kind != clierr.KindInput || cerr.Details["reason"] != "start_file" {
		t.Fatalf("missing start-file = %v, want input/start_file", err)
	}

	// A wrong password is an input failure, never a report.
	_, stderr, err = executeAnalyzeLikeMain(t,
		"--host", "127.0.0.1", "--user", "binlogviz_remote", "--password", "wrong")
	if err == nil {
		t.Fatal("a wrong password must fail")
	}
	if !errors.As(err, &cerr) || cerr.Kind != clierr.KindInput {
		t.Fatalf("auth failure = %v, want input; stderr=%s", err, stderr)
	}
}
