// Package binlogviz runs --thread-id against a live MySQL server, gated on
// BINLOGVIZ_FLASHBACK_E2E like the other server tests (ADR 0027).
// input: a local MySQL 8.0 with FULL row image, two client sessions whose
// CONNECTION_ID()s are captured in the same batch as their writes.
// output: analyze keeps only the requested connection's transactions and
// flashback emits undo only for that connection's rows.
// pos: live-server e2e for the --thread-id flag; unit and fixture tests own
// the rest.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// e2eLastConnID returns the connection id a single-client batch printed last:
// each e2eMySQL call is one server session, so the id is the session's own.
func e2eLastConnID(t *testing.T, out string) string {
	t.Helper()
	var last string
	for _, line := range strings.Split(out, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			last = line
		}
	}
	if _, err := strconv.ParseUint(last, 10, 32); err != nil {
		t.Fatalf("no connection id in batch output %q: %v", out, err)
	}
	return last
}

// TestAnalyzeThreadIDMySQL80 writes from two different server sessions and
// selects one by --thread-id: the connection scope of the selector is the
// contract a live server proves (ADR 0027).
func TestAnalyzeThreadIDMySQL80(t *testing.T) {
	if os.Getenv("BINLOGVIZ_FLASHBACK_E2E") == "" {
		t.Skip("set BINLOGVIZ_FLASHBACK_E2E=1 to run against a local MySQL server")
	}
	forceEnglishRuntimeOutput(t)
	if _, err := exec.LookPath("sudo"); err != nil && os.Getenv("BINLOGVIZ_MYSQL") == "" {
		t.Fatal("BINLOGVIZ_FLASHBACK_E2E requires sudo mysql or BINLOGVIZ_MYSQL")
	}
	ver := mysqlVersion(e2eMySQL(t, "SELECT @@version"))
	if !strings.HasPrefix(ver, "8.0") {
		t.Fatalf("MySQL 8.0 required for binlog_row_metadata=FULL, got %s", ver)
	}
	e2eMySQL(t, `
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';
SET GLOBAL binlog_row_metadata = 'FULL';`)
	e2eMySQL(t, `
DROP DATABASE IF EXISTS threadidt;
CREATE DATABASE threadidt;
CREATE TABLE threadidt.t (id INT NOT NULL PRIMARY KEY, note VARCHAR(32) NULL);
FLUSH LOGS`)
	t.Cleanup(func() {
		_, _ = e2eMySQLResult(t, "DROP DATABASE IF EXISTS threadidt;")
	})

	// Each call is one client session; the DML and its CONNECTION_ID() share
	// it, so the printed id attributes these rows to this session.
	connA := e2eLastConnID(t, e2eMySQL(t, `
INSERT INTO threadidt.t VALUES (1, 'a'), (2, 'b');
SELECT CONNECTION_ID();`))
	connB := e2eLastConnID(t, e2eMySQL(t, `
INSERT INTO threadidt.t VALUES (9, 'z');
SELECT CONNECTION_ID();`))
	if connA == connB {
		t.Fatalf("both sessions landed on connection id %s", connA)
	}
	path := e2eIncidentBinlog(t)

	var report struct {
		Summary struct {
			TotalRows int `json:"total_rows"`
		} `json:"summary"`
		Transactions []struct {
			ThreadID uint32 `json:"thread_id"`
		} `json:"transactions"`
		Selection struct {
			ThreadIDs []uint32 `json:"thread_ids"`
		} `json:"selection"`
	}
	runAnalyze := func(threadID string) {
		report = struct {
			Summary struct {
				TotalRows int `json:"total_rows"`
			} `json:"summary"`
			Transactions []struct {
				ThreadID uint32 `json:"thread_id"`
			} `json:"transactions"`
			Selection struct {
				ThreadIDs []uint32 `json:"thread_ids"`
			} `json:"selection"`
		}{}
		stdout, stderr, err := executeAnalyzeLikeMain(t, path,
			"--include-schema", "threadidt", "--thread-id", threadID, "--format", "json")
		if err != nil {
			t.Fatalf("analyze --thread-id %s: %v\n%s", threadID, err, stderr)
		}
		if err := json.Unmarshal([]byte(stdout), &report); err != nil {
			t.Fatalf("decode report: %v\n%s", err, stdout)
		}
	}

	runAnalyze(connA)
	wantID, _ := strconv.ParseUint(connA, 10, 32)
	if report.Summary.TotalRows != 2 || len(report.Transactions) != 1 || report.Transactions[0].ThreadID != uint32(wantID) {
		t.Fatalf("conn A report = %+v, want 2 rows in one transaction of thread %d", report, wantID)
	}
	if len(report.Selection.ThreadIDs) != 1 || report.Selection.ThreadIDs[0] != uint32(wantID) {
		t.Fatalf("selection.thread_ids = %v, want [%d]", report.Selection.ThreadIDs, wantID)
	}

	runAnalyze(connB)
	wantID, _ = strconv.ParseUint(connB, 10, 32)
	if report.Summary.TotalRows != 1 || len(report.Transactions) != 1 || report.Transactions[0].ThreadID != uint32(wantID) {
		t.Fatalf("conn B report = %+v, want 1 row in one transaction of thread %d", report, wantID)
	}

	// Flashback honors the same selection: session B's insert undoes to a
	// single DELETE on id 9; session A's rows never reach the script.
	stdout, stderr, err := executeFlashbackLikeMain(t, path,
		"--include-schema", "threadidt", "--thread-id", connB)
	if err != nil {
		t.Fatalf("flashback --thread-id %s: %v\n%s", connB, err, stderr)
	}
	var dml []string
	for _, stmt := range undoStatements(stdout) {
		if strings.HasPrefix(stmt, "INSERT") || strings.HasPrefix(stmt, "UPDATE") || strings.HasPrefix(stmt, "DELETE") {
			dml = append(dml, stmt)
		}
	}
	joined := strings.Join(dml, "\n")
	if !strings.Contains(joined, "`id` <=> 9") {
		t.Fatalf("missing undo for session B's row:\n%s", stdout)
	}
	if strings.Contains(joined, "`id` <=> 1") || strings.Contains(joined, "`id` <=> 2") {
		t.Fatalf("session A rows staged under --thread-id %s:\n%s", connB, joined)
	}
}
