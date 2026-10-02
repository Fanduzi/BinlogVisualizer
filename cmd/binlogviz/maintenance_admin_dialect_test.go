// Package binlogviz verifies the CHECK TABLE and SET ROLE dialect fixtures at the analyze command I/O seam.
// input: runAnalysis on the MySQL 8.0.46 CHECK TABLE and SET ROLE binlogs through the real parser.
// output: exit 0, one business transaction with one row on the following GTID, maintenance statement absent from the DDL timeline.
// pos: operator I/O check for ADR-0003 admission of CHECK TABLE and SET ROLE; ParseFiles is exercised by runAnalysis.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"strings"
	"testing"

	"binlogviz/internal/analyzer"
)

func TestAnalyzeCheckTableDialectFixtureExitsZero(t *testing.T) {
	assertAnalyzeMaintenanceAdminDialect(t, "mysql-8.0.46-check-table.binlog", "CHECK TABLE")
}

func TestAnalyzeSetRoleDialectFixtureExitsZero(t *testing.T) {
	assertAnalyzeMaintenanceAdminDialect(t, "mysql-8.0.46-set-role.binlog", "SET ROLE")
}

func assertAnalyzeMaintenanceAdminDialect(t *testing.T, file, ddlNeedle string) {
	t.Helper()
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, file)
	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return runAnalysis([]string{fixture}, analyzer.DefaultOptions(), "json")
	})
	if err != nil {
		t.Fatalf("dialect %s then business must exit 0, got %v stderr=%q", file, err, stderr)
	}
	if strings.Contains(stderr, "Error:") {
		t.Fatalf("successful analyze must not print Error:, got %q", stderr)
	}
	var decoded struct {
		Summary struct {
			TotalTransactions int `json:"total_transactions"`
			TotalRows         int `json:"total_rows"`
		} `json:"summary"`
		Transactions []struct {
			GTID      string `json:"gtid"`
			TotalRows int    `json:"total_rows"`
		} `json:"transactions"`
		Diagnostics struct {
			DDLEvents []struct {
				Statement string `json:"statement"`
				Operation string `json:"operation"`
			} `json:"ddl_events"`
		} `json:"diagnostics"`
	}
	if jsonErr := json.Unmarshal([]byte(stdout), &decoded); jsonErr != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", jsonErr, stdout)
	}
	if decoded.Summary.TotalTransactions != 1 || decoded.Summary.TotalRows != 1 {
		t.Fatalf("want one business transaction with 1 row, got txns=%d rows=%d", decoded.Summary.TotalTransactions, decoded.Summary.TotalRows)
	}
	if len(decoded.Transactions) != 1 || decoded.Transactions[0].GTID == "" || decoded.Transactions[0].TotalRows != 1 {
		t.Fatalf("business transaction = %+v, want one GTID with 1 row", decoded.Transactions)
	}
	needle := strings.ToUpper(ddlNeedle)
	for _, ddl := range decoded.Diagnostics.DDLEvents {
		blob := strings.ToUpper(ddl.Statement + " " + ddl.Operation)
		if strings.Contains(blob, needle) {
			t.Fatalf("%s leaked onto the DDL timeline: %+v", ddlNeedle, ddl)
		}
	}
}
