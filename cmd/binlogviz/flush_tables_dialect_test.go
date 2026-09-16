// Package binlogviz verifies the FLUSH TABLES dialect fixture at the analyze command I/O seam.
// input: runAnalysis on internal/binlog/testdata/mysql-8.0.46-flush-tables.binlog through the real parser.
// output: exit 0, one business transaction on the following GTID, empty DDL timeline for FLUSH TABLES.
// pos: operator I/O check for ADR-0003 admission of FLUSH TABLES; ParseFiles is exercised by runAnalysis.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"strings"
	"testing"

	"binlogviz/internal/analyzer"
)

func TestAnalyzeFlushTablesDialectFixtureExitsZero(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "mysql-8.0.46-flush-tables.binlog")
	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return runAnalysis([]string{fixture}, analyzer.DefaultOptions(), "json")
	})
	if err != nil {
		t.Fatalf("dialect FLUSH TABLES then business must exit 0, got %v stderr=%q", err, stderr)
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
			GTID string `json:"gtid"`
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
	if len(decoded.Transactions) != 1 || decoded.Transactions[0].GTID == "" {
		t.Fatalf("business GTID missing: %+v", decoded.Transactions)
	}
	for _, ddl := range decoded.Diagnostics.DDLEvents {
		blob := strings.ToUpper(ddl.Statement + " " + ddl.Operation)
		if strings.Contains(blob, "FLUSH TABLES") {
			t.Fatalf("FLUSH TABLES leaked onto the DDL timeline: %+v", ddl)
		}
	}
}
