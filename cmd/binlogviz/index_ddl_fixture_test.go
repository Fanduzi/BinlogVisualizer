// Package binlogviz verifies index DDL from a MySQL 8.0.46 ROW+GTID binlog.
// input: runAnalysis on internal/binlog/testdata/mysql-8.0.46-index-ddl.binlog through the real parser.
// output: CREATE/DROP INDEX, including UNIQUE and FULLTEXT, land on the table after ON; a selected session database does not replace a qualified name; --include-table keeps those statements.
// pos: operator I/O check that index DDL is not reported as a fake table named after the index.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"strings"
	"testing"

	"binlogviz/internal/analyzer"
)

func TestAnalyzeIndexDDLFixtureAttributesIndexesToTheTable(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "mysql-8.0.46-index-ddl.binlog")
	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return runAnalysis([]string{fixture}, analyzer.DefaultOptions(), "json")
	})
	if err != nil {
		t.Fatalf("index DDL fixture must analyze, got %v stderr=%q", err, stderr)
	}
	decoded := decodeIndexDDLReport(t, stdout)
	if decoded.Summary.TotalRows != 2 {
		t.Fatalf("rows = %d, want 2", decoded.Summary.TotalRows)
	}
	assertIndexDDLIdentity(t, decoded)

	opts := analyzer.DefaultOptions()
	opts.IncludeTables = []string{"idxbug.orders"}
	filteredOut, filteredErr, err := captureStdoutStderrRun(t, func() error {
		return runAnalysis([]string{fixture}, opts, "json")
	})
	if err != nil {
		t.Fatalf("include-table idxbug.orders: %v stderr=%q", err, filteredErr)
	}
	filtered := decodeIndexDDLReport(t, filteredOut)
	if filtered.Summary.TotalRows != 2 || len(filtered.Tables) != 1 || filtered.Tables[0].Schema != "idxbug" || filtered.Tables[0].Table != "orders" {
		t.Fatalf("filtered tables = %+v rows=%d, want only idxbug.orders with 2 rows", filtered.Tables, filtered.Summary.TotalRows)
	}
	wantOps := map[string]int{
		"CREATE TABLE":          1,
		"CREATE INDEX":          1,
		"CREATE UNIQUE INDEX":   1,
		"CREATE FULLTEXT INDEX": 1,
		"DROP INDEX":            1,
	}
	gotOps := map[string]int{}
	for _, ddl := range filtered.Diagnostics.DDLEvents {
		if ddl.Schema != "idxbug" || ddl.Table != "orders" {
			t.Fatalf("filtered DDL is not idxbug.orders: %+v", ddl)
		}
		gotOps[ddl.Operation]++
	}
	if len(gotOps) != len(wantOps) {
		t.Fatalf("filtered DDL ops = %v, want %v", gotOps, wantOps)
	}
	for op, n := range wantOps {
		if gotOps[op] != n {
			t.Fatalf("filtered %s count = %d, want %d (%v)", op, gotOps[op], n, gotOps)
		}
	}
}

func assertIndexDDLIdentity(t *testing.T, decoded indexDDLReport) {
	t.Helper()
	tables := map[string]int{}
	for _, table := range decoded.Tables {
		tables[table.Schema+"."+table.Table] = table.TotalRows
	}
	if tables["idxbug.orders"] != 2 || tables["idxbug.widgets"] != 0 || len(tables) != 2 {
		t.Fatalf("tables = %v, want idxbug.orders=2 and idxbug.widgets=0 only", tables)
	}
	for name := range tables {
		if strings.Contains(name, "idx_customer") || strings.Contains(name, "idx_w") || strings.HasPrefix(name, "sessiondb.") {
			t.Fatalf("index name or session schema used as a table: %v", tables)
		}
	}
	type key struct{ op, schema, table string }
	got := map[key]int{}
	for _, ddl := range decoded.Diagnostics.DDLEvents {
		got[key{ddl.Operation, ddl.Schema, ddl.Table}]++
	}
	want := []key{
		{"CREATE INDEX", "idxbug", "orders"},
		{"CREATE UNIQUE INDEX", "idxbug", "orders"},
		{"CREATE FULLTEXT INDEX", "idxbug", "orders"},
		{"DROP INDEX", "idxbug", "orders"},
		{"CREATE INDEX", "idxbug", "widgets"},
		{"CREATE TABLE", "idxbug", "widgets"},
	}
	for _, item := range want {
		if got[item] != 1 {
			t.Fatalf("DDL %v count = %d, events = %+v", item, got[item], decoded.Diagnostics.DDLEvents)
		}
	}
}

type indexDDLReport struct {
	Summary struct {
		TotalRows int `json:"total_rows"`
	} `json:"summary"`
	Tables []struct {
		Schema    string `json:"schema"`
		Table     string `json:"table"`
		TotalRows int    `json:"total_rows"`
	} `json:"tables"`
	Diagnostics struct {
		DDLEvents []struct {
			Schema    string `json:"schema"`
			Table     string `json:"table"`
			Operation string `json:"operation"`
			Statement string `json:"statement"`
		} `json:"ddl_events"`
	} `json:"diagnostics"`
}

func decodeIndexDDLReport(t *testing.T, stdout string) indexDDLReport {
	t.Helper()
	var decoded indexDDLReport
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", err, stdout)
	}
	return decoded
}
