// Package analyzer admits CHECK TABLE and SET ROLE as ADMIN from committed dialect fixtures.
// input: ParseFiles on the MySQL 8.0.46 CHECK TABLE and SET ROLE binlogs, then NormalizeRawEvent and Analyzer.Consume.
// output: analyze success, ADMIN close of each maintenance GTID-started non-explicit group, one following business transaction with one row, no DDL leak, no maintenance report transaction.
// pos: ADR-0003 admission seam for CHECK TABLE and SET ROLE; synthetic RawEvent lists cannot admit these verbs.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/binlog"
)

func TestCheckTableDialectFixtureAdmitsAdminAndNextBusinessGTID(t *testing.T) {
	assertMaintenanceAdminDialect(t, "mysql-8.0.46-check-table.binlog", "CHECK TABLE testdb.users", "CHECK TABLE")
}

func TestSetRoleDialectFixtureAdmitsAdminAndNextBusinessGTID(t *testing.T) {
	assertMaintenanceAdminDialect(t, "mysql-8.0.46-set-role.binlog", "SET ROLE ALL", "SET ROLE")
}

func assertMaintenanceAdminDialect(t *testing.T, file, query, ddlNeedle string) {
	t.Helper()
	path := filepath.Join("..", "binlog", "testdata", file)
	p := binlog.NewParser()
	a := New(DefaultOptions())

	var (
		currentGTID string
		adminGTID   string
		sawSQL      bool
		kind        string
		flavor      string
		version     string
	)
	if err := p.ParseFiles([]string{path}, func(raw binlog.RawEvent) error {
		if raw.ServerFlavor != "" {
			flavor = raw.ServerFlavor
		}
		if raw.ServerVersion != "" {
			version = raw.ServerVersion
		}
		if raw.EventType == "GTID" && raw.GTID != "" {
			currentGTID = raw.GTID
		}
		if raw.EventType == "QUERY" && strings.EqualFold(strings.TrimSpace(raw.Query), query) {
			sawSQL = true
			adminGTID = currentGTID
		}
		ev, err := binlog.NormalizeRawEvent(raw)
		if err != nil {
			return err
		}
		if ev == nil {
			return nil
		}
		if ev.QuerySQL == query && (ev.EventType == "ADMIN" || ev.EventType == "UNCLASSIFIED_QUERY" || ev.EventType == "DDL") {
			kind = ev.EventType
		}
		return a.Consume(*ev)
	}); err != nil {
		t.Fatal(err)
	}
	if !sawSQL {
		t.Fatalf("fixture must contain QUERY %s", query)
	}
	if adminGTID == "" {
		t.Fatalf("%s must be the only work of a GTID-started group", query)
	}
	if flavor != "mysql" || !strings.HasPrefix(version, "8.0.46") {
		t.Fatalf("fixture must name MySQL 8.0.46, got flavor=%q version=%q", flavor, version)
	}
	if kind != "ADMIN" {
		t.Fatalf("%s must normalize as ADMIN, got %q", query, kind)
	}

	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("analyze after dialect %s: %v", query, err)
	}
	if len(result.Transactions) != 1 || result.Summary.TotalTransactions != 1 {
		t.Fatalf("report transactions = %+v, want exactly one business transaction", result.Transactions)
	}
	txn := result.Transactions[0]
	if txn.GTID == "" || txn.GTID == adminGTID {
		t.Fatalf("business GTID = %q, want the GTID after %s group %q", txn.GTID, query, adminGTID)
	}
	if txn.TotalRows != 1 || result.Summary.TotalRows != 1 {
		t.Fatalf("business rows = %d summary=%d, want 1", txn.TotalRows, result.Summary.TotalRows)
	}
	needle := strings.ToUpper(ddlNeedle)
	for _, ddl := range result.Diagnostics.DDLEvents {
		blob := strings.ToUpper(ddl.Statement + " " + ddl.Operation)
		if strings.Contains(blob, needle) {
			t.Fatalf("%s leaked onto the DDL timeline: %+v", query, ddl)
		}
	}
	for _, table := range result.Tables {
		if table.DDLCount != 0 {
			t.Fatalf("%s incremented table DDLCount: %+v", query, table)
		}
	}
}
