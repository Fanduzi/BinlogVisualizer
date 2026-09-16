// Package analyzer admits FLUSH TABLES as ADMIN from a committed dialect fixture.
// input: ParseFiles on testdata/mysql-8.0.46-flush-tables.binlog, then NormalizeRawEvent and Analyzer.Consume.
// output: analyze success, ADMIN close of the FLUSH TABLES GTID group, one following business transaction, no DDL leak, no maintenance report transaction.
// pos: ADR-0003 admission seam for a new ADMIN verb; synthetic RawEvent lists cannot admit this verb.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/binlog"
)

func TestFlushTablesDialectFixtureAdmitsAdminAndNextBusinessGTID(t *testing.T) {
	path := filepath.Join("..", "binlog", "testdata", "mysql-8.0.46-flush-tables.binlog")
	p := binlog.NewParser()
	a := New(DefaultOptions())

	var (
		currentGTID string
		flushGTID   string
		sawFlushSQL bool
		flushKind   string
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
		if raw.EventType == "QUERY" && strings.EqualFold(strings.TrimSpace(raw.Query), "FLUSH TABLES") {
			sawFlushSQL = true
			flushGTID = currentGTID
		}
		ev, err := binlog.NormalizeRawEvent(raw)
		if err != nil {
			return err
		}
		if ev == nil {
			return nil
		}
		if ev.EventType == "ADMIN" && ev.QuerySQL == "FLUSH TABLES" {
			flushKind = ev.EventType
		}
		if ev.EventType == "UNCLASSIFIED_QUERY" && ev.QuerySQL == "FLUSH TABLES" {
			flushKind = ev.EventType
		}
		if ev.EventType == "DDL" && ev.QuerySQL == "FLUSH TABLES" {
			flushKind = ev.EventType
		}
		return a.Consume(*ev)
	}); err != nil {
		t.Fatal(err)
	}
	if !sawFlushSQL {
		t.Fatal("fixture must contain a QUERY FLUSH TABLES")
	}
	if flushGTID == "" {
		t.Fatal("FLUSH TABLES must be the only work of a GTID-started group")
	}
	if flavor != "mysql" || !strings.HasPrefix(version, "8.0.46") {
		t.Fatalf("fixture must name MySQL 8.0.46, got flavor=%q version=%q", flavor, version)
	}
	if flushKind != "ADMIN" {
		t.Fatalf("FLUSH TABLES must normalize as ADMIN, got %q", flushKind)
	}

	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("analyze after dialect FLUSH TABLES: %v", err)
	}
	if len(result.Transactions) != 1 || result.Summary.TotalTransactions != 1 {
		t.Fatalf("report transactions = %+v, want exactly one business transaction", result.Transactions)
	}
	txn := result.Transactions[0]
	if txn.GTID == "" || txn.GTID == flushGTID {
		t.Fatalf("business GTID = %q, want the GTID after FLUSH TABLES group %q", txn.GTID, flushGTID)
	}
	if txn.TotalRows != 1 || result.Summary.TotalRows != 1 {
		t.Fatalf("business rows = %d summary=%d, want 1", txn.TotalRows, result.Summary.TotalRows)
	}
	for _, ddl := range result.Diagnostics.DDLEvents {
		if strings.Contains(strings.ToUpper(ddl.Statement), "FLUSH TABLES") || strings.Contains(strings.ToUpper(ddl.Operation), "FLUSH") {
			t.Fatalf("FLUSH TABLES leaked onto the DDL timeline: %+v", ddl)
		}
	}
	for _, table := range result.Tables {
		if table.DDLCount != 0 {
			t.Fatalf("FLUSH TABLES incremented table DDLCount: %+v", table)
		}
	}
}
