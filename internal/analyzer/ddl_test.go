// Package analyzer verifies DDL extraction helpers and deterministic DDL timeline aggregation.
// input: normalized events and SQL statements with binlog metadata.
// output: regression coverage for DDL parsing, filtering, metadata carry-through, and timeline ordering.
// pos: focused contract tests for future Analyzer diagnostics integration without wiring Analyzer yet.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestParseDDLStatementExtractsAlterTableMetadata(t *testing.T) {
	stmt, ok := ParseDDLStatement("  ALTER   TABLE `sales`.`orders` ADD COLUMN status TINYINT  ")
	if !ok {
		t.Fatal("expected ALTER TABLE to be recognized as DDL")
	}

	if stmt.Operation != "ALTER TABLE" {
		t.Fatalf("expected operation ALTER TABLE, got %q", stmt.Operation)
	}
	if stmt.Object != "table" {
		t.Fatalf("expected object table, got %q", stmt.Object)
	}
	if stmt.Schema != "sales" {
		t.Fatalf("expected schema sales, got %q", stmt.Schema)
	}
	if stmt.Table != "orders" {
		t.Fatalf("expected table orders, got %q", stmt.Table)
	}
	if stmt.Statement != "ALTER TABLE `sales`.`orders` ADD COLUMN status TINYINT" {
		t.Fatalf("unexpected normalized statement %q", stmt.Statement)
	}
}

func TestDDLAggregatorCollectsFromEventsAndStatements(t *testing.T) {
	base := time.Date(2026, 4, 15, 10, 30, 0, 0, time.UTC)

	agg := NewDDLAggregator()
	agg.ConsumeEvent(model.NormalizedEvent{
		Timestamp:     base.Add(2 * time.Minute),
		BinlogPath:    "mysql-bin.000002",
		PositionStart: 120,
		PositionEnd:   180,
		BinlogBytes:   60,
		EventType:     "QUERY",
		QuerySQL:      "CREATE TABLE inventory.items (id BIGINT PRIMARY KEY)",
	})
	agg.ConsumeEvent(model.NormalizedEvent{
		Timestamp: base.Add(time.Minute),
		EventType: "ROWS",
		Operation: "INSERT",
		RowCount:  3,
	})
	agg.ConsumeStatement(base, "mysql-bin.000001", 40, 80, 40, "DROP TABLE app.old_orders")

	got := agg.Snapshot()
	if len(got) != 2 {
		t.Fatalf("expected 2 DDL events, got %d", len(got))
	}

	if !got[0].Timestamp.Equal(base) {
		t.Fatalf("expected earliest DDL first, got %s", got[0].Timestamp)
	}
	if got[0].Operation != "DROP TABLE" {
		t.Fatalf("expected DROP TABLE first, got %q", got[0].Operation)
	}
	if got[0].Schema != "app" || got[0].Table != "old_orders" {
		t.Fatalf("expected app.old_orders, got %s.%s", got[0].Schema, got[0].Table)
	}
	if got[0].PositionStart != 40 || got[0].PositionEnd != 80 || got[0].BinlogBytes != 40 {
		t.Fatalf("expected binlog metadata to be preserved, got start=%d end=%d bytes=%d", got[0].PositionStart, got[0].PositionEnd, got[0].BinlogBytes)
	}

	if got[1].Operation != "CREATE TABLE" {
		t.Fatalf("expected CREATE TABLE second, got %q", got[1].Operation)
	}
	if got[1].Schema != "inventory" || got[1].Table != "items" {
		t.Fatalf("expected inventory.items, got %s.%s", got[1].Schema, got[1].Table)
	}
	if got[1].BinlogPath != "mysql-bin.000002" {
		t.Fatalf("expected second event to keep binlog path, got %q", got[1].BinlogPath)
	}
}

func TestParseDDLStatementCreateDatabaseAndRename(t *testing.T) {
	createDB, ok := ParseDDLStatement("CREATE DATABASE IF NOT EXISTS dogfood")
	if !ok {
		t.Fatal("expected CREATE DATABASE to be recognized")
	}
	if createDB.Operation != "CREATE DATABASE" || createDB.Object != "database" || createDB.Schema != "dogfood" {
		t.Fatalf("unexpected CREATE DATABASE parse: %+v", createDB)
	}

	rename, ok := ParseDDLStatement("RENAME TABLE shop.old_users TO shop.users")
	if !ok {
		t.Fatal("expected RENAME TABLE to be recognized")
	}
	if rename.Operation != "RENAME TABLE" || rename.Schema != "shop" || rename.Table != "old_users" {
		t.Fatalf("unexpected RENAME parse: %+v", rename)
	}

	trunc, ok := ParseDDLStatement("TRUNCATE audit_logs")
	if !ok || trunc.Operation != "TRUNCATE TABLE" || trunc.Table != "audit_logs" {
		t.Fatalf("unexpected TRUNCATE parse: ok=%v %+v", ok, trunc)
	}
}

func TestParseDDLStatementGrantAndCreateUser(t *testing.T) {
	grant, ok := ParseDDLStatement("GRANT REPLICATION SLAVE ON *.* TO 'repl'@'127.0.0.1'")
	if !ok || grant.Operation != "GRANT" || grant.Object != "privilege" {
		t.Fatalf("unexpected GRANT parse: ok=%v %+v", ok, grant)
	}

	createUser, ok := ParseDDLStatement("CREATE USER 'repl'@'%'")
	if !ok || createUser.Operation != "CREATE USER" || createUser.Object != "user" {
		t.Fatalf("unexpected CREATE USER parse: ok=%v %+v", ok, createUser)
	}

	revoke, ok := ParseDDLStatement("REVOKE ALL PRIVILEGES ON *.* FROM 'repl'@'%'")
	if !ok || revoke.Operation != "REVOKE" || revoke.Object != "privilege" {
		t.Fatalf("unexpected REVOKE parse: ok=%v %+v", ok, revoke)
	}
}

func TestDDLEventFromNormalizedEventUsesQuerySchemaFallback(t *testing.T) {
	got, ok := DDLEventFromNormalizedEvent(model.NormalizedEvent{
		EventType: "DDL",
		Schema:    "testdb",
		QuerySQL:  "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
	})
	if !ok {
		t.Fatal("expected CREATE TABLE to be recognized")
	}
	if got.Operation != "CREATE TABLE" || got.Schema != "testdb" || got.Table != "users" {
		t.Fatalf("expected testdb.users CREATE TABLE, got %+v", got)
	}
}

func TestDDLEventFromNormalizedEventIgnoresNonDDLQueries(t *testing.T) {
	_, ok := DDLEventFromNormalizedEvent(model.NormalizedEvent{
		EventType: "QUERY",
		QuerySQL:  "UPDATE users SET name = 'alice' WHERE id = 7",
	})
	if ok {
		t.Fatal("expected non-DDL query to be ignored")
	}
}

func BenchmarkParseDDLStatementNonDDLRowsQuery(b *testing.B) {
	sql := "UPDATE shop.orders SET status = 'paid', updated_at = NOW() WHERE id IN (" + strings.Repeat("?,", 200) + "?)"

	b.ReportAllocs()
	var ok bool
	for i := 0; i < b.N; i++ {
		_, ok = ParseDDLStatement(sql)
	}
	if ok {
		b.Fatal("expected non-DDL statement to be ignored")
	}
}
