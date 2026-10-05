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

func TestParseDDLStatementIndexNamesTheTableAfterON(t *testing.T) {
	cases := []struct {
		sql, operation, schema, table string
	}{
		{"CREATE INDEX idx_customer ON idxbug.orders (customer)", "CREATE INDEX", "idxbug", "orders"},
		{"CREATE UNIQUE INDEX idx_id_customer ON orders (id, customer)", "CREATE UNIQUE INDEX", "", "orders"},
		{"CREATE FULLTEXT INDEX ft_note ON orders (note)", "CREATE FULLTEXT INDEX", "", "orders"},
		{"CREATE SPATIAL INDEX g ON geo.places (loc)", "CREATE SPATIAL INDEX", "geo", "places"},
		{"DROP INDEX idx_customer ON orders", "DROP INDEX", "", "orders"},
		{"DROP INDEX `idx_w` ON `idxbug`.`widgets`", "DROP INDEX", "idxbug", "widgets"},
		{"CREATE INDEX i ON orders(customer)", "CREATE INDEX", "", "orders"},
		{"CREATE UNIQUE INDEX i ON idxbug.orders(id,customer)", "CREATE UNIQUE INDEX", "idxbug", "orders"},
		{"DROP INDEX i ON `idxbug`.`orders`", "DROP INDEX", "idxbug", "orders"},
	}
	for _, tc := range cases {
		stmt, ok := ParseDDLStatement(tc.sql)
		if !ok {
			t.Fatalf("expected %s to be recognized", tc.sql)
		}
		if stmt.Operation != tc.operation || stmt.Object != "index" || stmt.Schema != tc.schema || stmt.Table != tc.table {
			t.Fatalf("parse %q = %+v, want op=%s schema=%s table=%s", tc.sql, stmt, tc.operation, tc.schema, tc.table)
		}
	}
}

func TestParseDDLStatementCutsIdentifierAtParen(t *testing.T) {
	cases := []struct {
		sql, operation, object, schema, table string
	}{
		{"CREATE TABLE t(id INT PRIMARY KEY)", "CREATE TABLE", "table", "", "t"},
		{"CREATE TABLE idxbug.t2(id INT)", "CREATE TABLE", "table", "idxbug", "t2"},
		{"CREATE TABLE `t(id)`(x INT)", "CREATE TABLE", "table", "", "t(id)"},
		{"CREATE INDEX i ON `orders(customer)`(id)", "CREATE INDEX", "index", "", "orders(customer)"},
	}
	for _, tc := range cases {
		stmt, ok := ParseDDLStatement(tc.sql)
		if !ok {
			t.Fatalf("expected %s to be recognized", tc.sql)
		}
		if stmt.Operation != tc.operation || stmt.Object != tc.object || stmt.Schema != tc.schema || stmt.Table != tc.table {
			t.Fatalf("parse %q = %+v, want op=%s object=%s schema=%s table=%s", tc.sql, stmt, tc.operation, tc.object, tc.schema, tc.table)
		}
	}
}

func TestAnalyzeQualifiedIndexDDLIgnoresSessionSchema(t *testing.T) {
	events := []model.NormalizedEvent{
		{EventType: "DDL", Schema: "sessiondb", QuerySQL: "CREATE TABLE idxbug.widgets (id INT PRIMARY KEY)"},
		{EventType: "DDL", Schema: "sessiondb", QuerySQL: "CREATE INDEX idx_w ON idxbug.widgets (id)"},
		{EventType: "DDL", Schema: "idxbug", QuerySQL: "CREATE UNIQUE INDEX idx_id_customer ON orders (id, customer)"},
	}
	result, err := New(DefaultOptions()).Analyze(events)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	got := map[string]int{}
	for _, table := range result.Tables {
		got[table.Schema+"."+table.Table] = table.DDLCount
	}
	if got["idxbug.widgets"] != 2 || got["idxbug.orders"] != 1 {
		t.Fatalf("table DDL counts = %v, want idxbug.widgets=2 idxbug.orders=1", got)
	}
	if _, ok := got["sessiondb.widgets"]; ok {
		t.Fatalf("session schema replaced the qualified table: %v", got)
	}
	if _, ok := got["sessiondb.idx_w"]; ok || got["idxbug.idx_w"] != 0 {
		t.Fatalf("index name was used as a table: %v", got)
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
