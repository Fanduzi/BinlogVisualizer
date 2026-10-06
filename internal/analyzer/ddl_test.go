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

func TestParseDDLStatementObjectTypesAndGenericDDL(t *testing.T) {
	cases := []struct {
		sql, operation, object, schema, table string
	}{
		{"CREATE VIEW shop.v2 AS SELECT id FROM shop.small", "CREATE VIEW", "view", "shop", "v2"},
		{"DROP VIEW shop.v2", "DROP VIEW", "view", "shop", "v2"},
		{"CREATE OR REPLACE VIEW v AS SELECT 1", "CREATE VIEW", "view", "", "v"},
		{"CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW shop.v_orders AS SELECT 1", "CREATE VIEW", "view", "shop", "v_orders"},
		{"CREATE TRIGGER shop.trg_small BEFORE INSERT ON shop.small FOR EACH ROW SET NEW.v = NEW.v + 1", "CREATE TRIGGER", "trigger", "shop", "trg_small"},
		{"DROP TRIGGER shop.trg_small", "DROP TRIGGER", "trigger", "shop", "trg_small"},
		{"CREATE DEFINER=`root`@`localhost` PROCEDURE shop.p1() SELECT 1", "CREATE PROCEDURE", "routine", "shop", "p1"},
		{"DROP PROCEDURE IF EXISTS shop.p1", "DROP PROCEDURE", "routine", "shop", "p1"},
		{"CREATE FUNCTION shop.f1() RETURNS INT RETURN 1", "CREATE FUNCTION", "routine", "shop", "f1"},
		{"DROP FUNCTION shop.f1", "DROP FUNCTION", "routine", "shop", "f1"},
		{"CREATE EVENT shop.e1 ON SCHEDULE EVERY 1 HOUR DO SELECT 1", "CREATE EVENT", "event", "shop", "e1"},
		{"ALTER EVENT shop.e1 DISABLE", "ALTER EVENT", "event", "shop", "e1"},
		{"DROP EVENT IF EXISTS shop.e1", "DROP EVENT", "event", "shop", "e1"},
		{"CREATE TEMPORARY TABLE shop.tmp (id INT)", "CREATE TABLE", "table", "shop", "tmp"},
		{"CREATE TABLESPACE ts ADD DATAFILE 'x'", "DDL", "ddl", "", ""},
	}
	for _, tc := range cases {
		stmt, ok := ParseDDLStatement(tc.sql)
		if !ok {
			t.Fatalf("expected %s to be kept", tc.sql)
		}
		if stmt.Operation != tc.operation || stmt.Object != tc.object || stmt.Schema != tc.schema || stmt.Table != tc.table {
			t.Fatalf("parse %q = %+v, want op=%s object=%s schema=%s table=%s", tc.sql, stmt, tc.operation, tc.object, tc.schema, tc.table)
		}
	}
}

func TestAnalyzeKeepsViewTriggerRoutineAndUnknownDDL(t *testing.T) {
	statements := []string{
		"CREATE VIEW shop.v2 AS SELECT id FROM shop.small",
		"DROP VIEW shop.v2",
		"CREATE TRIGGER shop.trg_small BEFORE INSERT ON shop.small FOR EACH ROW SET NEW.v = NEW.v + 1",
		"DROP TRIGGER shop.trg_small",
		"CREATE PROCEDURE shop.p1() SELECT 1",
		"DROP PROCEDURE shop.p1",
		"CREATE FUNCTION shop.f1() RETURNS INT RETURN 1",
		"CREATE EVENT shop.e1 ON SCHEDULE EVERY 1 HOUR DO SELECT 1",
		"CREATE TABLESPACE ts ADD DATAFILE 'x'",
	}
	events := make([]model.NormalizedEvent, len(statements))
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	for i, sql := range statements {
		events[i] = model.NormalizedEvent{
			Timestamp: base.Add(time.Duration(i) * time.Second),
			EventType: "DDL",
			QuerySQL:  sql,
		}
	}
	result, err := New(DefaultOptions()).Analyze(events)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	if len(result.Diagnostics.DDLEvents) != len(statements) {
		t.Fatalf("DDL timeline kept %d events, want %d: %+v", len(result.Diagnostics.DDLEvents), len(statements), result.Diagnostics.DDLEvents)
	}
	got := map[string]string{}
	for _, event := range result.Diagnostics.DDLEvents {
		got[event.Operation] = event.Object
	}
	for _, want := range []struct{ op, object string }{
		{"CREATE VIEW", "view"},
		{"DROP TRIGGER", "trigger"},
		{"CREATE PROCEDURE", "routine"},
		{"CREATE FUNCTION", "routine"},
		{"CREATE EVENT", "event"},
		{"DDL", "ddl"},
	} {
		if got[want.op] != want.object {
			t.Fatalf("operation %s object = %q, want %q; events=%+v", want.op, got[want.op], want.object, result.Diagnostics.DDLEvents)
		}
	}
	trigger := result.Diagnostics.DDLEvents[2]
	if trigger.Schema != "shop" || trigger.Table != "trg_small" {
		t.Fatalf("CREATE TRIGGER should use the trigger name, got %s.%s", trigger.Schema, trigger.Table)
	}
	drop := result.Diagnostics.DDLEvents[3]
	if drop.Schema != "shop" || drop.Table != "trg_small" {
		t.Fatalf("DROP TRIGGER should use the same name, got %s.%s", drop.Schema, drop.Table)
	}
}

func TestParseDDLStatementRedactsCredentialMaterial(t *testing.T) {
	hash := "$A$005$" + string([]byte{0x01, 0x02, 0xff}) + "pINSRVdE/pm7UzcDqjMRZ4wRQ3p8g2Ic9"
	sql := "CREATE USER 'app'@'%' IDENTIFIED WITH 'caching_sha2_password' AS '" + hash + "'"
	stmt, ok := ParseDDLStatement(sql)
	if !ok {
		t.Fatal("expected CREATE USER")
	}
	if strings.Contains(stmt.Statement, "pINSRVdE") || strings.Contains(stmt.Statement, hash) || strings.ContainsRune(stmt.Statement, '\x01') {
		t.Fatalf("credential leaked: %q", stmt.Statement)
	}
	if !strings.Contains(stmt.Statement, "IDENTIFIED WITH 'caching_sha2_password' AS <secret>") {
		t.Fatalf("statement = %q", stmt.Statement)
	}

	alter, ok := ParseDDLStatement("ALTER USER 'app'@'%' IDENTIFIED BY 's3cret'")
	if !ok || strings.Contains(alter.Statement, "s3cret") || !strings.Contains(alter.Statement, "IDENTIFIED BY <secret>") {
		t.Fatalf("alter = %+v", alter)
	}
	setPwd, ok := ParseDDLStatement("SET PASSWORD FOR 'app'@'%' = PASSWORD('hidden')")
	if !ok || setPwd.Operation != "SET PASSWORD" || setPwd.Table != "'app'@'%'" || strings.Contains(setPwd.Statement, "hidden") || !strings.Contains(setPwd.Statement, "= <secret>") {
		t.Fatalf("set password = %+v", setPwd)
	}
	grant, ok := ParseDDLStatement("GRANT SELECT ON shop.* TO 'app'@'%' IDENTIFIED BY 'x'")
	if !ok || strings.Contains(grant.Statement, "'x'") || !strings.Contains(grant.Statement, "<secret>") {
		t.Fatalf("grant = %+v", grant)
	}
}

func TestParseDDLStatementRedactsMariaDBAuthForms(t *testing.T) {
	const password = "MariaVia8"
	const hash = "*FAA7A7FD08B88CBFE862FC3A7D612FB937B13476"
	cases := []struct {
		sql  string
		want string
	}{
		{
			"CREATE USER 'm5'@'%' IDENTIFIED VIA mysql_native_password USING PASSWORD('" + password + "')",
			"CREATE USER 'm5'@'%' IDENTIFIED VIA mysql_native_password USING <secret>",
		},
		{
			"ALTER USER 'm5'@'%' IDENTIFIED VIA mysql_native_password USING '" + hash + "'",
			"ALTER USER 'm5'@'%' IDENTIFIED VIA mysql_native_password USING <secret>",
		},
		{
			"ALTER USER 'm5'@'%' IDENTIFIED WITH mysql_native_password USING PASSWORD('" + password + "')",
			"ALTER USER 'm5'@'%' IDENTIFIED WITH mysql_native_password USING <secret>",
		},
		{
			"CREATE USER 'u'@'%' IDENTIFIED VIA mysql_native_password USING PASSWORD('chainA') OR ed25519 USING PASSWORD('chainB') OR unix_socket",
			"CREATE USER 'u'@'%' IDENTIFIED VIA mysql_native_password USING <secret> OR ed25519 USING <secret> OR unix_socket",
		},
		{
			"GRANT SELECT ON db.* TO 'app'@'%' IDENTIFIED VIA mysql_native_password USING PASSWORD('grantPw')",
			"GRANT SELECT ON db.* TO 'app'@'%' IDENTIFIED VIA mysql_native_password USING <secret>",
		},
		{
			"CREATE USER 'app'@'%' IDENTIFIED BY RANDOM PASSWORD",
			"CREATE USER 'app'@'%' IDENTIFIED BY RANDOM PASSWORD",
		},
	}
	for _, tc := range cases {
		stmt, ok := ParseDDLStatement(tc.sql)
		if !ok {
			t.Fatalf("expected DDL: %s", tc.sql)
		}
		if stmt.Statement != tc.want {
			t.Fatalf("statement = %q, want %q", stmt.Statement, tc.want)
		}
		for _, secret := range []string{password, hash, "chainA", "chainB", "grantPw"} {
			if strings.Contains(stmt.Statement, secret) {
				t.Fatalf("leaked %q in %q", secret, stmt.Statement)
			}
		}
	}
}

func TestParseDDLStatementCapsStoredStatement(t *testing.T) {
	tail := "TAILMARKER_ZZZ"
	sql := "CREATE TABLE lng.wide (" + strings.Repeat("c INT, ", 800) + tail + " INT)"
	full := strings.Join(strings.Fields(sql), " ")
	stmt, ok := ParseDDLStatement(sql)
	if !ok {
		t.Fatal("expected CREATE TABLE")
	}
	if !stmt.Truncated || stmt.OriginalBytes != len(full) {
		t.Fatalf("cap = truncated %v original %d, want true and %d", stmt.Truncated, stmt.OriginalBytes, len(full))
	}
	if len(stmt.Statement) > model.MaxStoredSQLBytes || len(stmt.Statement) == 0 {
		t.Fatalf("stored len = %d, want 1..%d", len(stmt.Statement), model.MaxStoredSQLBytes)
	}
	if strings.Contains(stmt.Statement, tail) {
		t.Fatalf("stored statement kept the tail: %q", stmt.Statement[len(stmt.Statement)-40:])
	}
	if len(stmt.Statement) != model.MaxStoredSQLBytes {
		t.Fatalf("stored len = %d, want %d", len(stmt.Statement), model.MaxStoredSQLBytes)
	}
	shown := model.DisplayStoredSQL(stmt.Statement, stmt.Truncated, stmt.OriginalBytes)
	marker := model.TruncationMarker(model.MaxStoredSQLBytes, len(full))
	if !strings.HasSuffix(shown, marker) {
		t.Fatalf("full display missing %q", marker)
	}
}

func TestIncludeTableKeepsNonTableObjectNames(t *testing.T) {
	base := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "DDL", QuerySQL: "CREATE VIEW obj.v1 AS SELECT 1"},
		{Timestamp: base.Add(time.Second), EventType: "DDL", QuerySQL: "CREATE EVENT obj.ev1 ON SCHEDULE EVERY 1 HOUR DO SELECT 1"},
		{Timestamp: base.Add(2 * time.Second), EventType: "DDL", QuerySQL: "CREATE FUNCTION obj.f1() RETURNS INT RETURN 1"},
		{Timestamp: base.Add(3 * time.Second), EventType: "DDL", QuerySQL: "CREATE PROCEDURE obj.p1() SELECT 1"},
		{Timestamp: base.Add(4 * time.Second), EventType: "DDL", QuerySQL: "CREATE TRIGGER obj.trg_ai BEFORE INSERT ON obj.t FOR EACH ROW SET NEW.v = 1"},
		{Timestamp: base.Add(5 * time.Second), EventType: "DDL", QuerySQL: "DROP TRIGGER obj.trg_ai"},
	}
	for _, name := range []string{"obj.v1", "obj.ev1", "obj.f1", "obj.p1"} {
		opts := DefaultOptions()
		opts.IncludeTables = []string{name}
		result, err := New(opts).Analyze(events)
		if err != nil {
			t.Fatalf("include %s: %v", name, err)
		}
		if len(result.Diagnostics.DDLEvents) != 1 || result.Diagnostics.DDLEvents[0].Schema+"."+result.Diagnostics.DDLEvents[0].Table != name {
			t.Fatalf("include %s kept %+v", name, result.Diagnostics.DDLEvents)
		}
	}
	opts := DefaultOptions()
	opts.IncludeTables = []string{"obj.trg_ai"}
	result, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatalf("include trigger: %v", err)
	}
	if len(result.Diagnostics.DDLEvents) != 2 {
		t.Fatalf("trigger filter kept %+v, want CREATE and DROP", result.Diagnostics.DDLEvents)
	}
	opts.IncludeTables = []string{"obj.t"}
	result, err = New(opts).Analyze(events)
	if err != nil {
		t.Fatalf("include base table: %v", err)
	}
	if len(result.Diagnostics.DDLEvents) != 0 {
		t.Fatalf("ON table must not keep the trigger: %+v", result.Diagnostics.DDLEvents)
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
