package flashback_test

import (
	"binlogviz/internal/analyzer"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
	"binlogviz/internal/schemacatalog"
)

// genNames is the generated column set in effect for schema.table, or nil.
func genNames(c *schemacatalog.Catalog, schema, table string) map[string]struct{} {
	res := c.Peek(schema, table)
	if res.Kind != schemacatalog.Exact && res.Kind != schemacatalog.Fold {
		return nil
	}
	return mysqlexpr.GeneratedNameSet(res.Cols)
}

func TestGeneratedColumnsOmittedOrRefused(t *testing.T) {
	const create = "CREATE TABLE shop.gen (id INT PRIMARY KEY, base INT, virt INT AS (base + 1) VIRTUAL, stor INT AS (base * 2) STORED)"
	learned := schemacatalog.New("")
	learned.ObserveDDL("shop", create)
	cols := genNames(learned, "shop", "gen")
	if _, ok := cols["virt"]; !ok || len(cols) != 2 {
		t.Fatalf("generated: %#v", cols)
	}
	if _, ok := cols["stor"]; !ok {
		t.Fatalf("stored: %#v", cols)
	}

	bad := schemacatalog.New("")
	bad.ObserveDDL("shop", "CREATE TABLE shop.t AS SELECT 1")
	bad.ObserveDDL("shop", "CREATE TABLE shop.copied LIKE shop.missing")
	bad.ObserveDDL("shop", "ALTER TABLE shop.fresh ADD COLUMN g INT AS (id + 1) VIRTUAL")
	for _, table := range []string{"t", "copied", "fresh"} {
		if bad.Resolve("shop", table).Kind != schemacatalog.Refused {
			t.Fatalf("%s should be unknown", table)
		}
	}

	learned.ObserveDDL("shop", "ALTER TABLE shop.gen ADD COLUMN extra INT AS (base + 2) VIRTUAL")
	learned.ObserveDDL("shop", "CREATE TABLE shop.gen2 LIKE shop.gen")
	if _, ok := genNames(learned, "shop", "gen2")["extra"]; !ok {
		t.Fatalf("like: %#v", genNames(learned, "shop", "gen2"))
	}
	learned.ObserveDDL("shop", "ALTER TABLE `shop`.`gen` DROP COLUMN virt")
	if _, ok := genNames(learned, "shop", "gen")["virt"]; ok {
		t.Fatal("drop column kept virt")
	}
	learned.ObserveDDL("shop", "DROP TABLE IF EXISTS shop.gen, shop.gen2")
	if genNames(learned, "shop", "gen") != nil || learned.Resolve("shop", "gen").Kind == schemacatalog.Refused {
		t.Fatal("drop table")
	}

	always := schemacatalog.New("")
	always.ObserveDDL("shop", "CREATE TABLE shop.g (id INT PRIMARY KEY, v INT GENERATED ALWAYS AS ((id + 1)) STORED)")
	if _, ok := genNames(always, "shop", "g")["v"]; !ok {
		t.Fatalf("generated always: %#v", genNames(always, "shop", "g"))
	}
}

func TestFlashbackLearnsExcludedCreateAndWarnsOnSplit(t *testing.T) {
	const gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	selector, err := analyzer.ParseGTIDSelector(nil, []string{gtid + ":1"})
	if err != nil {
		t.Fatal(err)
	}
	a := analyzer.New(analyzer.Options{
		Flashback:     true,
		IncludeTables: []string{"shop.gen"},
		GTIDSelector:  selector,
	})
	base := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid + ":1", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: base, EventType: "DDL", Schema: "shop", QuerySQL: "CREATE TABLE shop.gen (id INT PRIMARY KEY, base INT, virt INT AS (base + 1) VIRTUAL, stor INT AS (base * 2) STORED)", BinlogPath: "mysql-bin.000001", PositionStart: 140, PositionEnd: 200},
		{Timestamp: base.Add(time.Second), EventType: "GTID", GTID: gtid + ":2", BinlogPath: "mysql-bin.000001", PositionStart: 200, PositionEnd: 240},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 240, PositionEnd: 260},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "wide", BinlogPath: "mysql-bin.000001", PositionStart: 260, PositionEnd: 300, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "wide", Op: "DELETE", Columns: []string{"id"}, Before: []string{"1"}, PK: []int{0},
		}}},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "gen", BinlogPath: "mysql-bin.000001", PositionStart: 300, PositionEnd: 360, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "gen", Op: "DELETE", Columns: []string{"id", "base", "virt", "stor"}, Before: []string{"2", "20", "21", "40"}, PK: []int{0},
		}}},
		{Timestamp: base.Add(time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 360, PositionEnd: 400},
	}
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err := a.Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`gen` (`id`, `base`) VALUES (2, 20);") || strings.Contains(sql, "virt") || strings.Contains(sql, "shop`.`wide") {
		t.Fatalf("sql:\n%s", sql)
	}
	warnings := a.Planner().Warnings()
	if len(warnings) != 1 || !strings.Contains(warnings[0], gtid+":2") || !strings.Contains(warnings[0], "only partly undone") {
		t.Fatalf("warnings: %#v", warnings)
	}

	dml := analyzer.New(analyzer.Options{Flashback: true, IncludeDML: []string{"DELETE"}})
	dmlEvents := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid + ":1", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: base, EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 140, PositionEnd: 160},
		{Timestamp: base, EventType: "ROWS", Operation: "UPDATE", Schema: "shop", Table: "wide", BinlogPath: "mysql-bin.000001", PositionStart: 160, PositionEnd: 200, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "wide", Op: "UPDATE", Columns: []string{"id", "note"}, Before: []string{"1", "'a'"}, After: []string{"1", "'b'"}, PK: []int{0},
		}}},
		{Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "wide", BinlogPath: "mysql-bin.000001", PositionStart: 200, PositionEnd: 240, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "wide", Op: "DELETE", Columns: []string{"id", "note"}, Before: []string{"1", "'a'"}, PK: []int{0},
		}}},
		{Timestamp: base, EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 240, PositionEnd: 280},
	}
	for _, ev := range dmlEvents {
		if err := dml.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dml.Finalize(); err != nil {
		t.Fatal(err)
	}
	if _, err := dml.Planner().SQL(); err != nil {
		t.Fatal(err)
	}
	if warnings = dml.Planner().Warnings(); len(warnings) != 2 ||
		!strings.Contains(warnings[0], "shop.wide") ||
		!strings.Contains(warnings[0], "generated columns cannot be ruled out") ||
		!strings.Contains(warnings[1], "only partly undone") {
		t.Fatalf("dml warnings: %#v", warnings)
	}

	plain := analyzer.New(analyzer.Options{Flashback: true})
	row := model.NormalizedEvent{
		Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "gen",
		BinlogPath: "mysql-bin.000001", PositionStart: 10, PositionEnd: 20,
		FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "gen", Op: "DELETE", Columns: []string{"id", "virt"}, Before: []string{"1", "2"}, PK: []int{0},
		}},
	}
	if err := plain.Consume(row); err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err = plain.Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "`virt`") {
		t.Fatalf("missing create should still emit the column:\n%s", sql)
	}
	warnings = plain.Planner().Warnings()
	if len(warnings) != 1 || !strings.Contains(warnings[0], "shop.gen") || !strings.Contains(warnings[0], "generated columns cannot be ruled out") {
		t.Fatalf("plain warnings: %#v", warnings)
	}

	refused := analyzer.New(analyzer.Options{Flashback: true, GTIDSelector: selector})
	badEvents := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid + ":1", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: base, EventType: "DDL", Schema: "shop", QuerySQL: "CREATE TABLE shop.t AS SELECT 1", BinlogPath: "mysql-bin.000001", PositionStart: 140, PositionEnd: 180},
		{Timestamp: base.Add(time.Second), EventType: "GTID", GTID: gtid + ":2", BinlogPath: "mysql-bin.000001", PositionStart: 180, PositionEnd: 220},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 220, PositionEnd: 240},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "t", BinlogPath: "mysql-bin.000001", PositionStart: 240, PositionEnd: 280, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "t", Op: "DELETE", Columns: []string{"id"}, Before: []string{"1"}, PK: []int{0},
		}}},
		{Timestamp: base.Add(time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 280, PositionEnd: 300},
	}
	for _, ev := range badEvents {
		if err := refused.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := refused.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err = refused.Planner().SQL()
	if err == nil || sql != "" || !strings.Contains(err.Error(), "shop.t: generated columns are unknown") {
		t.Fatalf("sql %q err %v", sql, err)
	}
}

func TestSchemaFileOmitsGeneratedColumns(t *testing.T) {
	const dump = "USE `shop`;\n" +
		"/*!40101 SET @saved_cs_client = @@character_set_client */;\n" +
		"DROP TABLE IF EXISTS `gen`;\n" +
		"CREATE TABLE `gen` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `base` int DEFAULT NULL,\n" +
		"  `virt` int GENERATED ALWAYS AS ((`base` + 1)) VIRTUAL,\n" +
		"  `stor` int GENERATED ALWAYS AS ((`base` * 2)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB;\n" +
		"USE `shop`;\n" +
		"wide\tCREATE TABLE `wide` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `note` varchar(64) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		")"
	base := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	a := analyzer.New(analyzer.Options{Flashback: true, SchemaSQL: dump})
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "gen", BinlogPath: "mysql-bin.000001", PositionStart: 10, PositionEnd: 20, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "gen", Op: "DELETE", Columns: []string{"id", "base", "virt", "stor"}, Before: []string{"1", "10", "11", "20"}, PK: []int{0},
		}}},
		{Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "wide", BinlogPath: "mysql-bin.000001", PositionStart: 20, PositionEnd: 30, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "wide", Op: "DELETE", Columns: []string{"id", "note"}, Before: []string{"1", "'a'"}, PK: []int{0},
		}}},
		{Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "other", BinlogPath: "mysql-bin.000001", PositionStart: 30, PositionEnd: 40, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "other", Op: "DELETE", Columns: []string{"id", "virt"}, Before: []string{"1", "2"}, PK: []int{0},
		}}},
	}
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err := a.Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`gen` (`id`, `base`) SELECT 1, 10 FROM DUAL WHERE @binlogviz_ok <=> '") || strings.Contains(sql, "`stor`") {
		t.Fatalf("schema file sql:\n%s", sql)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`wide` (`id`, `note`) SELECT 1, 'a' FROM DUAL WHERE @binlogviz_ok <=> '") {
		t.Fatalf("show create sql:\n%s", sql)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`other` (`id`, `virt`) SELECT 1, 2 FROM DUAL WHERE @binlogviz_ok <=> '") {
		t.Fatalf("unknown table sql:\n%s", sql)
	}
	warnings := a.Planner().Warnings()
	if len(warnings) != 1 || !strings.Contains(warnings[0], "shop.other") || !strings.Contains(warnings[0], "generated columns cannot be ruled out") || !strings.Contains(warnings[0], "--schema-file") {
		t.Fatalf("warnings: %#v", warnings)
	}

	const gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	selector, err := analyzer.ParseGTIDSelector(nil, []string{gtid + ":1"})
	if err != nil {
		t.Fatal(err)
	}
	replaced := analyzer.New(analyzer.Options{Flashback: true, SchemaSQL: dump, GTIDSelector: selector})
	replacedEvents := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid + ":1", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: base, EventType: "DDL", Schema: "shop", QuerySQL: "CREATE TABLE shop.gen (id INT PRIMARY KEY, virt INT)", BinlogPath: "mysql-bin.000001", PositionStart: 140, PositionEnd: 200},
		{Timestamp: base.Add(time.Second), EventType: "GTID", GTID: gtid + ":2", BinlogPath: "mysql-bin.000001", PositionStart: 200, PositionEnd: 240},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 240, PositionEnd: 260},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "gen", BinlogPath: "mysql-bin.000001", PositionStart: 260, PositionEnd: 300, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "gen", Op: "DELETE", Columns: []string{"id", "virt"}, Before: []string{"1", "2"}, PK: []int{0},
		}}},
		{Timestamp: base.Add(time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 300, PositionEnd: 340},
	}
	for _, ev := range replacedEvents {
		if err := replaced.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := replaced.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err = replaced.Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "`virt`") {
		t.Fatalf("later create should replace the schema file:\n%s", sql)
	}
	if warnings = replaced.Planner().Warnings(); len(warnings) != 0 {
		t.Fatalf("replaced warnings: %#v", warnings)
	}

	bad := analyzer.New(analyzer.Options{Flashback: true, SchemaSQL: "USE `shop`;\nCREATE TABLE t AS SELECT 1;\n"})
	if err := bad.Consume(model.NormalizedEvent{
		Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "t",
		BinlogPath: "mysql-bin.000001", PositionStart: 10, PositionEnd: 20,
		FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "t", Op: "DELETE", Columns: []string{"id"}, Before: []string{"1"}, PK: []int{0},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err = bad.Planner().SQL()
	if err == nil || sql != "" || !strings.Contains(err.Error(), "shop.t: generated columns are unknown") {
		t.Fatalf("sql %q err %v", sql, err)
	}
}

func TestSchemaFileReadsBatchShowCreate(t *testing.T) {
	// mysql --batch keeps the column break as a real tab and writes each
	// newline inside the CREATE field as the two characters '\' and 'n'.
	// -N drops the "Create Table" header; without -N that header is a real row.
	const create = "gen\tCREATE TABLE `gen` (\\n" +
		"  `id` int NOT NULL,\\n" +
		"  `base` int DEFAULT NULL,\\n" +
		"  `virt` int GENERATED ALWAYS AS ((`base` + 1)) VIRTUAL,\\n" +
		"  `stor` int GENERATED ALWAYS AS ((`base` * 2)) STORED,\\n" +
		"  PRIMARY KEY (`id`)\\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"
	for _, batch := range []string{
		"USE `shop`;\n" + create,
		"USE `shop`;\nTable\tCreate Table\n" + create,
	} {
		assertBatchShowCreate(t, batch)
	}
}

func assertBatchShowCreate(t *testing.T, batch string) {
	t.Helper()
	base := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	a := analyzer.New(analyzer.Options{Flashback: true, SchemaSQL: batch})
	if err := a.Consume(model.NormalizedEvent{
		Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "gen",
		BinlogPath: "mysql-bin.000001", PositionStart: 10, PositionEnd: 20,
		FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "gen", Op: "DELETE",
			Columns: []string{"id", "base", "virt", "stor"}, Before: []string{"1", "10", "11", "20"}, PK: []int{0},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err := a.Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`gen` (`id`, `base`) SELECT 1, 10 FROM DUAL WHERE @binlogviz_ok <=> '") || strings.Contains(sql, "`virt`") || strings.Contains(sql, "`stor`") {
		t.Fatalf("batch show create:\n%s", sql)
	}
	if warnings := a.Planner().Warnings(); len(warnings) != 0 {
		t.Fatalf("warnings: %#v", warnings)
	}
}
