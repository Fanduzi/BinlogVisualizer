package analyzer

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestSchemaFileRefusesMismatches(t *testing.T) {
	const base = "USE `shop`;\nCREATE TABLE `t` (\n  `id` int NOT NULL,\n  `a` int DEFAULT NULL,\n  `c` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	row := func(names, before []string, cols []model.FlashCol) model.FlashRow {
		return model.FlashRow{
			Schema: "shop", Table: "t", Op: "DELETE",
			Columns: names, Cols: cols, Before: before, PK: []int{0},
		}
	}
	ints := intCols(3)
	cases := []struct {
		name   string
		schema string
		flash  model.FlashRow
		want   []string
	}{
		{
			name:   "extra",
			schema: strings.Replace(base, "  `c` int DEFAULT NULL,\n", "  `c` int DEFAULT NULL,\n  `d` int DEFAULT NULL,\n", 1),
			flash:  row([]string{"id", "a", "c"}, []string{"1", "10", "99"}, ints),
			want:   []string{"shop.t", "extra d"},
		},
		{
			name:   "missing",
			schema: strings.Replace(base, "  `c` int DEFAULT NULL,\n", "", 1),
			flash:  row([]string{"id", "a", "c"}, []string{"1", "10", "99"}, ints),
			want:   []string{"shop.t", "missing c"},
		},
		{
			name:   "reordered",
			schema: strings.Replace(base, "  `a` int DEFAULT NULL,\n  `c` int DEFAULT NULL,\n", "  `c` int DEFAULT NULL,\n  `a` int DEFAULT NULL,\n", 1),
			flash:  row([]string{"id", "a", "c"}, []string{"1", "10", "99"}, ints),
			want:   []string{"shop.t", "reordered", "c, a", "a, c"},
		},
		{
			name:   "renamed",
			schema: strings.Replace(base, "`c`", "`d`", 1),
			flash:  row([]string{"id", "a", "c"}, []string{"1", "10", "99"}, ints),
			want:   []string{"shop.t", "extra d", "missing c"},
		},
		{
			name:   "type",
			schema: strings.Replace(base, "  `c` int DEFAULT NULL,\n", "  `c` bigint DEFAULT NULL,\n", 1),
			flash:  row([]string{"id", "a", "c"}, []string{"1", "10", "99"}, ints),
			want:   []string{"shop.t", "c is bigint", "int"},
		},
		{
			name:   "unsigned",
			schema: strings.Replace(base, "  `c` int DEFAULT NULL,\n", "  `c` int unsigned DEFAULT NULL,\n", 1),
			flash:  row([]string{"id", "a", "c"}, []string{"1", "10", "99"}, ints),
			want:   []string{"shop.t", "c is unsigned", "signed"},
		},
		{
			name:   "charset",
			schema: "USE `shop`;\nCREATE TABLE `t` (`id` int NOT NULL, `note` varchar(32) CHARACTER SET utf8mb4, PRIMARY KEY (`id`));\n",
			flash: row([]string{"id", "note"}, []string{"1", "'a'"}, []model.FlashCol{
				{Base: "int", HasSign: true},
				{Base: "varchar", Charset: "latin1"},
			}),
			want: []string{"shop.t", "note is utf8mb4", "latin1"},
		},
		{
			name:   "enum members",
			schema: "USE `shop`;\nCREATE TABLE `t` (`id` int NOT NULL, `e` enum('red','blue'), PRIMARY KEY (`id`));\n",
			flash: row([]string{"id", "e"}, []string{"1", "1"}, []model.FlashCol{
				{Base: "int", HasSign: true},
				{Base: "enum", Members: []string{"red", "green"}},
			}),
			want: []string{"shop.t", "e is"},
		},
		{
			name:   "decimal",
			schema: "USE `shop`;\nCREATE TABLE `t` (`id` int NOT NULL, `price` decimal(10,2), PRIMARY KEY (`id`));\n",
			flash: row([]string{"id", "price"}, []string{"1", "1.00"}, []model.FlashCol{
				{Base: "int", HasSign: true},
				{Base: "decimal", HasPrec: true, Prec: 18, Scale: 4},
			}),
			want: []string{"shop.t", "price is decimal(10,2)", "decimal(18,4)"},
		},
		{
			name:   "fsp",
			schema: "USE `shop`;\nCREATE TABLE `t` (`id` int NOT NULL, `ts` datetime(3), PRIMARY KEY (`id`));\n",
			flash: row([]string{"id", "ts"}, []string{"1", "'2026-10-07 00:00:00.000000'"}, []model.FlashCol{
				{Base: "int", HasSign: true},
				{Base: "datetime", HasFSP: true, FSP: 6},
			}),
			want: []string{"shop.t", "ts is datetime(3)", "datetime(6)"},
		},
		{
			name:   "generated values differ",
			schema: strings.Replace(base, "  `c` int DEFAULT NULL,\n", "  `c` int GENERATED ALWAYS AS ((`a` + 1)) STORED,\n", 1),
			flash:  row([]string{"id", "a", "c"}, []string{"1", "10", "99"}, ints),
			want:   []string{"shop.t", "c is generated", "a` + 1"},
		},
		{
			name:   "generated expression unchecked",
			schema: strings.Replace(base, "  `c` int DEFAULT NULL,\n", "  `c` int GENERATED ALWAYS AS (lower(`a`)) VIRTUAL,\n", 1),
			flash:  row([]string{"id", "a", "c"}, []string{"1", "10", "11"}, ints),
			want:   []string{"shop.t", "c is generated", "cannot be checked"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql, err := flashSchemaSQL(t, tc.schema, "", tc.flash)
			if err == nil || sql != "" {
				t.Fatalf("sql %q err %v", sql, err)
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %v, missing %q", err, want)
				}
			}
		})
	}
}

func TestSchemaFileAcceptsMatchingGeneratedAndSkipsIncomparableEnums(t *testing.T) {
	const schema = "USE `shop`;\nCREATE TABLE `gen` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `base` int DEFAULT NULL,\n" +
		"  `virt` int GENERATED ALWAYS AS ((`base` + 1)) VIRTUAL,\n" +
		"  `stor` int GENERATED ALWAYS AS ((`base` * 2)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n" +
		"CREATE TABLE `es` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `e` enum('plain','café') CHARACTER SET latin1,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	sql, err := flashSchemaSQL(t, schema, "",
		model.FlashRow{
			Schema: "shop", Table: "gen", Op: "DELETE",
			Columns: []string{"id", "base", "virt", "stor"},
			Cols:    intCols(4),
			Before:  []string{"1", "10", "11", "20"},
			PK:      []int{0},
		},
		model.FlashRow{
			Schema: "shop", Table: "gen", Op: "DELETE",
			Columns: []string{"id", "base", "virt", "stor"},
			Cols:    intCols(4),
			Before:  []string{"3", "NULL", "NULL", "NULL"},
			PK:      []int{0},
		},
		model.FlashRow{
			Schema: "shop", Table: "es", Op: "DELETE",
			Columns: []string{"id", "e"},
			Cols: []model.FlashCol{
				{Base: "int", HasSign: true},
				{Base: "enum", Charset: "latin1", Members: []string{"plain", "caf\xe9"}},
			},
			Before: []string{"1", "2"},
			PK:     []int{0},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`gen` (`id`, `base`) VALUES (3, NULL);") || strings.Contains(sql, "`virt`") || strings.Contains(sql, "`stor`") {
		t.Fatalf("generated sql:\n%s", sql)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`es` (`id`, `e`) VALUES (1, 2);") {
		t.Fatalf("enum sql:\n%s", sql)
	}
}

func TestSchemaFileLayoutsBindWithoutUSE(t *testing.T) {
	const gen = "CREATE TABLE `gen` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `base` int DEFAULT NULL,\n" +
		"  `virt` int GENERATED ALWAYS AS ((`base` + 1)) VIRTUAL,\n" +
		"  PRIMARY KEY (`id`)\n)"
	const wide = "CREATE TABLE `wide` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `note` varchar(64) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n)"
	genRow := model.FlashRow{
		Schema: "shop", Table: "gen", Op: "DELETE",
		Columns: []string{"id", "base", "virt"}, Cols: intCols(3),
		Before: []string{"1", "10", "11"}, PK: []int{0},
	}
	wideRow := model.FlashRow{
		Schema: "shop", Table: "wide", Op: "DELETE",
		Columns: []string{"id", "note"}, Cols: []model.FlashCol{{Base: "int", HasSign: true}, {Base: "varchar"}},
		Before: []string{"1", "'a'"}, PK: []int{0},
	}
	header := "-- Host: localhost    Database: shop\r\n" + gen + ";\n"
	sql, err := flashSchemaSQL(t, header, "", genRow)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "`virt`") {
		t.Fatalf("header sql:\n%s", sql)
	}

	sql, err = flashSchemaSQL(t, gen+";\n", "shop", genRow)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "`virt`") {
		t.Fatalf("schema-file-db sql:\n%s", sql)
	}

	// Same database spelled differently is still that database.
	sql, err = flashSchemaSQL(t, header, "Shop", genRow)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "`virt`") {
		t.Fatalf("equal fold sql:\n%s", sql)
	}

	batch := "gen\t" + strings.ReplaceAll(gen, "\n", "\\n") + "\nwide\t" + strings.ReplaceAll(wide, "\n", "\\n")
	a := New(Options{Flashback: true, SchemaSQL: batch})
	consumeFlash(t, a, flashEvent(genRow), flashEvent(wideRow))
	sql, err = a.FlashbackSQL()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "`virt`") || !strings.Contains(sql, "INSERT INTO `shop`.`wide` (`id`, `note`) VALUES (1, 'a');") {
		t.Fatalf("multi show create:\n%s", sql)
	}
	if warnings := a.FlashbackWarnings(); len(warnings) != 0 {
		t.Fatalf("warnings: %#v", warnings)
	}
}

func TestSchemaFileAmbiguousDoesNotGuess(t *testing.T) {
	const gen = "CREATE TABLE `gen` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `virt` int GENERATED ALWAYS AS ((`id` + 1)) VIRTUAL,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	row := model.FlashRow{
		Schema: "shop", Table: "gen", Op: "DELETE",
		Columns: []string{"id", "virt"}, Cols: intCols(2),
		Before: []string{"1", "2"}, PK: []int{0},
	}
	a := New(Options{
		Flashback:    true,
		SchemaSQL:    "-- Host: localhost    Database: shop\n" + gen,
		SchemaFileDB: "other",
	})
	consumeFlash(t, a, flashEvent(row))
	sql, err := a.FlashbackSQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "`virt`") {
		t.Fatalf("ambiguous db omitted the column:\n%s", sql)
	}
	warnings := a.FlashbackWarnings()
	if len(warnings) < 2 || !strings.Contains(warnings[0], "shop") || !strings.Contains(warnings[0], "other") || !strings.Contains(strings.Join(warnings, "\n"), "generated columns cannot be ruled out") {
		t.Fatalf("warnings: %#v", warnings)
	}

	both := New(Options{Flashback: true, SchemaSQL: gen})
	other := row
	other.Schema = "other"
	consumeFlash(t, both, flashEvent(row), flashEvent(other))
	sql, err = both.FlashbackSQL()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(sql, "`virt`") != 2 {
		t.Fatalf("two schemas guessed:\n%s", sql)
	}
	text := strings.Join(both.FlashbackWarnings(), "\n")
	if !strings.Contains(text, "gen") || !strings.Contains(text, "shop") || !strings.Contains(text, "other") || !strings.Contains(text, "was not used") {
		t.Fatalf("warnings: %s", text)
	}
}

func TestSchemaFileAlterUsesTheDefinitionInEffect(t *testing.T) {
	const schema = "USE `shop`;\nCREATE TABLE `t` (\n  `id` int NOT NULL,\n  `a` int DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	const gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	selector, err := ParseGTIDSelector(nil, []string{gtid + ":2"})
	if err != nil {
		t.Fatal(err)
	}
	a := New(Options{Flashback: true, SchemaSQL: schema, GTIDSelector: selector})
	base := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid + ":1", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: base, EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 140, PositionEnd: 160},
		{Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "t", BinlogPath: "mysql-bin.000001", PositionStart: 160, PositionEnd: 200, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "t", Op: "DELETE",
			Columns: []string{"id", "a"}, Cols: intCols(2), Before: []string{"1", "10"}, PK: []int{0},
		}}},
		{Timestamp: base, EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 200, PositionEnd: 240},
		{Timestamp: base.Add(time.Second), EventType: "GTID", GTID: gtid + ":2", BinlogPath: "mysql-bin.000001", PositionStart: 240, PositionEnd: 280},
		{Timestamp: base.Add(time.Second), EventType: "DDL", Schema: "shop", Table: "t", QuerySQL: "ALTER TABLE shop.t ADD COLUMN b INT AFTER id", BinlogPath: "mysql-bin.000001", PositionStart: 280, PositionEnd: 320},
		{Timestamp: base.Add(2 * time.Second), EventType: "GTID", GTID: gtid + ":3", BinlogPath: "mysql-bin.000001", PositionStart: 320, PositionEnd: 360},
		{Timestamp: base.Add(2 * time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 360, PositionEnd: 380},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "t", BinlogPath: "mysql-bin.000001", PositionStart: 380, PositionEnd: 420, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "t", Op: "DELETE",
			Columns: []string{"id", "b", "a"}, Cols: intCols(3), Before: []string{"2", "7", "20"}, PK: []int{0},
		}}},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 420, PositionEnd: 460},
	}
	consumeFlash(t, a, events...)
	sql, err := a.FlashbackSQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`t` (`id`, `a`) VALUES (1, 10);") || !strings.Contains(sql, "INSERT INTO `shop`.`t` (`id`, `b`, `a`) VALUES (2, 7, 20);") {
		t.Fatalf("alter sql:\n%s", sql)
	}

	refused := New(Options{Flashback: true, SchemaSQL: schema})
	consumeFlash(t, refused, events...)
	sql, err = refused.FlashbackSQL()
	if err == nil || sql != "" || !strings.Contains(err.Error(), "DDL in the selected range") {
		t.Fatalf("sql %q err %v", sql, err)
	}

	badSel, err := ParseGTIDSelector(nil, []string{gtid + ":1"})
	if err != nil {
		t.Fatal(err)
	}
	bad := New(Options{Flashback: true, SchemaSQL: schema, GTIDSelector: badSel})
	badEvents := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid + ":1", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: base, EventType: "DDL", Schema: "shop", Table: "t", QuerySQL: "ALTER TABLE shop.t ADD COLUMN", BinlogPath: "mysql-bin.000001", PositionStart: 140, PositionEnd: 180},
		{Timestamp: base.Add(time.Second), EventType: "GTID", GTID: gtid + ":2", BinlogPath: "mysql-bin.000001", PositionStart: 180, PositionEnd: 220},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 220, PositionEnd: 240},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "t", BinlogPath: "mysql-bin.000001", PositionStart: 240, PositionEnd: 280, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "t", Op: "DELETE",
			Columns: []string{"id", "a"}, Cols: intCols(2), Before: []string{"1", "10"}, PK: []int{0},
		}}},
		{Timestamp: base.Add(time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 280, PositionEnd: 300},
	}
	consumeFlash(t, bad, badEvents...)
	sql, err = bad.FlashbackSQL()
	if err == nil || sql != "" || !strings.Contains(err.Error(), "shop.t: generated columns are unknown") {
		t.Fatalf("sql %q err %v", sql, err)
	}
}

func TestEnumIndexZeroWrapsOnlyThatStatement(t *testing.T) {
	a := New(Options{Flashback: true})
	consumeFlash(t, a,
		flashEvent(model.FlashRow{
			Schema: "shop", Table: "ezero", Op: "DELETE",
			Columns: []string{"id", "e"}, Before: []string{"2", "1"}, PK: []int{0},
		}),
		flashEvent(model.FlashRow{
			Schema: "shop", Table: "ezero", Op: "DELETE", NonStrict: true,
			Columns: []string{"id", "e"}, Before: []string{"1", "0"}, PK: []int{0},
		}),
	)
	sql, err := a.FlashbackSQL()
	if err != nil {
		t.Fatal(err)
	}
	const save = "SET @binlogviz_sql_mode = @@SESSION.sql_mode;"
	const restore = "SET SESSION sql_mode = @binlogviz_sql_mode;"
	if strings.Count(sql, save) != 1 || strings.Count(sql, restore) != 1 || !strings.Contains(sql, "STRICT_TRANS_TABLES") || !strings.Contains(sql, "STRICT_ALL_TABLES") {
		t.Fatalf("wrap:\n%s", sql)
	}
	i := strings.Index(sql, save)
	j := strings.Index(sql, restore)
	mid := sql[i:j]
	if !strings.Contains(mid, "VALUES (1, 0)") || strings.Contains(mid, "VALUES (2, 1)") {
		t.Fatalf("wrapped the wrong statement:\n%s", sql)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`ezero` (`id`, `e`) VALUES (2, 1);") {
		t.Fatalf("missing strict row:\n%s", sql)
	}
}

func intCols(n int) []model.FlashCol {
	cols := make([]model.FlashCol, n)
	for i := range cols {
		cols[i] = model.FlashCol{Base: "int", HasSign: true}
	}
	return cols
}

func flashEvent(row model.FlashRow) model.NormalizedEvent {
	return model.NormalizedEvent{
		Timestamp: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC),
		EventType: "ROWS", Operation: "DELETE", Schema: row.Schema, Table: row.Table,
		BinlogPath: "mysql-bin.000001", PositionStart: 10, PositionEnd: 20,
		FlashRows: []model.FlashRow{row},
	}
}

func flashSchemaSQL(t *testing.T, schema, db string, rows ...model.FlashRow) (string, error) {
	t.Helper()
	a := New(Options{Flashback: true, SchemaSQL: schema, SchemaFileDB: db})
	events := make([]model.NormalizedEvent, len(rows))
	for i, row := range rows {
		events[i] = flashEvent(row)
		events[i].PositionStart = int64(10 + i*10)
		events[i].PositionEnd = events[i].PositionStart + 10
	}
	consumeFlash(t, a, events...)
	return a.FlashbackSQL()
}

func consumeFlash(t *testing.T, a *Analyzer, events ...model.NormalizedEvent) {
	t.Helper()
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
}
