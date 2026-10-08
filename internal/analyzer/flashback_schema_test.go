package analyzer

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

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
			want:   []string{"shop.t", "does not match", "c is generated", "a` + 1", "example", "--include-table", "incident time"},
		},
		{
			name:   "division truncated",
			schema: strings.Replace(base, "  `c` int DEFAULT NULL,\n", "  `c` int GENERATED ALWAYS AS ((`a` / 2)) STORED,\n", 1),
			flash:  row([]string{"id", "a", "c"}, []string{"1", "5", "2"}, ints),
			want:   []string{"shop.t", "does not match", "c is generated", "/ 2", "--include-table"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql, err := flashSchemaSQL(t, tc.schema, "", tc.flash)
			if err == nil || sql != "" {
				t.Fatalf("sql %q err %v", sql, err)
			}
			if strings.Contains(err.Error(), "cannot verify") {
				t.Fatalf("mismatch was reported as unverifiable: %v", err)
			}
			for _, want := range append(tc.want, "does not match", "--include-table", "incident time", "ALTER") {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %v, missing %q", err, want)
				}
			}
		})
	}
}

func TestSchemaFileVerifiesJSONAndUpper(t *testing.T) {
	const schema = "USE `p160`;\nCREATE TABLE `gen` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `j` json DEFAULT NULL,\n" +
		"  `jv` varchar(20) GENERATED ALWAYS AS (j->>'$.k') VIRTUAL,\n" +
		"  PRIMARY KEY (`id`)\n);\n" +
		"CREATE TABLE `gen_nopk` (\n" +
		"  `a` int DEFAULT NULL,\n" +
		"  `v` varchar(10) GENERATED ALWAYS AS (UPPER(`a`)) VIRTUAL\n);\n" +
		"CREATE TABLE `names` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `first` varchar(40) DEFAULT NULL,\n" +
		"  `last` varchar(40) DEFAULT NULL,\n" +
		"  `full_name` varchar(80) GENERATED ALWAYS AS (concat(`first`,_utf8mb4' ',`last`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n" +
		"CREATE TABLE `j2` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `j` json DEFAULT NULL,\n" +
		"  `jv` varchar(20) GENERATED ALWAYS AS (json_unquote(json_extract(`j`,_utf8mb4'$.k'))) VIRTUAL,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	rows := []model.FlashRow{
		{
			Schema: "p160", Table: "gen", Op: "DELETE",
			Columns: []string{"id", "j", "jv"},
			Cols: []model.FlashCol{
				{Base: "int", HasSign: true},
				{Base: "json"},
				{Base: "varchar", Charset: "utf8mb4"},
			},
			Before: []string{"1", "CAST('{\"k\":\"ab\"}' AS JSON)", "'ab'"},
			PK:     []int{0},
		},
		{
			Schema: "p160", Table: "gen", Op: "DELETE",
			Columns: []string{"id", "j", "jv"},
			Cols: []model.FlashCol{
				{Base: "int", HasSign: true},
				{Base: "json"},
				{Base: "varchar", Charset: "utf8mb4"},
			},
			Before: []string{"2", "CAST('{\"k\":\"cd\"}' AS JSON)", "'cd'"},
			PK:     []int{0},
		},
		{
			Schema: "p160", Table: "gen_nopk", Op: "DELETE", NoPK: true,
			Columns: []string{"a", "v"},
			Cols: []model.FlashCol{
				{Base: "int", HasSign: true},
				{Base: "varchar"},
			},
			Before: []string{"1", "'1'"},
		},
		{
			Schema: "p160", Table: "names", Op: "DELETE",
			Columns: []string{"id", "first", "last", "full_name"},
			Cols: []model.FlashCol{
				{Base: "int", HasSign: true},
				{Base: "varchar", Charset: "utf8mb4"},
				{Base: "varchar", Charset: "utf8mb4"},
				{Base: "varchar", Charset: "utf8mb4"},
			},
			Before: []string{"1", "'Ada'", "'Lovelace'", "'Ada Lovelace'"},
			PK:     []int{0},
		},
		{
			Schema: "p160", Table: "names", Op: "DELETE",
			Columns: []string{"id", "first", "last", "full_name"},
			Cols: []model.FlashCol{
				{Base: "int", HasSign: true},
				{Base: "varchar", Charset: "utf8mb4"},
				{Base: "varchar", Charset: "utf8mb4"},
				{Base: "varchar", Charset: "utf8mb4"},
			},
			Before: []string{"2", "'Grace'", "NULL", "NULL"},
			PK:     []int{0},
		},
		{
			Schema: "p160", Table: "j2", Op: "DELETE",
			Columns: []string{"id", "j", "jv"},
			Cols: []model.FlashCol{
				{Base: "int", HasSign: true},
				{Base: "json"},
				{Base: "varchar", Charset: "utf8mb4"},
			},
			Before: []string{"1", "JSON_OBJECT('k', 'ab')", "'ab'"},
			PK:     []int{0},
		},
	}
	sql, warnings, err := flashSchemaResult(t, schema, "", rows...)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "`jv`") || strings.Contains(sql, "`v`") || strings.Contains(sql, "`full_name`") || !strings.Contains(sql, "INSERT INTO `p160`.`gen` (`id`, `j`)") || !strings.Contains(sql, "VALUES (2, 'Grace', NULL)") {
		t.Fatalf("sql:\n%s", sql)
	}
	if len(warnings) != 0 {
		t.Fatalf("verifiable expressions should not warn: %#v", warnings)
	}
}

func TestSchemaFileIntegerGeneratedOps(t *testing.T) {
	const schema = "USE `p164`;\nCREATE TABLE `ar` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `a` int DEFAULT NULL,\n" +
		"  `b` int DEFAULT NULL,\n" +
		"  `s1` int GENERATED ALWAYS AS (((`a` + `b`) * 3) - 1) STORED,\n" +
		"  `s2` int GENERATED ALWAYS AS ((`a` / 2)) STORED,\n" +
		"  `s3` bigint GENERATED ALWAYS AS ((-(`a`) * `b`)) VIRTUAL,\n" +
		"  `s4` int GENERATED ALWAYS AS ((`a` DIV 2)) VIRTUAL,\n" +
		"  `s5` int GENERATED ALWAYS AS ((`a` % 2)) VIRTUAL,\n" +
		"  `s6` int GENERATED ALWAYS AS (MOD(`a`, 2)) VIRTUAL,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	cols := []model.FlashCol{
		{Base: "int", HasSign: true},
		{Base: "int", HasSign: true},
		{Base: "int", HasSign: true},
		{Base: "int", HasSign: true},
		{Base: "int", HasSign: true},
		{Base: "bigint", HasSign: true},
		{Base: "int", HasSign: true},
		{Base: "int", HasSign: true},
		{Base: "int", HasSign: true},
	}
	names := []string{"id", "a", "b", "s1", "s2", "s3", "s4", "s5", "s6"}
	row := func(before []string) model.FlashRow {
		return model.FlashRow{
			Schema: "p164", Table: "ar", Op: "DELETE",
			Columns: names, Cols: cols, Before: before, PK: []int{0},
		}
	}
	sql, warnings, err := flashSchemaResult(t, schema, "",
		row([]string{"1", "5", "7", "35", "3", "-35", "2", "1", "1"}),
		row([]string{"2", "-5", "NULL", "NULL", "-3", "NULL", "-2", "-1", "-1"}),
		row([]string{"3", "4", "0", "11", "2", "0", "2", "0", "0"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "`s1`") || strings.Contains(sql, "`s2`") || strings.Contains(sql, "`s4`") || !strings.Contains(sql, "VALUES (2, -5, NULL)") {
		t.Fatalf("sql:\n%s", sql)
	}
	if len(warnings) != 0 {
		t.Fatalf("integer expressions should be checked, warnings: %#v", warnings)
	}

	bad, err := flashSchemaSQL(t, schema, "", row([]string{"1", "5", "7", "35", "2", "-35", "3", "1", "1"}))
	if err == nil || bad != "" || !strings.Contains(err.Error(), "does not match") || !strings.Contains(err.Error(), "s2") {
		t.Fatalf("sql %q err %v", bad, err)
	}
}

func TestSchemaFileRefusesGeneratedContradiction(t *testing.T) {
	const schema = "USE `shop`;\nCREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `x` varchar(20) DEFAULT NULL,\n" +
		"  `c` varchar(20) GENERATED ALWAYS AS (UPPER(`x`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	cols := []model.FlashCol{
		{Base: "int", HasSign: true},
		{Base: "varchar", Charset: "utf8mb4"},
		{Base: "varchar", Charset: "utf8mb4"},
	}
	row := model.FlashRow{
		Schema: "shop", Table: "t", Op: "UPDATE",
		Columns: []string{"id", "x", "c"}, Cols: cols,
		Before: []string{"1", "'abc'", "'manual-1'"},
		After:  []string{"1", "'abc'", "'oops'"},
		PK:     []int{0},
	}
	sql, _, err := flashSchemaResult(t, schema, "", row)
	if err == nil || sql != "" {
		t.Fatalf("sql %q err %v", sql, err)
	}
	for _, want := range []string{"shop.t", "c is generated", "UPPER", "manual-1", "does not match", "--include-table"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %v, missing %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "cannot verify") || strings.Contains(err.Error(), "--allow-unverified-generated") {
		t.Fatalf("contradiction looked unverified: %v", err)
	}
}

func TestSchemaFileAsciiExampleIsText(t *testing.T) {
	const schema = "USE `shop`;\nCREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `code` varchar(20) CHARACTER SET ascii DEFAULT NULL,\n" +
		"  `c` varchar(20) CHARACTER SET ascii GENERATED ALWAYS AS (upper(`code`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	row := model.FlashRow{
		Schema: "shop", Table: "t", Op: "DELETE",
		Columns: []string{"id", "code", "c"},
		Cols: []model.FlashCol{
			{Base: "int", HasSign: true},
			{Base: "varchar", Charset: "ascii"},
			{Base: "varchar", Charset: "ascii"},
		},
		Before: []string{"1", "_ascii 0x6162", "_ascii 0x6E6F74652D31"},
		PK:     []int{0},
	}
	sql, _, err := flashSchemaResult(t, schema, "", row)
	if err == nil || sql != "" {
		t.Fatalf("sql %q err %v", sql, err)
	}
	if !strings.Contains(err.Error(), "note-1") || !strings.Contains(err.Error(), "code='ab'") || strings.Contains(err.Error(), "0x6E6F74652D31") {
		t.Fatalf("example stayed hex: %v", err)
	}
}

func TestSchemaFileEnumSetExampleUsesLabels(t *testing.T) {
	const schema = "USE `shop`;\nCREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `e` enum('a','b') CHARACTER SET utf8mb4,\n" +
		"  `s` set('x','y') CHARACTER SET utf8mb4,\n" +
		"  `c` varchar(20) CHARACTER SET utf8mb4 GENERATED ALWAYS AS (upper(`e`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	row := model.FlashRow{
		Schema: "shop", Table: "t", Op: "DELETE",
		Columns: []string{"id", "e", "s", "c"},
		Cols: []model.FlashCol{
			{Base: "int", HasSign: true},
			{Base: "enum", Members: []string{"a", "b"}, Charset: "utf8mb4"},
			{Base: "set", Members: []string{"x", "y"}, Charset: "utf8mb4"},
			{Base: "varchar", Charset: "utf8mb4"},
		},
		Before: []string{"1", "1", "3", "'no'"},
		PK:     []int{0},
	}
	sql, _, err := flashSchemaResult(t, schema, "", row)
	if err == nil || sql != "" {
		t.Fatalf("sql %q err %v", sql, err)
	}
	if !strings.Contains(err.Error(), "e='a'") || !strings.Contains(err.Error(), "s='x,y'") {
		t.Fatalf("example kept indexes: %v", err)
	}
}

func TestSchemaFileRefusesDependencyClash(t *testing.T) {
	const schema = "USE `shop`;\nCREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `x` varchar(20) DEFAULT NULL,\n" +
		"  `c` varchar(40) GENERATED ALWAYS AS (MD5(`x`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	cols := []model.FlashCol{
		{Base: "int", HasSign: true},
		{Base: "varchar", Charset: "utf8mb4"},
		{Base: "varchar", Charset: "utf8mb4"},
	}
	update := model.FlashRow{
		Schema: "shop", Table: "t", Op: "UPDATE",
		Columns: []string{"id", "x", "c"}, Cols: cols,
		Before: []string{"1", "'abc'", "'manual-1'"},
		After:  []string{"1", "'abc'", "'oops'"},
		PK:     []int{0},
	}
	insert := model.FlashRow{
		Schema: "shop", Table: "t", Op: "INSERT",
		Columns: []string{"id", "x", "c"}, Cols: cols,
		After: []string{"1", "'abc'", "'one'"},
		PK:    []int{0},
	}
	deleted := model.FlashRow{
		Schema: "shop", Table: "t", Op: "DELETE",
		Columns: []string{"id", "x", "c"}, Cols: cols,
		Before: []string{"2", "'abc'", "'two'"},
		PK:     []int{0},
	}
	for _, rows := range [][]model.FlashRow{{update}, {insert, deleted}} {
		sql, _, err := flashSchemaResult(t, schema, "", rows...)
		if err == nil || sql != "" {
			t.Fatalf("sql %q err %v", sql, err)
		}
		for _, want := range []string{"shop.t", "c is generated", "MD5", "does not match"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %v, missing %q", err, want)
			}
		}
	}
}

func TestSchemaFileReportsEveryContradiction(t *testing.T) {
	const schema = "USE `shop`;\nCREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `x` varchar(20) DEFAULT NULL,\n" +
		"  `c` varchar(20) GENERATED ALWAYS AS (UPPER(`x`)) STORED,\n" +
		"  `d` varchar(20) GENERATED ALWAYS AS (UPPER(`x`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n" +
		"CREATE TABLE `u` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `x` varchar(20) DEFAULT NULL,\n" +
		"  `c` varchar(20) GENERATED ALWAYS AS (UPPER(`x`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	cols := []model.FlashCol{
		{Base: "int", HasSign: true},
		{Base: "varchar", Charset: "utf8mb4"},
		{Base: "varchar", Charset: "utf8mb4"},
		{Base: "varchar", Charset: "utf8mb4"},
	}
	ucols := cols[:3]
	one := model.FlashRow{
		Schema: "shop", Table: "t", Op: "UPDATE",
		Columns: []string{"id", "x", "c", "d"}, Cols: cols,
		Before: []string{"1", "'ab'", "'no'", "'no'"},
		After:  []string{"1", "'ab'", "'NO'", "'NO'"},
		PK:     []int{0},
	}
	two := model.FlashRow{
		Schema: "shop", Table: "u", Op: "UPDATE",
		Columns: []string{"id", "x", "c"}, Cols: ucols,
		Before: []string{"1", "'ab'", "'zz'"},
		After:  []string{"1", "'ab'", "'zz'"},
		PK:     []int{0},
	}
	sql, _, err := flashSchemaResult(t, schema, "", one, two)
	if err == nil || sql != "" {
		t.Fatalf("sql %q err %v", sql, err)
	}
	if strings.Count(err.Error(), "Error:") != 0 {
		t.Fatalf("contradiction text contains Error:: %v", err)
	}
	for _, want := range []string{"shop.t", "shop.u", "c is generated", "d is generated"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %v, missing %q", err, want)
		}
	}
}

func TestEscapedQuoteGeneratedExpr(t *testing.T) {
	const schema = "USE `shop`;\nCREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `s` varchar(20) DEFAULT NULL,\n" +
		"  `g` varchar(40) GENERATED ALWAYS AS (concat(`s`,_utf8mb4'it\\'s',_utf8mb4'\\\\t')) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	row := model.FlashRow{
		Schema: "shop", Table: "t", Op: "DELETE",
		Columns: []string{"id", "s", "g"},
		Cols: []model.FlashCol{
			{Base: "int", HasSign: true},
			{Base: "varchar", Charset: "utf8mb4"},
			{Base: "varchar", Charset: "utf8mb4"},
		},
		Before: []string{"1", "'a'", `'ait\'s\\t'`},
		PK:     []int{0},
	}
	sql, warnings, err := flashSchemaResult(t, schema, "", row)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 || strings.Contains(sql, "`g`") && strings.Contains(sql, "VALUES") && strings.Contains(sql, "`g`,") {
		t.Fatalf("warnings %#v sql:\n%s", warnings, sql)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`t` (`id`, `s`) VALUES (1, 'a');") {
		t.Fatalf("sql:\n%s", sql)
	}
	if !strings.Contains(sql, "'shop.t.g'") {
		t.Fatalf("guard missing shop.t.g:\n%s", sql)
	}
}

func TestSchemaFileUnverifiedGeneratedWarns(t *testing.T) {
	const schema = "USE `shop`;\nCREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `x` varchar(20) DEFAULT NULL,\n" +
		"  `c` varchar(40) GENERATED ALWAYS AS (MD5(`x`)) STORED,\n" +
		"  `note` varchar(20) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	cols := []model.FlashCol{
		{Base: "int", HasSign: true},
		{Base: "varchar", Charset: "utf8mb4"},
		{Base: "varchar", Charset: "utf8mb4"},
		{Base: "varchar", Charset: "utf8mb4"},
	}
	row := model.FlashRow{
		Schema: "shop", Table: "t", Op: "UPDATE",
		Columns: []string{"id", "x", "c", "note"}, Cols: cols,
		Before: []string{"1", "'abc'", "'aaa'", "'keep'"},
		After:  []string{"1", "'def'", "'bbb'", "'keep'"},
		PK:     []int{0},
	}
	sql, warnings, err := flashSchemaResult(t, schema, "", row)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "SET `c`") || strings.Contains(sql, "`c`,") || strings.Contains(sql, ", `c`") || !strings.Contains(sql, "`note`") || !strings.Contains(sql, "-- WARNING: generated column shop.t.c not verified") || !strings.Contains(sql, "The guard below checks") || !strings.Contains(sql, "schema file does not match the target") || !strings.Contains(sql, "'shop.t.c'") {
		t.Fatalf("sql:\n%s", sql)
	}
	if !guardBeforeTransaction(sql) {
		t.Fatalf("guard is not in the header:\n%s", sql)
	}
	if !strings.Contains(sql, "FROM DUAL WHERE NOT EXISTS") || strings.Contains(sql, "AS q WHERE") {
		t.Fatalf("guard arm is not SELECT ... FROM DUAL:\n%s", sql)
	}
	text := strings.Join(warnings, "\n")
	if !strings.Contains(text, "shop.t.c") || !strings.Contains(text, "not verified") || !strings.Contains(text, "MD5") || !strings.Contains(text, "guard") {
		t.Fatalf("warnings:\n%s", text)
	}
}

func guardBeforeTransaction(sql string) bool {
	guard := strings.Index(sql, "binlogviz_mismatch")
	gtid := strings.Index(sql, "-- gtid:")
	sets := strings.Index(sql, "SET NAMES utf8mb4;")
	return guard > sets && (gtid < 0 || guard < gtid)
}

func TestGeneratedGuardReadOnlyText(t *testing.T) {
	const schema = "USE `shop`;\nCREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `x` varchar(20) DEFAULT NULL,\n" +
		"  `c` varchar(32) GENERATED ALWAYS AS (md5(`x`)) STORED,\n" +
		"  `d` varchar(40) GENERATED ALWAYS AS (sha(`x`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	cols := []model.FlashCol{
		{Base: "int", HasSign: true},
		{Base: "varchar", Charset: "utf8mb4"},
		{Base: "varchar", Charset: "utf8mb4"},
		{Base: "varchar", Charset: "utf8mb4"},
	}
	row := model.FlashRow{
		Schema: "shop", Table: "t", Op: "UPDATE",
		Columns: []string{"id", "x", "c", "d"}, Cols: cols,
		Before: []string{"1", "'ab'", "'old-c'", "'old-d'"},
		After:  []string{"1", "'cd'", "'new-c'", "'new-d'"},
		PK:     []int{0},
	}
	sql, _, err := flashSchemaResult(t, schema, "", row)
	if err != nil {
		t.Fatal(err)
	}
	execAt := strings.Index(sql, "EXECUTE binlogviz_guard;")
	lockAt := strings.Index(sql, "PREPARE binlogviz_lock FROM @binlogviz_lock_sql;")
	modeAt := strings.Index(sql, "SET SESSION sql_mode = @binlogviz_mode;")
	if execAt < 0 || lockAt < 0 || modeAt < 0 || execAt > lockAt || lockAt > modeAt {
		t.Fatalf("message, lock, then sql_mode:\n%s", sql)
	}
	if strings.Count(sql, guardLockSQL) < 2 {
		t.Fatalf("lock is not repeated before the transaction:\n%s", sql)
	}
	start := strings.Index(sql, "START TRANSACTION;")
	if start < 0 || !strings.HasSuffix(sql[:start], guardLockSQL) {
		t.Fatalf("lock is not immediately before START TRANSACTION:\n%s", sql)
	}
	for _, want := range []string{"SEPARATOR ', '", "SEPARATOR ' | '", "200", "columns)", "COMMIT;"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("missing %q\n%s", want, sql)
		}
	}
	if !guardBeforeTransaction(sql) {
		t.Fatalf("guard is not in the header:\n%s", sql)
	}
}

func TestGeneratedGuardOldServerSQL(t *testing.T) {
	const schema = "USE `shop`;\nCREATE TABLE `t` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `c` varchar(32) GENERATED ALWAYS AS (md5(`id`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	cols := []model.FlashCol{
		{Base: "int", HasSign: true},
		{Base: "varchar", Charset: "utf8mb4"},
	}
	row := model.FlashRow{
		Schema: "shop", Table: "t", Op: "DELETE",
		Columns: []string{"id", "c"}, Cols: cols,
		Before: []string{"1", "'abc'"},
		PK:     []int{0},
	}
	sql, _, err := flashSchemaResult(t, schema, "", row)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(guardOldServerMsg, ",") || len(guardOldServerMsg) > guardValueLimit || guardOldServerMsg != "binlogviz: target MySQL < 5.7.0 is not supported for apply" {
		t.Fatalf("message: %q", guardOldServerMsg)
	}
	oldAt := strings.Index(sql, guardOldServerCheckSQL())
	execAt := strings.Index(sql, "EXECUTE binlogviz_guard;")
	early := "SET SESSION sql_mode = IF(@binlogviz_old, @binlogviz_mode, @@SESSION.sql_mode);"
	earlyAt := strings.Index(sql, early)
	lockSQL := "SET @binlogviz_lock_sql = IF(@binlogviz_mismatch IS NULL OR @binlogviz_old, 'DO 0', CONCAT('SET SESSION ', @binlogviz_ro, ' = 1'));"
	lockAt := strings.Index(sql, lockSQL)
	prepAt := strings.Index(sql, "PREPARE binlogviz_lock FROM @binlogviz_lock_sql;")
	if oldAt < 0 || execAt < 0 || earlyAt < 0 || lockAt < 0 || prepAt < 0 || oldAt > execAt || execAt > earlyAt || earlyAt > lockAt || lockAt > prepAt {
		t.Fatalf("version check, message, then prepared lock:\n%s", sql)
	}
	if strings.Contains(sql, "SET SESSION transaction_read_only =") || strings.Contains(sql, "SET SESSION tx_read_only =") {
		t.Fatalf("lock names the variable in a direct SET:\n%s", sql)
	}
	for _, want := range []string{
		guardOldServerMsg,
		"SET @binlogviz_mode = IF(@binlogviz_old, " + sqlQuote(guardOldServerMsg) + ", @binlogviz_mode);",
		"SUBSTRING_INDEX(@@version, '-', 1)",
		"@binlogviz_minor >= 7",
		"'transaction_read_only'",
		"'tx_read_only'",
		"@binlogviz_patch >= 20",
		"Apply requires MySQL 5.7 or newer.",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("missing %q\n%s", want, sql)
		}
	}
	if !guardBeforeTransaction(sql) {
		t.Fatalf("guard is not in the header:\n%s", sql)
	}
}

func TestGuardSafeLabel(t *testing.T) {
	if got := guardSafeLabel("db.t.a,b"); got != "db.t.a;b" {
		t.Fatalf("comma: %q", got)
	}
	if got := guardSafeLabel("db.t.c | d"); got != "db.t.c / d" {
		t.Fatalf("separator: %q", got)
	}
	if got := guardSafeLabel("shop.t.c"); got != "shop.t.c" {
		t.Fatalf("plain: %q", got)
	}
}

func TestTrimGuardList(t *testing.T) {
	const list = "aa | bb | cc"
	cases := []struct {
		room int
		want string
	}{
		{room: 7, want: "aa | bb"},
		{room: 6, want: "aa"},
		{room: 2, want: "aa"},
		{room: 1, want: ""},
		{room: len(list), want: list},
		{room: -1, want: ""},
	}
	for _, tc := range cases {
		if got := trimGuardList(list, tc.room); got != tc.want {
			t.Fatalf("room %d: %q want %q", tc.room, got, tc.want)
		}
	}
}

func TestGuardErrorValue(t *testing.T) {
	two := guardErrorValue([]string{guardSafeLabel("p183.t.c"), guardSafeLabel("p183.heap.note")})
	if strings.Contains(two, ",") || strings.Contains(two, "(2 columns)") || two != guardMismatchLead+": p183.t.c | p183.heap.note" {
		t.Fatalf("two: %q", two)
	}
	safe := []string{guardSafeLabel("db.t.a,b"), guardSafeLabel("db.t.c | d")}
	mixed := guardErrorValue(safe)
	if strings.Contains(mixed, ",") || !strings.Contains(mixed, "db.t.a;b") || !strings.Contains(mixed, "db.t.c / d") {
		t.Fatalf("safe names: %q", mixed)
	}
	names := make([]string, 12)
	for i := range names {
		names[i] = fmt.Sprintf("p186.guardwide.col_%02d", i+1)
	}
	wide := guardErrorValue(names)
	if !strings.Contains(wide, "(12 columns)") || !strings.Contains(wide, "p186.guardwide.col_01") || !strings.Contains(wide, "p186.guardwide.col_05") || strings.Contains(wide, "p186.guardwide.col_06") || strings.Contains(wide, ",") {
		t.Fatalf("wide: %q", wide)
	}
	if utf8.RuneCountInString(wide) > guardValueLimit || len(wide) > guardValueLimit {
		t.Fatalf("wide length runes=%d bytes=%d", utf8.RuneCountInString(wide), len(wide))
	}
	huge := guardErrorValue([]string{strings.Repeat("字", 60)})
	if huge != guardMismatchLead+" (1 columns)" {
		t.Fatalf("multibyte: %q", huge)
	}
}

func TestEvalIntExprMySQLAssignment(t *testing.T) {
	cases := []struct {
		expr string
		env  map[string]string
		want string
		ok   bool
	}{
		{expr: "(a + b) * 3 - 1", env: map[string]string{"a": "5", "b": "7"}, want: "35", ok: true},
		{expr: "a / 2", env: map[string]string{"a": "5"}, want: "3", ok: true},
		{expr: "a / 2", env: map[string]string{"a": "-5"}, want: "-3", ok: true},
		{expr: "-a * b", env: map[string]string{"a": "5", "b": "7"}, want: "-35", ok: true},
		{expr: "a DIV 2", env: map[string]string{"a": "5"}, want: "2", ok: true},
		{expr: "a div 2", env: map[string]string{"a": "-5"}, want: "-2", ok: true},
		{expr: "a % 2", env: map[string]string{"a": "5"}, want: "1", ok: true},
		{expr: "a MOD 2", env: map[string]string{"a": "-5"}, want: "-1", ok: true},
		{expr: "MOD(a, 2)", env: map[string]string{"a": "5"}, want: "1", ok: true},
		{expr: "5 % -2", want: "1", ok: true},
		{expr: "-5 DIV -2", want: "2", ok: true},
		{expr: "1/2", want: "1", ok: true},
		{expr: "-1/2", want: "-1", ok: true},
		{expr: "2/3", want: "1", ok: true},
		{expr: "1/3", want: "0", ok: true},
		{expr: "-2/3", want: "-1", ok: true},
		{expr: "7/2", want: "4", ok: true},
		{expr: "-7/2", want: "-4", ok: true},
		{expr: "(a + b) * 3 - 1", env: map[string]string{"a": "-5", "b": "NULL"}, want: "NULL", ok: true},
		{expr: "-a * b", env: map[string]string{"a": "-5", "b": "NULL"}, want: "NULL", ok: true},
		{expr: "(a + b) * 3 - 1", env: map[string]string{"a": "4", "b": "0"}, want: "11", ok: true},
		{expr: "-a * b", env: map[string]string{"a": "4", "b": "0"}, want: "0", ok: true},
		{expr: "a % b", env: map[string]string{"a": "4", "b": "0"}, want: "NULL", ok: true},
		{expr: "a / 0", env: map[string]string{"a": "4"}, want: "NULL", ok: true},
		{expr: "UPPER(a)", env: map[string]string{"a": "1"}, ok: false},
		{expr: "j->>'$.k'", env: map[string]string{"j": "1"}, ok: false},
		{expr: "CONCAT(a, b)", env: map[string]string{"a": "1", "b": "2"}, ok: false},
	}
	for _, tc := range cases {
		v, ok := evalIntExpr(tc.expr, tc.env)
		if ok != tc.ok {
			t.Fatalf("%s ok=%v want %v", tc.expr, ok, tc.ok)
		}
		if !tc.ok {
			continue
		}
		got := "NULL"
		if !v.null {
			got = roundRatHalfAway(v.n).String()
		}
		if got != tc.want {
			t.Fatalf("%s = %s, want %s", tc.expr, got, tc.want)
		}
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
	if strings.Count(sql, save) != 1 || strings.Count(sql, restore) != 1 || !strings.Contains(sql, "STRICT_TRANS_TABLES") || !strings.Contains(sql, "STRICT_ALL_TABLES") || !strings.Contains(sql, ",TRADITIONAL,") {
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
	sql, _, err := flashSchemaResult(t, schema, db, rows...)
	return sql, err
}

func flashSchemaResult(t *testing.T, schema, db string, rows ...model.FlashRow) (string, []string, error) {
	t.Helper()
	a := New(Options{Flashback: true, SchemaSQL: schema, SchemaFileDB: db})
	events := make([]model.NormalizedEvent, len(rows))
	for i, row := range rows {
		events[i] = flashEvent(row)
		events[i].PositionStart = int64(10 + i*10)
		events[i].PositionEnd = events[i].PositionStart + 10
	}
	consumeFlash(t, a, events...)
	sql, err := a.FlashbackSQL()
	return sql, a.FlashbackWarnings(), err
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
