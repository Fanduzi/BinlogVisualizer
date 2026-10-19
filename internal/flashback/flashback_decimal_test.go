package flashback_test

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

func TestDecimalGeneratedSchemaFile(t *testing.T) {
	const schema = "USE `p179`;\nCREATE TABLE `qd` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `a` bigint DEFAULT NULL,\n" +
		"  `b` bigint DEFAULT NULL,\n" +
		"  `g` decimal(40,4) GENERATED ALWAYS AS ((`a` / `b`)) STORED,\n" +
		"  `g9` decimal(40,9) GENERATED ALWAYS AS ((`a` / `b`)) STORED,\n" +
		"  `mul` decimal(40,4) GENERATED ALWAYS AS (((`a` / `b`) * `b`)) STORED,\n" +
		"  `s1` decimal(40,4) GENERATED ALWAYS AS ((((`a` + `b`) * 3) - 1)) STORED,\n" +
		"  `dv` decimal(20,4) GENERATED ALWAYS AS ((`a` DIV `b`)) STORED,\n" +
		"  `md` decimal(20,4) GENERATED ALWAYS AS (MOD(`a`, `b`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	names := []string{"id", "a", "b", "g", "g9", "mul", "s1", "dv", "md"}
	cols := []model.FlashCol{
		{Base: "int", HasSign: true},
		{Base: "bigint", HasSign: true},
		{Base: "bigint", HasSign: true},
		{Base: "decimal", HasSign: true, HasPrec: true, Prec: 40, Scale: 4},
		{Base: "decimal", HasSign: true, HasPrec: true, Prec: 40, Scale: 9},
		{Base: "decimal", HasSign: true, HasPrec: true, Prec: 40, Scale: 4},
		{Base: "decimal", HasSign: true, HasPrec: true, Prec: 40, Scale: 4},
		{Base: "decimal", HasSign: true, HasPrec: true, Prec: 20, Scale: 4},
		{Base: "decimal", HasSign: true, HasPrec: true, Prec: 20, Scale: 4},
	}
	row := func(before []string) model.FlashRow {
		return model.FlashRow{
			Schema: "p179", Table: "qd", Op: "DELETE",
			Columns: names, Cols: cols, Before: before, PK: []int{0},
		}
	}
	sql, warnings, err := flashSchemaResult(t, schema, "",
		row([]string{"1", "5", "2", "2.5000", "2.500000000", "5.0000", "20.0000", "2.0000", "1.0000"}),
		row([]string{"2", "15", "4", "3.7500", "3.750000000", "15.0000", "56.0000", "3.0000", "3.0000"}),
		row([]string{"3", "1", "7", "0.1429", "0.142857142", "1.0000", "23.0000", "0.0000", "1.0000"}),
		row([]string{"4", "1", "10000000000", "0.0000", "0.000000000", "0.0000", "30000000002.0000", "0.0000", "1.0000"}),
		row([]string{"5", "-5", "2", "-2.5000", "-2.500000000", "-5.0000", "-10.0000", "-2.0000", "-1.0000"}),
		row([]string{"6", "NULL", "2", "NULL", "NULL", "NULL", "NULL", "NULL", "NULL"}),
		row([]string{"7", "1", "0", "NULL", "NULL", "NULL", "2.0000", "NULL", "NULL"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(warnings) != 0 {
		t.Fatalf("correct decimal dump warned: %#v\n%s", warnings, sql)
	}
	for _, col := range []string{"`g`", "`g9`", "`mul`", "`s1`", "`dv`", "`md`"} {
		if sqlAssignsGenerated(sql, col) {
			t.Fatalf("sql assigns %s:\n%s", col, sql)
		}
	}
	if !strings.Contains(sql, "SELECT 1, 5, 2 FROM DUAL WHERE ") || strings.Contains(sql, "2.5000") {
		t.Fatalf("sql:\n%s", sql)
	}
	if strings.Contains(sql, "not verified") {
		t.Fatalf("verified decimal column was marked unverified:\n%s", sql)
	}

	bad, err := flashSchemaSQL(t, schema, "", row([]string{"1", "5", "2", "2.0000", "2.500000000", "5.0000", "20.0000", "2.0000", "1.0000"}))
	if err == nil || bad != "" || !strings.Contains(err.Error(), "does not match") || strings.Contains(err.Error(), "cannot verify") {
		t.Fatalf("wrong decimal sql %q err %v", bad, err)
	}
	bad, err = flashSchemaSQL(t, schema, "", row([]string{"1", "1", "7", "0.1429", "0.142857143", "1.0000", "23.0000", "0.0000", "1.0000"}))
	if err == nil || bad != "" || !strings.Contains(err.Error(), "does not match") || !strings.Contains(err.Error(), "g9") {
		t.Fatalf("scale9 sql %q err %v", bad, err)
	}
}

func TestOutOfRangeSchemaWarns(t *testing.T) {
	const schema = "USE `p179`;\nCREATE TABLE `clip` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `a` int DEFAULT NULL,\n" +
		"  `t` tinyint GENERATED ALWAYS AS ((`a` * 100)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	cols := []model.FlashCol{
		{Base: "int", HasSign: true},
		{Base: "int", HasSign: true},
		{Base: "tinyint", HasSign: true},
	}
	row := func(before []string) model.FlashRow {
		return model.FlashRow{
			Schema: "p179", Table: "clip", Op: "DELETE",
			Columns: []string{"id", "a", "t"}, Cols: cols, Before: before, PK: []int{0},
		}
	}
	sql, warnings, err := flashSchemaResult(t, schema, "",
		row([]string{"1", "1", "100"}),
		row([]string{"2", "5", "127"}),
	)
	if err != nil {
		t.Fatalf("clip dump refused: %v", err)
	}
	if sqlAssignsGenerated(sql, "`t`") {
		t.Fatalf("sql assigns clipped column:\n%s", sql)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "not verified") || !strings.Contains(warnings[0], "t") {
		t.Fatalf("warnings %#v", warnings)
	}
	if !strings.Contains(sql, "not verified") {
		t.Fatalf("sql:\n%s", sql)
	}

	bad, err := flashSchemaSQL(t, schema, "", row([]string{"2", "5", "50"}))
	if err == nil || bad != "" || !strings.Contains(err.Error(), "does not match") || strings.Contains(err.Error(), "cannot verify") {
		t.Fatalf("non-endpoint sql %q err %v", bad, err)
	}
}

func sqlAssignsGenerated(sql, col string) bool {
	for _, line := range strings.Split(sql, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		if strings.Contains(line, col) {
			return true
		}
	}
	return false
}
