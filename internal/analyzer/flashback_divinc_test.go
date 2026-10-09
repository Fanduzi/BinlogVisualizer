package analyzer

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

// #193: a correct dump whose / column was stored under a non-default
// div_precision_increment is still refused, but the error names the
// increments that fit instead of only blaming the dump.
func TestDivIncrementHint(t *testing.T) {
	schema := "CREATE TABLE `r` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `amt` decimal(20,6) DEFAULT NULL,\n" +
		"  `n` int DEFAULT NULL,\n" +
		"  `share` decimal(40,20) GENERATED ALWAYS AS ((`amt` / `n`)) STORED,\n" +
		"  PRIMARY KEY (`id`)\n);\n"
	names := []string{"id", "amt", "n", "share"}
	cols := []model.FlashCol{
		{Base: "int", HasSign: true},
		{Base: "decimal", HasSign: true, HasPrec: true, Prec: 20, Scale: 6},
		{Base: "int", HasSign: true},
		{Base: "decimal", HasSign: true, HasPrec: true, Prec: 40, Scale: 20},
	}
	row := func(before []string) model.FlashRow {
		return model.FlashRow{
			Schema: "p179p", Table: "r", Op: "DELETE",
			Columns: names, Cols: cols, Before: before, PK: []int{0},
		}
	}

	// Default increment: accepted.
	if _, err := flashSchemaSQL(t, schema, "",
		row([]string{"1", "1.000000", "3", "0.33333333333333333300"}),
		row([]string{"2", "100.000000", "7", "14.28571428571428571400"}),
	); err != nil {
		t.Fatalf("default increment refused: %v", err)
	}

	// Increment 0 to 3 (what MySQL stores): refused, with the hint.
	_, err := flashSchemaSQL(t, schema, "",
		row([]string{"1", "1.000000", "3", "0.33333333300000000000"}),
		row([]string{"2", "100.000000", "7", "14.28571428500000000000"}),
	)
	if err == nil {
		t.Fatal("non-default increment accepted")
	}
	msg := err.Error()
	for _, want := range []string{"div_precision_increment 0-3", "share is generated", "--exclude-table", "does not match"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("missing %q in %s", want, msg)
		}
	}

	// A value no increment produces keeps the plain dump message.
	_, err = flashSchemaSQL(t, schema, "",
		row([]string{"1", "1.000000", "3", "0.50000000000000000000"}),
	)
	if err == nil || strings.Contains(err.Error(), "div_precision_increment") || !strings.Contains(err.Error(), "Use a dump from incident time") {
		t.Fatalf("garbage value err %v", err)
	}
}

func TestIntRanges(t *testing.T) {
	if got := intRanges([]int{0, 1, 2, 3, 5, 8, 9, 10}); got != "0-3, 5, 8-10" {
		t.Fatal(got)
	}
}
