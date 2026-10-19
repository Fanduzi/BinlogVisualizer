package flashback_test

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

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
	// #227: 22-30 put the quotient past 30 fractional digits, so they are
	// named as unchecked instead of being dropped from an "only" range.
	for _, want := range []string{"div_precision_increment 0-3", "could not check them at 22-30", "share is generated", "--exclude-table", "does not match", "then FLUSH TABLES `", "waits for statements still running on the table", "Waiting for table flush", "FLUSH TABLES `db`.`tbl`", "printed once per run", "fresh connection", "for example 1"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("missing %q in %s", want, msg)
		}
	}
	if strings.Contains(msg, "open transactions") || strings.Contains(msg, "\n") {
		t.Fatalf("stale FLUSH note or multi-line refusal: %s", msg)
	}
	if strings.Contains(msg, "only at") {
		t.Fatalf("partial fit printed as exact: %s", msg)
	}

	// A value no checked increment produces still names the unchecked ones
	// and says the dump may be right there (#243), keeping the wrong-dump
	// advice last.
	_, err = flashSchemaSQL(t, schema, "",
		row([]string{"1", "1.000000", "3", "0.50000000000000000000"}),
	)
	if err == nil {
		t.Fatal("garbage value accepted")
	}
	for _, want := range []string{"match no increment flashback could check", "div_precision_increment 22-30", "the dump may be right", "for example 22", "use a dump from incident time"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("garbage value: missing %q in %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "the logged values are not") {
		t.Fatalf("unchecked increments got the dump-is-wrong lead: %v", err)
	}

	// Two images with the same inputs and different stored values fit no
	// increment, checked or not: the plain dump message stays.
	_, err = flashSchemaSQL(t, schema, "",
		row([]string{"1", "1.000000", "3", "0.33333333300000000000"}),
		row([]string{"2", "1.000000", "3", "0.33333333333333333300"}),
	)
	if err == nil || strings.Contains(err.Error(), "div_precision_increment") || !strings.Contains(err.Error(), "Use a dump from incident time") {
		t.Fatalf("clash err %v", err)
	}
}
