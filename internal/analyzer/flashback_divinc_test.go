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
	// #227: 22-30 put the quotient past 30 fractional digits, so they are
	// named as unchecked instead of being dropped from an "only" range.
	for _, want := range []string{"div_precision_increment 0-3", "could not check them at 22-30", "share is generated", "--exclude-table", "does not match"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("missing %q in %s", want, msg)
		}
	}
	if strings.Contains(msg, "only at") {
		t.Fatalf("partial fit printed as exact: %s", msg)
	}

	// A value no checked increment produces still names the unchecked ones
	// and keeps the wrong-dump advice.
	_, err = flashSchemaSQL(t, schema, "",
		row([]string{"1", "1.000000", "3", "0.50000000000000000000"}),
	)
	if err == nil {
		t.Fatal("garbage value accepted")
	}
	for _, want := range []string{"match no increment flashback could check", "div_precision_increment 22-30", "use a dump from incident time"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("garbage value: missing %q in %v", want, err)
		}
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

// #226: a refused column re-checked at every increment must reach the same
// fit as checking each increment from scratch, whatever image refuses first.
func TestDivIncrementFitMatchesPerIncrement(t *testing.T) {
	meta := []schemaCol{
		{name: "a", base: "bigint"},
		{name: "b", base: "bigint"},
		{name: "g", base: "decimal", prec: 12, scale: 4, hasPrec: true, generated: true, expr: "(`a` / `b`)"},
	}
	cols := []string{"a", "b", "g"}
	rows := []loggedImage{
		{columns: cols, values: []string{"-616", "-374", "1.0000"}},
		{columns: cols, values: []string{"7", "3", "2.0000"}},
		{columns: cols, values: []string{"10", "4", "2.0000"}},
	}
	g := prepareGenImages(rows, meta)
	fit, unchecked := g.divIncrementFit(meta[2])
	var want, wantUnchecked []int
	for inc := 0; inc <= mysqlMaxDivPrecIncrement; inc++ {
		if inc == mysqlDefaultDivPrecIncrement {
			continue
		}
		bad, unknown := g.checkInc(meta[2], inc, -1)
		switch {
		case bad >= 0:
		case unknown:
			wantUnchecked = append(wantUnchecked, inc)
		default:
			want = append(want, inc)
		}
	}
	if intRanges(fit) != intRanges(want) || intRanges(unchecked) != intRanges(wantUnchecked) {
		t.Fatalf("fit %v unchecked %v, want %v %v", fit, unchecked, want, wantUnchecked)
	}
	if intRanges(fit) != "0" {
		t.Fatalf("integer quotient truncates only at increment 0, got fit %v", fit)
	}
}

func TestIntRanges(t *testing.T) {
	if got := intRanges([]int{0, 1, 2, 3, 5, 8, 9, 10}); got != "0-3, 5, 8-10" {
		t.Fatal(got)
	}
}
