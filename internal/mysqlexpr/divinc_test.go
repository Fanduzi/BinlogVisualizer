package mysqlexpr

import "testing"

// #193: a correct dump whose / column was stored under a non-default
// div_precision_increment is still refused, but the error names the
// increments that fit instead of only blaming the dump.
func TestExampleIncrement(t *testing.T) {
	for _, c := range []struct {
		in   []int
		want int
	}{{[]int{0, 28, 29, 30}, 28}, {[]int{0, 1, 2, 3}, 1}, {[]int{0}, 0}, {[]int{5, 9}, 5}, {[]int{0, 12}, 12}, {nil, 0}} {
		if got := exampleIncrement(c.in); got != c.want {
			t.Errorf("exampleIncrement(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

// #226: a refused column re-checked at every increment must reach the same
// fit as checking each increment from scratch, whatever image refuses first.
func TestDivIncrementFitMatchesPerIncrement(t *testing.T) {
	meta := []SchemaCol{
		{Name: "a", base: "bigint"},
		{Name: "b", base: "bigint"},
		{Name: "g", base: "decimal", prec: 12, scale: 4, hasPrec: true, Generated: true, Expr: "(`a` / `b`)"},
	}
	cols := []string{"a", "b", "g"}
	rows := []LoggedImage{
		{Columns: cols, Values: []string{"-616", "-374", "1.0000"}},
		{Columns: cols, Values: []string{"7", "3", "2.0000"}},
		{Columns: cols, Values: []string{"10", "4", "2.0000"}},
	}
	g := PrepareGenImages(rows, meta)
	fit, unchecked := g.DivIncrementFit(meta[2])
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
