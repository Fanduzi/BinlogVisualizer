package analyzer

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

func TestMySQLDivFracWidth(t *testing.T) {
	cases := []struct{ f1, f2, inc, want int }{
		{0, 0, 4, 9},
		{0, 0, 0, 0},
		{0, 0, 1, 9},
		{0, 0, 9, 9},
		{0, 0, 10, 18},
		{2, 2, 4, 18},
		{1, 0, 4, 9},
		{4, 4, 4, 18},
	}
	for _, tc := range cases {
		if got := mysqlDivFrac(tc.f1, tc.f2, tc.inc); got != tc.want {
			t.Fatalf("mysqlDivFrac(%d,%d,%d)=%d want %d", tc.f1, tc.f2, tc.inc, got, tc.want)
		}
	}
}

func TestDecimalGeneratedDivisionMatrix(t *testing.T) {
	// Logged values are MySQL 8.0.46, div_precision_increment=4.
	// Integer / integer keeps 9 fractional digits and cuts the rest off.
	// Assignment then rounds half away from zero to the column scale.
	type row struct {
		name       string
		a, b       string
		scale      int
		lit        string
		want       string
		unsignedOp bool
	}
	const (
		match      = "match"
		mismatch   = "mismatch"
		unverified = "unverified"
	)
	rows := []row{
		{name: "5/2 scale4", a: "5", b: "2", scale: 4, lit: "2.5000", want: match},
		{name: "5/2 stripped", a: "5", b: "2", scale: 4, lit: "2.5", want: match},
		{name: "5/2 scale9", a: "5", b: "2", scale: 9, lit: "2.500000000", want: match},
		{name: "5/2 scale0", a: "5", b: "2", scale: 0, lit: "3", want: match},
		{name: "7/2 scale4", a: "7", b: "2", scale: 4, lit: "3.5000", want: match},
		{name: "15/4 exact", a: "15", b: "4", scale: 4, lit: "3.7500", want: match},
		{name: "15/4 scale0", a: "15", b: "4", scale: 0, lit: "4", want: match},
		{name: "-5/2", a: "-5", b: "2", scale: 4, lit: "-2.5000", want: match},
		{name: "-5/2 scale0", a: "-5", b: "2", scale: 0, lit: "-3", want: match},
		{name: "5/-2", a: "5", b: "-2", scale: 4, lit: "-2.5000", want: match},
		{name: "-5/-2", a: "-5", b: "-2", scale: 4, lit: "2.5000", want: match},
		{name: "-7/2", a: "-7", b: "2", scale: 4, lit: "-3.5000", want: match},
		{name: "-7/2 scale0", a: "-7", b: "2", scale: 0, lit: "-4", want: match},
		{name: "9/2 half up", a: "9", b: "2", scale: 4, lit: "4.5000", want: match},
		{name: "9/2 scale0", a: "9", b: "2", scale: 0, lit: "5", want: match},
		{name: "1/2 scale0", a: "1", b: "2", scale: 0, lit: "1", want: match},
		{name: "-1/2 scale0", a: "-1", b: "2", scale: 0, lit: "-1", want: match},
		{name: "-1/2 scale1", a: "-1", b: "2", scale: 1, lit: "-0.5", want: match},
		{name: "1/2 scale1", a: "1", b: "2", scale: 1, lit: "0.5", want: match},
		{name: "3/2 scale1", a: "3", b: "2", scale: 1, lit: "1.5", want: match},
		{name: "-3/2 scale0", a: "-3", b: "2", scale: 0, lit: "-2", want: match},
		{name: "1/8 scale1", a: "1", b: "8", scale: 1, lit: "0.1", want: match},
		{name: "1/8 scale2", a: "1", b: "8", scale: 2, lit: "0.13", want: match},
		{name: "1/8 scale4", a: "1", b: "8", scale: 4, lit: "0.1250", want: match},
		{name: "3/8 scale1", a: "3", b: "8", scale: 1, lit: "0.4", want: match},
		{name: "-1/8 scale1", a: "-1", b: "8", scale: 1, lit: "-0.1", want: match},
		{name: "-3/8 scale1", a: "-3", b: "8", scale: 1, lit: "-0.4", want: match},
		{name: "5/4 scale1", a: "5", b: "4", scale: 1, lit: "1.3", want: match},
		{name: "-5/4 scale1", a: "-5", b: "4", scale: 1, lit: "-1.3", want: match},
		{name: "-5/4 scale0", a: "-5", b: "4", scale: 0, lit: "-1", want: match},
		{name: "1/16 scale1", a: "1", b: "16", scale: 1, lit: "0.1", want: match},
		{name: "3/16 scale1", a: "3", b: "16", scale: 1, lit: "0.2", want: match},
		{name: "1/3 scale4", a: "1", b: "3", scale: 4, lit: "0.3333", want: match},
		{name: "1/3 scale2", a: "1", b: "3", scale: 2, lit: "0.33", want: match},
		{name: "1/3 scale6", a: "1", b: "3", scale: 6, lit: "0.333333", want: match},
		{name: "1/3 scale9", a: "1", b: "3", scale: 9, lit: "0.333333333", want: match},
		{name: "1/3 scale10", a: "1", b: "3", scale: 10, lit: "0.3333333330", want: match},
		{name: "1/3 scale0", a: "1", b: "3", scale: 0, lit: "0", want: match},
		{name: "2/3 scale4", a: "2", b: "3", scale: 4, lit: "0.6667", want: match},
		{name: "2/3 scale9 not rounded", a: "2", b: "3", scale: 9, lit: "0.666666666", want: match},
		{name: "2/3 scale9 exact-round is wrong", a: "2", b: "3", scale: 9, lit: "0.666666667", want: mismatch},
		{name: "2/3 scale8", a: "2", b: "3", scale: 8, lit: "0.66666667", want: match},
		{name: "-1/3 scale4", a: "-1", b: "3", scale: 4, lit: "-0.3333", want: match},
		{name: "-1/3 scale9", a: "-1", b: "3", scale: 9, lit: "-0.333333333", want: match},
		{name: "-2/3 scale9", a: "-2", b: "3", scale: 9, lit: "-0.666666666", want: match},
		{name: "-2/3 scale8", a: "-2", b: "3", scale: 8, lit: "-0.66666667", want: match},
		{name: "1/6 scale4", a: "1", b: "6", scale: 4, lit: "0.1667", want: match},
		{name: "1/6 scale2", a: "1", b: "6", scale: 2, lit: "0.17", want: match},
		{name: "1/6 scale9", a: "1", b: "6", scale: 9, lit: "0.166666666", want: match},
		{name: "1/7 scale4", a: "1", b: "7", scale: 4, lit: "0.1429", want: match},
		{name: "1/7 scale6", a: "1", b: "7", scale: 6, lit: "0.142857", want: match},
		{name: "1/7 scale8", a: "1", b: "7", scale: 8, lit: "0.14285714", want: match},
		{name: "1/7 scale9 truncated", a: "1", b: "7", scale: 9, lit: "0.142857142", want: match},
		{name: "1/7 scale9 exact-round is wrong", a: "1", b: "7", scale: 9, lit: "0.142857143", want: mismatch},
		{name: "1/7 scale10 pad", a: "1", b: "7", scale: 10, lit: "0.1428571420", want: match},
		{name: "22/7 scale4", a: "22", b: "7", scale: 4, lit: "3.1429", want: match},
		{name: "22/7 scale9", a: "22", b: "7", scale: 9, lit: "3.142857142", want: match},
		{name: "1/9 scale9", a: "1", b: "9", scale: 9, lit: "0.111111111", want: match},
		{name: "5/6 scale9", a: "5", b: "6", scale: 9, lit: "0.833333333", want: match},
		{name: "1/11 scale9", a: "1", b: "11", scale: 9, lit: "0.090909090", want: match},
		{name: "1/13 scale9", a: "1", b: "13", scale: 9, lit: "0.076923076", want: match},
		{name: "10/3 scale9", a: "10", b: "3", scale: 9, lit: "3.333333333", want: match},
		{name: "2/-3 scale4", a: "2", b: "-3", scale: 4, lit: "-0.6667", want: match},
		{name: "2/-3 scale9", a: "2", b: "-3", scale: 9, lit: "-0.666666666", want: match},
		{name: "49999/20000 scale4", a: "49999", b: "20000", scale: 4, lit: "2.5000", want: match},
		{name: "49999/20000 scale6", a: "49999", b: "20000", scale: 6, lit: "2.499950", want: match},
		{name: "49999/20000 scale9", a: "49999", b: "20000", scale: 9, lit: "2.499950000", want: match},
		{name: "49999/20000 scale0 not 3", a: "49999", b: "20000", scale: 0, lit: "2", want: match},
		{name: "49999/20000 scale0 rounded 3 is wrong", a: "49999", b: "20000", scale: 0, lit: "3", want: mismatch},
		{name: "1/30000 scale4", a: "1", b: "30000", scale: 4, lit: "0.0000", want: match},
		{name: "1/30000 scale6", a: "1", b: "30000", scale: 6, lit: "0.000033", want: match},
		{name: "1/30000 scale9", a: "1", b: "30000", scale: 9, lit: "0.000033333", want: match},
		{name: "1/1e10 scale4", a: "1", b: "10000000000", scale: 4, lit: "0.0000", want: match},
		{name: "1/1e10 scale9", a: "1", b: "10000000000", scale: 9, lit: "0.000000000", want: match},
		{name: "1/1e9 scale9", a: "1", b: "1000000000", scale: 9, lit: "0.000000001", want: match},
		{name: "1/1e9 scale4", a: "1", b: "1000000000", scale: 4, lit: "0.0000", want: match},
		{name: "9999999995/1e10 scale4", a: "9999999995", b: "10000000000", scale: 4, lit: "1.0000", want: match},
		{name: "9999999995/1e10 scale6", a: "9999999995", b: "10000000000", scale: 6, lit: "1.000000", want: match},
		{name: "9999999995/1e10 scale9", a: "9999999995", b: "10000000000", scale: 9, lit: "0.999999999", want: match},
		{name: "9999999995/1e10 scale0", a: "9999999995", b: "10000000000", scale: 0, lit: "1", want: match},
		{name: "4999999995/1e10 scale4", a: "4999999995", b: "10000000000", scale: 4, lit: "0.5000", want: match},
		{name: "4999999995/1e10 scale9", a: "4999999995", b: "10000000000", scale: 9, lit: "0.499999999", want: match},
		{name: "4999999995/1e10 scale0", a: "4999999995", b: "10000000000", scale: 0, lit: "0", want: match},
		{name: "0/5", a: "0", b: "5", scale: 4, lit: "0.0000", want: match},
		{name: "10/5", a: "10", b: "5", scale: 4, lit: "2.0000", want: match},
		{name: "889/2000 scale4", a: "889", b: "2000", scale: 4, lit: "0.4445", want: match},
		{name: "889/2000 scale2", a: "889", b: "2000", scale: 2, lit: "0.44", want: match},
		{name: "maxint/2 scale4", a: "9223372036854775807", b: "2", scale: 4, lit: "4611686018427387903.5000", want: match},
		{name: "maxint/2 scale9", a: "9223372036854775807", b: "2", scale: 9, lit: "4611686018427387903.500000000", want: match},
		{name: "maxint/3 scale4", a: "9223372036854775807", b: "3", scale: 4, lit: "3074457345618258602.3333", want: match},
		{name: "maxint/3 scale9", a: "9223372036854775807", b: "3", scale: 9, lit: "3074457345618258602.333333333", want: match},
		{name: "minint/2", a: "-9223372036854775808", b: "2", scale: 4, lit: "-4611686018427387904.0000", want: match},
		{name: "minint/-2", a: "-9223372036854775808", b: "-2", scale: 4, lit: "4611686018427387904.0000", want: match},
		{name: "umax/2", a: "18446744073709551615", b: "2", scale: 4, lit: "9223372036854775807.5000", want: match, unsignedOp: true},
		{name: "umax/3", a: "18446744073709551615", b: "3", scale: 4, lit: "6148914691236517205.0000", want: match, unsignedOp: true},
		{name: "umax/3 scale9", a: "18446744073709551615", b: "3", scale: 9, lit: "6148914691236517205.000000000", want: match, unsignedOp: true},
		{name: "umax/7 scale4", a: "18446744073709551615", b: "7", scale: 4, lit: "2635249153387078802.1429", want: match, unsignedOp: true},
		{name: "umax/7 scale9", a: "18446744073709551615", b: "7", scale: 9, lit: "2635249153387078802.142857142", want: match, unsignedOp: true},
		{name: "wrong extra digit", a: "5", b: "2", scale: 4, lit: "2.5001", want: mismatch},
		{name: "inc0 5/2 stored 2", a: "5", b: "2", scale: 4, lit: "2.0000", want: mismatch},
		{name: "inc0 7/4 stored 1", a: "7", b: "4", scale: 4, lit: "1.0000", want: mismatch},
		{name: "inc0 1/7 stored 0", a: "1", b: "7", scale: 9, lit: "0.000000000", want: mismatch},
		{name: "inc0 15/4 stored 3", a: "15", b: "4", scale: 4, lit: "3.0000", want: mismatch},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			base := "bigint"
			a := schemaCol{name: "a", base: base}
			b := schemaCol{name: "b", base: base}
			if tc.unsignedOp {
				a.unsigned = true
				b.unsigned = true
			}
			g := schemaCol{name: "g", base: "decimal", expr: "(`a` / `b`)", prec: 40, scale: tc.scale, hasPrec: true}
			assertGenOutcome(t, g, []schemaCol{a, b, g}, []string{"a", "b", "g"}, []string{tc.a, tc.b, tc.lit}, tc.want)
		})
	}
}

func TestDecimalGeneratedCompoundAndNull(t *testing.T) {
	const (
		match    = "match"
		mismatch = "mismatch"
	)
	a := schemaCol{name: "a", base: "bigint"}
	b := schemaCol{name: "b", base: "bigint"}
	check := func(t *testing.T, name, expr, lit, aa, bb, want string, prec, scale int, base string) {
		t.Helper()
		g := schemaCol{name: "g", base: base, expr: expr, prec: prec, scale: scale, hasPrec: base == "decimal"}
		assertGenOutcome(t, g, []schemaCol{a, b, g}, []string{"a", "b", "g"}, []string{aa, bb, lit}, want)
	}
	t.Run("mul restores small", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "5.0000", "5", "2", match, 40, 4, "decimal")
	})
	t.Run("mul 1/7 scale4", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "1.0000", "1", "7", match, 40, 4, "decimal")
	})
	t.Run("mul 1/7 scale9", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "0.999999994", "1", "7", match, 40, 9, "decimal")
	})
	t.Run("mul 1/7 scale8", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "0.99999999", "1", "7", match, 40, 8, "decimal")
	})
	t.Run("mul 2/3 scale9", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "1.999999998", "2", "3", match, 40, 9, "decimal")
	})
	t.Run("mul 22/7 scale9", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "21.999999994", "22", "7", match, 40, 9, "decimal")
	})
	t.Run("mul 1/30000 scale4", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "1.0000", "1", "30000", match, 40, 4, "decimal")
	})
	t.Run("mul 1/30000 scale9", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "0.999990000", "1", "30000", match, 40, 9, "decimal")
	})
	t.Run("mul 1/1e10 is zero", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "0.0000", "1", "10000000000", match, 40, 4, "decimal")
	})
	t.Run("mul 1/1e10 exact 1 is wrong", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "1.0000", "1", "10000000000", mismatch, 40, 4, "decimal")
	})
	t.Run("mul 1/1e9 restores", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "1.0000", "1", "1000000000", match, 40, 4, "decimal")
	})
	t.Run("mul lossy bigint", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "0", "1", "10000000000", match, 0, 0, "bigint")
	})
	t.Run("mul lossy bigint exact 1 is wrong", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "1", "1", "10000000000", mismatch, 0, 0, "bigint")
	})
	t.Run("mul 9999999995 not restored", func(t *testing.T) {
		check(t, "mul", "((`a` / `b`) * `b`)", "9999999990", "9999999995", "10000000000", match, 0, 0, "bigint")
	})
	t.Run("bigint 5/2 rounds to 3", func(t *testing.T) {
		check(t, "gi", "(`a` / `b`)", "3", "5", "2", match, 0, 0, "bigint")
	})
	t.Run("bigint 5/2 truncated 2 is refused", func(t *testing.T) {
		check(t, "gi", "(`a` / `b`)", "2", "5", "2", mismatch, 0, 0, "int")
	})
	t.Run("bigint max/2 rounds", func(t *testing.T) {
		check(t, "gi", "(`a` / `b`)", "4611686018427387904", "9223372036854775807", "2", match, 0, 0, "bigint")
	})
	t.Run("bigint min/2 exact", func(t *testing.T) {
		check(t, "gi", "(`a` / `b`)", "-4611686018427387904", "-9223372036854775808", "2", match, 0, 0, "bigint")
	})
	t.Run("unsigned max/2 rounds", func(t *testing.T) {
		au := schemaCol{name: "a", base: "bigint", unsigned: true}
		bu := schemaCol{name: "b", base: "bigint", unsigned: true}
		g := schemaCol{name: "g", base: "bigint", unsigned: true, expr: "(`a` / `b`)"}
		assertGenOutcome(t, g, []schemaCol{au, bu, g}, []string{"a", "b", "g"}, []string{"18446744073709551615", "2", "9223372036854775808"}, match)
	})
	t.Run("unsigned max/7 integer", func(t *testing.T) {
		au := schemaCol{name: "a", base: "bigint", unsigned: true}
		bu := schemaCol{name: "b", base: "bigint", unsigned: true}
		g := schemaCol{name: "g", base: "bigint", unsigned: true, expr: "(`a` / `b`)"}
		assertGenOutcome(t, g, []schemaCol{au, bu, g}, []string{"a", "b", "g"}, []string{"18446744073709551615", "7", "2635249153387078802"}, match)
	})
	t.Run("div", func(t *testing.T) {
		check(t, "dv", "(`a` DIV `b`)", "-2.0000", "-5", "2", match, 20, 4, "decimal")
	})
	t.Run("mod", func(t *testing.T) {
		check(t, "md", "(MOD(`a`, `b`))", "-1.0000", "-5", "2", match, 20, 4, "decimal")
	})
	t.Run("percent", func(t *testing.T) {
		check(t, "pct", "(`a` % `b`)", "1.0000", "5", "2", match, 20, 4, "decimal")
	})
	t.Run("add mul", func(t *testing.T) {
		check(t, "s1", "(((`a` + `b`) * 3) - 1)", "35.0000", "5", "7", match, 40, 4, "decimal")
	})
	t.Run("neg mul", func(t *testing.T) {
		check(t, "neg", "(-(`a` * `b`))", "-35.0000", "5", "7", match, 40, 4, "decimal")
	})
	t.Run("half literal", func(t *testing.T) {
		check(t, "half", "(`a` / 2)", "2.5000", "5", "7", match, 20, 4, "decimal")
	})
	t.Run("null operand", func(t *testing.T) {
		check(t, "g", "(`a` / `b`)", "NULL", "NULL", "2", match, 40, 4, "decimal")
	})
	t.Run("null operand not zero", func(t *testing.T) {
		check(t, "g", "(`a` / `b`)", "0.0000", "NULL", "2", mismatch, 40, 4, "decimal")
	})
	t.Run("div by zero", func(t *testing.T) {
		check(t, "g", "(`a` / `b`)", "NULL", "1", "0", match, 40, 4, "decimal")
	})
	t.Run("div by zero not zero", func(t *testing.T) {
		check(t, "g", "(`a` / `b`)", "0.0000", "1", "0", mismatch, 40, 4, "decimal")
	})
	t.Run("zero div zero", func(t *testing.T) {
		check(t, "g", "(`a` / `b`)", "NULL", "0", "0", match, 40, 4, "decimal")
	})
	t.Run("div by null", func(t *testing.T) {
		check(t, "g", "(`a` / `b`)", "NULL", "5", "NULL", match, 40, 4, "decimal")
	})
	t.Run("integer div by zero", func(t *testing.T) {
		check(t, "g", "(`a` / `b`)", "NULL", "1", "0", match, 0, 0, "int")
	})
	t.Run("mod by zero", func(t *testing.T) {
		check(t, "g", "(MOD(`a`, `b`))", "NULL", "5", "0", match, 20, 4, "decimal")
	})
}

func TestDecimalOperandDivision(t *testing.T) {
	total := schemaCol{name: "total", base: "decimal", prec: 14, scale: 2, hasPrec: true}
	qty := schemaCol{name: "qty", base: "decimal", prec: 10, scale: 2, hasPrec: true}
	price := schemaCol{name: "unit_price", base: "decimal", expr: "(`total` / `qty`)", prec: 12, scale: 4, hasPrec: true}
	wide := schemaCol{name: "unit_price", base: "decimal", expr: "(`total` / `qty`)", prec: 40, scale: 20, hasPrec: true}
	meta := []schemaCol{total, qty, price}
	cases := []struct{ name, total, qty, lit, want string }{
		{"15/4", "15.00", "4.00", "3.7500", "match"},
		{"10/3", "10.00", "3.00", "3.3333", "match"},
		{"1/3", "1.00", "3.00", "0.3333", "match"},
		{"1/7", "1.00", "7.00", "0.1429", "match"},
		{"49999/20000", "49999.00", "20000.00", "2.5000", "match"},
		{"neg", "-15.50", "2.00", "-7.7500", "match"},
		{"zero", "0.00", "1.00", "0.0000", "match"},
		{"5/2.5", "5.00", "2.50", "2.0000", "match"},
		{"1/6", "1.00", "6.00", "0.1667", "match"},
		{"22/7", "22.00", "7.00", "3.1429", "match"},
		{"wrong", "15.00", "4.00", "3.7499", "mismatch"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertGenOutcome(t, price, meta, []string{"total", "qty", "unit_price"}, []string{tc.total, tc.qty, tc.lit}, tc.want)
		})
	}
	wideCases := []struct{ name, total, qty, lit string }{
		{"1/7 18 digits", "1.00", "7.00", "0.14285714285714285700"},
		{"1/3 18 digits", "1.00", "3.00", "0.33333333333333333300"},
		{"2.49995 padded", "49999.00", "20000.00", "2.49995000000000000000"},
	}
	for _, tc := range wideCases {
		t.Run(tc.name, func(t *testing.T) {
			assertGenOutcome(t, wide, []schemaCol{total, qty, wide}, []string{"total", "qty", "unit_price"}, []string{tc.total, tc.qty, tc.lit}, "match")
		})
	}
	a := schemaCol{name: "a", base: "decimal", prec: 10, scale: 1, hasPrec: true}
	b := schemaCol{name: "b", base: "int"}
	g := schemaCol{name: "g", base: "decimal", expr: "(`a` / `b`)", prec: 40, scale: 12, hasPrec: true}
	assertGenOutcome(t, g, []schemaCol{a, b, g}, []string{"a", "b", "g"}, []string{"1.0", "3", "0.333333333000"}, "match")
	assertGenOutcome(t, g, []schemaCol{a, b, g}, []string{"a", "b", "g"}, []string{"1.0", "7", "0.142857142000"}, "match")
}

func TestGeneratedOutOfRangeIsUnverified(t *testing.T) {
	// A non-strict session stores the type's endpoint. Matching that endpoint
	// would also accept a real column that happens to hold it, so the checker
	// warns. Any other logged value is still a mismatch.
	a := schemaCol{name: "a", base: "int"}
	cases := []struct {
		name, expr, base string
		unsigned         bool
		prec, scale      int
		aval, lit, want  string
	}{
		{name: "tiny in range", expr: "(`a` * 100)", base: "tinyint", aval: "1", lit: "100", want: "match"},
		{name: "tiny clip", expr: "(`a` * 100)", base: "tinyint", aval: "5", lit: "127", want: "unverified"},
		{name: "tiny clip neg", expr: "(`a` * 100)", base: "tinyint", aval: "-5", lit: "-128", want: "unverified"},
		{name: "tiny other value", expr: "(`a` * 100)", base: "tinyint", aval: "5", lit: "50", want: "mismatch"},
		{name: "tiny zero in range", expr: "(`a` * 100)", base: "tinyint", aval: "0", lit: "0", want: "match"},
		{name: "utiny clip zero", expr: "(`a` - 10)", base: "tinyint", unsigned: true, aval: "1", lit: "0", want: "unverified"},
		{name: "utiny exact zero", expr: "(`a` - 10)", base: "tinyint", unsigned: true, aval: "10", lit: "0", want: "match"},
		{name: "utiny in range", expr: "(`a` - 10)", base: "tinyint", unsigned: true, aval: "20", lit: "10", want: "match"},
		{name: "utiny not endpoint", expr: "(`a` - 10)", base: "tinyint", unsigned: true, aval: "1", lit: "1", want: "mismatch"},
		{name: "utiny mul clip", expr: "(`a` * 100)", base: "tinyint", unsigned: true, aval: "5", lit: "255", want: "unverified"},
		{name: "utiny mul in range", expr: "(`a` * 100)", base: "tinyint", unsigned: true, aval: "2", lit: "200", want: "match"},
		{name: "utiny mul neg clip", expr: "(`a` * 100)", base: "tinyint", unsigned: true, aval: "-1", lit: "0", want: "unverified"},
		{name: "int clip", expr: "(`a` * 100)", base: "int", aval: "30000000", lit: "2147483647", want: "unverified"},
		{name: "int clip neg", expr: "(`a` * 100)", base: "int", aval: "-30000000", lit: "-2147483648", want: "unverified"},
		{name: "int in range", expr: "(`a` * 100)", base: "int", aval: "5", lit: "500", want: "match"},
		{name: "int not endpoint", expr: "(`a` * 100)", base: "int", aval: "30000000", lit: "2147483646", want: "mismatch"},
		{name: "decimal in range", expr: "(`a` / 1)", base: "decimal", prec: 4, scale: 2, aval: "99", lit: "99.00", want: "match"},
		{name: "decimal clip", expr: "(`a` / 1)", base: "decimal", prec: 4, scale: 2, aval: "100", lit: "99.99", want: "unverified"},
		{name: "decimal clip neg", expr: "(`a` / 1)", base: "decimal", prec: 4, scale: 2, aval: "-100", lit: "-99.99", want: "unverified"},
		{name: "decimal not endpoint", expr: "(`a` / 1)", base: "decimal", prec: 4, scale: 2, aval: "100", lit: "99.98", want: "mismatch"},
		{name: "decimal one", expr: "(`a` / 1)", base: "decimal", prec: 4, scale: 2, aval: "1", lit: "1.00", want: "match"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := schemaCol{name: "g", base: tc.base, expr: tc.expr, unsigned: tc.unsigned, prec: tc.prec, scale: tc.scale, hasPrec: tc.prec > 0}
			assertGenOutcome(t, g, []schemaCol{a, g}, []string{"a", "g"}, []string{tc.aval, tc.lit}, tc.want)
		})
	}

	// One clipped row makes the column unverifiable even when another row matches.
	g := schemaCol{name: "g", base: "tinyint", expr: "(`a` * 100)"}
	meta := []schemaCol{a, g}
	example, unknown := generatedColumnOutcome(g, []loggedImage{
		{columns: []string{"a", "g"}, values: []string{"1", "100"}},
		{columns: []string{"a", "g"}, values: []string{"5", "127"}},
	}, meta)
	if example != "" || !unknown {
		t.Fatalf("mixed clip: example %q unverified %v", example, unknown)
	}
	example, unknown = generatedColumnOutcome(g, []loggedImage{
		{columns: []string{"a", "g"}, values: []string{"5", "127"}},
		{columns: []string{"a", "g"}, values: []string{"5", "50"}},
	}, meta)
	if example == "" || unknown {
		t.Fatalf("clip plus contradiction: example %q unverified %v", example, unknown)
	}
}

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

func TestDecimalMulDoesNotRoundIntermediate(t *testing.T) {
	// DECIMAL(31,30) * DECIMAL(6,5). The exact product is 0.4999…5, so one
	// half-up round stores 0. Rounding the product to 30 places first makes
	// 0.5, and the column round then stores 1.
	const (
		aLit = "0.500005000050000500005000050000"
		bLit = "0.99999"
		expr = "(`a` * `b`)"
	)
	a := schemaCol{name: "a", base: "decimal", prec: 31, scale: 30, hasPrec: true}
	b := schemaCol{name: "b", base: "decimal", prec: 6, scale: 5, hasPrec: true}
	t.Run("decimal(10,0)", func(t *testing.T) {
		g := schemaCol{name: "g", base: "decimal", expr: expr, prec: 10, scale: 0, hasPrec: true}
		meta := []schemaCol{a, b, g}
		assertGenOutcome(t, g, meta, []string{"a", "b", "g"}, []string{aLit, bLit, "0"}, "unverified")
		assertGenOutcome(t, g, meta, []string{"a", "b", "g"}, []string{aLit, bLit, "1"}, "unverified")
	})
	t.Run("int", func(t *testing.T) {
		g := schemaCol{name: "g", base: "int", expr: expr}
		meta := []schemaCol{a, b, g}
		assertGenOutcome(t, g, meta, []string{"a", "b", "g"}, []string{aLit, bLit, "0"}, "match")
		assertGenOutcome(t, g, meta, []string{"a", "b", "g"}, []string{aLit, bLit, "1"}, "mismatch")
	})
}

func TestDecimalPrecisionOverflowUnverified(t *testing.T) {
	// 41 digits times 41 digits is 81 digits, past DECIMAL's 65-digit precision.
	wide := "1" + strings.Repeat("0", 40)
	a := schemaCol{name: "a", base: "decimal", prec: 50, scale: 0, hasPrec: true}
	b := schemaCol{name: "b", base: "decimal", prec: 50, scale: 0, hasPrec: true}
	metaDec := func(g schemaCol) []schemaCol { return []schemaCol{a, b, g} }
	gDec := schemaCol{name: "g", base: "decimal", expr: "(`a` * `b`)", prec: 10, scale: 0, hasPrec: true}
	gInt := schemaCol{name: "g", base: "int", expr: "(`a` * `b`)"}
	for _, lit := range []string{"0", "1", "9999999999"} {
		assertGenOutcome(t, gDec, metaDec(gDec), []string{"a", "b", "g"}, []string{wide, wide, lit}, "unverified")
	}
	for _, lit := range []string{"0", "1", "2147483647"} {
		assertGenOutcome(t, gInt, metaDec(gInt), []string{"a", "b", "g"}, []string{wide, wide, lit}, "unverified")
	}

	// 5*10^64 + 5*10^64 = 10^65, which is 66 digits.
	hi := "5" + strings.Repeat("0", 64)
	addA := schemaCol{name: "a", base: "decimal", prec: 65, scale: 0, hasPrec: true}
	addB := schemaCol{name: "b", base: "decimal", prec: 65, scale: 0, hasPrec: true}
	add := schemaCol{name: "g", base: "int", expr: "(`a` + `b`)"}
	assertGenOutcome(t, add, []schemaCol{addA, addB, add}, []string{"a", "b", "g"}, []string{hi, hi, "0"}, "unverified")
	sub := schemaCol{name: "g", base: "decimal", expr: "(`a` - `b`)", prec: 65, scale: 0, hasPrec: true}
	// 5*10^64 - (-5*10^64) is the same overflow. -5*10^64 still fits in DECIMAL(65,0).
	assertGenOutcome(t, sub, []schemaCol{addA, addB, sub}, []string{"a", "b", "g"}, []string{hi, "-" + hi, "0"}, "unverified")

	// Division width for scale 15/15 is 36, past 30. Do not claim either target.
	frac := "1." + strings.Repeat("0", 15)
	seven := "7." + strings.Repeat("0", 15)
	da := schemaCol{name: "a", base: "decimal", prec: 20, scale: 15, hasPrec: true}
	db := schemaCol{name: "b", base: "decimal", prec: 20, scale: 15, hasPrec: true}
	for _, base := range []string{"decimal", "int"} {
		g := schemaCol{name: "g", base: base, expr: "(`a` / `b`)", prec: 10, scale: 0, hasPrec: base == "decimal"}
		for _, lit := range []string{"0", "1"} {
			assertGenOutcome(t, g, []schemaCol{da, db, g}, []string{"a", "b", "g"}, []string{frac, seven, lit}, "unverified")
		}
	}
}

func assertGenOutcome(t *testing.T, col schemaCol, meta []schemaCol, cols, vals []string, want string) {
	t.Helper()
	example, unknown := generatedColumnOutcome(col, []loggedImage{{columns: cols, values: vals}}, meta)
	got := "match"
	if example != "" {
		got = "mismatch"
	} else if unknown {
		got = "unverified"
	}
	if got != want {
		t.Fatalf("got %s want %s example %q", got, want, example)
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
