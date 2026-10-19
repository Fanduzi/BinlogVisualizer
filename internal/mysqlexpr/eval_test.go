package mysqlexpr

import (
	"fmt"
	"testing"
)

// Tests at the evaluator's interface: EvalGenExpr / EvalDecExprInc.

func TestEvaluatorOneTreeBothDomains(t *testing.T) {
	env := loggedEnv([]string{"a", "b"}, []string{"7", "2"})
	g, ok := EvalGenExpr("a*3 + MOD(a, b) - b DIV 2", env)
	if !ok || g.kind != genNum || g.n.String() != "21/1" {
		t.Fatalf("gen = %+v %v", g, ok)
	}
	d, ok := EvalDecExprInc("a*3 + MOD(a, b) - b DIV 2", env, 4)
	if !ok || d.null || d.n.String() != "21/1" {
		t.Fatalf("dec = %+v %v", d, ok)
	}
	if _, ok := parseExprTree("a*3 + MOD(a, b) - b DIV 2"); !ok {
		t.Fatal("tree not cached/parsed")
	}
}

func TestEvaluatorDecimalRejectsUnmodelled(t *testing.T) {
	env := loggedEnv([]string{"a", "j"}, []string{"1", `{"k":1}`})
	for _, expr := range []string{"j->>'$.k'", "'x'", "UPPER(a)", "MOD(a)", "a COLLATE utf8mb4_bin", "`MOD`(a,2)"} {
		if _, ok := EvalDecExprInc(expr, env, 4); ok {
			t.Errorf("%s: DECIMAL domain accepted it", expr)
		}
	}
	if _, ok := EvalGenExpr("a +", env); ok {
		t.Error("bad syntax parsed")
	}
}

func TestEvaluatorLiteralNotMutatedAcrossCalls(t *testing.T) {
	for i := 0; i < 3; i++ {
		v, ok := EvalGenExpr("'ab'", nil)
		if !ok || v.text != "ab" || string(v.raw) != "ab" {
			t.Fatalf("run %d: %+v %v", i, v, ok)
		}
		v.raw[0] = 'Z' // a caller scribbling on the result must not reach the cached tree
	}
}

// envFor builds the evaluator env the way flashback does: from a schema
// body and one logged row.
func envFor(t *testing.T, body string, cols, vals []string) map[string]GenVal {
	t.Helper()
	meta, ok := ParseSchemaColumns(body)
	if !ok {
		t.Fatalf("schema %s", body)
	}
	return PrepareGenImages([]LoggedImage{{Columns: cols, Values: vals}}, meta).envs[0]
}

// #237: an array index on a non-array autowraps it, as MySQL 8.0 does.
func TestEvaluatorJSONIndexAutowrap(t *testing.T) {
	env := envFor(t, "`j` JSON", []string{"j"}, []string{"JSON_OBJECT('v', -7, 'w', JSON_OBJECT('v', 0), 'a', JSON_ARRAY(5))"})
	for expr, want := range map[string]string{
		"`j`->>'$.v[0]'":   "-7",
		"`j`->>'$.a[0]'":   "5",
		"`j`->>'$.w[0].v'": "0",
	} {
		v, ok := EvalGenExpr(expr, env)
		if !ok || v.kind != genText || v.text != want {
			t.Errorf("%s = %+v %v, want %s", expr, v, ok, want)
		}
	}
	for _, expr := range []string{"`j`->'$.v[1]'", "`j`->'$.a[1]'"} {
		if v, ok := EvalGenExpr(expr, env); !ok || v.kind != genNull {
			t.Errorf("%s = %+v %v, want NULL", expr, v, ok)
		}
	}
}

// #238: DIV and % over a rounded / follow MySQL's per-operation rounding
// (values from MySQL 8.0.46).
func TestEvaluatorDivisionUnderDivMod(t *testing.T) {
	env := envFor(t, "`a` TINYINT UNSIGNED, `c` INT, `x` DECIMAL(20,10), `y` DECIMAL(20,10)",
		[]string{"a", "c", "x", "y"}, []string{"255", "427516445", "60.5369981968", "7.0000000000"})
	cases := []struct {
		expr string
		inc  int
		want string // RatString, or NULL
	}{
		{"(((c / a) * a) DIV c)", 17, "0"},  // case 437524: MySQL 0, exact 1
		{"(10 / 3) DIV (1 / 3)", 0, "NULL"}, // 1/3 is 0 at increment 0
		{"(10 / 3) DIV (1 / 3)", 4, "10"},
		{"MOD(y, 7) - 31610", 4, "-31610"},
		{"(x / 7) % 1", 4, "324071299771428571/500000000000000000"},
	}
	for _, tc := range cases {
		d, ok := EvalDecExprInc(tc.expr, env, tc.inc)
		got := "NULL"
		if !d.null {
			got = fmt.Sprintf("omit(mode %d)", d.mode)
			if d.n != nil {
				got = d.n.RatString()
			}
		}
		if !ok || got != tc.want {
			t.Errorf("%s @%d = %s %v, want %s", tc.expr, tc.inc, got, ok, tc.want)
		}
	}
	// A zero result is a plain 0 (scale 0), so a later / is sized from it:
	// MySQL 8.0.46 gives x/((x % x) - 31610) at increment 0 eighteen
	// fraction digits, -0.001915121739854476.
	d, ok := EvalDecExprInc("x / ((x % x) - 31610)", env, 0)
	if !ok || d.n == nil || d.scale != 18 || d.n.FloatString(18) != "-0.001915121739854476" {
		t.Errorf("zero-scale: %+v %v", d, ok)
	}
}

func TestEvalGenExprIntegerAssignment(t *testing.T) {
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
		var cols, vals []string
		for name, val := range tc.env {
			cols, vals = append(cols, name), append(vals, val)
		}
		v, ok := EvalGenExpr(tc.expr, loggedEnv(cols, vals))
		if !tc.ok {
			// Not integer arithmetic: the evaluator must not claim a number.
			if ok && v.kind == genNum {
				t.Fatalf("%s: claimed a number", tc.expr)
			}
			continue
		}
		if !ok {
			t.Fatalf("%s: not evaluated", tc.expr)
		}
		got := "NULL"
		if v.kind != genNull {
			got = roundRatHalfAway(v.n).String()
		}
		if got != tc.want {
			t.Fatalf("%s = %s, want %s", tc.expr, got, tc.want)
		}
	}
}
