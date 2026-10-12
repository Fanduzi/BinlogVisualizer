package analyzer

import (
	"fmt"
	"testing"
)

// Tests at the evaluator's interface: evalGenExpr / evalDecExprInc.

func TestEvaluatorOneTreeBothDomains(t *testing.T) {
	env := loggedEnv([]string{"a", "b"}, []string{"7", "2"})
	g, ok := evalGenExpr("a*3 + MOD(a, b) - b DIV 2", env)
	if !ok || g.kind != genNum || g.n.String() != "21/1" {
		t.Fatalf("gen = %+v %v", g, ok)
	}
	d, ok := evalDecExprInc("a*3 + MOD(a, b) - b DIV 2", env, 4)
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
		if _, ok := evalDecExprInc(expr, env, 4); ok {
			t.Errorf("%s: DECIMAL domain accepted it", expr)
		}
	}
	if _, ok := evalGenExpr("a +", env); ok {
		t.Error("bad syntax parsed")
	}
}

func TestEvaluatorLiteralNotMutatedAcrossCalls(t *testing.T) {
	for i := 0; i < 3; i++ {
		v, ok := evalGenExpr("'ab'", nil)
		if !ok || v.text != "ab" || string(v.raw) != "ab" {
			t.Fatalf("run %d: %+v %v", i, v, ok)
		}
		v.raw[0] = 'Z' // a caller scribbling on the result must not reach the cached tree
	}
}

// envFor builds the evaluator env the way flashback does: from a schema
// body and one logged row.
func envFor(t *testing.T, body string, cols, vals []string) map[string]genVal {
	t.Helper()
	meta, ok := parseSchemaColumns(body)
	if !ok {
		t.Fatalf("schema %s", body)
	}
	return prepareGenImages([]loggedImage{{columns: cols, values: vals}}, meta).envs[0]
}

// #237: an array index on a non-array autowraps it, as MySQL 8.0 does.
func TestEvaluatorJSONIndexAutowrap(t *testing.T) {
	env := envFor(t, "`j` JSON", []string{"j"}, []string{"JSON_OBJECT('v', -7, 'w', JSON_OBJECT('v', 0), 'a', JSON_ARRAY(5))"})
	for expr, want := range map[string]string{
		"`j`->>'$.v[0]'":   "-7",
		"`j`->>'$.a[0]'":   "5",
		"`j`->>'$.w[0].v'": "0",
	} {
		v, ok := evalGenExpr(expr, env)
		if !ok || v.kind != genText || v.text != want {
			t.Errorf("%s = %+v %v, want %s", expr, v, ok, want)
		}
	}
	for _, expr := range []string{"`j`->'$.v[1]'", "`j`->'$.a[1]'"} {
		if v, ok := evalGenExpr(expr, env); !ok || v.kind != genNull {
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
		d, ok := evalDecExprInc(tc.expr, env, tc.inc)
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
	d, ok := evalDecExprInc("x / ((x % x) - 31610)", env, 0)
	if !ok || d.n == nil || d.scale != 18 || d.n.FloatString(18) != "-0.001915121739854476" {
		t.Errorf("zero-scale: %+v %v", d, ok)
	}
}
