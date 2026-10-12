package analyzer

import "testing"

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
