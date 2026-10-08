package analyzer

import (
	"strings"
	"testing"
)

func TestEvalGenExprStringsAndJSON(t *testing.T) {
	cases := []struct {
		name string
		expr string
		env  map[string]string
		kind genKind
		text string
		ok   bool
	}{
		{name: "upper ascii", expr: "UPPER(`x`)", env: map[string]string{"x": "'abc'"}, kind: genText, text: "ABC", ok: true},
		{name: "lower ascii", expr: "LOWER(`x`)", env: map[string]string{"x": "'ABC'"}, kind: genText, text: "abc", ok: true},
		{name: "upper e acute", expr: "UPPER(x)", env: map[string]string{"x": "'é'"}, kind: genText, text: "É", ok: true},
		{name: "upper null", expr: "UPPER(x)", env: map[string]string{"x": "NULL"}, kind: genNull, ok: true},
		{name: "upper binary", expr: "UPPER(x)", env: map[string]string{"x": "X'416263'"}, kind: genText, text: "Abc", ok: true},
		{name: "sharp s unverified", expr: "UPPER(x)", env: map[string]string{"x": "'ß'"}, ok: false},
		{name: "y diaeresis unverified", expr: "LOWER(x)", env: map[string]string{"x": "'ÿ'"}, ok: false},
		{name: "concat null", expr: "CONCAT(a, ' ', b)", env: map[string]string{"a": "'Ada'", "b": "NULL"}, kind: genNull, ok: true},
		{name: "concat", expr: "concat(`first`,_utf8mb4' ',`last`)", env: map[string]string{"first": "'Ada'", "last": "'Lovelace'"}, kind: genText, text: "Ada Lovelace", ok: true},
		{name: "concat_ws skip null", expr: "CONCAT_WS(',', a, NULL, b)", env: map[string]string{"a": "'a'", "b": "'b'"}, kind: genText, text: "a,b", ok: true},
		{name: "concat_ws sep null", expr: "CONCAT_WS(NULL, a)", env: map[string]string{"a": "'a'"}, kind: genNull, ok: true},
		{name: "concat_ws all null", expr: "CONCAT_WS(',', NULL, NULL)", kind: genText, text: "", ok: true},
		{name: "length utf8", expr: "LENGTH(s)", env: map[string]string{"s": "'á'"}, kind: genNum, text: "2", ok: true},
		{name: "char length utf8", expr: "CHAR_LENGTH(s)", env: map[string]string{"s": "'á'"}, kind: genNum, text: "1", ok: true},
		{name: "length latin1", expr: "LENGTH(s)", env: map[string]string{"s": "_latin1 0xE9E9"}, kind: genNum, text: "2", ok: true},
		{name: "char length latin1", expr: "CHARACTER_LENGTH(s)", env: map[string]string{"s": "_latin1 0xE9E9"}, kind: genNum, text: "2", ok: true},
		{name: "json extract", expr: "json_unquote(json_extract(`j`,_utf8mb4'$.k'))", env: map[string]string{"j": "JSON_OBJECT('k', 'ab')"}, kind: genText, text: "ab", ok: true},
		{name: "json arrow", expr: "j->>'$.k'", env: map[string]string{"j": "CAST('{\"k\":\"ab\"}' AS JSON)"}, kind: genText, text: "ab", ok: true},
		{name: "json missing", expr: "j->>'$.missing'", env: map[string]string{"j": "JSON_OBJECT('k', 'ab')"}, kind: genNull, ok: true},
		{name: "json null unquote", expr: "JSON_UNQUOTE(JSON_EXTRACT(j, '$.k'))", env: map[string]string{"j": "JSON_OBJECT('k', NULL)"}, kind: genNull, ok: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := evalGenExpr(tc.expr, loggedEnv(nil, nil))
			if tc.env != nil {
				cols := make([]string, 0, len(tc.env))
				vals := make([]string, 0, len(tc.env))
				for name, lit := range tc.env {
					cols = append(cols, name)
					vals = append(vals, lit)
				}
				got, ok = evalGenExpr(tc.expr, loggedEnv(cols, vals))
			}
			if ok != tc.ok {
				t.Fatalf("ok=%v want %v (%#v)", ok, tc.ok, got)
			}
			if !tc.ok {
				return
			}
			if got.kind != tc.kind {
				t.Fatalf("kind %d want %d (%#v)", got.kind, tc.kind, got)
			}
			switch tc.kind {
			case genText:
				if got.text != tc.text {
					t.Fatalf("text %q want %q", got.text, tc.text)
				}
			case genNum:
				if got.n == nil || got.n.RatString() != tc.text && got.n.FloatString(0) != tc.text {
					t.Fatalf("num %v want %s", got.n, tc.text)
				}
			}
		})
	}
}

func TestGeneratedRefs(t *testing.T) {
	cases := []struct {
		expr string
		cols []string
		want string
	}{
		{expr: "upper(`x`)", cols: []string{"x"}, want: "x"},
		{expr: "concat(`first`,_utf8mb4' ',`last`)", cols: []string{"first", "last"}, want: "first,last"},
		{expr: "json_unquote(json_extract(`j`,_utf8mb4'$.k'))", cols: []string{"j"}, want: "j"},
		{expr: "(`a` + `b`) * 3", cols: []string{"a", "b"}, want: "a,b"},
		{expr: "date_format(`d`, '%Y')", cols: []string{"d"}, want: "d"},
		{expr: "`a` DIV 2", cols: []string{"a", "div"}, want: "a"},
		{expr: "`div`", cols: []string{"div"}, want: "div"},
		{expr: "a + year", cols: []string{"a", "year"}, want: "a,year"},
	}
	for _, tc := range cases {
		refs, ok := generatedRefs(tc.expr, tc.cols)
		if !ok {
			t.Fatalf("%s not ok", tc.expr)
		}
		if strings.Join(refs, ",") != tc.want {
			t.Fatalf("%s refs %v want %s", tc.expr, refs, tc.want)
		}
	}
}
