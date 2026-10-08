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
		{name: "json null unquote", expr: "JSON_UNQUOTE(JSON_EXTRACT(j, '$.k'))", env: map[string]string{"j": "JSON_OBJECT('k', NULL)"}, kind: genText, text: "null", ok: true},
		{name: "json null arrow", expr: "j->>'$.k'", env: map[string]string{"j": "CAST('{\"k\":null}' AS JSON)"}, kind: genText, text: "null", ok: true},
		{name: "json decimal unquote", expr: "j->>'$.v'", env: map[string]string{"j": "JSON_OBJECT('v', 1.0)"}, kind: genText, text: "1.0", ok: true},
		{name: "json tenth unquote", expr: "j->>'$.v'", env: map[string]string{"j": "JSON_OBJECT('v', 0.1)"}, kind: genText, text: "0.1", ok: true},
		{name: "json exponent unquote", expr: "j->>'$.v'", env: map[string]string{"j": "JSON_OBJECT('v', 1e2)"}, kind: genText, text: "100.0", ok: true},
		{name: "upper micro", expr: "UPPER(s)", env: map[string]string{"s": "'µ'"}, kind: genText, text: "\u039c", ok: true},
		{name: "upper ascii bytes", expr: "UPPER(s)", env: map[string]string{"s": "_ascii 0x6162"}, kind: genText, text: "AB", ok: true},
		{name: "unknown charset unverified", expr: "UPPER(s)", env: map[string]string{"s": "_gbk 0x6162"}, ok: false},
		{name: "escaped quote", expr: "concat(`s`,_utf8mb4'it\\'s',_utf8mb4'\\\\t')", env: map[string]string{"s": "'a'"}, kind: genText, text: "ait's\\t", ok: true},
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

func TestGeneratedExactTypesAndJSONKinds(t *testing.T) {
	meta := []schemaCol{
		{name: "e", base: "enum", members: []string{"red", "blue"}, charset: "utf8mb4"},
		{name: "s", base: "set", members: []string{"a", "b", "c"}, charset: "utf8mb4"},
		{name: "d", base: "decimal", prec: 10, scale: 2, hasPrec: true},
		{name: "tm", base: "time", prec: 2, hasPrec: true},
		{name: "ts", base: "timestamp"},
		{name: "bin", base: "binary", prec: 8, hasPrec: true},
	}
	env := loggedEnv(
		[]string{"e", "s", "d", "tm", "ts", "bin"},
		[]string{"2", "5", "2.00", "'12:00:00.5'", "'2020-01-01 00:00:00'", "X'41'"},
		meta,
	)
	got, ok := evalGenExpr("CONCAT(e, ':', s)", env)
	if !ok || got.text != "blue:a,c" {
		t.Fatalf("enum/set concat ok=%v text=%q", ok, got.text)
	}
	got, ok = evalGenExpr("CONCAT(d)", env)
	if !ok || got.text != "2.00" {
		t.Fatalf("decimal concat ok=%v text=%q", ok, got.text)
	}
	got, ok = evalGenExpr("CONCAT(tm)", env)
	if !ok || got.text != "12:00:00.50" {
		t.Fatalf("time concat ok=%v text=%q", ok, got.text)
	}
	if _, ok = evalGenExpr("CONCAT(ts)", env); ok {
		t.Fatal("timestamp concat was treated as exact")
	}
	got, ok = evalGenExpr("LENGTH(bin)", env)
	if !ok || got.n == nil || got.n.FloatString(0) != "8" {
		t.Fatalf("binary length ok=%v num=%v", ok, got.n)
	}
	one, ok1 := parseJSONSQLString("1")
	oneDot, ok2 := parseJSONSQLString("1.0")
	if !ok1 || !ok2 || jsonEqual(one, oneDot) {
		t.Fatalf("JSON 1 and 1.0 compared equal (%v %v)", ok1, ok2)
	}
	for _, tc := range []struct{ in, want string }{
		{"1.0", "1.0"},
		{"0.1", "0.1"},
		{"1e2", "100.0"},
		{"-0.0", "-0.0"},
		{"1.5e300", "1.5e300"},
		{"1e-5", "0.00001"},
		{"1e-16", "1e-16"},
		{"1e20", "1e20"},
	} {
		f, ok := parseJSONFloat(tc.in)
		if !ok || mysqlJSONFloatString(f) != tc.want && tc.in != "1.0" && tc.in != "0.1" {
			// 1.0 and 0.1 are decimals, not doubles. The float formatter still has a defined result.
		}
		if tc.in == "1.0" || tc.in == "0.1" {
			node, ok := parseJSONSQLString(tc.in)
			if !ok || node.nkind != jnDec {
				t.Fatalf("%s kind %v", tc.in, node)
			}
			text, ok := jsonUnquote(genVal{kind: genJSON, j: node})
			if !ok || text.text != tc.want {
				t.Fatalf("%s unquote %q ok=%v", tc.in, text.text, ok)
			}
			continue
		}
		if !ok {
			t.Fatalf("parse %s", tc.in)
		}
		if got := mysqlJSONFloatString(f); got != tc.want {
			t.Fatalf("mysql JSON float %s = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestIssue182ListedCases(t *testing.T) {
	check := func(t *testing.T, col schemaCol, meta []schemaCol, columns, values []string, contradict, unverified bool) {
		t.Helper()
		example, unknown := generatedColumnOutcome(col, []loggedImage{{columns: columns, values: values}}, meta)
		if contradict {
			if example == "" {
				t.Fatalf("%s: expected a contradiction, unverified=%v", col.expr, unknown)
			}
			return
		}
		if example != "" {
			t.Fatalf("%s: false refusal: %s", col.expr, example)
		}
		if unknown != unverified {
			t.Fatalf("%s: unverified=%v want %v", col.expr, unknown, unverified)
		}
	}
	varchar := func(name, expr string, prec int) schemaCol {
		return schemaCol{name: name, base: "varchar", expr: expr, charset: "utf8mb4", prec: prec, hasPrec: prec > 0}
	}
	jsonNull := []schemaCol{{name: "j", base: "json"}, varchar("g", "j->>'$.v'", 64)}
	check(t, jsonNull[1], jsonNull, []string{"j", "g"}, []string{"JSON_OBJECT('v', NULL)", "'null'"}, false, false)
	jsonBig := []schemaCol{{name: "j", base: "json"}, {name: "g", base: "bigint", expr: "j->>'$.v'"}}
	check(t, jsonBig[1], jsonBig, []string{"j", "g"}, []string{"JSON_OBJECT('v', NULL)", "0"}, false, true)

	for _, tc := range []struct{ lit, stored string }{
		{"1.0", "1.0"},
		{"1e+00", "1.0"},
		{"0.1", "0.1"},
		{"1e-01", "0.1"},
		{"1e2", "100.0"},
		{"1e+02", "100.0"},
		{"-0.0", "-0.0"},
		{"-0.0E0", "-0.0"},
		{"1.5e300", "1.5e300"},
		{"3.14159265358979e+00", "3.14159265358979"},
	} {
		meta := []schemaCol{{name: "j", base: "json"}, varchar("g", "j->>'$.v'", 64)}
		check(t, meta[1], meta, []string{"j", "g"}, []string{"JSON_OBJECT('v', " + tc.lit + ")", "'" + tc.stored + "'"}, false, false)
	}

	enumMeta := []schemaCol{
		{name: "e", base: "enum", members: []string{"low", "Mid"}, charset: "utf8mb4"},
		varchar("g", "UPPER(e)", 20),
	}
	check(t, enumMeta[1], enumMeta, []string{"e", "g"}, []string{"1", "'LOW'"}, false, false)
	enumLen := []schemaCol{
		{name: "e", base: "enum", members: []string{"low", "Mid"}, charset: "utf8mb4"},
		{name: "g", base: "int", expr: "LENGTH(e)"},
	}
	check(t, enumLen[1], enumLen, []string{"e", "g"}, []string{"1", "3"}, false, false)
	setMeta := []schemaCol{
		{name: "st", base: "set", members: []string{"x", "y", "z"}, charset: "utf8mb4"},
		varchar("g", "CONCAT(st)", 20),
	}
	check(t, setMeta[1], setMeta, []string{"st", "g"}, []string{"5", "'x,z'"}, false, false)

	ascii := schemaCol{name: "country", base: "char", charset: "ascii", prec: 2, hasPrec: true}
	upperASCII := []schemaCol{ascii, varchar("g", "UPPER(country)", 8)}
	check(t, upperASCII[1], upperASCII, []string{"country", "g"}, []string{"_ascii 0x636E", "'CN'"}, false, false)
	lenASCII := []schemaCol{ascii, {name: "g", base: "int", expr: "CHAR_LENGTH(country)"}}
	check(t, lenASCII[1], lenASCII, []string{"country", "g"}, []string{"_ascii 0x636E", "2"}, false, false)
	badASCII := []schemaCol{
		{name: "code", base: "varchar", charset: "ascii", prec: 10, hasPrec: true},
		{name: "c", base: "varchar", charset: "ascii", prec: 20, hasPrec: true, expr: "UPPER(code)"},
	}
	check(t, badASCII[1], badASCII, []string{"code", "c"}, []string{"_ascii 0x6162", "_ascii 0x6E6F74652D31"}, true, false)

	dec := schemaCol{name: "d", base: "decimal", prec: 10, scale: 2, hasPrec: true}
	for _, tc := range []struct{ lit, stored string }{
		{"2.00", "2.00"},
		{"7", "7.00"},
		{"-0.00", "0.00"},
	} {
		meta := []schemaCol{dec, varchar("g", "CONCAT(d)", 20)}
		check(t, meta[1], meta, []string{"d", "g"}, []string{tc.lit, "'" + tc.stored + "'"}, false, false)
	}
	tm := schemaCol{name: "tm", base: "time", prec: 2, hasPrec: true}
	for _, tc := range []struct{ lit, stored string }{
		{"'00:00:00'", "00:00:00.00"},
		{"'-838:59:59'", "-838:59:59.00"},
	} {
		meta := []schemaCol{tm, varchar("g", "CONCAT(tm)", 20)}
		check(t, meta[1], meta, []string{"tm", "g"}, []string{tc.lit, "'" + tc.stored + "'"}, false, false)
	}
	ts := []schemaCol{{name: "ts", base: "timestamp"}, varchar("g", "CONCAT(ts)", 32)}
	check(t, ts[1], ts, []string{"ts", "g"}, []string{"'2026-10-08 02:00:00'", "'2026-10-08 10:00:00'"}, false, true)
	bin := []schemaCol{{name: "s", base: "binary", prec: 8, hasPrec: true}, {name: "g", base: "int", expr: "LENGTH(s)"}}
	check(t, bin[1], bin, []string{"s", "g"}, []string{"X'616263'", "8"}, false, false)
	micro := []schemaCol{{name: "s", base: "varchar", charset: "utf8mb4"}, varchar("g", "UPPER(s)", 8)}
	check(t, micro[1], micro, []string{"s", "g"}, []string{"'µ'", "'\u039c'"}, false, false)
	wide := []schemaCol{{name: "s", base: "varchar", charset: "utf8mb4"}, varchar("g", "CONCAT(s, s)", 3)}
	check(t, wide[1], wide, []string{"s", "g"}, []string{"'ab'", "'aba'"}, false, true)
	kinds := []schemaCol{{name: "j", base: "json"}, {name: "g", base: "json", expr: "j->'$.v'"}}
	check(t, kinds[1], kinds, []string{"j", "g"}, []string{"JSON_OBJECT('v', 1.0)", "CAST(1 AS JSON)"}, true, false)
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
