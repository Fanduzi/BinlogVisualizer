package mysqlexpr

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

// Differential check of EvalGenExpr and EvalDecExprInc against MySQL
// SELECT <expr> (ADR 0020). A returned value must match the server. ok=false,
// and a DECIMAL result that is not decClaim, are the explicit unsupported
// outcome: they are counted, not treated as a match. BINLOGVIZ_DIFFTEST=1
// and BINLOGVIZ_DIFFTEST_MYSQL (the mysql client command) enable the test.
// BINLOGVIZ_DIFFTEST_NIGHTLY=1 enlarges the seeded corpus.

func TestDiffMySQLExpressions(t *testing.T) {
	if os.Getenv("BINLOGVIZ_DIFFTEST") != "1" {
		t.Skip("set BINLOGVIZ_DIFFTEST=1 and BINLOGVIZ_DIFFTEST_MYSQL to compare expressions with MySQL")
	}
	if strings.TrimSpace(os.Getenv("BINLOGVIZ_DIFFTEST_MYSQL")) == "" {
		t.Fatal("BINLOGVIZ_DIFFTEST_MYSQL is required")
	}
	ver := diffMySQLMust(t, "SELECT @@version")
	ver = strings.TrimSpace(ver)
	if !strings.HasPrefix(ver, "8.0") && !strings.HasPrefix(ver, "8.4") {
		t.Fatalf("server %q is not MySQL 8.0 or 8.4", ver)
	}

	n := 24
	if os.Getenv("BINLOGVIZ_DIFFTEST_NIGHTLY") == "1" {
		n = 400
	}
	cases := append(fixedDiffCases(t), generatedDiffCases(n)...)
	matched := map[string]int{}
	unsupported := map[string]int{}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			gotOK, kind, detail := evalDiff(c)
			if !gotOK {
				unsupported[c.cat]++
				if c.mustOK {
					t.Fatalf("expression is unverified, category %s requires a value: %s", c.cat, c.expr)
				}
				if c.mustRefuse {
					if _, _, err := selectDiff(t, c, "", ""); err == nil {
						t.Fatalf("checker refused %s but MySQL accepted it", c.expr)
					}
				}
				return
			}
			null, text, err := selectDiff(t, c, kind, detail)
			if err != nil {
				t.Fatalf("checker returned %s %s but MySQL rejected %s: %v", kind, detail, c.expr, err)
			}
			if err := matchDiff(kind, detail, null, text); err != nil {
				t.Fatalf("%s\nexpr: %s\nchecker: %s %s\nmysql null=%v text=%q", err, c.expr, kind, detail, null, text)
			}
			matched[c.cat]++
		})
	}
	for _, cat := range []string{"null", "case", "numeric", "decimal", "json", "string", "temporal"} {
		if matched[cat] == 0 {
			t.Errorf("category %s had no result that matched MySQL (unsupported %d)", cat, unsupported[cat])
		}
	}
	for _, cat := range []string{"collation", "coercion"} {
		if matched[cat]+unsupported[cat] == 0 {
			t.Errorf("category %s was not in the corpus", cat)
		}
	}
	t.Logf("version %s cases %d matched %v unsupported %v", ver, len(cases), matched, unsupported)
}

type diffCase struct {
	name       string
	cat        string
	expr       string
	cols       []diffCol
	domain     string // gen or dec
	inc        int
	mustOK     bool
	mustRefuse bool // checker and MySQL both refuse
}

type diffCol struct {
	name string
	sql  string
	val  GenVal
}

func evalDiff(c diffCase) (ok bool, kind, detail string) {
	env := map[string]GenVal{}
	for _, col := range c.cols {
		env[col.name] = col.val
	}
	if c.domain == "dec" {
		v, ok := EvalDecExprInc(c.expr, env, c.inc)
		if !ok || v.mode != decClaim {
			return false, "", ""
		}
		if v.null {
			return true, "null", ""
		}
		if v.n == nil {
			return false, "", ""
		}
		return true, "dec", v.n.RatString() + "#" + strconv.Itoa(v.scale)
	}
	v, ok := EvalGenExpr(c.expr, env)
	if !ok {
		return false, "", ""
	}
	switch v.kind {
	case genNull:
		return true, "null", ""
	case genNum:
		if v.n == nil {
			return false, "", ""
		}
		return true, "num", v.n.RatString()
	case genText:
		return true, "text", v.text
	case genJSON:
		raw, err := json.Marshal(jToNorm(v.j))
		if err != nil {
			return false, "", ""
		}
		return true, "json", string(raw)
	default:
		return false, "", ""
	}
}

func matchDiff(kind, detail string, mysqlNull bool, text string) error {
	switch kind {
	case "null":
		if !mysqlNull {
			return fmt.Errorf("checker NULL, MySQL %q", text)
		}
		return nil
	case "num":
		if mysqlNull {
			return fmt.Errorf("checker number, MySQL NULL")
		}
		want, ok := new(big.Rat).SetString(detail)
		if !ok {
			return fmt.Errorf("checker rat %s", detail)
		}
		got, ok := new(big.Rat).SetString(text)
		if !ok {
			return fmt.Errorf("MySQL text is not a number")
		}
		if want.Cmp(got) != 0 {
			return fmt.Errorf("number %s != %s", want.RatString(), got.RatString())
		}
		return nil
	case "dec":
		if mysqlNull {
			return fmt.Errorf("checker decimal, MySQL NULL")
		}
		rat, scaleText, ok := strings.Cut(detail, "#")
		if !ok {
			return fmt.Errorf("checker detail %s", detail)
		}
		wantScale, err := strconv.Atoi(scaleText)
		if err != nil {
			return err
		}
		want, ok := new(big.Rat).SetString(rat)
		if !ok {
			return fmt.Errorf("checker rat %s", rat)
		}
		got, ok := new(big.Rat).SetString(text)
		if !ok {
			return fmt.Errorf("MySQL text is not a decimal")
		}
		if want.Cmp(got) != 0 {
			return fmt.Errorf("decimal %s != %s", want.RatString(), got.RatString())
		}
		scale := 0
		if i := strings.IndexByte(text, '.'); i >= 0 {
			scale = len(text) - i - 1
		}
		if scale != wantScale {
			return fmt.Errorf("scale %d != MySQL %d (%s)", wantScale, scale, text)
		}
		return nil
	case "text":
		if mysqlNull {
			return fmt.Errorf("checker text %q, MySQL NULL", detail)
		}
		if text != detail {
			return fmt.Errorf("text %q != %q", detail, text)
		}
		return nil
	case "json":
		if mysqlNull {
			return fmt.Errorf("checker JSON, MySQL NULL")
		}
		var want, got any
		if err := json.Unmarshal([]byte(detail), &want); err != nil {
			return err
		}
		norm, err := mysqlJSONNorm(text)
		if err != nil {
			return fmt.Errorf("MySQL JSON %q: %w", text, err)
		}
		raw, err := json.Marshal(norm)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &got); err != nil {
			return err
		}
		if !normJSONEqual(want, got) {
			return fmt.Errorf("json %s != %s", detail, string(raw))
		}
		return nil
	default:
		return fmt.Errorf("kind %s", kind)
	}
}

func jToNorm(n *jNode) any {
	if n == nil {
		return nil
	}
	switch n.kind {
	case jNull:
		return nil
	case jBool:
		return n.b
	case jNum:
		r, ok := new(big.Rat).SetString(n.num)
		if !ok {
			return n.num
		}
		return r.FloatString(30)
	case jStr:
		return n.str
	case jArr:
		a := make([]any, len(n.arr))
		for i, e := range n.arr {
			a[i] = jToNorm(e)
		}
		return a
	case jObj:
		m := map[string]any{}
		for _, p := range n.obj {
			m[p.k] = jToNorm(p.v)
		}
		return m
	default:
		return nil
	}
}

func mysqlJSONNorm(text string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		// CAST of a JSON scalar is the raw text, not a JSON document.
		return text, nil
	}
	return normJSONValue(v), nil
}

func normJSONValue(v any) any {
	switch x := v.(type) {
	case json.Number:
		r, ok := new(big.Rat).SetString(x.String())
		if !ok {
			return x.String()
		}
		return r.FloatString(30)
	case string:
		return x
	case bool:
		return x
	case nil:
		return nil
	case []any:
		a := make([]any, len(x))
		for i, e := range x {
			a[i] = normJSONValue(e)
		}
		return a
	case map[string]any:
		m := map[string]any{}
		for k, e := range x {
			m[k] = normJSONValue(e)
		}
		return m
	default:
		return fmt.Sprint(x)
	}
}

func normJSONEqual(a, b any) bool {
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	return bytes.Equal(aj, bj)
}

func selectDiff(t *testing.T, c diffCase, kind, detail string) (null bool, text string, err error) {
	t.Helper()
	inc := c.inc
	if c.domain != "dec" {
		inc = 4
	}
	var q string
	if c.domain == "dec" && kind == "dec" {
		q, err = decSelectSQL(c, inc, detail)
		if err != nil {
			return false, "", err
		}
	} else {
		var from string
		if len(c.cols) > 0 {
			parts := make([]string, len(c.cols))
			for i, col := range c.cols {
				parts[i] = col.sql + " AS `" + col.name + "`"
			}
			from = " FROM (SELECT " + strings.Join(parts, ", ") + ") AS dv"
		}
		expr := "(" + c.expr + ")"
		q = fmt.Sprintf("SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci; SET SESSION div_precision_increment = %d; SELECT CONCAT(IF(%s IS NULL, 'N', 'V'), ':', IF(%s IS NULL, '', HEX(CAST(%s AS CHAR CHARACTER SET utf8mb4))))%s", inc, expr, expr, expr, from)
	}
	out, err := diffMySQL(q)
	if err != nil {
		return false, "", err
	}
	line := lastNonEmpty(out)
	flag, rest, ok := strings.Cut(line, ":")
	if !ok || (flag != "N" && flag != "V") {
		return false, "", fmt.Errorf("mysql output %q", out)
	}
	if flag == "N" {
		return true, "", nil
	}
	raw, err := hex.DecodeString(rest)
	if err != nil {
		return false, "", err
	}
	return false, string(raw), nil
}

func lastNonEmpty(s string) string {
	var last string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) != "" {
			last = line
		}
	}
	return last
}

// decSelectSQL reads the checker's claimed scale from a generated
// DECIMAL(65, scale) column. A bare SELECT of integer division uses the
// expression result scale (div_precision_increment) and drops digits the
// checker keeps for a DECIMAL target (ADR 0020).
func decSelectSQL(c diffCase, inc int, detail string) (string, error) {
	_, scaleText, ok := strings.Cut(detail, "#")
	if !ok {
		return "", fmt.Errorf("checker detail %s", detail)
	}
	scale, err := strconv.Atoi(scaleText)
	if err != nil {
		return "", err
	}
	defs := make([]string, 0, len(c.cols)+1)
	names := make([]string, 0, len(c.cols))
	vals := make([]string, 0, len(c.cols))
	for _, col := range c.cols {
		defs = append(defs, "`"+col.name+"` "+col.mysqlType())
		names = append(names, "`"+col.name+"`")
		vals = append(vals, col.sql)
	}
	defs = append(defs, fmt.Sprintf("g DECIMAL(65,%d) AS (%s) STORED", scale, c.expr))
	insert := "VALUES ()"
	if len(names) > 0 {
		insert = "(" + strings.Join(names, ", ") + ") VALUES (" + strings.Join(vals, ", ") + ")"
	}
	return fmt.Sprintf(`SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci;
SET SESSION div_precision_increment = %d;
CREATE DATABASE IF NOT EXISTS bvdiffexpr;
DROP TABLE IF EXISTS bvdiffexpr.d;
CREATE TABLE bvdiffexpr.d (%s);
INSERT INTO bvdiffexpr.d %s;
SELECT CONCAT(IF(g IS NULL, 'N', 'V'), ':', IF(g IS NULL, '', HEX(CAST(g AS CHAR CHARACTER SET utf8mb4)))) FROM bvdiffexpr.d`,
		inc, strings.Join(defs, ", "), insert), nil
}

func (c diffCol) mysqlType() string {
	switch c.val.base {
	case "decimal":
		prec, scale := c.val.prec, c.val.scale
		if prec <= 0 {
			prec = 10
		}
		return fmt.Sprintf("DECIMAL(%d,%d)", prec, scale)
	case "datetime":
		return fmt.Sprintf("DATETIME(%d)", c.val.prec)
	case "timestamp":
		return "TIMESTAMP"
	}
	switch c.val.kind {
	case genJSON:
		return "JSON"
	case genText:
		return "VARCHAR(255)"
	default:
		return "BIGINT"
	}
}

func diffMySQLMust(t *testing.T, sql string) string {
	t.Helper()
	out, err := diffMySQL(sql)
	if err != nil {
		t.Fatalf("mysql: %v\n%s", err, out)
	}
	return out
}

func diffMySQL(sql string) (string, error) {
	base := strings.Fields(os.Getenv("BINLOGVIZ_DIFFTEST_MYSQL"))
	args := append(append([]string{}, base[1:]...), "--default-character-set=utf8mb4", "--batch", "--raw", "--skip-column-names", "--connect-timeout=10")
	cmd := exec.Command(base[0], args...)
	cmd.Stdin = strings.NewReader(sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return stdout.String(), fmt.Errorf("%w: %s", err, stderr.String())
	}
	return stdout.String(), nil
}

func fixedDiffCases(t *testing.T) []diffCase {
	t.Helper()
	jCol := jsonCol(t, "c_j", `{"a":1,"b":"x","n":null}`)
	n := intCol("c_n", "7")
	s := textCol("c_s", "AbC")
	d := decCol("c_d", "1.50", 10, 2)
	dt := dtCol("c_dt", "2020-01-02 03:04:05.120", 3)
	ts := diffCol{
		name: "c_ts",
		sql:  "TIMESTAMP(" + sqlQuote("2020-01-02 03:04:05") + ")",
		val:  GenVal{kind: genText, text: "2020-01-02 03:04:05", base: "timestamp", decoded: true, charset: "utf8mb4"},
	}
	return []diffCase{
		{name: "null-plus", cat: "null", expr: "1 + NULL", domain: "gen", mustOK: true},
		{name: "null-plus-left", cat: "null", expr: "NULL + c_n", cols: []diffCol{n}, domain: "gen", mustOK: true},
		{name: "concat-null", cat: "null", expr: "CONCAT(NULL, 'a')", domain: "gen", mustOK: true},
		{name: "concat-ws-null", cat: "null", expr: "CONCAT_WS(',', NULL, 'b')", domain: "gen", mustOK: true},
		{name: "concat-ws-sep-null", cat: "null", expr: "CONCAT_WS(NULL, 'a', 'b')", domain: "gen", mustOK: true},
		{name: "upper-null", cat: "null", expr: "UPPER(NULL)", domain: "gen", mustOK: true},
		{name: "div-zero", cat: "null", expr: "1 / 0", domain: "gen", mustOK: true},
		{name: "div-zero-word", cat: "null", expr: "1 DIV 0", domain: "gen", mustOK: true},
		{name: "mod-zero", cat: "null", expr: "MOD(1, 0)", domain: "gen", mustOK: true},
		{name: "upper-ascii", cat: "case", expr: "UPPER('AbC')", domain: "gen", mustOK: true},
		{name: "lower-ascii", cat: "case", expr: "LOWER('AbC')", domain: "gen", mustOK: true},
		{name: "upper-micro", cat: "case", expr: "UPPER('µ')", domain: "gen", mustOK: true},
		{name: "upper-col", cat: "case", expr: "UPPER(c_s)", cols: []diffCol{s}, domain: "gen", mustOK: true},
		{name: "lower-micro-col", cat: "case", expr: "LOWER(c_mu)", cols: []diffCol{textCol("c_mu", "µ")}, domain: "gen", mustOK: true},
		{name: "upper-eszett", cat: "case", expr: "UPPER('ß')", domain: "gen"},
		{name: "upper-latin1-micro", cat: "case", expr: "UPPER(_latin1 X'B5')", domain: "gen", mustOK: true},
		{name: "collate-turkish", cat: "collation", expr: "UPPER('i' COLLATE utf8mb4_tr_0900_ai_ci)", domain: "gen"},
		{name: "collate-bin", cat: "collation", expr: "UPPER(c_s COLLATE utf8mb4_bin)", cols: []diffCol{s}, domain: "gen"},
		{name: "collate-literal", cat: "collation", expr: "'a' COLLATE utf8mb4_0900_ai_ci", domain: "gen"},
		{name: "add", cat: "numeric", expr: "c_n + 1", cols: []diffCol{n}, domain: "gen", mustOK: true},
		{name: "sub", cat: "numeric", expr: "c_n - 2", cols: []diffCol{n}, domain: "gen", mustOK: true},
		{name: "mul", cat: "numeric", expr: "c_n * 3", cols: []diffCol{n}, domain: "gen", mustOK: true},
		{name: "div", cat: "numeric", expr: "5 / 2", domain: "gen", mustOK: true},
		{name: "neg-div", cat: "numeric", expr: "-5 / 2", domain: "gen", mustOK: true},
		{name: "div-word", cat: "numeric", expr: "5 DIV 2", domain: "gen", mustOK: true},
		{name: "neg-div-word", cat: "numeric", expr: "-5 DIV 2", domain: "gen", mustOK: true},
		{name: "mod", cat: "numeric", expr: "-5 % 2", domain: "gen", mustOK: true},
		{name: "mod-call", cat: "numeric", expr: "MOD(c_n, 2)", cols: []diffCol{n}, domain: "gen", mustOK: true},
		{name: "big-add", cat: "numeric", expr: "9223372036854775807 + 1", domain: "gen", mustRefuse: true},
		{name: "big-sub", cat: "numeric", expr: "-9223372036854775808 - 1", domain: "gen", mustRefuse: true},
		{name: "big-mul", cat: "numeric", expr: "9223372036854775807 * 2", domain: "gen", mustRefuse: true},
		{name: "big-div", cat: "numeric", expr: "-9223372036854775808 DIV -1", domain: "gen", mustRefuse: true},
		{name: "dec-div", cat: "decimal", expr: "5 / 2", domain: "dec", inc: 4, mustOK: true},
		{name: "dec-div-inc0", cat: "decimal", expr: "1 / 7", domain: "dec", inc: 0, mustOK: true},
		{name: "dec-div-inc4", cat: "decimal", expr: "1 / 7", domain: "dec", inc: 4, mustOK: true},
		{name: "dec-add", cat: "decimal", expr: "c_d + c_d2", cols: []diffCol{d, decCol("c_d2", "2.25", 10, 2)}, domain: "dec", inc: 4, mustOK: true},
		{name: "dec-big-sub", cat: "decimal", expr: "c_n - 2", cols: []diffCol{intCol("c_n", "-9223372036854775808")}, domain: "dec", inc: 4, mustRefuse: true},
		{name: "dec-wide-sub", cat: "decimal", expr: "c_w - 2", cols: []diffCol{decCol("c_w", "-9223372036854775808", 65, 0)}, domain: "dec", inc: 4, mustOK: true},
		{name: "json-arrow", cat: "json", expr: "c_j -> '$.a'", cols: []diffCol{jCol}, domain: "gen", mustOK: true},
		{name: "json-unquote", cat: "json", expr: "c_j ->> '$.b'", cols: []diffCol{jCol}, domain: "gen", mustOK: true},
		{name: "json-null-path", cat: "json", expr: "c_j -> '$.missing'", cols: []diffCol{jCol}, domain: "gen", mustOK: true},
		{name: "json-sql-null", cat: "json", expr: "c_j ->> '$.n'", cols: []diffCol{jCol}, domain: "gen", mustOK: true},
		{name: "json-extract", cat: "json", expr: "JSON_EXTRACT(c_j, '$.a')", cols: []diffCol{jCol}, domain: "gen", mustOK: true},
		{name: "json-autowrap", cat: "json", expr: "c_j ->> '$.a[0]'", cols: []diffCol{jCol}, domain: "gen", mustOK: true},
		{name: "concat", cat: "string", expr: "CONCAT('a', 'b')", domain: "gen", mustOK: true},
		{name: "concat-int", cat: "string", expr: "CONCAT(c_n, 'a')", cols: []diffCol{n}, domain: "gen", mustOK: true},
		{name: "concat-two-int", cat: "string", expr: "CONCAT(1, 2)", domain: "gen", mustOK: true},
		{name: "length", cat: "string", expr: "LENGTH(c_s)", cols: []diffCol{s}, domain: "gen", mustOK: true},
		{name: "char-length", cat: "string", expr: "CHAR_LENGTH('µ')", domain: "gen", mustOK: true},
		{name: "concat-ws", cat: "string", expr: "CONCAT_WS('-', 'a', 'b', 'c')", domain: "gen", mustOK: true},
		{name: "concat-dec", cat: "string", expr: "CONCAT(c_d, 'x')", cols: []diffCol{d}, domain: "gen", mustOK: true},
		{name: "temporal-concat", cat: "temporal", expr: "CONCAT(c_dt, 'Z')", cols: []diffCol{dt}, domain: "gen", mustOK: true},
		{name: "temporal-date-add", cat: "temporal", expr: "DATE_ADD('2020-01-01', INTERVAL 1 DAY)", domain: "gen"},
		{name: "temporal-timestamp", cat: "temporal", expr: "CONCAT(c_ts, 'Z')", cols: []diffCol{ts}, domain: "gen"},
		{name: "coerce-string-add", cat: "coercion", expr: "'1' + c_n", cols: []diffCol{n}, domain: "gen"},
		{name: "coerce-concat", cat: "coercion", expr: "CONCAT(c_n, c_s)", cols: []diffCol{n, s}, domain: "gen", mustOK: true},
	}
}

func generatedDiffCases(n int) []diffCase {
	rng := rand.New(rand.NewSource(20261027))
	ints := []string{"0", "1", "-5", "7", "2147483647", "-9223372036854775808"}
	strs := []string{"AbC", "", "a", "µ", "i", "İ"}
	docs := []string{`{"a":1}`, `{"a":"x"}`, `{"a":null}`, `{"a":true}`, `{"a":[1,2]}`}
	decs := []string{"0.00", "1.50", "-2.25", "10.00"}
	ops := []string{"+", "-", "*", "/", "DIV", "%"}
	incs := []int{0, 4}
	var out []diffCase
	for len(out) < n {
		switch rng.Intn(7) {
		case 0:
			val := ints[rng.Intn(len(ints))]
			op := ops[rng.Intn(len(ops))]
			right := []string{"1", "2", "0"}[rng.Intn(3)]
			expr := "c_n " + op + " " + right
			col := intCol("c_n", val)
			out = append(out, diffCase{name: fmt.Sprintf("g-num-%d", len(out)), cat: "numeric", expr: expr, cols: []diffCol{col}, domain: "gen"})
			out = append(out, diffCase{name: fmt.Sprintf("g-dec-%d", len(out)), cat: "decimal", expr: expr, cols: []diffCol{col}, domain: "dec", inc: incs[rng.Intn(len(incs))]})
		case 1:
			text := strs[rng.Intn(len(strs))]
			col := textCol("c_s", text)
			expr := []string{"UPPER(c_s)", "LOWER(c_s)", "LENGTH(c_s)", "CHAR_LENGTH(c_s)", "CONCAT(c_s, 'x')"}[rng.Intn(5)]
			out = append(out, diffCase{name: fmt.Sprintf("g-str-%d", len(out)), cat: "string", expr: expr, cols: []diffCol{col}, domain: "gen"})
		case 2:
			doc := docs[rng.Intn(len(docs))]
			node, ok := parseJSONText(doc)
			if !ok {
				continue
			}
			col := diffCol{name: "c_j", sql: "CAST(" + sqlQuote(doc) + " AS JSON)", val: GenVal{kind: genJSON, j: node}}
			expr := []string{"c_j -> '$.a'", "c_j ->> '$.a'", "JSON_EXTRACT(c_j, '$.a')", "c_j -> '$.a[0]'"}[rng.Intn(4)]
			out = append(out, diffCase{name: fmt.Sprintf("g-json-%d", len(out)), cat: "json", expr: expr, cols: []diffCol{col}, domain: "gen"})
		case 3:
			text := decs[rng.Intn(len(decs))]
			col := decCol("c_d", text, 10, 2)
			out = append(out, diffCase{name: fmt.Sprintf("g-dconcat-%d", len(out)), cat: "string", expr: "CONCAT(c_d, 'x')", cols: []diffCol{col}, domain: "gen"})
			out = append(out, diffCase{name: fmt.Sprintf("g-dadd-%d", len(out)), cat: "decimal", expr: "c_d + 1", cols: []diffCol{col}, domain: "dec", inc: 4})
		case 4:
			text := []string{"2020-01-02 03:04:05", "2020-01-02 03:04:05.120"}[rng.Intn(2)]
			fsp := 0
			if strings.Contains(text, ".") {
				fsp = 3
			}
			out = append(out, diffCase{name: fmt.Sprintf("g-dt-%d", len(out)), cat: "temporal", expr: "CONCAT(c_dt, 'Z')", cols: []diffCol{dtCol("c_dt", text, fsp)}, domain: "gen"})
		case 5:
			val := ints[rng.Intn(len(ints))]
			out = append(out, diffCase{name: fmt.Sprintf("g-co-%d", len(out)), cat: "coercion", expr: "'1' + c_n", cols: []diffCol{intCol("c_n", val)}, domain: "gen"})
		default:
			text := strs[rng.Intn(len(strs))]
			out = append(out, diffCase{name: fmt.Sprintf("g-col-%d", len(out)), cat: "collation", expr: "UPPER(c_s COLLATE utf8mb4_bin)", cols: []diffCol{textCol("c_s", text)}, domain: "gen"})
		}
	}
	return out[:n]
}

func jsonCol(t *testing.T, name, doc string) diffCol {
	t.Helper()
	node, ok := parseJSONText(doc)
	if !ok {
		t.Fatalf("json %s", doc)
	}
	return diffCol{name: name, sql: "CAST(" + sqlQuote(doc) + " AS JSON)", val: GenVal{kind: genJSON, j: node}}
}

func intCol(name, n string) diffCol {
	v, ok := genNumVal(n)
	if !ok {
		panic(n)
	}
	v.base = "bigint"
	return diffCol{name: name, sql: n, val: v}
}

func textCol(name, text string) diffCol {
	return diffCol{name: name, sql: sqlQuote(text), val: genTextVal(text, "utf8mb4")}
}

func decCol(name, text string, prec, scale int) diffCol {
	v, ok := genNumVal(text)
	if !ok {
		panic(text)
	}
	v.base = "decimal"
	v.hasPrec = true
	v.prec = prec
	v.scale = scale
	v.numText = text
	return diffCol{name: name, sql: fmt.Sprintf("CAST(%s AS DECIMAL(%d,%d))", sqlQuote(text), prec, scale), val: v}
}

func dtCol(name, text string, fsp int) diffCol {
	return diffCol{
		name: name,
		sql:  fmt.Sprintf("CAST(%s AS DATETIME(%d))", sqlQuote(text), fsp),
		val:  GenVal{kind: genText, text: text, base: "datetime", decoded: true, hasPrec: true, prec: fsp, charset: "utf8mb4"},
	}
}

func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
