// Run: go test -fuzz=FuzzCompileWhere -fuzztime=20s ./internal/mysqlexpr
package mysqlexpr

import (
	"testing"

	"binlogviz/internal/model"
)

func FuzzCompileWhere(f *testing.F) {
	for _, expr := range []string{
		// Grammar coverage from where_test.go: comparisons, boolean ops, IN,
		// IS, <=>, parens, whitelisted functions, JSON paths, literals.
		"user_id < 400", "`user_id` < 400", "a = 1", "a <=> NULL",
		"a <> 'x'", "a != 'x'", "a <= 5", "a >= -5", "a IS NULL",
		"a IS NOT NULL", "a IS TRUE", "a IS NOT FALSE",
		"a IN (1, 'x', NULL)", "a NOT IN (1)",
		"a = 1 AND b = 2 OR c = 3", "a = 1 XOR b = 2", "NOT a = 1",
		"(a = 1 AND b = 2) OR c = 3", "((a = 1))", "a + 1 < b * 2",
		"UPPER(a) = 'X'", "LOWER(a) = 'x'", "CONCAT(a, b) = 'xy'",
		"LENGTH(a) > 3", "a MOD 2 = 0", "j->>'$.id' = 7",
		"a = b'00101'", "n = 1e2", "n < 2.5", "a = b", "user_id",
		"1 = 1", "0 AND a = 1",
		// Malformed and unsupported near-misses: compile errors are correct.
		"", "user_id <", "user_id = ", "(", "user_id <> <> 1",
		"a BETWEEN 1 AND 2", "t.a = 1", "a LIKE 'x%'", "a REGEXP 'x'",
		"a = NOW()", "a = LEFT(b, 1)", "a COLLATE utf8mb4_bin = 'x'",
		"a IN ()", "a = (SELECT 1)", "a IS", "a NOT", "AND a = 1",
		"a = 1 OR", "NOT", "'x' 0x41",
		// Quoting, unicode and edge lexemes.
		"`ta``ble` = 1", "名前 = '値'", "a = 'it''s'", "a = _utf8mb4'x'",
		"a = x'ff'", "a = 0x1F", `a = "q"`, "NOT NOT a", "a AND",
		"a = '日' OR b >= .5", "a in(1)", "(a)", "a<=>b", "!a",
		"a = ' '", "a IS UNKNOWN", "a NOT IN (NULL)",
	} {
		f.Add(expr)
	}
	// A fixed env exercises the three-valued evaluator on compiled input:
	// integer, text, NULL, enum-index and bit cells plus a JSON document.
	env := whereEnv(
		[]string{"a", "b", "c", "n", "j", "user_id", "name", "status", "flags", "x"},
		[]string{"1", "2", "NULL", "4.5", `'{"id":7}'`, "400", "'x'", "2", "b'00101'", "0"},
		[]model.FlashCol{
			intFlashCol(), intFlashCol(), intFlashCol(), intFlashCol(),
			{Base: "json"}, intFlashCol(), textFlashCol(),
			{Base: "enum", Members: []string{"a", "b", "c"}},
			{Base: "bit"}, intFlashCol(),
		},
	)
	f.Fuzz(func(t *testing.T, expr string) {
		w, err := CompileWhere(expr)
		if err != nil {
			return
		}
		_ = w.Refs()
		_ = w.MissingRef([]string{"a", "b", "n", "j", "user_id", "name", "status", "flags", "x"})
		_ = w.Match(env)
	})
}
