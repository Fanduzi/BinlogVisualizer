package mysqlexpr

import (
	"testing"

	"binlogviz/internal/model"
)

// whereEnv builds the per-image env the planners build: literal text parsed
// by LiteralCell, tagged with the column's facts. Only the referenced cells
// cost a parse at runtime, but tests may pass every column.
func whereEnv(names []string, lits []string, cols []model.FlashCol) map[string]GenVal {
	env := make(map[string]GenVal, len(names))
	for i, name := range names {
		var col model.FlashCol
		if i < len(cols) {
			col = cols[i]
		}
		env[name] = LiteralCell(lits[i], col)
	}
	return env
}

func intFlashCol() model.FlashCol  { return model.FlashCol{Base: "int", HasSign: true} }
func textFlashCol() model.FlashCol { return model.FlashCol{Base: "varchar", Charset: "utf8mb4"} }

// match compiles the predicate and evaluates it against one image.
func match(t *testing.T, expr string, names []string, lits []string, cols []model.FlashCol) bool {
	t.Helper()
	w, err := CompileWhere(expr)
	if err != nil {
		t.Fatalf("CompileWhere(%q): %v", expr, err)
	}
	return w.Match(whereEnv(names, lits, cols))
}

func TestWhereCompileErrors(t *testing.T) {
	// Compile failures are usage errors decided before any input is read:
	// malformed text and everything outside the documented subset (ADR 0024).
	for _, expr := range []string{
		"", "user_id <", "user_id = ", "(", "user_id <> <> 1",
		"a BETWEEN 1 AND 2", "t.a = 1", "a LIKE 'x%'", "a REGEXP 'x'",
		"a = NOW()", "a = LEFT(b, 1)", "a COLLATE utf8mb4_bin = 'x'",
		"a IN ()", "a = (SELECT 1)", "a IS", "a NOT", "AND a = 1",
		"a = 1 OR", "NOT", "'x' 0x41",
	} {
		if _, err := CompileWhere(expr); err == nil {
			t.Fatalf("CompileWhere(%q) should fail", expr)
		}
	}
}

func TestWhereCompiles(t *testing.T) {
	for _, expr := range []string{
		"user_id < 400", "`user_id` < 400", "a = 1", "a <=> NULL",
		"a <> 'x'", "a != 'x'", "a <= 5", "a >= -5", "a IS NULL", "a IS NOT NULL",
		"a IS TRUE", "a IS NOT FALSE", "a IN (1, 'x', NULL)", "a NOT IN (1)",
		"a = 1 AND b = 2 OR c = 3", "a = 1 XOR b = 2", "NOT a = 1",
		"(a = 1 AND b = 2) OR c = 3", "((a = 1))", "a + 1 < b * 2",
		"UPPER(a) = 'X'", "j->>'$.id' = 7", "a = b'00101'", "n = 1e2", "n < 2.5",
		"a = b", "user_id", "1 = 1", "0 AND a = 1",
	} {
		if _, err := CompileWhere(expr); err != nil {
			t.Fatalf("CompileWhere(%q): %v", expr, err)
		}
	}
}

func TestWhereRefsAndMissing(t *testing.T) {
	w, err := CompileWhere("User_ID < 400 AND `status` IN ('a')")
	if err != nil {
		t.Fatal(err)
	}
	refs := w.Refs()
	if len(refs) != 2 || refs[0] != "status" || refs[1] != "user_id" {
		t.Fatalf("refs: %#v", refs)
	}
	if !w.References("USER_ID") || w.References("missing") {
		t.Fatal("References")
	}
	if got := w.MissingRef([]string{"user_id"}); got != "status" {
		t.Fatalf("MissingRef: %q", got)
	}
	if got := w.MissingRef([]string{"USER_ID", "Status"}); got != "" {
		t.Fatalf("MissingRef case fold: %q", got)
	}
}

func TestWhereNumericComparisons(t *testing.T) {
	names := []string{"user_id"}
	cols := []model.FlashCol{intFlashCol()}
	for _, tc := range []struct {
		expr string
		lit  string
		want bool
	}{
		{"user_id < 400", "399", true},
		{"user_id < 400", "400", false},
		{"user_id < 400", "401", false},
		{"user_id <= 400", "400", true},
		{"user_id = 400", "400", true},
		{"user_id != 400", "400", false},
		{"user_id > 399", "400", true},
		{"user_id >= 400", "399", false},
		{"user_id + 1 = 400", "399", true},
		{"user_id = '400'", "400", true},
		{"user_id = 'abc'", "400", false}, // 'abc' is not a number: unverifiable, never a match
		{"user_id = 4e2", "400", true},
		{"user_id = 4.0e2", "400", true},
		{"user_id < 400.5", "400", true},
		{"user_id < 400", "NULL", false}, // three-valued: NULL is not a match
		{"user_id IS NULL", "NULL", true},
		{"user_id IS NOT NULL", "399", true},
		{"user_id <=> NULL", "NULL", true},
		{"user_id <=> NULL", "399", false},
		{"user_id <=> 399", "399", true},
	} {
		if got := match(t, tc.expr, names, []string{tc.lit}, cols); got != tc.want {
			t.Fatalf("%q on %s: got %v, want %v", tc.expr, tc.lit, got, tc.want)
		}
	}
}

func TestWhereBooleanLayer(t *testing.T) {
	names := []string{"a", "b"}
	cols := []model.FlashCol{intFlashCol(), intFlashCol()}
	for _, tc := range []struct {
		expr string
		lits []string
		want bool
	}{
		{"a = 1 AND b = 2", []string{"1", "2"}, true},
		{"a = 1 AND b = 2", []string{"1", "3"}, false},
		{"a = 1 OR b = 2", []string{"9", "2"}, true},
		{"a = 1 OR b = 2", []string{"9", "9"}, false},
		{"a = 1 XOR b = 2", []string{"1", "9"}, true},
		{"a = 1 XOR b = 2", []string{"1", "2"}, false},
		{"NOT a = 1", []string{"9", "0"}, true},
		{"NOT (a = 1 AND b = 2)", []string{"1", "9"}, true},
		{"(a = 1 AND b = 2) OR b = 9", []string{"9", "9"}, true},
		// Three-valued: NULL AND false is false; NULL OR true is true.
		{"a = 1 AND b = 2", []string{"NULL", "9"}, false},
		{"a = 1 OR b = 2", []string{"NULL", "2"}, true},
		{"a = 1 OR b = 2", []string{"NULL", "9"}, false},
		{"a = 1 XOR b = 2", []string{"NULL", "2"}, false},
		{"NOT a = 1", []string{"NULL", "0"}, false},
		{"a IN (1, 2, 3)", []string{"2", "0"}, true},
		{"a IN (1, 2, 3)", []string{"9", "0"}, false},
		{"a NOT IN (1, 2)", []string{"9", "0"}, true},
		{"a NOT IN (1, 2)", []string{"1", "0"}, false},
		{"a IN (1, NULL)", []string{"9", "0"}, false}, // no hit, NULL member: unknown
		{"a IS TRUE", []string{"5", "0"}, true},
		{"a IS TRUE", []string{"0", "0"}, false},
		{"a IS FALSE", []string{"0", "0"}, true},
		{"a IS TRUE", []string{"NULL", "0"}, false},
		{"a", []string{"3", "0"}, true},
		{"a", []string{"0", "0"}, false},
	} {
		if got := match(t, tc.expr, names, tc.lits, cols); got != tc.want {
			t.Fatalf("%q on %#v: got %v, want %v", tc.expr, tc.lits, got, tc.want)
		}
	}
}

func TestWhereTextClaims(t *testing.T) {
	names := []string{"name"}
	cols := []model.FlashCol{textFlashCol()}
	for _, tc := range []struct {
		expr string
		lit  string
		want bool
	}{
		{"name = 'x'", "'x'", true},
		{"name = 'x'", "'y'", false},
		{"name < 'm'", "'a'", true},
		{"name < 'm'", "'z'", false},
		// Only a letter-case difference: the collation is not in the binlog,
		// so the claim is unverifiable and the row is excluded (ADR 0024).
		{"name = 'X'", "'x'", false},
		{"name != 'X'", "'x'", false},
		// A trailing-space-only difference: PAD SPACE collations say equal,
		// NO PAD says not — unverifiable in either direction.
		{"name = 'x'", "'x '", false},
		{"name != 'x'", "'x '", false},
		{"name = 'abc' AND 1 = 1", "'abc'", true},
		{"UPPER(name) = 'X'", "'x'", true},
		{"name IN ('a', 'b')", "'b'", true},
	} {
		if got := match(t, tc.expr, names, []string{tc.lit}, cols); got != tc.want {
			t.Fatalf("%q on %s: got %v, want %v", tc.expr, tc.lit, got, tc.want)
		}
	}
}

func TestWhereEnumAndBitCells(t *testing.T) {
	// An ENUM cell is its index literal; the label is what MySQL compares.
	enumCol := model.FlashCol{Base: "enum", Members: []string{"a", "b", "c"}}
	names := []string{"status"}
	if !match(t, "status = 'b'", names, []string{"2"}, []model.FlashCol{enumCol}) {
		t.Fatal("enum index 2 should equal label 'b'")
	}
	if match(t, "status = 'z'", names, []string{"2"}, []model.FlashCol{enumCol}) {
		t.Fatal("enum index 2 should not equal 'z'")
	}
	// A BIT cell is the b'..' literal's numeric value.
	bitCol := model.FlashCol{Base: "bit"}
	if !match(t, "flags = 5", []string{"flags"}, []string{"b'00101'"}, []model.FlashCol{bitCol}) {
		t.Fatal("b'00101' should equal 5")
	}
	if !match(t, "flags = b'101'", []string{"flags"}, []string{"b'00101'"}, []model.FlashCol{bitCol}) {
		t.Fatal("b'00101' should equal b'101'")
	}
}

func TestWhereUnverifiableIsNotAMatch(t *testing.T) {
	// A missing env entry, an unparseable literal, and an undecoded text cell
	// are unknown — excluded, never guessed (ADR 0024).
	w, err := CompileWhere("a = 1")
	if err != nil {
		t.Fatal(err)
	}
	if w.Match(map[string]GenVal{}) {
		t.Fatal("empty env should not match")
	}
	if w.Match(map[string]GenVal{"a": {kind: genBad}}) {
		t.Fatal("bad cell should not match")
	}
	if w.Match(map[string]GenVal{"a": {kind: genText, text: "1", decoded: false}}) {
		t.Fatal("undecoded text should not match")
	}
	if w.Match(nil) {
		t.Fatal("nil env should not match")
	}
	var nilW *WhereExpr
	if nilW.Match(nil) {
		t.Fatal("nil predicate should not match")
	}
}
