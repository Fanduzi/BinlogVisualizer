package mysqlexpr

import "testing"

func TestParseRenameTableListsPairs(t *testing.T) {
	pairs, isRename, ok := ParseRenameTable("RENAME TABLE `a` TO `b`, `s2`.`c` TO `s2`.`d`", Dialect{})
	if !isRename || !ok {
		t.Fatalf("isRename=%v ok=%v", isRename, ok)
	}
	if len(pairs) != 2 {
		t.Fatalf("pairs = %+v", pairs)
	}
	if pairs[0].Old.Table != "a" || pairs[0].New.Table != "b" {
		t.Fatalf("pair0 = %+v", pairs[0])
	}
	if pairs[1].Old.Schema != "s2" || pairs[1].Old.Table != "c" || pairs[1].New.Schema != "s2" || pairs[1].New.Table != "d" {
		t.Fatalf("pair1 = %+v", pairs[1])
	}
}

func TestParseRenameTableSinglePairAndSemicolon(t *testing.T) {
	pairs, isRename, ok := ParseRenameTable("  rename table db1.t TO db2.u ;", Dialect{})
	if !isRename || !ok || len(pairs) != 1 {
		t.Fatalf("isRename=%v ok=%v pairs=%+v", isRename, ok, pairs)
	}
	if pairs[0].Old.Schema != "db1" || pairs[0].Old.Table != "t" || pairs[0].New.Schema != "db2" || pairs[0].New.Table != "u" {
		t.Fatalf("pairs = %+v", pairs)
	}
}

func TestParseRenameTableRejectsUnreadable(t *testing.T) {
	for _, sql := range []string{
		"RENAME TABLE a",                    // no TO
		"RENAME TABLE a TO",                 // missing new name
		"RENAME TABLE a TO b, c",            // second pair incomplete
		"RENAME TABLE a TO `b",              // unterminated quote
		"RENAME TABLE a TO b; SELECT 1",     // second statement
		"RENAME TABLE a TO b EXTRA WORDS Z", // garbage tail
	} {
		if pairs, isRename, ok := ParseRenameTable(sql, Dialect{}); !isRename || ok {
			t.Fatalf("%q: isRename=%v ok=%v pairs=%+v", sql, isRename, ok, pairs)
		}
	}
}

func TestParseRenameTableIgnoresOtherStatements(t *testing.T) {
	for _, sql := range []string{
		"RENAME USER a@h TO b@h",
		"DROP TABLE a",
		"ALTER TABLE a ADD COLUMN c INT",
	} {
		if _, isRename, _ := ParseRenameTable(sql, Dialect{}); isRename {
			t.Fatalf("%q parsed as RENAME TABLE", sql)
		}
	}
}

// Schema-qualified names that look numeric (1e5, 0x1f, 0b10, 123, 00) are
// still names when quoted, the way a server writes them. Unquoted the same
// tokens are number literals to MySQL's lexer, so the reader refuses them
// the way the server would.
func TestDDLNumericLookingNamesStayNames(t *testing.T) {
	for _, sql := range []string{
		"DROP TABLE `qa412n`.`1e5`",
		"DROP TABLE `d`.`0x1f`",
		"DROP TABLE `d`.`0b10`",
		"DROP TABLE `d`.`123`",
		"DROP TABLE `d`.`00`",
	} {
		names, isDrop, ok := ParseDropTable(sql, Dialect{})
		if !isDrop || !ok || len(names) != 1 || names[0].Table == "" {
			t.Fatalf("%q: isDrop=%v ok=%v names=%+v", sql, isDrop, ok, names)
		}
	}
	for _, sql := range []string{"DROP TABLE d.1e5", "DROP TABLE d.123"} {
		if _, isDrop, ok := ParseDropTable(sql, Dialect{}); !isDrop || ok {
			t.Fatalf("%q: unquoted numeric name accepted", sql)
		}
	}
	pairs, isRename, ok := ParseRenameTable("RENAME TABLE `d`.`00` TO `d`.`1e5`", Dialect{})
	if !isRename || !ok || len(pairs) != 1 || pairs[0].New.Table != "1e5" {
		t.Fatalf("rename: %v %v %+v", isRename, ok, pairs)
	}
}

func TestOneLineCollapsesWhitespace(t *testing.T) {
	if got := OneLine("  DROP\tTABLE   a  \nmore", 0); got != "DROP TABLE a" {
		t.Fatalf("%q", got)
	}
	if got := OneLine("a\tb", 3); got != "a b" {
		t.Fatalf("%q", got)
	}
}
