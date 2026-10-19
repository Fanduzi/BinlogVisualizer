package flashback_test

import "strings"

// scriptHas reports whether fragment appears as executable text or as the
// quoted payload of a prepared statement.
// indexStmt finds fragment at the start of a line. Digest payloads quote the
// same text in the middle of a line, and those copies are not executed.
func indexStmt(sql, fragment string) int {
	from := 0
	for {
		i := strings.Index(sql[from:], fragment)
		if i < 0 {
			return -1
		}
		at := from + i
		j := at - 1
		for j >= 0 && (sql[j] == ' ' || sql[j] == '\t') {
			j--
		}
		if j < 0 || sql[j] == '\n' {
			return at
		}
		from = at + 1
	}
}

func scriptHas(sql, fragment string) bool {
	if strings.Contains(sql, fragment) {
		return true
	}
	if strings.Contains(sql, mysqlEscapeLiteral(fragment)) {
		return true
	}
	// Payloads with no backslash use doubled quotes.
	return strings.Contains(sql, strings.ReplaceAll(fragment, "'", "''"))
}

func mysqlEscapeLiteral(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\', '\'':
			b.WriteByte('\\')
			b.WriteByte(s[i])
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case 0:
			b.WriteString(`\0`)
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}
