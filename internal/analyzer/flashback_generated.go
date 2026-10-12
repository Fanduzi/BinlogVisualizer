// Package analyzer: parsing helpers for CREATE and ALTER text used by the schema catalog (schema_catalog.go).
// input: one CREATE/ALTER statement or a whole schema file. mysql --batch SHOW CREATE TABLE writes field newlines as \n. A plain mysqldump header and several SHOW CREATE rows without ';' are read too. A backslash inside a quoted string, including \', is part of that string.
// output: dump segments, statements, column lists, and ALTER replays; or false when a definition cannot be read. The schema catalog decides what that means for a table.
// pos: flashback-only helper. Analyze does not call it.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"strings"
	"unicode"
)

func isTableConstraint(def string) bool {
	switch strings.ToUpper((&sqlScan{s: def}).word()) {
	case "PRIMARY", "UNIQUE", "KEY", "INDEX", "FULLTEXT", "SPATIAL", "CONSTRAINT", "CHECK", "FOREIGN":
		return true
	default:
		return false
	}
}

func copyNames(src map[string]struct{}) map[string]struct{} {
	dst := make(map[string]struct{}, len(src))
	for name := range src {
		dst[name] = struct{}{}
	}
	return dst
}

// createTableHead reports whether CREATE TABLE is followed by a table name
// and a definition. The mysql batch header is the words "Create Table" and
// then the next row, which is not a definition.
func createTableHead(sc *sqlScan) bool {
	save, have, last := sc.i, sc.have, sc.last
	defer func() {
		sc.i, sc.have, sc.last = save, have, last
	}()
	if sc.wordIs("IF") {
		sc.wordIs("NOT")
		sc.wordIs("EXISTS")
	}
	if _, ok := sc.ident(); !ok {
		return false
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '.' {
		sc.i++
		if _, ok := sc.ident(); !ok {
			return false
		}
		sc.skip()
	}
	if sc.i >= len(sc.s) {
		return false
	}
	if sc.s[sc.i] == '(' {
		return true
	}
	word := sc.peekWord()
	return strings.EqualFold(word, "LIKE") || strings.EqualFold(word, "AS") || strings.EqualFold(word, "SELECT")
}

func indexCreateTable(s string) int {
	sc := &sqlScan{s: s}
	for sc.i < len(sc.s) {
		sc.skip()
		if sc.i >= len(sc.s) {
			return -1
		}
		switch sc.s[sc.i] {
		case '\'', '"', '`':
			sc.skipString(sc.s[sc.i])
			continue
		}
		if !identStart(sc.s[sc.i]) {
			sc.i++
			continue
		}
		start := sc.i
		w := sc.bare()
		if strings.EqualFold(w, "CREATE") && sc.wordIs("TABLE") && createTableHead(sc) {
			return start
		}
	}
	return -1
}

func splitSQL(s string) []string {
	var parts []string
	start := 0
	inString := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inString != 0 {
			if c == inString {
				if i+1 < len(s) && s[i+1] == inString {
					i++
					continue
				}
				inString = 0
			} else if c == '\\' && inString != '`' && i+1 < len(s) {
				i++
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			inString = c
		case '#':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case '-':
			if i+1 < len(s) && s[i+1] == '-' && (i+2 >= len(s) || s[i+2] == ' ' || s[i+2] == '\t' || s[i+2] == '\r' || s[i+2] == '\n') {
				for i < len(s) && s[i] != '\n' {
					i++
				}
			}
		case '/':
			if i+1 < len(s) && s[i+1] == '*' {
				i += 2
				for i+1 < len(s) && !(s[i] == '*' && s[i+1] == '/') {
					i++
				}
				if i+1 < len(s) {
					i++
				}
			}
		case ';':
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	if strings.TrimSpace(s[start:]) != "" {
		parts = append(parts, s[start:])
	}
	return parts
}

func splitComma(s string) []string {
	var parts []string
	start := 0
	depth := 0
	inString := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inString != 0 {
			if c == '\\' && inString != '`' && i+1 < len(s) {
				i++
				continue
			}
			if c == inString {
				if i+1 < len(s) && s[i+1] == inString {
					i++
					continue
				}
				inString = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			inString = c
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

type sqlScan struct {
	s    string
	i    int
	last int
	have bool
}

func (sc *sqlScan) skip() {
	for sc.i < len(sc.s) {
		switch sc.s[sc.i] {
		case ' ', '\t', '\n', '\r':
			sc.i++
		case '\\':
			// mysql --batch escapes a newline, tab, CR, or NUL inside a field as
			// \n, \t, \r, or \0. Those are whitespace in SHOW CREATE TABLE text.
			if sc.i+1 < len(sc.s) {
				switch sc.s[sc.i+1] {
				case 'n', 't', 'r', '0':
					sc.i += 2
					continue
				}
			}
			return
		case '#':
			for sc.i < len(sc.s) && sc.s[sc.i] != '\n' {
				sc.i++
			}
		case '-':
			if sc.i+1 < len(sc.s) && sc.s[sc.i+1] == '-' {
				for sc.i < len(sc.s) && sc.s[sc.i] != '\n' {
					sc.i++
				}
				continue
			}
			return
		case '/':
			if sc.i+1 < len(sc.s) && sc.s[sc.i+1] == '*' {
				sc.i += 2
				for sc.i+1 < len(sc.s) && !(sc.s[sc.i] == '*' && sc.s[sc.i+1] == '/') {
					sc.i++
				}
				if sc.i+1 < len(sc.s) {
					sc.i += 2
				}
				continue
			}
			return
		default:
			return
		}
	}
}

func (sc *sqlScan) peekByte() byte {
	sc.skip()
	if sc.i >= len(sc.s) {
		return 0
	}
	return sc.s[sc.i]
}

func (sc *sqlScan) rest() string {
	sc.skip()
	return sc.s[sc.i:]
}

func (sc *sqlScan) word() string {
	w, _ := sc.ident()
	return w
}

func (sc *sqlScan) peekWord() string {
	save, have, last := sc.i, sc.have, sc.last
	w := sc.word()
	sc.i, sc.have, sc.last = save, have, last
	return w
}

func (sc *sqlScan) unreadWord() {
	if sc.have {
		sc.i = sc.last
		sc.have = false
	}
}

func (sc *sqlScan) wordIs(want string) bool {
	save := sc.i
	w, ok := sc.ident()
	if ok && strings.EqualFold(w, want) {
		return true
	}
	sc.i = save
	sc.have = false
	return false
}

func (sc *sqlScan) ident() (string, bool) {
	sc.skip()
	if sc.i >= len(sc.s) {
		return "", false
	}
	sc.last = sc.i
	sc.have = true
	if sc.s[sc.i] == '`' {
		sc.i++
		start := sc.i
		var b strings.Builder
		for sc.i < len(sc.s) {
			if sc.s[sc.i] == '`' {
				if sc.i+1 < len(sc.s) && sc.s[sc.i+1] == '`' {
					b.WriteString(sc.s[start:sc.i])
					b.WriteByte('`')
					sc.i += 2
					start = sc.i
					continue
				}
				b.WriteString(sc.s[start:sc.i])
				sc.i++
				return b.String(), true
			}
			sc.i++
		}
		return "", false
	}
	if !identStart(sc.s[sc.i]) {
		sc.have = false
		return "", false
	}
	return sc.bare(), true
}

func (sc *sqlScan) bare() string {
	start := sc.i
	for sc.i < len(sc.s) && identPart(sc.s[sc.i]) {
		sc.i++
	}
	return sc.s[start:sc.i]
}

func (sc *sqlScan) qualified(session string) (schema, table string, ok bool) {
	first, ok := sc.ident()
	if !ok {
		return "", "", false
	}
	sc.skip()
	if sc.i < len(sc.s) && sc.s[sc.i] == '.' {
		sc.i++
		second, ok := sc.ident()
		if !ok {
			return "", "", false
		}
		return first, second, true
	}
	return session, first, true
}

func (sc *sqlScan) parenBody() (string, bool) {
	sc.skip()
	if sc.i >= len(sc.s) || sc.s[sc.i] != '(' {
		return "", false
	}
	start := sc.i + 1
	depth := 1
	inString := byte(0)
	for i := start; i < len(sc.s); i++ {
		c := sc.s[i]
		if inString != 0 {
			if c == '\\' && inString != '`' && i+1 < len(sc.s) {
				i++
				continue
			}
			if c == inString {
				if i+1 < len(sc.s) && sc.s[i+1] == inString {
					i++
					continue
				}
				inString = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			inString = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				body := sc.s[start:i]
				sc.i = i + 1
				return body, true
			}
		}
	}
	return "", false
}

func (sc *sqlScan) skipString(quote byte) {
	sc.i++
	for sc.i < len(sc.s) {
		if quote != '`' && sc.s[sc.i] == '\\' && sc.i+1 < len(sc.s) {
			sc.i += 2
			continue
		}
		if sc.s[sc.i] == quote {
			sc.i++
			if sc.i < len(sc.s) && sc.s[sc.i] == quote {
				sc.i++
				continue
			}
			return
		}
		sc.i++
	}
}

func identStart(b byte) bool {
	return b == '_' || b == '$' || b >= 0x80 || unicode.IsLetter(rune(b))
}

func identPart(b byte) bool {
	return identStart(b) || (b >= '0' && b <= '9')
}
