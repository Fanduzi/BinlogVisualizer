// Package mysqlexpr: parsing helpers for CREATE and ALTER text used by the schema catalog (internal/schemacatalog).
// input: one CREATE/ALTER statement or a whole schema file. mysql --batch SHOW CREATE TABLE writes field newlines as \n. A plain mysqldump header and several SHOW CREATE rows without ';' are read too. A backslash inside a quoted string, including \', is part of that string.
// output: dump segments, statements, column lists, and ALTER replays; or false when a definition cannot be read. The schema catalog decides what that means for a table.
// pos: flashback-only helper. Analyze does not call it.
// note: if this file changes, update this header and module README.md.
package mysqlexpr

import (
	"strings"
	"unicode"
)

func isTableConstraint(def string) bool {
	switch strings.ToUpper((&Scan{S: def}).Word()) {
	case "PRIMARY", "UNIQUE", "KEY", "INDEX", "FULLTEXT", "SPATIAL", "CONSTRAINT", "CHECK", "FOREIGN":
		return true
	default:
		return false
	}
}

func CopyNames(src map[string]struct{}) map[string]struct{} {
	dst := make(map[string]struct{}, len(src))
	for name := range src {
		dst[name] = struct{}{}
	}
	return dst
}

// createTableHead reports whether CREATE TABLE is followed by a table name
// and a definition. The mysql batch header is the words "Create Table" and
// then the next row, which is not a definition.
func createTableHead(sc *Scan) bool {
	save, have, last := sc.I, sc.have, sc.last
	defer func() {
		sc.I, sc.have, sc.last = save, have, last
	}()
	if sc.WordIs("IF") {
		sc.WordIs("NOT")
		sc.WordIs("EXISTS")
	}
	if _, ok := sc.Ident(); !ok {
		return false
	}
	sc.Skip()
	if sc.I < len(sc.S) && sc.S[sc.I] == '.' {
		sc.I++
		if _, ok := sc.Ident(); !ok {
			return false
		}
		sc.Skip()
	}
	if sc.I >= len(sc.S) {
		return false
	}
	if sc.S[sc.I] == '(' {
		return true
	}
	word := sc.PeekWord()
	return strings.EqualFold(word, "LIKE") || strings.EqualFold(word, "AS") || strings.EqualFold(word, "SELECT")
}

func IndexCreateTable(s string) int {
	sc := &Scan{S: s}
	for sc.I < len(sc.S) {
		sc.Skip()
		if sc.I >= len(sc.S) {
			return -1
		}
		switch sc.S[sc.I] {
		case '\'', '"', '`':
			sc.skipString(sc.S[sc.I])
			continue
		}
		if !identStart(sc.S[sc.I]) {
			sc.I++
			continue
		}
		start := sc.I
		w := sc.bare()
		if strings.EqualFold(w, "CREATE") && sc.WordIs("TABLE") && createTableHead(sc) {
			return start
		}
	}
	return -1
}

func SplitSQL(s string) []string {
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

func SplitComma(s string) []string {
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

type Scan struct {
	S    string
	I    int
	last int
	have bool
	// noIntroducer rejects _charset'..' literals outside the dump-safe UTF
	// family. Expression text from SHOW CREATE TABLE can carry a mangled
	// literal ('?' for a lost byte), so a generated expression declines
	// them; a hex or binary literal names its bytes exactly, so it stays
	// allowed in any charset; logged values still parse every introducer.
	noIntroducer bool
}

func (sc *Scan) Skip() {
	for sc.I < len(sc.S) {
		switch sc.S[sc.I] {
		case ' ', '\t', '\n', '\r':
			sc.I++
		case '\\':
			// mysql --batch escapes a newline, tab, CR, or NUL inside a field as
			// \n, \t, \r, or \0. Those are whitespace in SHOW CREATE TABLE text.
			if sc.I+1 < len(sc.S) {
				switch sc.S[sc.I+1] {
				case 'n', 't', 'r', '0':
					sc.I += 2
					continue
				}
			}
			return
		case '#':
			for sc.I < len(sc.S) && sc.S[sc.I] != '\n' {
				sc.I++
			}
		case '-':
			if sc.I+1 < len(sc.S) && sc.S[sc.I+1] == '-' {
				for sc.I < len(sc.S) && sc.S[sc.I] != '\n' {
					sc.I++
				}
				continue
			}
			return
		case '/':
			if sc.I+1 < len(sc.S) && sc.S[sc.I+1] == '*' {
				sc.I += 2
				for sc.I+1 < len(sc.S) && !(sc.S[sc.I] == '*' && sc.S[sc.I+1] == '/') {
					sc.I++
				}
				if sc.I+1 < len(sc.S) {
					sc.I += 2
				}
				continue
			}
			return
		default:
			return
		}
	}
}

func (sc *Scan) PeekByte() byte {
	sc.Skip()
	if sc.I >= len(sc.S) {
		return 0
	}
	return sc.S[sc.I]
}

func (sc *Scan) Rest() string {
	sc.Skip()
	return sc.S[sc.I:]
}

func (sc *Scan) Word() string {
	w, _ := sc.Ident()
	return w
}

func (sc *Scan) PeekWord() string {
	save, have, last := sc.I, sc.have, sc.last
	w := sc.Word()
	sc.I, sc.have, sc.last = save, have, last
	return w
}

func (sc *Scan) UnreadWord() {
	if sc.have {
		sc.I = sc.last
		sc.have = false
	}
}

func (sc *Scan) WordIs(want string) bool {
	save := sc.I
	w, ok := sc.Ident()
	if ok && strings.EqualFold(w, want) {
		return true
	}
	sc.I = save
	sc.have = false
	return false
}

func (sc *Scan) Ident() (string, bool) {
	sc.Skip()
	if sc.I >= len(sc.S) {
		return "", false
	}
	sc.last = sc.I
	sc.have = true
	if sc.S[sc.I] == '`' {
		sc.I++
		start := sc.I
		var b strings.Builder
		for sc.I < len(sc.S) {
			if sc.S[sc.I] == '`' {
				if sc.I+1 < len(sc.S) && sc.S[sc.I+1] == '`' {
					b.WriteString(sc.S[start:sc.I])
					b.WriteByte('`')
					sc.I += 2
					start = sc.I
					continue
				}
				b.WriteString(sc.S[start:sc.I])
				sc.I++
				return b.String(), true
			}
			sc.I++
		}
		return "", false
	}
	if !identStart(sc.S[sc.I]) {
		sc.have = false
		return "", false
	}
	return sc.bare(), true
}

func (sc *Scan) bare() string {
	start := sc.I
	for sc.I < len(sc.S) && identPart(sc.S[sc.I]) {
		sc.I++
	}
	return sc.S[start:sc.I]
}

func (sc *Scan) Qualified(session string) (schema, table string, ok bool) {
	first, ok := sc.Ident()
	if !ok {
		return "", "", false
	}
	sc.Skip()
	if sc.I < len(sc.S) && sc.S[sc.I] == '.' {
		sc.I++
		second, ok := sc.Ident()
		if !ok {
			return "", "", false
		}
		return first, second, true
	}
	return session, first, true
}

func (sc *Scan) ParenBody() (string, bool) {
	sc.Skip()
	if sc.I >= len(sc.S) || sc.S[sc.I] != '(' {
		return "", false
	}
	start := sc.I + 1
	depth := 1
	inString := byte(0)
	for i := start; i < len(sc.S); i++ {
		c := sc.S[i]
		if inString != 0 {
			if c == '\\' && inString != '`' && i+1 < len(sc.S) {
				i++
				continue
			}
			if c == inString {
				if i+1 < len(sc.S) && sc.S[i+1] == inString {
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
				body := sc.S[start:i]
				sc.I = i + 1
				return body, true
			}
		}
	}
	return "", false
}

func (sc *Scan) skipString(quote byte) {
	sc.I++
	for sc.I < len(sc.S) {
		if quote != '`' && sc.S[sc.I] == '\\' && sc.I+1 < len(sc.S) {
			sc.I += 2
			continue
		}
		if sc.S[sc.I] == quote {
			sc.I++
			if sc.I < len(sc.S) && sc.S[sc.I] == quote {
				sc.I++
				continue
			}
			return
		}
		sc.I++
	}
}

func identStart(b byte) bool {
	return b == '_' || b == '$' || b >= 0x80 || unicode.IsLetter(rune(b))
}

func identPart(b byte) bool {
	return identStart(b) || (b >= '0' && b <= '9')
}

// NewScan starts a scan at the beginning of s.
func NewScan(s string) *Scan { return &Scan{S: s} }
