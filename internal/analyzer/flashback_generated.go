// Package analyzer learns generated column names from CREATE and ALTER text.
// input: DDL statements in binlog order, including statements later excluded by GTID or time.
// output: column names to omit from undo SQL, or a bad mark when a definition cannot be read.
// pos: flashback-only helper. Analyze does not call it.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"strings"
	"unicode"
)

// generatedTables records generated column names learned from CREATE and ALTER
// statements in the parsed binlog. A table missing from cols was not defined
// in this input. bad means a definition was seen and could not be read, so
// undo SQL must refuse that table instead of assigning a generated column.
type generatedTables struct {
	cols map[string]map[string]struct{}
	bad  map[string]struct{}
}

func (g *generatedTables) note(sessionSchema, sql string) {
	if g == nil {
		return
	}
	sc := &sqlScan{s: strings.TrimSpace(sql)}
	verb := strings.ToUpper(sc.word())
	switch verb {
	case "CREATE":
		g.noteCreate(sessionSchema, sc)
	case "ALTER":
		g.noteAlter(sessionSchema, sc)
	case "DROP":
		g.noteDrop(sessionSchema, sc)
	case "RENAME":
		g.noteRename(sessionSchema, sc)
	}
}

func (g *generatedTables) columns(schema, table string) map[string]struct{} {
	if g == nil || g.cols == nil {
		return nil
	}
	return g.cols[generatedKey(schema, table)]
}

func (g *generatedTables) unknown(schema, table string) bool {
	if g == nil || g.bad == nil {
		return false
	}
	_, ok := g.bad[generatedKey(schema, table)]
	return ok
}

func generatedKey(schema, table string) string {
	return schema + "\x00" + table
}

func (g *generatedTables) ensure() {
	if g.cols == nil {
		g.cols = map[string]map[string]struct{}{}
	}
	if g.bad == nil {
		g.bad = map[string]struct{}{}
	}
}

func (g *generatedTables) noteCreate(session string, sc *sqlScan) {
	if strings.EqualFold(sc.word(), "TEMPORARY") {
		// consumed
	} else {
		sc.unreadWord()
	}
	if !sc.wordIs("TABLE") {
		return
	}
	if sc.wordIs("IF") {
		sc.wordIs("NOT")
		sc.wordIs("EXISTS")
	}
	schema, table, ok := sc.qualified(session)
	if !ok {
		return
	}
	key := generatedKey(schema, table)
	g.ensure()
	if sc.wordIs("LIKE") {
		srcSchema, src, ok := sc.qualified(session)
		if !ok {
			g.bad[key] = struct{}{}
			return
		}
		srcKey := generatedKey(srcSchema, src)
		if _, bad := g.bad[srcKey]; bad {
			g.bad[key] = struct{}{}
			return
		}
		if cols, known := g.cols[srcKey]; known {
			g.cols[key] = copyNames(cols)
			delete(g.bad, key)
			return
		}
		g.bad[key] = struct{}{}
		return
	}
	if sc.wordIs("AS") || sc.wordIs("SELECT") {
		g.bad[key] = struct{}{}
		return
	}
	body, ok := sc.parenBody()
	if !ok {
		g.bad[key] = struct{}{}
		return
	}
	cols, ok := generatedNames(body)
	if !ok {
		g.bad[key] = struct{}{}
		return
	}
	g.cols[key] = cols
	delete(g.bad, key)
}

func (g *generatedTables) noteAlter(session string, sc *sqlScan) {
	if !sc.wordIs("TABLE") {
		return
	}
	schema, table, ok := sc.qualified(session)
	if !ok {
		return
	}
	key := generatedKey(schema, table)
	g.ensure()
	for _, action := range splitComma(sc.rest()) {
		g.applyAlter(key, action)
	}
}

func (g *generatedTables) applyAlter(key, action string) {
	sc := &sqlScan{s: strings.TrimSpace(action)}
	verb := strings.ToUpper(sc.word())
	switch verb {
	case "ADD":
		kind := strings.ToUpper(sc.peekWord())
		switch kind {
		case "INDEX", "KEY", "UNIQUE", "FULLTEXT", "SPATIAL", "CONSTRAINT", "PRIMARY", "FOREIGN":
			return
		case "COLUMN":
			sc.word()
		}
		if sc.wordIs("IF") {
			sc.wordIs("NOT")
			sc.wordIs("EXISTS")
		}
		if !g.known(key) {
			g.bad[key] = struct{}{}
			return
		}
		if sc.peekByte() == '(' {
			body, ok := sc.parenBody()
			if !ok {
				g.bad[key] = struct{}{}
				return
			}
			cols, ok := generatedNames(body)
			if !ok {
				g.bad[key] = struct{}{}
				return
			}
			for name := range cols {
				g.cols[key][name] = struct{}{}
			}
			return
		}
		g.addColumnDef(key, sc.rest())
	case "DROP":
		kind := strings.ToUpper(sc.peekWord())
		switch kind {
		case "INDEX", "KEY", "PRIMARY", "FOREIGN", "CONSTRAINT", "CHECK":
			return
		case "COLUMN":
			sc.word()
		}
		if !g.known(key) {
			g.bad[key] = struct{}{}
			return
		}
		name, ok := sc.ident()
		if !ok {
			g.bad[key] = struct{}{}
			return
		}
		delete(g.cols[key], strings.ToLower(name))
	case "MODIFY":
		if sc.wordIs("COLUMN") {
			// consumed
		}
		if !g.known(key) {
			g.bad[key] = struct{}{}
			return
		}
		g.addColumnDef(key, sc.rest())
	case "CHANGE":
		if sc.wordIs("COLUMN") {
			// consumed
		}
		if !g.known(key) {
			g.bad[key] = struct{}{}
			return
		}
		old, ok := sc.ident()
		if !ok {
			g.bad[key] = struct{}{}
			return
		}
		delete(g.cols[key], strings.ToLower(old))
		g.addColumnDef(key, sc.rest())
	case "RENAME":
		if !sc.wordIs("COLUMN") || !g.known(key) {
			return
		}
		old, ok := sc.ident()
		if !ok || !sc.wordIs("TO") {
			g.bad[key] = struct{}{}
			return
		}
		next, ok := sc.ident()
		if !ok {
			g.bad[key] = struct{}{}
			return
		}
		if _, was := g.cols[key][strings.ToLower(old)]; was {
			delete(g.cols[key], strings.ToLower(old))
			g.cols[key][strings.ToLower(next)] = struct{}{}
		}
	}
}

func (g *generatedTables) known(key string) bool {
	_, ok := g.cols[key]
	return ok
}

func (g *generatedTables) addColumnDef(key, def string) {
	name, gen, ok := columnDef(def)
	if !ok {
		g.bad[key] = struct{}{}
		return
	}
	if g.cols[key] == nil {
		g.cols[key] = map[string]struct{}{}
	}
	if gen {
		g.cols[key][strings.ToLower(name)] = struct{}{}
		return
	}
	delete(g.cols[key], strings.ToLower(name))
}

func (g *generatedTables) noteDrop(session string, sc *sqlScan) {
	if !sc.wordIs("TABLE") {
		return
	}
	sc.wordIs("IF")
	if strings.EqualFold(sc.peekWord(), "EXISTS") {
		sc.word()
	}
	for {
		schema, table, ok := sc.qualified(session)
		if !ok {
			return
		}
		key := generatedKey(schema, table)
		delete(g.cols, key)
		delete(g.bad, key)
		if !sc.wordIs(",") && sc.peekByte() != ',' {
			sc.skip()
			if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
				sc.i++
				continue
			}
			return
		}
	}
}

func (g *generatedTables) noteRename(session string, sc *sqlScan) {
	if !sc.wordIs("TABLE") {
		return
	}
	for {
		srcSchema, src, ok := sc.qualified(session)
		if !ok || !sc.wordIs("TO") {
			return
		}
		dstSchema, dst, ok := sc.qualified(session)
		if !ok {
			return
		}
		srcKey := generatedKey(srcSchema, src)
		dstKey := generatedKey(dstSchema, dst)
		g.ensure()
		if cols, known := g.cols[srcKey]; known {
			g.cols[dstKey] = cols
			delete(g.cols, srcKey)
		}
		if _, bad := g.bad[srcKey]; bad {
			g.bad[dstKey] = struct{}{}
			delete(g.bad, srcKey)
		}
		if !sc.wordIs(",") && sc.peekByte() != ',' {
			return
		}
		sc.skip()
		if sc.i < len(sc.s) && sc.s[sc.i] == ',' {
			sc.i++
		}
	}
}

func generatedNames(body string) (map[string]struct{}, bool) {
	cols := map[string]struct{}{}
	for _, part := range splitComma(body) {
		if strings.TrimSpace(part) == "" || isTableConstraint(part) {
			continue
		}
		name, gen, ok := columnDef(part)
		if !ok {
			return nil, false
		}
		if gen {
			cols[strings.ToLower(name)] = struct{}{}
		}
	}
	return cols, true
}

func columnDef(def string) (name string, generated bool, ok bool) {
	sc := &sqlScan{s: strings.TrimSpace(def)}
	name, ok = sc.ident()
	if !ok || isTableConstraint(name) {
		return "", false, false
	}
	return name, definesGenerated(def), true
}

func isTableConstraint(def string) bool {
	switch strings.ToUpper((&sqlScan{s: def}).word()) {
	case "PRIMARY", "UNIQUE", "KEY", "INDEX", "FULLTEXT", "SPATIAL", "CONSTRAINT", "CHECK", "FOREIGN":
		return true
	default:
		return false
	}
}

func definesGenerated(def string) bool {
	sc := &sqlScan{s: def}
	depth := 0
	for sc.i < len(sc.s) {
		sc.skip()
		if sc.i >= len(sc.s) {
			return false
		}
		switch sc.s[sc.i] {
		case '(':
			depth++
			sc.i++
		case ')':
			if depth > 0 {
				depth--
			}
			sc.i++
		case '\'', '"':
			sc.skipString(sc.s[sc.i])
		case '`':
			sc.i++
			for sc.i < len(sc.s) {
				if sc.s[sc.i] == '`' {
					sc.i++
					if sc.i < len(sc.s) && sc.s[sc.i] == '`' {
						sc.i++
						continue
					}
					break
				}
				sc.i++
			}
		default:
			if depth == 0 && identStart(sc.s[sc.i]) {
				w := sc.bare()
				if strings.EqualFold(w, "AS") {
					sc.skip()
					if sc.i < len(sc.s) && sc.s[sc.i] == '(' {
						return true
					}
				}
				continue
			}
			sc.i++
		}
	}
	return false
}

func copyNames(src map[string]struct{}) map[string]struct{} {
	dst := make(map[string]struct{}, len(src))
	for name := range src {
		dst[name] = struct{}{}
	}
	return dst
}

func splitComma(s string) []string {
	var parts []string
	start := 0
	depth := 0
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
