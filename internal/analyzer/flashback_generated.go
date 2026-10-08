// Package analyzer learns generated column names from CREATE and ALTER text.
// input: DDL statements in binlog order, including statements later excluded by GTID or time, plus optional CREATE/ALTER text from a schema file. mysql --batch SHOW CREATE TABLE writes field newlines as \n. A plain mysqldump header and several SHOW CREATE rows without ';' are read too. A backslash inside a quoted string, including \', is part of that string.
// output: column names to omit from undo SQL, the column list used to check a schema file, or a bad mark when a definition cannot be read. A table that was never defined stays absent so flashback can warn.
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
	cols           map[string]map[string]struct{}
	defs           map[string][]schemaCol
	fromFile       map[string]bool
	pendingBound   map[string]bool
	bad            map[string]struct{}
	pending        map[string]*pendingDef
	seen           map[string]map[string]struct{}
	abandoned      []abandonedTable
	flagDB         string
	headerDB       string
	ambiguousDB    bool
	sawUnqualified bool
	readingFile    bool
}

// noteScript reads mysqldump --no-data output or SHOW CREATE TABLE text.
// USE sets the schema for a following unqualified CREATE. Without USE, the
// mysqldump "-- Host: ... Database:" header or flagDB is that schema.
// A row of "name<TAB>CREATE TABLE ..." is the mysql batch format. Several of
// those rows may sit in one file with no ';' between them. mysql --batch
// writes each newline inside a field as the two characters \ n.
func (g *generatedTables) noteScript(sql string) {
	if g == nil || strings.TrimSpace(sql) == "" {
		return
	}
	g.readingFile = true
	defer func() { g.readingFile = false }()
	g.headerDB = dumpDatabase(sql)
	session := ""
	switch {
	case g.headerDB != "" && g.flagDB != "" && !strings.EqualFold(g.headerDB, g.flagDB):
		g.ambiguousDB = true
	case g.headerDB != "":
		session = g.headerDB
	case g.flagDB != "":
		session = g.flagDB
	}
	for _, stmt := range splitSQL(sql) {
		g.noteChunk(&session, stmt)
	}
}

func (g *generatedTables) noteChunk(session *string, stmt string) {
	stmt = strings.TrimSpace(stmt)
	for stmt != "" {
		sc := &sqlScan{s: stmt}
		if sc.wordIs("USE") {
			name, ok := sc.ident()
			if ok {
				*session = name
			}
			stmt = strings.TrimSpace(sc.rest())
			continue
		}
		at := indexCreateTable(stmt)
		if at < 0 {
			g.note(*session, stmt)
			return
		}
		rest := stmt[at:]
		next := -1
		if len(rest) > 1 {
			next = indexCreateTable(rest[1:])
		}
		if next < 0 {
			g.note(*session, rest)
			return
		}
		cut := next + 1
		g.note(*session, rest[:cut])
		stmt = strings.TrimSpace(rest[cut:])
	}
}

func (g *generatedTables) defined(schema, table string) bool {
	if g == nil || g.cols == nil {
		return false
	}
	_, ok := g.cols[generatedKey(schema, table)]
	return ok
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
			g.markBad(key)
			return
		}
		if srcSchema == "" {
			if p := g.pending[src]; p != nil && !p.abandoned && !p.bad {
				g.installCreate(schema, table, cloneSchemaCols(p.cols), true)
				return
			}
			if schema == "" {
				g.putPendingBad(table)
				return
			}
			g.markBad(key)
			return
		}
		srcKey := generatedKey(srcSchema, src)
		if _, bad := g.bad[srcKey]; bad {
			g.markBad(key)
			return
		}
		if cols, known := g.defs[srcKey]; known {
			g.installCreate(schema, table, cols, g.fromFile[srcKey] || g.readingFile)
			return
		}
		g.markBad(key)
		return
	}
	if sc.wordIs("AS") || sc.wordIs("SELECT") {
		if schema == "" {
			g.putPendingBad(table)
			return
		}
		g.markBad(key)
		return
	}
	body, ok := sc.parenBody()
	if !ok {
		if schema == "" {
			g.putPendingBad(table)
			return
		}
		g.markBad(key)
		return
	}
	cols, ok := parseSchemaColumns(body)
	if !ok {
		if schema == "" {
			g.putPendingBad(table)
			return
		}
		g.markBad(key)
		return
	}
	g.installCreate(schema, table, applyTableCharset(cols, sc.rest()), g.readingFile)
}

func (g *generatedTables) installCreate(schema, table string, cols []schemaCol, fromFile bool) {
	if schema == "" {
		g.sawUnqualified = true
		if g.ambiguousDB {
			return
		}
		g.putPending(table, cols)
		return
	}
	key := generatedKey(schema, table)
	delete(g.pending, table)
	delete(g.pendingBound, key)
	g.setDef(key, cols, fromFile)
}

func (g *generatedTables) noteAlter(session string, sc *sqlScan) {
	if !sc.wordIs("TABLE") {
		return
	}
	schema, table, ok := sc.qualified(session)
	if !ok {
		return
	}
	if schema == "" {
		g.alterPending(table, sc.rest())
		return
	}
	if !g.readingFile {
		g.observe(schema, table)
	}
	if p := g.pending[table]; p != nil && p.abandoned {
		return
	}
	key := generatedKey(schema, table)
	g.ensure()
	for _, action := range splitComma(sc.rest()) {
		g.applyAlter(key, action)
	}
}

func (g *generatedTables) alterPending(table, rest string) {
	g.sawUnqualified = true
	if g.ambiguousDB {
		return
	}
	p := g.pending[table]
	if p == nil || p.abandoned {
		g.putPendingBad(table)
		return
	}
	if p.bad {
		return
	}
	for _, action := range splitComma(rest) {
		if !alterSlice(&p.cols, action) {
			p.bad = true
			p.cols = nil
			return
		}
	}
}

func (g *generatedTables) applyAlter(key, action string) {
	if alterNoColumnChange(action) {
		return
	}
	cols, ok := g.defs[key]
	if !ok {
		g.markBad(key)
		return
	}
	next := cloneSchemaCols(cols)
	if !alterSlice(&next, action) {
		g.markBad(key)
		return
	}
	g.setDef(key, next, g.fromFile[key])
}

func (g *generatedTables) known(key string) bool {
	_, ok := g.cols[key]
	return ok
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
		delete(g.defs, key)
		delete(g.bad, key)
		delete(g.fromFile, key)
		delete(g.pendingBound, key)
		delete(g.pending, table)
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
		if cols, known := g.defs[srcKey]; known {
			g.defs[dstKey] = cols
			delete(g.defs, srcKey)
		}
		if g.fromFile[srcKey] {
			if g.fromFile == nil {
				g.fromFile = map[string]bool{}
			}
			g.fromFile[dstKey] = true
			delete(g.fromFile, srcKey)
		}
		if g.pendingBound[srcKey] {
			if g.pendingBound == nil {
				g.pendingBound = map[string]bool{}
			}
			g.pendingBound[dstKey] = true
			delete(g.pendingBound, srcKey)
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
