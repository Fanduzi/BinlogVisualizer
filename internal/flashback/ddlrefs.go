// ddlrefs.go: the tables and schemas one DDL statement can change, for the
// DDL-in-range refusal under a table or schema filter (ADR 0015).
// input: the statement text of a DDL query event and the session's default schema.
// output: every table and schema the statement names as a source or target, or ok=false
// when the statement is not one of the shapes read here. ok=false refuses.
// pos: flashback-only helper. Analyze does not call it. Names come from mysqlexpr's identifier reader. DROP TABLE names come from mysqlexpr.ParseDropTable.
// note: if this file changes, update this header, README.md, and ADR 0015.
package flashback

import (
	"strings"

	"binlogviz/internal/mysqlexpr"
)

// ddlRef is one object a DDL names. table is empty for a whole schema.
// schema is empty when the name is unqualified and the session had no
// default schema, so any schema may be meant.
type ddlRef struct {
	schema string
	table  string
}

type ddlTok struct {
	kind byte // 'w' bare word, 'q' backtick identifier, 's' string, 'n' number, 'p' punctuation
	text string
}

func (t ddlTok) word(w string) bool { return t.kind == 'w' && strings.EqualFold(t.text, w) }

// lexDDL splits a statement into tokens. Plain comments are dropped. A
// versioned comment is code when the dialect's server version runs it, and
// unreadable when the version is unknown. A double-quoted name is an
// identifier when the dialect has ANSI_QUOTES. An unterminated quote or
// comment returns ok=false. Names use the shared identifier reader.
func lexDDL(s string, d mysqlexpr.Dialect) ([]ddlTok, bool) {
	p := mysqlexpr.NewReader(s, d)
	var toks []ddlTok
	for !p.EOF() {
		p.Skip()
		if p.Err() {
			return nil, false
		}
		if p.EOF() {
			break
		}
		c := p.Peek()
		switch {
		case c == '`' || (d.ANSIQuotes && c == '"') || mysqlexpr.IdentStart(c):
			id, ok := p.Ident()
			if !ok || p.Err() {
				return nil, false
			}
			kind := byte('w')
			if id.Quoted {
				kind = 'q'
			}
			toks = append(toks, ddlTok{kind: kind, text: id.Text})
		case c == '\'':
			start := p.Pos()
			if !p.ReadString('\'') {
				return nil, false
			}
			toks = append(toks, ddlTok{kind: 's', text: p.Slice(start, p.Pos())})
		case c == '"':
			return nil, false
		case c >= '0' && c <= '9':
			start := p.Pos()
			for !p.EOF() && p.Peek() >= '0' && p.Peek() <= '9' {
				p.Bump()
			}
			toks = append(toks, ddlTok{kind: 'n', text: p.Slice(start, p.Pos())})
		default:
			p.Bump()
			toks = append(toks, ddlTok{kind: 'p', text: string(c)})
		}
	}
	if !p.Complete() {
		return nil, false
	}
	if n := len(toks); n > 0 && toks[n-1].kind == 'p' && toks[n-1].text == ";" {
		toks = toks[:n-1]
	}
	for _, t := range toks {
		if t.kind == 'p' && t.text == ";" {
			return nil, false
		}
	}
	return toks, true
}

type ddlParser struct {
	toks    []ddlTok
	i       int
	session string
	refs    []ddlRef
}

func (p *ddlParser) done() bool { return p.i >= len(p.toks) }

func (p *ddlParser) peek() ddlTok {
	if p.done() {
		return ddlTok{}
	}
	return p.toks[p.i]
}

func (p *ddlParser) accept(w string) bool {
	if p.peek().word(w) {
		p.i++
		return true
	}
	return false
}

func (p *ddlParser) punct(c string) bool {
	if t := p.peek(); t.kind == 'p' && t.text == c {
		p.i++
		return true
	}
	return false
}

func (p *ddlParser) ident() (string, bool) {
	t := p.peek()
	if t.kind != 'w' && t.kind != 'q' {
		return "", false
	}
	p.i++
	return t.text, true
}

// name reads [schema.]table and records it as a reference.
func (p *ddlParser) name() bool {
	first, ok := p.ident()
	if !ok {
		return false
	}
	if p.punct(".") {
		second, ok := p.ident()
		if !ok {
			return false
		}
		p.refs = append(p.refs, ddlRef{schema: first, table: second})
		return true
	}
	p.refs = append(p.refs, ddlRef{schema: p.session, table: first})
	return true
}

func (p *ddlParser) schemaName() bool {
	s, ok := p.ident()
	if !ok {
		return false
	}
	p.refs = append(p.refs, ddlRef{schema: s})
	return true
}

func (p *ddlParser) ifExists() {
	if p.accept("IF") {
		p.accept("EXISTS")
	}
}

func (p *ddlParser) ifNotExists() bool {
	if !p.accept("IF") {
		return true
	}
	return p.accept("NOT") && p.accept("EXISTS")
}

// nameList reads name {, name}.
func (p *ddlParser) nameList() bool {
	for {
		if !p.name() {
			return false
		}
		if !p.punct(",") {
			return true
		}
	}
}

// ddlReferences returns every table and schema a DDL statement can change,
// read from its full text. ok=false means the statement is not a shape this
// reader knows completely: the caller refuses. A statement that changes no
// table (account and privilege statements) returns no references and ok=true.
func ddlReferences(sql, querySchema, eventSchema string, d mysqlexpr.Dialect) ([]ddlRef, bool) {
	// DROP TABLE names come from the shared list parser, not this lexer's nameList.
	if names, isDrop, ok := mysqlexpr.ParseDropTable(sql, d); isDrop {
		if !ok {
			return nil, false
		}
		names = mysqlexpr.WithSession(names, querySchema, eventSchema)
		refs := make([]ddlRef, len(names))
		for i, name := range names {
			refs[i] = ddlRef{schema: name.Schema, table: name.Table}
		}
		return refs, true
	}
	toks, ok := lexDDL(sql, d)
	if !ok || len(toks) == 0 {
		return nil, false
	}
	session := querySchema
	if session == "" {
		session = eventSchema
	}
	p := &ddlParser{toks: toks, session: session}
	verb := p.peek()
	p.i++
	switch {
	case verb.word("RENAME"):
		ok = p.rename()
	case verb.word("DROP"):
		ok = p.drop()
	case verb.word("TRUNCATE"):
		p.accept("TABLE")
		ok = p.name() && p.done()
	case verb.word("ALTER"):
		ok = p.alter()
	case verb.word("CREATE"):
		ok = p.create()
	case verb.word("GRANT"), verb.word("REVOKE"):
		// Privileges. No table definition or row changes.
		return nil, true
	case verb.word("SET"):
		if p.accept("PASSWORD") {
			return nil, true
		}
		return nil, false
	default:
		return nil, false
	}
	if !ok {
		return nil, false
	}
	return p.refs, true
}

func (p *ddlParser) rename() bool {
	if p.accept("USER") {
		return true
	}
	if !p.accept("TABLE") && !p.accept("TABLES") {
		return false
	}
	for {
		if !p.name() || !p.accept("TO") || !p.name() {
			return false
		}
		if !p.punct(",") {
			return p.done()
		}
	}
}

func (p *ddlParser) drop() bool {
	switch {
	case p.accept("USER"), p.accept("ROLE"):
		return true
	case p.accept("DATABASE"), p.accept("SCHEMA"):
		p.ifExists()
		return p.schemaName() && p.done()
	case p.accept("INDEX"):
		// DROP INDEX [ONLINE|OFFLINE] [IF EXISTS] name ON tbl [ALGORITHM|LOCK ...]
		p.accept("ONLINE")
		p.accept("OFFLINE")
		p.ifExists()
		if _, ok := p.ident(); !ok {
			return false
		}
		return p.accept("ON") && p.name() && p.indexOptions()
	case p.accept("VIEW"):
		p.ifExists()
		if !p.nameList() {
			return false
		}
	default:
		p.accept("TEMPORARY")
		if !p.accept("TABLE") && !p.accept("TABLES") {
			return false
		}
		p.ifExists()
		if !p.nameList() {
			return false
		}
	}
	if !p.accept("RESTRICT") {
		p.accept("CASCADE")
	}
	return p.done()
}

// indexOptions accepts the ALGORITHM and LOCK clauses after DROP INDEX ... ON tbl.
func (p *ddlParser) indexOptions() bool {
	for !p.done() {
		if !p.accept("ALGORITHM") && !p.accept("LOCK") {
			return false
		}
		p.punct("=")
		if _, ok := p.ident(); !ok {
			return false
		}
	}
	return true
}

func (p *ddlParser) alter() bool {
	switch {
	case p.accept("USER"), p.accept("ROLE"):
		return true
	case p.accept("DATABASE"), p.accept("SCHEMA"):
		if t := p.peek(); t.kind == 'w' && isDatabaseOption(t.text) {
			// ALTER DATABASE with no name changes the session's default schema.
			p.refs = append(p.refs, ddlRef{schema: p.session})
		} else if !p.schemaName() {
			return false
		}
		return p.tail(false)
	}
	p.accept("ONLINE")
	p.accept("OFFLINE")
	p.accept("IGNORE")
	if !p.accept("TABLE") {
		return false
	}
	p.ifExists()
	if !p.name() {
		return false
	}
	return p.tail(true)
}

func isDatabaseOption(w string) bool {
	switch strings.ToUpper(w) {
	case "CHARACTER", "CHARSET", "DEFAULT", "COLLATE", "ENCRYPTION", "READ", "UPGRADE", "COMMENT":
		return true
	}
	return false
}

// tail reads the rest of an ALTER or CREATE. Another table can enter a
// table statement through RENAME [TO|AS] name, REFERENCES name, or LIKE name;
// each is recorded. EXCHANGE PARTITION, a query body (SELECT, TABLE, VALUES,
// WITH), or a word this reader does not place is refused.
func (p *ddlParser) tail(table bool) bool {
	for !p.done() {
		t := p.peek()
		p.i++
		if t.kind != 'w' {
			continue
		}
		switch {
		case t.word("RENAME"):
			if !table {
				return false
			}
			switch {
			case p.accept("COLUMN"), p.accept("INDEX"), p.accept("KEY"), p.accept("CONSTRAINT"):
				// Renames inside this table; the table itself is already a reference.
			default:
				if !p.accept("TO") && !p.accept("AS") {
					p.punct("=")
				}
				if !p.name() {
					return false
				}
			}
		case t.word("REFERENCES"), t.word("LIKE"):
			if !table || !p.name() {
				return false
			}
		case t.word("EXCHANGE"), t.word("SELECT"), t.word("TABLE"), t.word("VALUES"), t.word("WITH"):
			return false
		}
	}
	return true
}

func (p *ddlParser) create() bool {
	p.createPrefix()
	switch {
	case p.accept("USER"), p.accept("ROLE"):
		return true
	case p.accept("DATABASE"), p.accept("SCHEMA"):
		if !p.ifNotExists() || !p.schemaName() {
			return false
		}
		return p.tail(false)
	case p.accept("INDEX"):
		if !p.ifNotExists() {
			return false
		}
		if _, ok := p.ident(); !ok {
			return false
		}
		if p.accept("USING") {
			if _, ok := p.ident(); !ok {
				return false
			}
		}
		return p.accept("ON") && p.name() && p.tail(true)
	case p.accept("TABLE"):
		if !p.ifNotExists() || !p.name() {
			return false
		}
		return p.tail(true)
	}
	return false
}

// createPrefix skips the words that may come between CREATE and the object
// keyword of a table or index statement. Views, triggers, routines, and
// events are not read here and refuse at the object keyword.
func (p *ddlParser) createPrefix() {
	for {
		switch {
		case p.accept("OR"):
			if !p.accept("REPLACE") {
				p.i--
				return
			}
		case p.accept("TEMPORARY"), p.accept("UNIQUE"), p.accept("FULLTEXT"), p.accept("SPATIAL"),
			p.accept("ONLINE"), p.accept("OFFLINE"):
		default:
			return
		}
	}
}
