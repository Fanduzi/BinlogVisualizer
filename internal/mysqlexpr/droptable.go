package mysqlexpr

import "strings"

// ParseDropTable reads DROP [TEMPORARY] TABLE[S] [IF EXISTS] name [, name] ...
// [RESTRICT | CASCADE]. Names come from the shared identifier reader: a
// doubled backtick is one backtick, a quoted name is not a keyword, a
// versioned comment runs when the dialect's server version runs it, and a
// double-quoted name is an identifier when the dialect has ANSI_QUOTES.
// An unknown version on a versioned comment, a double quote without
// ANSI_QUOTES, an unterminated quote or comment, or a second statement is
// not ok. isDrop is false when sql is not this statement.
func ParseDropTable(sql string, d Dialect) (names []QualifiedIdent, isDrop, ok bool) {
	p := NewReader(sql, d)
	word, good := p.Ident()
	if !good || word.Quoted || p.Err() || !strings.EqualFold(word.Text, "DROP") {
		return nil, false, false
	}
	word, good = p.Ident()
	if p.Err() {
		return nil, false, false
	}
	if good && !word.Quoted && strings.EqualFold(word.Text, "TEMPORARY") {
		word, good = p.Ident()
		if p.Err() {
			return nil, false, false
		}
	}
	if !good || word.Quoted || (!strings.EqualFold(word.Text, "TABLE") && !strings.EqualFold(word.Text, "TABLES")) {
		return nil, false, false
	}
	if !p.skipOptionalIf() || p.Err() {
		return nil, true, false
	}
	for {
		name, good := p.Qualified()
		if !good || p.Err() {
			return nil, true, false
		}
		names = append(names, name)
		p.Skip()
		if p.Err() {
			return nil, true, false
		}
		if !p.EOF() && p.Peek() == ',' {
			p.Bump()
			continue
		}
		break
	}
	if !p.trailing() || len(names) == 0 {
		return nil, true, false
	}
	return names, true, true
}

func (p *Reader) trailing() bool {
	mark := p.mark()
	word, ok := p.Ident()
	if p.bad {
		return false
	}
	if ok && !word.Quoted && (strings.EqualFold(word.Text, "RESTRICT") || strings.EqualFold(word.Text, "CASCADE")) {
		mark = p.mark()
	} else {
		p.rewind(mark)
	}
	p.Skip()
	if p.bad {
		return false
	}
	if !p.EOF() && p.Peek() == ';' {
		p.Bump()
		p.Skip()
	}
	return p.Complete()
}
