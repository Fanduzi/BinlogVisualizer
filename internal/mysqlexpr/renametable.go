package mysqlexpr

import "strings"

// RenamePair is one "old TO new" pair of a RENAME TABLE statement. Old and
// New keep their quoted spelling through the shared identifier reader.
type RenamePair struct {
	Old QualifiedIdent
	New QualifiedIdent
}

// ParseRenameTable reads RENAME TABLE name TO name [, name TO name] ...
// Names come from the shared identifier reader with the same quoting and
// versioned-comment rules as ParseDropTable. An unterminated quote, a
// second statement, or a missing TO is not ok. isRename is false when sql
// is not this statement.
func ParseRenameTable(sql string, d Dialect) (pairs []RenamePair, isRename, ok bool) {
	p := NewReader(sql, d)
	word, good := p.Ident()
	if !good || word.Quoted || p.Err() || !strings.EqualFold(word.Text, "RENAME") {
		return nil, false, false
	}
	word, good = p.Ident()
	if !good || word.Quoted || p.Err() || !strings.EqualFold(word.Text, "TABLE") {
		return nil, false, false
	}
	for {
		oldName, good := p.Qualified()
		if !good || p.Err() {
			return nil, true, false
		}
		word, good = p.Ident()
		if !good || word.Quoted || p.Err() || !strings.EqualFold(word.Text, "TO") {
			return nil, true, false
		}
		newName, good := p.Qualified()
		if !good || p.Err() {
			return nil, true, false
		}
		pairs = append(pairs, RenamePair{Old: oldName, New: newName})
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
	if !p.trailing() || len(pairs) == 0 {
		return nil, true, false
	}
	return pairs, true, true
}
