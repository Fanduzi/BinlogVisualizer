package mysqlexpr

import "strings"

// ParseDropUser reads DROP USER [IF EXISTS] account [, account] ... Accounts
// keep their quoted spelling, like AccountAfter. An unterminated quote, a
// second statement, or trailing words are not ok. isDrop is false when sql is
// not this statement.
func ParseDropUser(sql string, d Dialect) (accounts []string, isDrop, ok bool) {
	p := NewReader(sql, d)
	word, good := p.Ident()
	if !good || word.Quoted || p.Err() || !strings.EqualFold(word.Text, "DROP") {
		return nil, false, false
	}
	word, good = p.Ident()
	if !good || word.Quoted || p.Err() || !strings.EqualFold(word.Text, "USER") {
		return nil, false, false
	}
	if !p.skipOptionalIf() || p.Err() {
		return nil, true, false
	}
	for {
		account, good := p.rawAccount()
		if !good || p.Err() {
			return nil, true, false
		}
		accounts = append(accounts, account)
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
	if !accountTail(p) || len(accounts) == 0 {
		return nil, true, false
	}
	return accounts, true, true
}

// ParseRenameUser reads RENAME USER account TO account [, account TO account]
// ... Each target is one "old TO new" pair with the quoted spelling kept, so a
// binlog reader sees which login disappeared and which appeared. isRename is
// false when sql is not this statement.
func ParseRenameUser(sql string, d Dialect) (targets []string, isRename, ok bool) {
	p := NewReader(sql, d)
	word, good := p.Ident()
	if !good || word.Quoted || p.Err() || !strings.EqualFold(word.Text, "RENAME") {
		return nil, false, false
	}
	word, good = p.Ident()
	if !good || word.Quoted || p.Err() || !strings.EqualFold(word.Text, "USER") {
		return nil, false, false
	}
	for {
		oldAccount, good := p.rawAccount()
		if !good || p.Err() {
			return nil, true, false
		}
		word, good = p.Ident()
		if !good || word.Quoted || p.Err() || !strings.EqualFold(word.Text, "TO") {
			return nil, true, false
		}
		newAccount, good := p.rawAccount()
		if !good || p.Err() {
			return nil, true, false
		}
		targets = append(targets, oldAccount+" TO "+newAccount)
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
	if !accountTail(p) || len(targets) == 0 {
		return nil, true, false
	}
	return targets, true, true
}

// accountTail accepts what may follow an account list: whitespace, comments,
// a semicolon, then the end of the statement.
func accountTail(p *Reader) bool {
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
