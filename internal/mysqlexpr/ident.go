package mysqlexpr

import (
	"fmt"
	"strings"
)

// ModeANSIQuotes is the ANSI_QUOTES bit of a query event's sql_mode
// (Q_SQL_MODE_CODE, the 8-byte little-endian value).
const ModeANSIQuotes uint64 = 4

// Dialect is how the server that wrote a statement read it.
// VersionID is major*10000+minor*100+patch. Zero means the version is
// unknown, and a versioned comment then cannot be read.
// MariaDB is the server flavor. ANSIQuotes is ModeANSIQuotes in sql_mode.
type Dialect struct {
	VersionID  int
	MariaDB    bool
	ANSIQuotes bool
}

// EventDialect reads the dialect off a query event's version, flavor, and sql_mode.
func EventDialect(version, flavor string, sqlMode uint64) Dialect {
	maria := flavor == "mariadb" || strings.Contains(strings.ToLower(version), "mariadb")
	return Dialect{
		VersionID:  MySQLVersionID(version),
		MariaDB:    maria,
		ANSIQuotes: sqlMode&ModeANSIQuotes != 0,
	}
}

// MySQLVersionID parses the leading major.minor.patch of a server version.
// "8.0.36-log" and "11.8.9-MariaDB" both parse. Zero means it did not.
func MySQLVersionID(version string) int {
	var major, minor, patch int
	n, _ := fmt.Sscanf(version, "%d.%d.%d", &major, &minor, &patch)
	if n < 2 {
		return 0
	}
	return major*10000 + minor*100 + patch
}

// QualifiedIdent is one schema-qualified name. Schema is empty when the
// statement did not qualify it.
type QualifiedIdent struct {
	Schema string
	Table  string
}

// Ident is one identifier. Quoted is set for a backtick name and, when the
// dialect says so, a double-quoted name. A quoted name is never a keyword.
type Ident struct {
	Text   string
	Quoted bool
}

// WithSession fills an empty schema from the query's default database.
// The event schema is that default only when the query schema was not kept
// and the first name is itself unqualified.
func WithSession(names []QualifiedIdent, querySchema, eventSchema string) []QualifiedIdent {
	if len(names) == 0 {
		return nil
	}
	session := querySchema
	if session == "" && names[0].Schema == "" {
		session = eventSchema
	}
	out := make([]QualifiedIdent, len(names))
	for i, name := range names {
		out[i] = name
		if out[i].Schema == "" {
			out[i].Schema = session
		}
	}
	return out
}

// NameOpt selects the optional words between a keyword and the name.
type NameOpt struct {
	// OptionalTable skips an unquoted TABLE, for TRUNCATE [TABLE] name.
	OptionalTable bool
}

// ReadNameAfter returns the qualified name after the first unquoted keyword.
// IF EXISTS and IF NOT EXISTS are skipped. ok is false when the name is not readable.
func ReadNameAfter(sql, keyword string, d Dialect, opt NameOpt) (QualifiedIdent, bool) {
	p := NewReader(sql, d)
	for {
		word, ok := p.nextWord()
		if !ok || p.bad {
			return QualifiedIdent{}, false
		}
		if word.Quoted || !strings.EqualFold(word.Text, keyword) {
			continue
		}
		if !p.skipOptionalIf() || p.bad {
			return QualifiedIdent{}, false
		}
		if opt.OptionalTable {
			mark := p.mark()
			next, ok := p.Ident()
			if p.bad {
				return QualifiedIdent{}, false
			}
			if !ok || next.Quoted || !strings.EqualFold(next.Text, "TABLE") {
				p.rewind(mark)
			}
		}
		name, ok := p.Qualified()
		if !ok || p.bad {
			return QualifiedIdent{}, false
		}
		return name, true
	}
}

// AccountAfter returns the account after the first unquoted keyword, quotes
// included: 'repl'@'%'. CREATE, ALTER, and DROP USER use it. The read stops
// at a comma or a following word such as IDENTIFIED.
func AccountAfter(sql, keyword string, d Dialect) string {
	p := NewReader(sql, d)
	for {
		word, ok := p.nextWord()
		if !ok || p.bad {
			return ""
		}
		if word.Quoted || !strings.EqualFold(word.Text, keyword) {
			continue
		}
		if !p.skipOptionalIf() || p.bad {
			return ""
		}
		raw, ok := p.rawAccount()
		if !ok || p.bad {
			return ""
		}
		return raw
	}
}

// RestAfter returns the raw text after the first unquoted keyword, up to '='.
// SET PASSWORD FOR keeps the account spelling, quotes included.
func RestAfter(sql, keyword string, d Dialect) string {
	p := NewReader(sql, d)
	for {
		word, ok := p.nextWord()
		if !ok || p.bad {
			return ""
		}
		if word.Quoted || !strings.EqualFold(word.Text, keyword) {
			continue
		}
		p.Skip()
		rest := ""
		if p.i < len(p.s) {
			rest = p.s[p.i:]
		}
		if cut := strings.IndexByte(rest, '='); cut >= 0 {
			rest = rest[:cut]
		}
		return strings.TrimSpace(rest)
	}
}

// Reader walks one statement. Versioned comments the server would run are
// part of the statement. A doubled quote inside a quoted name is one quote.
type Reader struct {
	s       string
	i       int
	bad     bool
	ansi    bool
	ver     int
	maria   bool
	known   bool // flavor was mysql or mariadb
	inVer   int
	skipVer bool // rewritten DROP: a preserved version comment is not a name list
}

type readerMark struct {
	i     int
	inVer int
}

func NewReader(sql string, d Dialect) *Reader {
	return &Reader{
		s:     sql,
		i:     0,
		ansi:  d.ANSIQuotes,
		ver:   d.VersionID,
		maria: d.MariaDB,
		known: d.MariaDB || d.VersionID > 0,
	}
}

func (p *Reader) Err() bool { return p.bad }

func (p *Reader) EOF() bool { return p.i >= len(p.s) }

func (p *Reader) Peek() byte {
	if p.EOF() {
		return 0
	}
	return p.s[p.i]
}

func (p *Reader) Pos() int { return p.i }

func (p *Reader) Slice(start, end int) string {
	if start < 0 || end > len(p.s) || start > end {
		return ""
	}
	return p.s[start:end]
}

func (p *Reader) Bump() {
	if !p.EOF() {
		p.i++
	}
}

func (p *Reader) mark() readerMark { return readerMark{p.i, p.inVer} }

func (p *Reader) rewind(m readerMark) { p.i, p.inVer = m.i, m.inVer }

// Complete reports a finished read: no error, no open version comment, no leftover bytes.
func (p *Reader) Complete() bool {
	if p.inVer > 0 {
		p.bad = true
	}
	return !p.bad && p.EOF()
}

func (p *Reader) Skip() {
	if p.bad {
		return
	}
	for p.i < len(p.s) {
		switch p.s[p.i] {
		case ' ', '\t', '\n', '\r', '\f', '\v':
			p.i++
		case '#':
			for p.i < len(p.s) && p.s[p.i] != '\n' {
				p.i++
			}
		case '-':
			if p.lineComment() {
				continue
			}
			return
		case '/':
			if p.i+1 < len(p.s) && p.s[p.i+1] == '*' {
				p.comment()
				if p.bad {
					return
				}
				continue
			}
			return
		case '*':
			if p.inVer > 0 && p.i+1 < len(p.s) && p.s[p.i+1] == '/' {
				p.i += 2
				p.inVer--
				continue
			}
			return
		default:
			return
		}
	}
}

func (p *Reader) lineComment() bool {
	if p.i+1 >= len(p.s) || p.s[p.i+1] != '-' {
		return false
	}
	if p.i+2 < len(p.s) {
		c := p.s[p.i+2]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			return false
		}
	}
	for p.i < len(p.s) && p.s[p.i] != '\n' {
		p.i++
	}
	return true
}

// comment consumes a plain comment, or enters a versioned comment the server runs.
func (p *Reader) comment() {
	// p.i at '/'
	body, maria, ver, hasVer, ok := versionComment(p.s, p.i, !p.maria, p.ver)
	if !ok {
		p.bad = true
		return
	}
	if body < 0 || p.skipVer {
		// Plain comment, or a version comment MariaDB preserved on a rewritten
		// DROP. The names that were dropped are the list outside that comment.
		end := strings.Index(p.s[p.i+2:], "*/")
		if end < 0 {
			p.bad = true
			return
		}
		p.i += 2 + end + 2
		return
	}
	if maria && !p.maria {
		// MySQL treats /*M! as an ordinary comment. An unknown server does not.
		if !p.known {
			p.bad = true
			return
		}
		end := strings.Index(p.s[p.i+2:], "*/")
		if end < 0 {
			p.bad = true
			return
		}
		p.i += 2 + end + 2
		return
	}
	run := false
	switch {
	case !hasVer:
		run = true
	case p.ver == 0:
		p.bad = true
		return
	default:
		run = ver <= p.ver
	}
	if !run {
		end := strings.Index(p.s[body:], "*/")
		if end < 0 {
			p.bad = true
			return
		}
		p.i = body + end + 2
		return
	}
	p.i = body
	p.inVer++
}

// versionComment parses /*!N or /*M!N at i (which points at '/').
// body is the first byte of the body, or -1 when this is a plain comment.
// mysql is the server: MariaDB takes 5 digits, or 6 when a sixth digit is
// present. MySQL takes 5, and from 8.1 also 6 when whitespace follows the
// sixth. Fewer than 5 digits is not a version. Extra digits stay in the body.
func versionComment(s string, i int, mysql bool, verID int) (body int, maria bool, ver int, hasVer, ok bool) {
	if i+1 >= len(s) || s[i+1] != '*' {
		return 0, false, 0, false, false
	}
	j := i + 2
	if j >= len(s) {
		return 0, false, 0, false, false
	}
	if s[j] == '!' {
		j++
	} else if s[j] == 'M' && j+1 < len(s) && s[j+1] == '!' {
		maria = true
		j += 2
	} else {
		return -1, false, 0, false, true
	}
	n := 0
	for n < 7 && j+n < len(s) && s[j+n] >= '0' && s[j+n] <= '9' {
		n++
	}
	if n < 5 {
		return j, maria, 0, false, true
	}
	take := 5
	if !mysql {
		if n >= 6 {
			take = 6
		}
	} else if verID >= 80100 && n >= 6 && j+6 < len(s) && isSpaceByte(s[j+6]) {
		take = 6
	}
	for k := 0; k < take; k++ {
		ver = ver*10 + int(s[j+k]-'0')
	}
	return j + take, maria, ver, true, true
}

func isSpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}

// Ident reads one name. A double quote without ANSI_QUOTES is not a name.
func (p *Reader) Ident() (Ident, bool) {
	p.Skip()
	if p.bad || p.EOF() {
		return Ident{}, false
	}
	c := p.s[p.i]
	if c == '"' && !p.ansi {
		p.bad = true
		return Ident{}, false
	}
	if c >= '0' && c <= '9' {
		mark := p.i
		text, isIdent := p.DigitToken()
		if !isIdent {
			p.i = mark
			return Ident{}, false
		}
		return Ident{Text: text}, true
	}
	if c == '`' || (c == '"' && p.ansi) || identStart(c) {
		return p.readIdent()
	}
	return Ident{}, false
}

// DigitToken reads one digit-led token, the way MySQL's lexer does.
// isIdent is true for a name such as 2fa_codes. A pure number, a hex or bit
// literal, and an exponent (1e5, 1e+10) are not names; those are still consumed.
func (p *Reader) DigitToken() (text string, isIdent bool) {
	if p.EOF() || p.s[p.i] < '0' || p.s[p.i] > '9' {
		return "", false
	}
	start := p.i
	s := p.s
	if s[p.i] == '0' && p.i+1 < len(s) {
		switch s[p.i+1] {
		case 'x', 'X':
			k := p.i + 2
			for k < len(s) && isHex(s[k]) {
				k++
			}
			if k > p.i+2 && (k >= len(s) || !identPart(s[k])) {
				p.i = k
				return s[start:p.i], false
			}
		case 'b', 'B':
			k := p.i + 2
			for k < len(s) && (s[k] == '0' || s[k] == '1') {
				k++
			}
			if k > p.i+2 && (k >= len(s) || !identPart(s[k])) {
				p.i = k
				return s[start:p.i], false
			}
		}
	}
	p.i = start
	for p.i < len(s) && s[p.i] >= '0' && s[p.i] <= '9' {
		p.i++
	}
	if p.i < len(s) && (s[p.i] == 'e' || s[p.i] == 'E') {
		k := p.i + 1
		if k < len(s) && (s[k] == '+' || s[k] == '-') {
			k++
		}
		if k < len(s) && s[k] >= '0' && s[k] <= '9' {
			p.i = k
			for p.i < len(s) && s[p.i] >= '0' && s[p.i] <= '9' {
				p.i++
			}
			return s[start:p.i], false
		}
	}
	if p.i < len(s) && identPart(s[p.i]) {
		for p.i < len(s) && identPart(s[p.i]) {
			p.i++
		}
		return s[start:p.i], true
	}
	return s[start:p.i], false
}

func (p *Reader) readIdent() (Ident, bool) {
	c := p.s[p.i]
	if c == '`' || c == '"' {
		text, ok := p.readQuoted(c)
		if !ok {
			return Ident{}, false
		}
		return Ident{Text: text, Quoted: true}, true
	}
	start := p.i
	p.i++
	for p.i < len(p.s) && identPart(p.s[p.i]) {
		p.i++
	}
	return Ident{Text: p.s[start:p.i]}, true
}

// readQuoted undoubles a doubled quote of the same kind. `a“b` is a`b.
func (p *Reader) readQuoted(quote byte) (string, bool) {
	var b strings.Builder
	p.i++
	for p.i < len(p.s) {
		if p.s[p.i] == quote {
			if p.i+1 < len(p.s) && p.s[p.i+1] == quote {
				b.WriteByte(quote)
				p.i += 2
				continue
			}
			p.i++
			return b.String(), true
		}
		b.WriteByte(p.s[p.i])
		p.i++
	}
	p.bad = true
	return "", false
}

// Qualified reads [schema '.'] name.
func (p *Reader) Qualified() (QualifiedIdent, bool) {
	first, ok := p.Ident()
	if !ok {
		return QualifiedIdent{}, false
	}
	p.Skip()
	if p.bad {
		return QualifiedIdent{}, false
	}
	if !p.EOF() && p.s[p.i] == '.' {
		p.i++
		second, ok := p.Ident()
		if !ok {
			return QualifiedIdent{}, false
		}
		return QualifiedIdent{Schema: first.Text, Table: second.Text}, true
	}
	return QualifiedIdent{Table: first.Text}, true
}

func (p *Reader) nextWord() (Ident, bool) {
	for {
		p.Skip()
		if p.bad || p.EOF() {
			return Ident{}, false
		}
		c := p.s[p.i]
		switch {
		case c == '`' || (p.ansi && c == '"') || identStart(c):
			return p.readIdent()
		case c >= '0' && c <= '9':
			text, isIdent := p.DigitToken()
			if isIdent {
				return Ident{Text: text}, true
			}
		case c == '\'' || (c == '"' && !p.ansi):
			if !p.readString(c) {
				return Ident{}, false
			}
		default:
			p.i++
		}
	}
}

// ReadString consumes a quoted string starting at the current byte.
func (p *Reader) ReadString(quote byte) bool {
	p.Skip()
	if p.bad || p.EOF() || p.s[p.i] != quote {
		p.bad = true
		return false
	}
	return p.readString(quote)
}

func (p *Reader) readString(quote byte) bool {
	p.i++
	for p.i < len(p.s) {
		if quote != '`' && p.s[p.i] == '\\' && p.i+1 < len(p.s) {
			p.i += 2
			continue
		}
		if p.s[p.i] == quote {
			if p.i+1 < len(p.s) && p.s[p.i+1] == quote {
				p.i += 2
				continue
			}
			p.i++
			return true
		}
		p.i++
	}
	p.bad = true
	return false
}

func (p *Reader) skipOptionalIf() bool {
	mark := p.mark()
	word, ok := p.Ident()
	if p.bad {
		return false
	}
	if !ok || word.Quoted || !strings.EqualFold(word.Text, "IF") {
		p.rewind(mark)
		return true
	}
	word, ok = p.Ident()
	if p.bad {
		return false
	}
	if ok && !word.Quoted && strings.EqualFold(word.Text, "NOT") {
		word, ok = p.Ident()
		return ok && !word.Quoted && strings.EqualFold(word.Text, "EXISTS") && !p.bad
	}
	return ok && !word.Quoted && strings.EqualFold(word.Text, "EXISTS") && !p.bad
}

// rawAccount is the account spelling at p, quotes kept.
func (p *Reader) rawAccount() (string, bool) {
	p.Skip()
	if p.bad || p.EOF() {
		return "", false
	}
	start := p.i
	for p.i < len(p.s) {
		c := p.s[p.i]
		switch c {
		case '\'', '"', '`':
			if !p.readString(c) {
				return "", false
			}
		case ' ', '\t', '\n', '\r', ',', ';':
			if p.i == start {
				return "", false
			}
			return p.s[start:p.i], true
		default:
			p.i++
		}
	}
	if p.i == start {
		return "", false
	}
	return p.s[start:p.i], true
}

// IdentStart reports a byte that can begin an unquoted name.
func IdentStart(c byte) bool { return identStart(c) }
