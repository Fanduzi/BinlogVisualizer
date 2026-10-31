// Package analyzer normalizes Rows_query statement text into fingerprints and
// folds per-transaction statement evidence into workload-level groups.
// input: bounded statement text captured on in-flight transactions.
// output: deterministic fingerprint strings and bounded ranked groups.
// pos: statement-text aggregation layer beside transaction-shape patterns (ADR 0026).
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"sort"
	"strings"

	"binlogviz/internal/model"
)

// fingerprintTrackLimit caps distinct fingerprint groups per report. It follows
// the HotRowTrackLimit precedent: an unbounded distinct-key map gets an explicit
// cap plus an overflow count instead of unbounded growth.
const fingerprintTrackLimit = 8192

const (
	fpLiteral = "?"
	fpGroup   = "(?)"
)

// fingerprintKeywords fold to lowercase when they appear as unquoted words.
// Deliberately scoped to DML vocabulary; anything else keeps source case.
var fingerprintKeywords = map[string]struct{}{
	// statement verbs and clauses
	"select": {}, "insert": {}, "update": {}, "delete": {}, "replace": {},
	"load": {}, "data": {}, "infile": {}, "into": {}, "outfile": {}, "dumpfile": {},
	"from": {}, "where": {}, "set": {}, "values": {}, "on": {}, "duplicate": {},
	"key": {}, "as": {}, "ignore": {}, "delayed": {}, "low_priority": {},
	"high_priority": {}, "concurrent": {}, "local": {}, "straight_join": {},
	"lock": {}, "share": {}, "mode": {}, "for": {}, "using": {},
	// boolean and condition words
	"and": {}, "or": {}, "not": {}, "xor": {}, "is": {}, "in": {}, "between": {},
	"like": {}, "rlike": {}, "regexp": {}, "sounds": {}, "null": {}, "exists": {},
	"case": {}, "when": {}, "then": {}, "else": {}, "end": {}, "if": {},
	"true": {}, "false": {}, "unknown": {},
	// projection and ordering
	"join": {}, "inner": {}, "left": {}, "right": {}, "outer": {}, "cross": {},
	"group": {}, "by": {}, "order": {}, "limit": {}, "offset": {}, "having": {},
	"union": {}, "all": {}, "distinct": {}, "distinctrow": {}, "asc": {},
	"desc": {}, "procedure": {}, "handler": {},
	// literal prefixes and interval units
	"date": {}, "time": {}, "timestamp": {}, "interval": {}, "day": {},
	"hour": {}, "minute": {}, "second": {}, "month": {}, "year": {},
	"week": {}, "quarter": {}, "microsecond": {}, "day_hour": {},
	"day_minute": {}, "day_second": {}, "day_microsecond": {},
	"hour_minute": {}, "hour_second": {}, "hour_microsecond": {},
	"minute_second": {}, "minute_microsecond": {}, "second_microsecond": {},
	"year_month": {},
}

type fpToken struct {
	text string
	// word marks identifiers, literals, keywords, collapsed groups, and '*' —
	// tokens that get a separating space when adjacent to another word.
	word bool
	// kw marks words folded from the keyword set (not function names): a
	// keyword keeps a space before '(', so "where (a,b)" and "where(a,b)"
	// group identically while function calls stay tight.
	kw bool
}

// FingerprintSQL normalizes a Rows_query statement into its grouping key:
// comments stripped, literals replaced by ?, whitespace folded, parenthesized
// literal lists collapsed to (?), a curated keyword set case-folded, and
// identifiers preserved (backticks stripped, case kept). pt-query-digest-style
// deviations are documented in ADR 0026: identifier case is preserved, (?) is
// emitted once per list instead of ?+ notation, and the input is the stored
// <=4096-byte statement text.
func FingerprintSQL(sql string) string {
	toks := fingerprintTokens(sql)
	toks = collapseParenGroups(toks)
	toks = collapseGroupRuns(toks)
	return joinFingerprintTokens(toks)
}

func fingerprintTokens(sql string) []fpToken {
	toks := make([]fpToken, 0, 48)
	i := 0
	for i < len(sql) {
		c := sql[i]
		switch {
		case isFPSpace(c):
			i++
		case c == ';':
			// A mid-statement separator never appears in Rows_query text; a
			// trailing one is dropped. Skip silently either way.
			i++
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-' && (i+2 >= len(sql) || isFPSpace(sql[i+2])):
			i = fpSkipToNewline(sql, i+2)
		case c == '#':
			i = fpSkipToNewline(sql, i+1)
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			i = fpSkipBlockComment(sql, i+2)
		case c == '\'' || c == '"':
			i = fpSkipQuoted(sql, i, c)
			toks = append(toks, fpToken{text: fpLiteral, word: true})
		case c == '`':
			ident, end := fpScanBacktick(sql, i)
			toks = append(toks, fpToken{text: ident, word: true})
			i = end
		case c == '?':
			toks = append(toks, fpToken{text: fpLiteral, word: true})
			i++
		case c == '*':
			// '*' is word-ish: "select *" stays spaced and "a*b" normalizes to
			// "a * b" so spacing variants group together.
			toks = append(toks, fpToken{text: "*", word: true})
			i++
		case c == '%' && i+1 < len(sql) && sql[i+1] == 's':
			toks = append(toks, fpToken{text: fpLiteral, word: true})
			i += 2
		case c == ':' && i+1 < len(sql) && isFPIdentStart(sql[i+1]):
			i++
			for i < len(sql) && isFPIdentPart(sql[i]) {
				i++
			}
			toks = append(toks, fpToken{text: fpLiteral, word: true})
		case isFPLiteralPrefix(sql, i):
			i = fpSkipQuoted(sql, i+1, '\'')
			toks = append(toks, fpToken{text: fpLiteral, word: true})
		case isFPDigit(c) || (c == '.' && i+1 < len(sql) && isFPDigit(sql[i+1])):
			end, ident := fpScanNumber(sql, i)
			if ident {
				toks = append(toks, fpToken{text: sql[i:end], word: true})
			} else {
				toks = append(toks, fpToken{text: fpLiteral, word: true})
			}
			i = end
		case isFPIdentStart(c):
			var tok fpToken
			tok, i = fpScanWord(sql, i)
			toks = append(toks, tok)
		default:
			toks = append(toks, fpToken{text: string(c), word: false})
			i++
		}
	}
	return toks
}

// isFPLiteralPrefix reports a literal prefix run like x'..', b'..', n'..'.
func isFPLiteralPrefix(sql string, i int) bool {
	c := sql[i]
	return (c == 'x' || c == 'X' || c == 'b' || c == 'B' || c == 'n' || c == 'N') && i+1 < len(sql) && sql[i+1] == '\''
}

// fpScanWord consumes one identifier or keyword. A word immediately followed
// by '(' is a function name and folds like a keyword; other words fold only
// from the curated set. The kw flag marks folded keywords so the joiner can
// keep a space before '(' while function calls stay tight.
func fpScanWord(sql string, i int) (fpToken, int) {
	j := i + 1
	for j < len(sql) && isFPIdentPart(sql[j]) {
		j++
	}
	word := sql[i:j]
	lower := strings.ToLower(word)
	_, isKeyword := fingerprintKeywords[lower]
	if isKeyword || (j < len(sql) && sql[j] == '(') {
		word = lower
	}
	return fpToken{text: word, word: true, kw: isKeyword}, j
}

// collapseParenGroups folds every parenthesized group whose content is only
// literal placeholders, commas, null, or already-collapsed groups into (?).
// Innermost groups collapse first, so IN ((?),(?)) becomes IN (?) in one pass.
func collapseParenGroups(toks []fpToken) []fpToken {
	out := make([]fpToken, 0, len(toks))
	var stack []int
	for _, tok := range toks {
		out = append(out, tok)
		switch tok.text {
		case "(":
			stack = append(stack, len(out)-1)
		case ")":
			if len(stack) == 0 {
				continue
			}
			open := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if fpParenContentCollapsible(out[open+1 : len(out)-1]) {
				out = append(out[:open], fpToken{text: fpGroup, word: true})
			}
		}
	}
	return out
}

func fpParenContentCollapsible(content []fpToken) bool {
	// An empty group is a zero-argument call — now() — not a literal list.
	if len(content) == 0 {
		return false
	}
	for _, t := range content {
		// '-' and '+' stay in the stream as literal signs: (1, -2) collapses
		// just like (1, 2).
		switch t.text {
		case fpLiteral, ",", "null", fpGroup, "-", "+":
		default:
			return false
		}
	}
	return true
}

// collapseGroupRuns folds a run of (?) groups joined by commas into one (?),
// so VALUES (...), (...) ends as values (?).
func collapseGroupRuns(toks []fpToken) []fpToken {
	out := toks[:0]
	for i := 0; i < len(toks); {
		if toks[i].text == fpGroup {
			j := i + 1
			for j+1 < len(toks) && toks[j].text == "," && toks[j+1].text == fpGroup {
				j += 2
			}
			out = append(out, toks[i])
			i = j
			continue
		}
		out = append(out, toks[i])
		i++
	}
	return out
}

// joinFingerprintTokens emits the normalized string. Spacing is fully
// normalized: a single space separates adjacent word tokens (including after
// ",", ")", or ";"); punctuation otherwise attaches tight. Style variants like
// "a = 1" vs "a=1" and "in (1)" vs "in(1)" group identically.
func joinFingerprintTokens(toks []fpToken) string {
	var b strings.Builder
	var prev fpToken
	for i, tok := range toks {
		if i > 0 && fpSpaceBefore(prev, tok) {
			b.WriteByte(' ')
		}
		b.WriteString(tok.text)
		prev = tok
	}
	return b.String()
}

func fpSpaceBefore(prev, cur fpToken) bool {
	switch cur.text {
	case ",", ")", ".", ":":
		return false
	}
	switch prev.text {
	case "(", ".", ":", "-", "+", "~", "/", "=", "<", ">", "!", "|", "&", "^", "%":
		return false
	}
	if cur.text == "(" {
		return prev.kw
	}
	if !cur.word {
		return false
	}
	return prev.word || prev.text == ")" || prev.text == ","
}

func isFPSpace(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	}
	return false
}

func isFPDigit(c byte) bool { return c >= '0' && c <= '9' }

func isFPAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// isFPIdentStart treats non-ASCII bytes as identifier characters so multibyte
// table names survive as opaque identifiers instead of splitting.
func isFPIdentStart(c byte) bool {
	return isFPAlpha(c) || c == '_' || c == '$' || c >= 0x80
}

func isFPIdentPart(c byte) bool {
	return isFPIdentStart(c) || isFPDigit(c)
}

func fpSkipToNewline(sql string, i int) int {
	for i < len(sql) && sql[i] != '\n' {
		i++
	}
	return i
}

func fpSkipBlockComment(sql string, i int) int {
	end := strings.Index(sql[i:], "*/")
	if end < 0 {
		return len(sql)
	}
	return i + end + 2
}

// fpSkipQuoted consumes a single- or double-quoted string starting at the
// opening quote. Both backslash escapes and doubled-quote escapes are honored;
// an unterminated literal consumes to end of input.
func fpSkipQuoted(sql string, i int, q byte) int {
	i++
	for i < len(sql) {
		c := sql[i]
		if c == '\\' && i+1 < len(sql) {
			i += 2
			continue
		}
		if c == q {
			if i+1 < len(sql) && sql[i+1] == q {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return len(sql)
}

// fpScanBacktick consumes a backtick-quoted identifier (doubled backtick is an
// escaped literal backtick) and returns the name without the quotes.
func fpScanBacktick(sql string, i int) (string, int) {
	i++
	start := i
	for i < len(sql) {
		if sql[i] == '`' {
			if i+1 < len(sql) && sql[i+1] == '`' {
				i += 2
				continue
			}
			return sql[start:i], i + 1
		}
		i++
	}
	return sql[start:], len(sql)
}

// fpScanNumber consumes a numeric token. If the numeric run is followed
// directly by identifier characters the whole token is a digit-leading
// identifier (2fa_codes) and reported as one.
func fpScanNumber(sql string, i int) (end int, ident bool) {
	n := len(sql)
	if sql[i] == '0' && i+1 < n && (sql[i+1] == 'x' || sql[i+1] == 'X' || sql[i+1] == 'b' || sql[i+1] == 'B') {
		j := i + 2
		for j < n && isFPIdentPart(sql[j]) {
			j++
		}
		if j == i+2 {
			// 0x with no digits: the 0 is a literal, x starts an identifier.
			return i + 1, false
		}
		if fpDigitsOnly(sql[i+2:j], sql[i+1]) {
			return j, false
		}
		return j, true
	}
	j := i
	for j < n && isFPDigit(sql[j]) {
		j++
	}
	if j < n && sql[j] == '.' {
		j++
		for j < n && isFPDigit(sql[j]) {
			j++
		}
	}
	if j < n && (sql[j] == 'e' || sql[j] == 'E') {
		k := j + 1
		if k < n && (sql[k] == '+' || sql[k] == '-') {
			k++
		}
		if k < n && isFPDigit(sql[k]) {
			for k < n && isFPDigit(sql[k]) {
				k++
			}
			j = k
		}
	}
	if j < n && isFPIdentPart(sql[j]) {
		for j < n && isFPIdentPart(sql[j]) {
			j++
		}
		return j, true
	}
	return j, false
}

func fpDigitsOnly(run string, prefix byte) bool {
	for i := 0; i < len(run); i++ {
		c := run[i]
		if prefix == 'x' || prefix == 'X' {
			if isFPDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') {
				continue
			}
			return false
		}
		if c != '0' && c != '1' {
			return false
		}
	}
	return len(run) > 0
}

// recordStatementOccurrence folds the retained Rows_query statement into
// per-transaction fingerprint evidence. The same scoping rules as
// retainedQuerySQL apply: only inside-window retained row events merge, and a
// statement cleared by a filtered event stops contributing (ADR 0026). One
// RetainedStatement is created per statement sequence — identical SQL under
// separate ROWS_QUERY events still counts as separate occurrences — then each
// row event attributes rows and tables to the latest occurrence.
func (b *TransactionBuilder) recordStatementOccurrence(ev model.NormalizedEvent) {
	if b.current.querySQL == "" {
		return
	}
	if b.current.emittedStmtSeq != b.current.stmtSeq {
		b.current.stmts = append(b.current.stmts, model.RetainedStatement{
			Fingerprint: FingerprintSQL(b.current.querySQL),
			Sample:      model.FormatQuerySummary(b.current.querySQL, b.current.queryOriginalBytes),
			Tables:      make(map[string]int, 1),
		})
		b.current.emittedStmtSeq = b.current.stmtSeq
	}
	st := &b.current.stmts[len(b.current.stmts)-1]
	if ev.Schema != "" && ev.Table != "" {
		st.Tables[ev.Schema+"."+ev.Table] += ev.RowCount
	}
	st.Rows += ev.RowCount
}

// fingerprintGroup accumulates one normalized statement's workload evidence.
type fingerprintGroup struct {
	sample     string
	statements int
	rows       int
	tables     map[string]int
}

// consumeFingerprints folds one transaction's retained statement occurrences
// into the workload groups. It runs for every persisted transaction —
// including partial and unknown ones — because their rows already count in
// the workload total. A new fingerprint past the group cap increments the
// overflow counter instead of opening a group.
func (a *ReportAggregator) consumeFingerprints(txn model.Transaction) {
	if len(txn.RetainedStatements) == 0 {
		return
	}
	if a.fingerprints == nil {
		a.fingerprints = make(map[string]*fingerprintGroup)
	}
	for i := range txn.RetainedStatements {
		st := txn.RetainedStatements[i]
		g := a.fingerprints[st.Fingerprint]
		if g == nil {
			if len(a.fingerprints) >= fingerprintTrackLimit {
				a.fingerprintOverflow++
				continue
			}
			g = &fingerprintGroup{tables: make(map[string]int, len(st.Tables))}
			a.fingerprints[st.Fingerprint] = g
		}
		g.statements++
		g.rows += st.Rows
		if g.sample == "" {
			g.sample = st.Sample
		}
		for t, n := range st.Tables {
			g.tables[t] += n
		}
	}
}

// snapshotFingerprints returns groups ranked by rows, then statement count,
// then fingerprint text — the same deterministic style as the pattern sort.
func (a *ReportAggregator) snapshotFingerprints() []model.StatementFingerprint {
	if len(a.fingerprints) == 0 {
		return []model.StatementFingerprint{}
	}
	out := make([]model.StatementFingerprint, 0, len(a.fingerprints))
	for fp, g := range a.fingerprints {
		sf := model.StatementFingerprint{
			Fingerprint: fp,
			Sample:      g.sample,
			Statements:  g.statements,
			Rows:        g.rows,
			Tables:      cloneMapEnsureNonNil(g.tables),
		}
		if a.totalRows > 0 {
			sf.ShareOfRows = float64(sf.Rows) / float64(a.totalRows)
		}
		out = append(out, sf)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Rows != out[j].Rows {
			return out[i].Rows > out[j].Rows
		}
		if out[i].Statements != out[j].Statements {
			return out[i].Statements > out[j].Statements
		}
		return out[i].Fingerprint < out[j].Fingerprint
	})
	return out
}
