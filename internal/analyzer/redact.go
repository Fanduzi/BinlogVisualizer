package analyzer

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// redactCredentials replaces password and authentication-hash literals so a
// report can be pasted into a ticket. The replacement matches MySQL's
// rewritten general log: the secret becomes <secret>.
func redactCredentials(sql string) string {
	spans := credentialSpans(sql)
	if len(spans) == 0 {
		return sql
	}
	var b strings.Builder
	b.Grow(len(sql))
	last := 0
	for _, sp := range spans {
		if sp.start < last || sp.end < sp.start || sp.end > len(sql) {
			continue
		}
		b.WriteString(sql[last:sp.start])
		b.WriteString("<secret>")
		last = sp.end
	}
	b.WriteString(sql[last:])
	return b.String()
}

type credSpan struct{ start, end int }

func credentialSpans(sql string) []credSpan {
	var spans []credSpan
	for i := 0; i < len(sql); {
		if hasWordAt(sql, i, "identified") {
			if end, ok := identifiedSecretEnd(sql, i); ok {
				spans = append(spans, credSpan{start: end.start, end: end.end})
				i = end.end
				continue
			}
		}
		if hasWordAt(sql, i, "set") {
			if end, ok := setPasswordSecret(sql, i); ok {
				spans = append(spans, end)
				i = end.end
				continue
			}
		}
		i++
	}
	return spans
}

type secretSpan struct{ start, end int }

func identifiedSecretEnd(sql string, identAt int) (secretSpan, bool) {
	j := skipSpace(sql, identAt+len("identified"))
	if hasWordAt(sql, j, "by") {
		j = skipSpace(sql, j+len("by"))
		if hasWordAt(sql, j, "random") {
			return secretSpan{}, false
		}
		if hasWordAt(sql, j, "password") {
			j = skipSpace(sql, j+len("password"))
		}
		return consumeSecret(sql, j)
	}
	if hasWordAt(sql, j, "with") {
		j = skipSpace(sql, j+len("with"))
		j = skipPluginName(sql, j)
		j = skipSpace(sql, j)
		if hasWordAt(sql, j, "as") || hasWordAt(sql, j, "by") {
			j = skipSpace(sql, j+2)
			return consumeSecret(sql, j)
		}
	}
	return secretSpan{}, false
}

func setPasswordSecret(sql string, setAt int) (credSpan, bool) {
	j := skipSpace(sql, setAt+len("set"))
	if !hasWordAt(sql, j, "password") {
		return credSpan{}, false
	}
	rest := sql[j+len("password"):]
	eq := strings.IndexByte(rest, '=')
	if eq < 0 {
		return credSpan{}, false
	}
	k := skipSpace(sql, j+len("password")+eq+1)
	if hasWordAt(sql, k, "password") {
		after := k + len("password")
		if after < len(sql) && sql[after] == '(' {
			end, ok := consumeCall(sql, after)
			if ok {
				return credSpan{start: k, end: end}, true
			}
		}
	}
	sec, ok := consumeSecret(sql, k)
	if !ok {
		return credSpan{}, false
	}
	return credSpan{start: sec.start, end: sec.end}, true
}

func consumeSecret(sql string, i int) (secretSpan, bool) {
	if i >= len(sql) {
		return secretSpan{}, false
	}
	switch sql[i] {
	case '\'', '"', '`':
		end, ok := consumeQuoted(sql, i)
		if !ok {
			return secretSpan{}, false
		}
		return secretSpan{start: i, end: end}, true
	default:
		end := i
		for end < len(sql) && !isSQLSpace(sql[end]) && sql[end] != ';' && sql[end] != ',' {
			end++
		}
		if end == i {
			return secretSpan{}, false
		}
		return secretSpan{start: i, end: end}, true
	}
}

func consumeCall(sql string, paren int) (int, bool) {
	depth := 0
	for j := paren; j < len(sql); j++ {
		switch sql[j] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return j + 1, true
			}
		case '\'', '"', '`':
			end, ok := consumeQuoted(sql, j)
			if !ok {
				return 0, false
			}
			j = end - 1
		}
	}
	return 0, false
}

func consumeQuoted(sql string, i int) (int, bool) {
	if i >= len(sql) {
		return 0, false
	}
	q := sql[i]
	for j := i + 1; j < len(sql); j++ {
		if sql[j] == '\\' && q != '`' && j+1 < len(sql) {
			j++
			continue
		}
		if sql[j] == q {
			if q == '\'' && j+1 < len(sql) && sql[j+1] == '\'' {
				j++
				continue
			}
			return j + 1, true
		}
	}
	return 0, false
}

func skipPluginName(sql string, i int) int {
	i = skipSpace(sql, i)
	if i >= len(sql) {
		return i
	}
	if sql[i] == '\'' || sql[i] == '"' || sql[i] == '`' {
		if end, ok := consumeQuoted(sql, i); ok {
			return end
		}
	}
	j := i
	for j < len(sql) && !isSQLSpace(sql[j]) {
		j++
	}
	return j
}

func skipSpace(sql string, i int) int {
	for i < len(sql) && isSQLSpace(sql[i]) {
		i++
	}
	return i
}

func isSQLSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

func hasWordAt(sql string, i int, word string) bool {
	if i < 0 || i+len(word) > len(sql) {
		return false
	}
	if !strings.EqualFold(sql[i:i+len(word)], word) {
		return false
	}
	if i > 0 && isIdentByte(sql[i-1]) {
		return false
	}
	if i+len(word) < len(sql) && isIdentByte(sql[i+len(word)]) {
		return false
	}
	return true
}

func isIdentByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

// sanitizeDisplaySQL drops bytes that are not safe to print on a terminal.
func sanitizeDisplaySQL(s string) string {
	if s == "" || (utf8.ValidString(s) && !hasControl(s)) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError || unicode.IsControl(r) {
			i += size
			continue
		}
		b.WriteRune(r)
		i += size
	}
	return b.String()
}

func hasControl(s string) bool {
	for _, r := range s {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}
