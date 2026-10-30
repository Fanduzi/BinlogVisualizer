// Package mysqlexpr: logged SQL literal decoding.
// input: one literal string from a FULL row image (or a schema dump).
// output: the typed GenVal the checkers evaluate, plus the column-name to
// value environment a row's literals build. A literal that cannot be read
// exactly is not a value: it yields ok=false or the genBad cell a check
// refuses to match on.
// pos: beside expr.go; the generated checker and the --where predicate
// (where.go's LiteralCell) share this decode.
// note: if this file changes, update this header and module README.md.
package mysqlexpr

import (
	"strings"
)

func parseLoggedValue(lit string) (GenVal, bool) {
	s := strings.TrimSpace(lit)
	switch {
	case s == "":
		return GenVal{}, false
	case strings.EqualFold(s, "NULL"):
		return genNullVal(), true
	case s[0] == '\'' || s[0] == '"':
		sc := &Scan{S: s}
		v, ok, _ := sc.quotedText("utf8mb4")
		if !ok {
			return GenVal{}, false
		}
		sc.Skip()
		if sc.I != len(sc.S) {
			return GenVal{}, false
		}
		return v, true
	case s[0] == '_' || s[0] == 'X' || s[0] == 'x' || strings.HasPrefix(strings.ToLower(s), "0x") ||
		(len(s) >= 2 && (s[0] == 'b' || s[0] == 'B') && s[1] == '\''):
		sc := &Scan{S: s}
		v, ok := sc.evalAtom()
		if !ok {
			return GenVal{}, false
		}
		sc.Skip()
		if sc.I != len(sc.S) {
			return GenVal{}, false
		}
		return v, true
	case strings.HasPrefix(strings.ToUpper(s), "JSON_") || strings.HasPrefix(strings.ToUpper(s), "CAST(") || strings.HasPrefix(strings.ToUpper(s), "CAST "):
		return parseJSONLogged(s)
	default:
		if v, ok := genNumVal(s); ok {
			return v, true
		}
		return GenVal{}, false
	}
}

func loggedEnv(columns, values []string, meta ...[]SchemaCol) map[string]GenVal {
	var cols []SchemaCol
	if len(meta) > 0 {
		cols = meta[0]
	}
	byName := make(map[string]SchemaCol, len(cols))
	for _, col := range cols {
		byName[strings.ToLower(col.Name)] = col
	}
	env := make(map[string]GenVal, len(columns))
	for i, name := range columns {
		lit := "NULL"
		if i < len(values) {
			lit = values[i]
		}
		v, ok := parseLoggedValue(lit)
		if !ok {
			env[strings.ToLower(name)] = GenVal{kind: genBad}
			continue
		}
		if col, found := byName[strings.ToLower(name)]; found {
			v = attachColMeta(v, col)
		}
		env[strings.ToLower(name)] = v
	}
	return env
}

func attachColMeta(v GenVal, col SchemaCol) GenVal {
	v.base = col.base
	if len(col.members) > 0 {
		v.members = append([]string(nil), col.members...)
	}
	v.prec = col.prec
	v.scale = col.scale
	v.hasPrec = col.hasPrec
	v.unsigned = col.unsigned
	if v.charset == "" && col.charset != "" {
		v.charset = col.charset
	}
	return v
}
