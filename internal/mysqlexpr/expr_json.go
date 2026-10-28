// Package mysqlexpr: the JSON side of the generated-column value domain.
// input: a JSON text GenVal and a constant path or literal from the logged SQL.
// output: extracted and unquoted GenVals, and equality on the parsed node tree using MySQL's number handling: JSON numbers compare by value (-0 is 0), fixed-point when the literal is not a float.
// pos: value helpers for expr.go and eval.go. Flashback-only module; analyze does not call it.
// note: if this file changes, update this header and module README.md.
package mysqlexpr

import (
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode"
)

type jKind uint8

const (
	jNull jKind = iota
	jBool
	jNum
	jStr
	jArr
	jObj
)

type jNumKind uint8

const (
	jnInt jNumKind = iota
	jnDec
	jnFloat
)

type jNode struct {
	kind  jKind
	b     bool
	num   string
	nkind jNumKind
	str   string
	arr   []*jNode
	obj   []jPair
}

type jPair struct {
	k string
	v *jNode
}

type jStep struct {
	key   string
	index int
	isIdx bool
}

func evalJSONExtract(args []GenVal) (GenVal, bool) {
	if len(args) != 2 {
		return GenVal{}, false
	}
	if args[0].kind == genNull || args[1].kind == genNull {
		return genNullVal(), true
	}
	pathText, ok := args[1].pathText()
	if !ok {
		return GenVal{}, false
	}
	return jsonAtPath(args[0], pathText, false)
}

func evalJSONUnquote(args []GenVal) (GenVal, bool) {
	if len(args) != 1 {
		return GenVal{}, false
	}
	return jsonUnquote(args[0])
}

func applyJSONArrow(v GenVal, path string, unquote bool) (GenVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	got, ok := jsonAtPath(v, path, unquote)
	if !ok {
		return GenVal{}, false
	}
	if unquote {
		return jsonUnquote(got)
	}
	return got, true
}

func jsonAtPath(v GenVal, path string, _ bool) (GenVal, bool) {
	if v.kind != genJSON || v.j == nil {
		return GenVal{}, false
	}
	steps, ok := parseJSONPath(path)
	if !ok {
		return GenVal{}, false
	}
	node, found := v.j.walk(steps)
	if !found {
		return genNullVal(), true
	}
	return GenVal{kind: genJSON, j: node}, true
}

func jsonUnquote(v GenVal) (GenVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	if v.kind != genJSON || v.j == nil {
		return GenVal{}, false
	}
	switch v.j.kind {
	case jNull:
		// ->> of a JSON null is the four characters null. A missing path stays SQL NULL.
		return genTextVal("null", "utf8mb4"), true
	case jStr:
		return genTextVal(v.j.str, "utf8mb4"), true
	case jBool:
		if v.j.b {
			return genTextVal("true", "utf8mb4"), true
		}
		return genTextVal("false", "utf8mb4"), true
	case jNum:
		switch v.j.nkind {
		case jnFloat:
			f, ok := parseJSONFloat(v.j.num)
			if !ok {
				return GenVal{}, false
			}
			return genTextVal(mysqlJSONFloatString(f), "utf8mb4"), true
		default:
			// Decimal text keeps trailing zeros. An integer has no ".0".
			return genTextVal(v.j.num, "utf8mb4"), true
		}
	default:
		// Object and array text depends on spacing MySQL chooses. Do not guess.
		return GenVal{}, false
	}
}

func (v GenVal) pathText() (string, bool) {
	if v.kind != genText {
		return "", false
	}
	return v.text, true
}

func jsonEqual(a, b *jNode) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.kind != b.kind {
		return false
	}
	switch a.kind {
	case jNull:
		return true
	case jBool:
		return a.b == b.b
	case jStr:
		return a.str == b.str
	case jNum:
		return jsonNumEqual(a, b)
	case jArr:
		if len(a.arr) != len(b.arr) {
			return false
		}
		for i := range a.arr {
			if !jsonEqual(a.arr[i], b.arr[i]) {
				return false
			}
		}
		return true
	case jObj:
		if len(a.obj) != len(b.obj) {
			return false
		}
		for _, pair := range a.obj {
			other, ok := objGet(b, pair.k)
			if !ok || !jsonEqual(pair.v, other) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func objGet(n *jNode, key string) (*jNode, bool) {
	for _, pair := range n.obj {
		if pair.k == key {
			return pair.v, true
		}
	}
	return nil, false
}

func jsonNumEqual(a, b *jNode) bool {
	if a == nil || b == nil || a.nkind != b.nkind {
		return false
	}
	switch a.nkind {
	case jnFloat:
		fa, oka := parseJSONFloat(a.num)
		fb, okb := parseJSONFloat(b.num)
		if !oka || !okb {
			return false
		}
		return fa == fb && math.Signbit(fa) == math.Signbit(fb)
	case jnDec:
		ra, oka := new(big.Rat).SetString(a.num)
		rb, okb := new(big.Rat).SetString(b.num)
		if !oka || !okb {
			return false
		}
		return ra.Cmp(rb) == 0
	default:
		ia, oka := new(big.Int).SetString(a.num, 10)
		ib, okb := new(big.Int).SetString(b.num, 10)
		if !oka || !okb {
			return false
		}
		return ia.Cmp(ib) == 0
	}
}

func parseJSONPath(path string) ([]jStep, bool) {
	if path == "" || path[0] != '$' {
		return nil, false
	}
	var steps []jStep
	i := 1
	for i < len(path) {
		switch path[i] {
		case '.':
			i++
			if i < len(path) && (path[i] == '"' || path[i] == '\'') {
				q := path[i]
				i++
				start := i
				for i < len(path) && path[i] != q {
					i++
				}
				if i >= len(path) {
					return nil, false
				}
				steps = append(steps, jStep{key: path[start:i]})
				i++
				continue
			}
			if i < len(path) && path[i] == '*' {
				return nil, false
			}
			start := i
			for i < len(path) && (path[i] == '_' || path[i] == '$' || unicode.IsLetter(rune(path[i])) || unicode.IsDigit(rune(path[i]))) {
				i++
			}
			if i == start {
				return nil, false
			}
			steps = append(steps, jStep{key: path[start:i]})
		case '[':
			i++
			if i < len(path) && (path[i] == '\'' || path[i] == '"') {
				q := path[i]
				i++
				start := i
				for i < len(path) && path[i] != q {
					i++
				}
				if i >= len(path) || i+1 >= len(path) || path[i+1] != ']' {
					return nil, false
				}
				steps = append(steps, jStep{key: path[start:i]})
				i += 2
				continue
			}
			if i < len(path) && path[i] == '*' {
				return nil, false
			}
			start := i
			for i < len(path) && path[i] >= '0' && path[i] <= '9' {
				i++
			}
			if i == start || i >= len(path) || path[i] != ']' {
				return nil, false
			}
			n, err := strconv.Atoi(path[start:i])
			if err != nil {
				return nil, false
			}
			steps = append(steps, jStep{index: n, isIdx: true})
			i++
		default:
			return nil, false
		}
	}
	return steps, true
}

func parseJSONLogged(lit string) (GenVal, bool) {
	node, ok := parseJSONSQLString(lit)
	if !ok {
		return GenVal{}, false
	}
	return GenVal{kind: genJSON, j: node}, true
}

func parseJSONSQLString(s string) (*jNode, bool) {
	sc := &Scan{S: s}
	node, ok := parseJSONNode(sc)
	if !ok {
		return nil, false
	}
	sc.Skip()
	if sc.I != len(sc.S) {
		return nil, false
	}
	return node, true
}

func parseJSONNode(sc *Scan) (*jNode, bool) {
	sc.Skip()
	if sc.I >= len(sc.S) {
		return nil, false
	}
	if sc.WordIs("JSON_OBJECT") {
		body, ok := sc.ParenBody()
		if !ok {
			return nil, false
		}
		node := &jNode{kind: jObj}
		if strings.TrimSpace(body) == "" {
			return node, true
		}
		parts := SplitComma(body)
		if len(parts)%2 != 0 {
			return nil, false
		}
		for i := 0; i < len(parts); i += 2 {
			key, ok := sqlConstString(parts[i])
			if !ok {
				return nil, false
			}
			val, ok := parseJSONSQLString(parts[i+1])
			if !ok {
				return nil, false
			}
			node.obj = append(node.obj, jPair{k: key, v: val})
		}
		return node, true
	}
	if sc.WordIs("JSON_ARRAY") {
		body, ok := sc.ParenBody()
		if !ok {
			return nil, false
		}
		node := &jNode{kind: jArr}
		if strings.TrimSpace(body) == "" {
			return node, true
		}
		for _, part := range SplitComma(body) {
			val, ok := parseJSONSQLString(part)
			if !ok {
				return nil, false
			}
			node.arr = append(node.arr, val)
		}
		return node, true
	}
	if sc.WordIs("CAST") {
		return parseJSONCast(sc)
	}
	if sc.WordIs("NULL") {
		return &jNode{kind: jNull}, true
	}
	if sc.WordIs("TRUE") {
		return &jNode{kind: jBool, b: true}, true
	}
	if sc.WordIs("FALSE") {
		return &jNode{kind: jBool, b: false}, true
	}
	if sc.S[sc.I] == '\'' || sc.S[sc.I] == '"' {
		text, ok := sc.decodeQuoted()
		if !ok {
			return nil, false
		}
		return &jNode{kind: jStr, str: text}, true
	}
	if sc.S[sc.I] == '-' || (sc.S[sc.I] >= '0' && sc.S[sc.I] <= '9') {
		start := sc.I
		if sc.S[sc.I] == '-' {
			sc.I++
		}
		for sc.I < len(sc.S) && (isNumChar(sc.S[sc.I])) {
			sc.I++
		}
		text := sc.S[start:sc.I]
		if text == "" || text == "-" {
			return nil, false
		}
		return newJNum(text), true
	}
	return nil, false
}

func isNumChar(b byte) bool {
	return (b >= '0' && b <= '9') || b == '.' || b == 'e' || b == 'E' || b == '+' || b == '-'
}

func parseJSONCast(sc *Scan) (*jNode, bool) {
	body, ok := sc.ParenBody()
	if !ok {
		return nil, false
	}
	inner, typ, ok := splitAsType(body)
	if !ok {
		return nil, false
	}
	base, _, _ := strings.Cut(strings.ToUpper(strings.TrimSpace(typ)), "(")
	base = strings.TrimSpace(base)
	switch base {
	case "JSON":
		inner = strings.TrimSpace(inner)
		if inner == "" {
			return nil, false
		}
		if inner[0] == '\'' || inner[0] == '"' {
			text, ok := decodeQuotedSQL(inner)
			if !ok {
				return nil, false
			}
			return parseJSONText(text)
		}
		return parseJSONSQLString(inner)
	case "UNSIGNED", "SIGNED", "DECIMAL", "DATE", "TIME", "DATETIME", "TIMESTAMP":
		text := strings.TrimSpace(inner)
		if text != "" && (text[0] == '\'' || text[0] == '"') {
			decoded, ok := decodeQuotedSQL(text)
			if !ok {
				return nil, false
			}
			text = decoded
		}
		if base == "DATE" || base == "TIME" || base == "DATETIME" || base == "TIMESTAMP" {
			return &jNode{kind: jStr, str: text}, true
		}
		return newJNum(text), true
	default:
		return nil, false
	}
}

func splitAsType(body string) (inner, typ string, ok bool) {
	sc := &Scan{S: body}
	depth := 0
	inString := byte(0)
	for sc.I < len(sc.S) {
		c := sc.S[sc.I]
		if inString != 0 {
			if c == inString {
				if sc.I+1 < len(sc.S) && sc.S[sc.I+1] == inString {
					sc.I += 2
					continue
				}
				inString = 0
			} else if c == '\\' && sc.I+1 < len(sc.S) {
				sc.I += 2
				continue
			}
			sc.I++
			continue
		}
		switch c {
		case '\'', '"', '`':
			inString = c
			sc.I++
		case '(':
			depth++
			sc.I++
		case ')':
			if depth > 0 {
				depth--
			}
			sc.I++
		default:
			if depth == 0 && identStart(c) {
				start := sc.I
				word := sc.bare()
				if strings.EqualFold(word, "AS") {
					return strings.TrimSpace(body[:start]), strings.TrimSpace(sc.S[sc.I:]), true
				}
				continue
			}
			sc.I++
		}
	}
	return "", "", false
}

func sqlConstString(s string) (string, bool) {
	return decodeQuotedSQL(strings.TrimSpace(s))
}

func decodeQuotedSQL(s string) (string, bool) {
	sc := &Scan{S: s}
	text, ok := sc.decodeQuoted()
	if !ok {
		return "", false
	}
	sc.Skip()
	if sc.I != len(sc.S) {
		return "", false
	}
	return text, true
}

func parseJSONText(s string) (*jNode, bool) {
	node, i, ok := parseJSONTextAt(s, 0)
	if !ok {
		return nil, false
	}
	for i < len(s) && (s[i] == ' ' || s[i] == '\n' || s[i] == '\t' || s[i] == '\r') {
		i++
	}
	if i != len(s) {
		return nil, false
	}
	return node, true
}

func parseJSONTextAt(s string, i int) (*jNode, int, bool) {
	i = skipJSONSpace(s, i)
	if i >= len(s) {
		return nil, i, false
	}
	switch s[i] {
	case '{':
		i++
		node := &jNode{kind: jObj}
		i = skipJSONSpace(s, i)
		if i < len(s) && s[i] == '}' {
			return node, i + 1, true
		}
		for {
			i = skipJSONSpace(s, i)
			key, next, ok := parseJSONString(s, i)
			if !ok {
				return nil, i, false
			}
			i = skipJSONSpace(s, next)
			if i >= len(s) || s[i] != ':' {
				return nil, i, false
			}
			val, next, ok := parseJSONTextAt(s, i+1)
			if !ok {
				return nil, i, false
			}
			node.obj = append(node.obj, jPair{k: key, v: val})
			i = skipJSONSpace(s, next)
			if i >= len(s) {
				return nil, i, false
			}
			if s[i] == '}' {
				return node, i + 1, true
			}
			if s[i] != ',' {
				return nil, i, false
			}
			i++
		}
	case '[':
		i++
		node := &jNode{kind: jArr}
		i = skipJSONSpace(s, i)
		if i < len(s) && s[i] == ']' {
			return node, i + 1, true
		}
		for {
			val, next, ok := parseJSONTextAt(s, i)
			if !ok {
				return nil, i, false
			}
			node.arr = append(node.arr, val)
			i = skipJSONSpace(s, next)
			if i >= len(s) {
				return nil, i, false
			}
			if s[i] == ']' {
				return node, i + 1, true
			}
			if s[i] != ',' {
				return nil, i, false
			}
			i++
		}
	case '"':
		text, next, ok := parseJSONString(s, i)
		if !ok {
			return nil, i, false
		}
		return &jNode{kind: jStr, str: text}, next, true
	case 't':
		if strings.HasPrefix(s[i:], "true") {
			return &jNode{kind: jBool, b: true}, i + 4, true
		}
	case 'f':
		if strings.HasPrefix(s[i:], "false") {
			return &jNode{kind: jBool, b: false}, i + 5, true
		}
	case 'n':
		if strings.HasPrefix(s[i:], "null") {
			return &jNode{kind: jNull}, i + 4, true
		}
	}
	if s[i] == '-' || (s[i] >= '0' && s[i] <= '9') {
		start := i
		if s[i] == '-' {
			i++
		}
		for i < len(s) && isNumChar(s[i]) {
			i++
		}
		if i == start || (i == start+1 && s[start] == '-') {
			return nil, start, false
		}
		return newJNum(s[start:i]), i, true
	}
	return nil, i, false
}

func skipJSONSpace(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\n' || s[i] == '\t' || s[i] == '\r') {
		i++
	}
	return i
}

func parseJSONString(s string, i int) (string, int, bool) {
	if i >= len(s) || s[i] != '"' {
		return "", i, false
	}
	i++
	var b strings.Builder
	for i < len(s) {
		c := s[i]
		if c == '"' {
			return b.String(), i + 1, true
		}
		if c == '\\' {
			i++
			if i >= len(s) {
				return "", i, false
			}
			switch s[i] {
			case '"', '\\', '/':
				b.WriteByte(s[i])
			case 'b':
				b.WriteByte('\b')
			case 'f':
				b.WriteByte('\f')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			case 't':
				b.WriteByte('\t')
			case 'u':
				if i+4 >= len(s) {
					return "", i, false
				}
				r, err := strconv.ParseUint(s[i+1:i+5], 16, 32)
				if err != nil {
					return "", i, false
				}
				b.WriteRune(rune(r))
				i += 4
			default:
				return "", i, false
			}
			i++
			continue
		}
		b.WriteByte(c)
		i++
	}
	return "", i, false
}

func newJNum(text string) *jNode {
	return &jNode{kind: jNum, num: text, nkind: classifyJSONNum(text)}
}

func classifyJSONNum(text string) jNumKind {
	if strings.ContainsAny(text, "eE") || negZeroJSON(text) {
		return jnFloat
	}
	if strings.Contains(text, ".") {
		return jnDec
	}
	return jnInt
}

func negZeroJSON(text string) bool {
	// -0 and -0.0 are JSON doubles. -0.00 still has a decimal scale, so it stays a decimal.
	return text == "-0" || text == "-0.0"
}

func parseJSONFloat(text string) (float64, bool) {
	f, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	if f == 0 && strings.HasPrefix(strings.TrimSpace(text), "-") {
		return math.Copysign(0, -1), true
	}
	return f, true
}

// mysqlJSONFloatString is MySQL's JSON double text: my_gcvt at field width 34,
// then ".0" when the result has neither a decimal point nor an exponent.
func mysqlJSONFloatString(f float64) string {
	neg := math.Signbit(f)
	abs := math.Abs(f)
	if abs == 0 {
		if neg {
			return "-0.0"
		}
		return "0.0"
	}
	sci := strconv.FormatFloat(abs, 'e', -1, 64)
	mant, expStr, ok := strings.Cut(sci, "e")
	if !ok {
		return sci
	}
	exp, err := strconv.Atoi(expStr)
	if err != nil {
		return sci
	}
	digits := strings.ReplaceAll(mant, ".", "")
	decpt := exp + 1
	useF := decpt >= -14 && (decpt <= 15 || len(digits) > decpt)
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	if useF {
		writeJSONFixed(&b, digits, decpt)
	} else {
		b.WriteByte(digits[0])
		if len(digits) > 1 {
			b.WriteByte('.')
			b.WriteString(digits[1:])
		}
		b.WriteByte('e')
		b.WriteString(strconv.Itoa(exp))
	}
	s := b.String()
	if !strings.ContainsAny(s, ".e") {
		s += ".0"
	}
	return s
}

func writeJSONFixed(b *strings.Builder, digits string, decpt int) {
	if decpt <= 0 {
		b.WriteString("0.")
		for i := 0; i < -decpt; i++ {
			b.WriteByte('0')
		}
		b.WriteString(digits)
		return
	}
	if decpt >= len(digits) {
		b.WriteString(digits)
		for i := len(digits); i < decpt; i++ {
			b.WriteByte('0')
		}
		return
	}
	b.WriteString(digits[:decpt])
	b.WriteByte('.')
	b.WriteString(digits[decpt:])
}
