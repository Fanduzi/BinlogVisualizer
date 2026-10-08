// Package analyzer evaluates generated-column expressions against logged row images.
// input: the expression text from a schema file, and SQL literals from FULL row images.
// output: a match, a contradiction (with one example image), or unverified when the expression cannot be modelled exactly and the images do not contradict it. The example prints printable text for a text charset and keeps the hex literal for binary or non-printable bytes. UPPER/LOWER cover ascii, latin1's 1:1 map, and utf8mb4 Latin-1 plus µ. NULL propagates, except CONCAT_WS, which skips NULL arguments. An unknown charset, an unmodelled type, or a value this checker will not claim is unverified, never a match and never a mismatch.
// pos: flashback-only helper. Analyze does not call it.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	xunicode "golang.org/x/text/encoding/unicode"

	"binlogviz/internal/i18n"
)

// genKind is the value of one generated-expression result or logged literal.
type genKind uint8

const (
	genBad genKind = iota
	genNull
	genNum
	genText
	genJSON
)

// genVal is a logged literal or an expression result.
// text is Unicode when decoded is set. An undecoded charset is unverifiable.
// raw is the original bytes when known. base and the precision fields are the
// schema column the literal came from, so string functions can use that type.
type genVal struct {
	kind    genKind
	n       *big.Rat
	numText string
	text    string
	raw     []byte
	haveRaw bool
	charset string
	decoded bool
	j       *jNode
	base    string
	members []string
	prec    int
	scale   int
	hasPrec bool
}

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

type loggedImage struct {
	columns []string
	values  []string
}

func genNullVal() genVal { return genVal{kind: genNull} }

func genNumVal(text string) (genVal, bool) {
	r, ok := new(big.Rat).SetString(text)
	if !ok {
		return genVal{}, false
	}
	return genVal{kind: genNum, n: r, numText: text}, true
}

func genTextVal(text, charset string) genVal {
	if charset == "" {
		charset = "utf8mb4"
	}
	return genVal{kind: genText, text: text, raw: []byte(text), haveRaw: true, charset: charset, decoded: true}
}

func (v genVal) asInt() (intVal, bool) {
	switch v.kind {
	case genNull:
		return intVal{null: true}, true
	case genNum:
		if v.n == nil {
			return intVal{}, false
		}
		return intVal{n: v.n}, true
	default:
		return intVal{}, false
	}
}

// evalGenExpr evaluates expr with logged column values.
// ok is false when the expression uses something this checker does not evaluate.
func evalGenExpr(expr string, env map[string]genVal) (genVal, bool) {
	sc := &sqlScan{s: expr}
	v, ok := parseGenAdd(sc, env)
	if !ok {
		return genVal{}, false
	}
	sc.skip()
	if sc.i != len(sc.s) {
		return genVal{}, false
	}
	return v, true
}

func parseGenAdd(sc *sqlScan, env map[string]genVal) (genVal, bool) {
	left, ok := parseGenMul(sc, env)
	if !ok {
		return genVal{}, false
	}
	for {
		sc.skip()
		if sc.i >= len(sc.s) || (sc.s[sc.i] != '+' && sc.s[sc.i] != '-') {
			return left, true
		}
		// `->` and `->>` are consumed inside a primary. A following arrow is not addition.
		if sc.s[sc.i] == '-' && sc.i+1 < len(sc.s) && sc.s[sc.i+1] == '>' {
			return left, true
		}
		op := sc.s[sc.i]
		sc.i++
		right, ok := parseGenMul(sc, env)
		if !ok {
			return genVal{}, false
		}
		next, ok := applyGenAdd(left, right, op)
		if !ok {
			return genVal{}, false
		}
		left = next
	}
}

func applyGenAdd(left, right genVal, op byte) (genVal, bool) {
	li, ok1 := left.asInt()
	ri, ok2 := right.asInt()
	if !ok1 || !ok2 {
		return genVal{}, false
	}
	if li.null || ri.null {
		return genNullVal(), true
	}
	n := new(big.Rat)
	if op == '+' {
		n.Add(li.n, ri.n)
	} else {
		n.Sub(li.n, ri.n)
	}
	return genVal{kind: genNum, n: n}, true
}

func parseGenMul(sc *sqlScan, env map[string]genVal) (genVal, bool) {
	left, ok := parseGenUnary(sc, env)
	if !ok {
		return genVal{}, false
	}
	for {
		op, ok := consumeMulOp(sc)
		if !ok {
			return left, true
		}
		right, ok := parseGenUnary(sc, env)
		if !ok {
			return genVal{}, false
		}
		li, ok1 := left.asInt()
		ri, ok2 := right.asInt()
		if !ok1 || !ok2 {
			return genVal{}, false
		}
		next, ok := applyMulOp(li, ri, op)
		if !ok {
			return genVal{}, false
		}
		left = genVal{kind: genNum, n: next.n}
		if next.null {
			left = genNullVal()
		}
	}
}

func parseGenUnary(sc *sqlScan, env map[string]genVal) (genVal, bool) {
	sc.skip()
	if sc.i < len(sc.s) && (sc.s[sc.i] == '-' || sc.s[sc.i] == '+') && !sc.startsArrow() {
		op := sc.s[sc.i]
		sc.i++
		v, ok := parseGenUnary(sc, env)
		if !ok || v.kind == genNull || op == '+' {
			return v, ok
		}
		iv, ok := v.asInt()
		if !ok || iv.null {
			return v, ok
		}
		return genVal{kind: genNum, n: new(big.Rat).Neg(iv.n)}, true
	}
	return parseGenPrimary(sc, env)
}

func (sc *sqlScan) startsArrow() bool {
	return sc.i+1 < len(sc.s) && sc.s[sc.i] == '-' && sc.s[sc.i+1] == '>'
}

func parseGenPrimary(sc *sqlScan, env map[string]genVal) (genVal, bool) {
	v, ok := parseGenAtom(sc, env)
	if !ok {
		return genVal{}, false
	}
	for {
		unquote, arrow := sc.consumeArrow()
		if !arrow {
			return v, true
		}
		path, ok := sc.constString()
		if !ok {
			return genVal{}, false
		}
		v, ok = applyJSONArrow(v, path, unquote)
		if !ok {
			return genVal{}, false
		}
	}
}

func (sc *sqlScan) consumeArrow() (unquote, ok bool) {
	sc.skip()
	if !sc.startsArrow() {
		return false, false
	}
	sc.i += 2
	if sc.i < len(sc.s) && sc.s[sc.i] == '>' {
		sc.i++
		return true, true
	}
	return false, true
}

func parseGenAtom(sc *sqlScan, env map[string]genVal) (genVal, bool) {
	sc.skip()
	if sc.i >= len(sc.s) {
		return genVal{}, false
	}
	switch sc.s[sc.i] {
	case '(':
		body, ok := sc.parenBody()
		if !ok {
			return genVal{}, false
		}
		return evalGenExpr(body, env)
	case '\'', '"':
		return sc.quotedText("utf8mb4")
	}
	if sc.s[sc.i] == 'X' || sc.s[sc.i] == 'x' {
		if v, ok, parsed := sc.hexBinary(); parsed {
			return v, ok
		}
	}
	if sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9' {
		return sc.numberAtom()
	}
	quoted := sc.s[sc.i] == '`'
	name, ok := sc.ident()
	if !ok {
		return genVal{}, false
	}
	if !quoted && strings.HasPrefix(name, "_") {
		if v, ok, parsed := sc.introducedLiteral(name); parsed {
			return v, ok
		}
	}
	if sc.peekByte() == '(' {
		return parseGenCall(sc, name, env)
	}
	sc.skipCollate()
	if !quoted && isNullWord(name) {
		return genNullVal(), true
	}
	if !quoted && isBoolWord(name) {
		if strings.EqualFold(name, "TRUE") {
			return genNumVal("1")
		}
		return genNumVal("0")
	}
	v, ok := env[strings.ToLower(name)]
	if !ok || v.kind == genBad {
		return genVal{}, false
	}
	return v, true
}

func parseGenCall(sc *sqlScan, name string, env map[string]genVal) (genVal, bool) {
	body, ok := sc.parenBody()
	if !ok {
		return genVal{}, false
	}
	var args []genVal
	if strings.TrimSpace(body) != "" {
		for _, part := range splitComma(body) {
			v, ok := evalGenExpr(part, env)
			if !ok {
				return genVal{}, false
			}
			args = append(args, v)
		}
	}
	return evalGenCall(name, args)
}

func evalGenCall(name string, args []genVal) (genVal, bool) {
	switch strings.ToUpper(name) {
	case "UPPER":
		return evalCaseFunc(args, true)
	case "LOWER":
		return evalCaseFunc(args, false)
	case "CONCAT":
		return evalConcat(args)
	case "CONCAT_WS":
		return evalConcatWS(args)
	case "LENGTH", "OCTET_LENGTH":
		return evalLength(args, false)
	case "CHAR_LENGTH", "CHARACTER_LENGTH":
		return evalLength(args, true)
	case "JSON_EXTRACT":
		return evalJSONExtract(args)
	case "JSON_UNQUOTE":
		return evalJSONUnquote(args)
	case "MOD":
		if len(args) != 2 {
			return genVal{}, false
		}
		li, ok1 := args[0].asInt()
		ri, ok2 := args[1].asInt()
		if !ok1 || !ok2 {
			return genVal{}, false
		}
		next, ok := applyIntDiv(li, ri, true)
		if !ok {
			return genVal{}, false
		}
		if next.null {
			return genNullVal(), true
		}
		return genVal{kind: genNum, n: next.n}, true
	default:
		return genVal{}, false
	}
}

func evalCaseFunc(args []genVal, upper bool) (genVal, bool) {
	if len(args) != 1 {
		return genVal{}, false
	}
	v := args[0]
	if v.kind == genNull {
		return genNullVal(), true
	}
	src, ok := v.coerceText()
	if !ok {
		return genVal{}, false
	}
	if src.kind == genNull {
		return genNullVal(), true
	}
	if src.charset == "binary" {
		return src, true
	}
	folded, ok := foldText(src.text, src.charset, upper)
	if !ok {
		return genVal{}, false
	}
	return genTextVal(folded, src.charset), true
}

func evalConcat(args []genVal) (genVal, bool) {
	if len(args) == 0 {
		return genVal{}, false
	}
	var b strings.Builder
	charset := "utf8mb4"
	for _, arg := range args {
		if arg.kind == genNull {
			return genNullVal(), true
		}
		piece, ok := arg.coerceText()
		if !ok || piece.kind == genNull {
			return genVal{}, false
		}
		if piece.charset == "binary" || !singleCharset(charset, piece.charset) {
			return genVal{}, false
		}
		if piece.charset != "ascii" && piece.charset != "" {
			charset = piece.charset
		}
		b.WriteString(piece.text)
	}
	if charset == "" {
		charset = "utf8mb4"
	}
	return genTextVal(b.String(), charset), true
}

func singleCharset(have, next string) bool {
	if next == "" || next == "ascii" || next == "utf8mb4" || next == "utf8" || next == "latin1" {
		if have == "" || have == "ascii" || have == "utf8mb4" || have == "utf8" || have == "latin1" {
			// latin1 and utf8mb4 both decode to Unicode here, so concatenation is the characters.
			return true
		}
	}
	return have == next
}

func evalConcatWS(args []genVal) (genVal, bool) {
	if len(args) < 1 {
		return genVal{}, false
	}
	if args[0].kind == genNull {
		return genNullVal(), true
	}
	sep, ok := args[0].coerceText()
	if !ok || sep.kind == genNull {
		return genVal{}, false
	}
	var b strings.Builder
	n := 0
	for _, arg := range args[1:] {
		if arg.kind == genNull {
			continue
		}
		piece, ok := arg.coerceText()
		if !ok || piece.kind == genNull {
			return genVal{}, false
		}
		if piece.charset == "binary" {
			return genVal{}, false
		}
		if n > 0 {
			b.WriteString(sep.text)
		}
		b.WriteString(piece.text)
		n++
	}
	return genTextVal(b.String(), "utf8mb4"), true
}

func evalLength(args []genVal, chars bool) (genVal, bool) {
	if len(args) != 1 {
		return genVal{}, false
	}
	v := args[0]
	if v.kind == genNull {
		return genNullVal(), true
	}
	// BINARY(n) is stored padded with 0x00. The binlog image drops that pad,
	// and both LENGTH and CHAR_LENGTH count the declared width. No width is BINARY(1).
	if v.base == "binary" {
		width := 1
		if v.hasPrec && v.prec > 0 {
			width = v.prec
		}
		return genNumVal(strconv.Itoa(width))
	}
	// LENGTH is the byte count of the stored value in any charset.
	if !chars && v.kind == genText && v.haveRaw && v.decoded {
		return genNumVal(strconv.Itoa(len(v.raw)))
	}
	src, ok := v.coerceText()
	if !ok || src.kind == genNull {
		return genVal{}, false
	}
	var n int
	switch src.charset {
	case "utf8mb4", "utf8", "ascii", "":
		if chars {
			n = utf8.RuneCountInString(src.text)
		} else {
			n = len(src.text)
		}
	case "latin1", "binary":
		if !src.haveRaw {
			return genVal{}, false
		}
		n = len(src.raw)
	default:
		return genVal{}, false
	}
	return genNumVal(strconv.Itoa(n))
}

func evalJSONExtract(args []genVal) (genVal, bool) {
	if len(args) != 2 {
		return genVal{}, false
	}
	if args[0].kind == genNull || args[1].kind == genNull {
		return genNullVal(), true
	}
	pathText, ok := args[1].pathText()
	if !ok {
		return genVal{}, false
	}
	return jsonAtPath(args[0], pathText, false)
}

func evalJSONUnquote(args []genVal) (genVal, bool) {
	if len(args) != 1 {
		return genVal{}, false
	}
	return jsonUnquote(args[0])
}

func applyJSONArrow(v genVal, path string, unquote bool) (genVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	got, ok := jsonAtPath(v, path, unquote)
	if !ok {
		return genVal{}, false
	}
	if unquote {
		return jsonUnquote(got)
	}
	return got, true
}

func jsonAtPath(v genVal, path string, _ bool) (genVal, bool) {
	if v.kind != genJSON || v.j == nil {
		return genVal{}, false
	}
	steps, ok := parseJSONPath(path)
	if !ok {
		return genVal{}, false
	}
	node, found := v.j.walk(steps)
	if !found {
		return genNullVal(), true
	}
	return genVal{kind: genJSON, j: node}, true
}

func jsonUnquote(v genVal) (genVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	if v.kind != genJSON || v.j == nil {
		return genVal{}, false
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
				return genVal{}, false
			}
			return genTextVal(mysqlJSONFloatString(f), "utf8mb4"), true
		default:
			// Decimal text keeps trailing zeros. An integer has no ".0".
			return genTextVal(v.j.num, "utf8mb4"), true
		}
	default:
		// Object and array text depends on spacing MySQL chooses. Do not guess.
		return genVal{}, false
	}
}

func (v genVal) pathText() (string, bool) {
	if v.kind != genText {
		return "", false
	}
	return v.text, true
}

func (v genVal) coerceText() (genVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	switch v.base {
	case "enum":
		return v.enumAsText()
	case "set":
		return v.setAsText()
	case "decimal":
		return v.decimalAsText()
	case "float", "double", "timestamp":
		return genVal{}, false
	case "time", "datetime":
		return v.temporalAsText()
	}
	switch v.kind {
	case genNull:
		return genNullVal(), true
	case genText:
		if !v.decoded {
			return genVal{}, false
		}
		switch v.charset {
		case "binary", "utf8mb4", "utf8", "ascii", "latin1", "":
			return v, true
		default:
			return genVal{}, false
		}
	case genNum:
		if v.n == nil || !v.n.IsInt() {
			return genVal{}, false
		}
		return genTextVal(v.n.Num().String(), "ascii"), true
	default:
		return genVal{}, false
	}
}

// foldText applies MySQL case conversion for the charsets this checker models.
// latin1 uses the charset's 1:1 map, so µ (0xB5) stays µ. utf8mb4 uses that map
// for Latin-1 letters and maps µ (U+00B5) to Μ (U+039C). ß and ÿ, and every
// other code point above Latin-1, stay unverified.
// binary is unchanged, which is MySQL's rule for the binary charset.
func foldText(text, charset string, upper bool) (string, bool) {
	switch charset {
	case "binary":
		return text, true
	case "latin1":
		for _, r := range text {
			if r > 0xFF {
				return "", false
			}
		}
		var b strings.Builder
		for _, r := range text {
			b.WriteRune(rune(latin1Case(byte(r), upper)))
		}
		return b.String(), true
	case "utf8mb4", "utf8", "ascii", "":
		var b strings.Builder
		for _, r := range text {
			mapped, ok := utf8Case(r, upper)
			if !ok {
				return "", false
			}
			b.WriteRune(mapped)
		}
		return b.String(), true
	default:
		return "", false
	}
}

func utf8Case(r rune, upper bool) (rune, bool) {
	if upper && r == 0x00B5 {
		return 0x039C, true
	}
	if r == 0xDF || r == 0xFF || r > 0xFF {
		return 0, false
	}
	return rune(latin1Case(byte(r), upper)), true
}

// latin1Case is MySQL's latin1 to_upper / to_lower map.
// ß (0xDF) and ÿ (0xFF) stay themselves. × and ÷ stay themselves.
func latin1Case(b byte, upper bool) byte {
	if upper {
		switch {
		case b >= 'a' && b <= 'z':
			return b - 32
		case b >= 0xE0 && b <= 0xF6:
			return b - 0x20
		case b >= 0xF8 && b <= 0xFE:
			return b - 0x20
		default:
			return b
		}
	}
	switch {
	case b >= 'A' && b <= 'Z':
		return b + 32
	case b >= 0xC0 && b <= 0xD6:
		return b + 0x20
	case b >= 0xD8 && b <= 0xDE:
		return b + 0x20
	default:
		return b
	}
}

func (sc *sqlScan) numberAtom() (genVal, bool) {
	start := sc.i
	for sc.i < len(sc.s) && sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9' {
		sc.i++
	}
	if sc.i < len(sc.s) && (sc.s[sc.i] == '.' || sc.s[sc.i] == 'e' || sc.s[sc.i] == 'E') {
		return genVal{}, false
	}
	return genNumVal(sc.s[start:sc.i])
}

func (sc *sqlScan) quotedText(charset string) (genVal, bool) {
	text, ok := sc.decodeQuoted()
	if !ok {
		return genVal{}, false
	}
	sc.skipCollate()
	switch charset {
	case "latin1":
		raw := make([]byte, 0, len(text))
		for _, r := range text {
			if r > 0xFF {
				return genVal{}, false
			}
			raw = append(raw, byte(r))
		}
		return genVal{kind: genText, text: text, raw: raw, haveRaw: true, charset: "latin1", decoded: true}, true
	case "binary":
		return genVal{kind: genText, text: text, raw: []byte(text), haveRaw: true, charset: "binary", decoded: true}, true
	case "utf8mb4", "utf8", "ascii", "":
		return genTextVal(text, "utf8mb4"), true
	default:
		return genVal{kind: genText, charset: charset}, true
	}
}

func (sc *sqlScan) decodeQuoted() (string, bool) {
	if sc.i >= len(sc.s) {
		return "", false
	}
	q := sc.s[sc.i]
	if q != '\'' && q != '"' {
		return "", false
	}
	sc.i++
	var b strings.Builder
	for sc.i < len(sc.s) {
		c := sc.s[sc.i]
		if c == q {
			if sc.i+1 < len(sc.s) && sc.s[sc.i+1] == q {
				b.WriteByte(q)
				sc.i += 2
				continue
			}
			sc.i++
			return b.String(), true
		}
		if c == '\\' && sc.i+1 < len(sc.s) {
			sc.i++
			b.WriteByte(decodeSQLEscape(sc.s[sc.i]))
			sc.i++
			continue
		}
		b.WriteByte(c)
		sc.i++
	}
	return "", false
}

func decodeSQLEscape(c byte) byte {
	switch c {
	case '0':
		return 0
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 't':
		return '\t'
	case 'b':
		return '\b'
	case 'Z':
		return 0x1a
	default:
		return c
	}
}

func (sc *sqlScan) constString() (string, bool) {
	v, ok := parseGenAtom(sc, nil)
	if !ok || v.kind != genText {
		return "", false
	}
	return v.text, true
}

func (sc *sqlScan) introducedLiteral(name string) (genVal, bool, bool) {
	charset := strings.ToLower(strings.TrimPrefix(name, "_"))
	save := *sc
	sc.skip()
	if sc.i >= len(sc.s) {
		*sc = save
		return genVal{}, false, false
	}
	if sc.s[sc.i] == '\'' || sc.s[sc.i] == '"' {
		v, ok := sc.quotedText(normalizeIntroCharset(charset))
		return v, ok, true
	}
	if v, ok, parsed := sc.hexBinary(); parsed {
		if !ok {
			return genVal{}, false, true
		}
		return tagCharset(v, normalizeIntroCharset(charset)), true, true
	}
	*sc = save
	return genVal{}, false, false
}

func normalizeIntroCharset(charset string) string {
	switch charset {
	case "utf8", "utf8mb3", "utf8mb4":
		return "utf8mb4"
	case "latin1":
		return "latin1"
	case "binary":
		return "binary"
	case "ascii":
		return "ascii"
	default:
		return charset
	}
}

func tagCharset(v genVal, charset string) genVal {
	v.decoded = false
	v.text = ""
	v.charset = charset
	switch charset {
	case "latin1":
		if !v.haveRaw {
			return v
		}
		v.charset = "latin1"
		v.text = latin1ToUTF8(v.raw)
		v.decoded = true
		return v
	case "utf8mb4", "utf8":
		if v.haveRaw && utf8.Valid(v.raw) {
			v.charset = "utf8mb4"
			v.text = string(v.raw)
			v.decoded = true
		}
		return v
	case "ascii":
		if v.haveRaw && asciiBytes(v.raw) {
			v.charset = "ascii"
			v.text = string(v.raw)
			v.decoded = true
		}
		return v
	case "binary":
		v.charset = "binary"
		v.text = string(v.raw)
		v.decoded = true
		return v
	default:
		return v
	}
}

func asciiBytes(raw []byte) bool {
	for _, b := range raw {
		if b >= 0x80 {
			return false
		}
	}
	return true
}

func latin1ToUTF8(raw []byte) string {
	var b strings.Builder
	for _, c := range raw {
		b.WriteRune(rune(c))
	}
	return b.String()
}

func (sc *sqlScan) hexBinary() (genVal, bool, bool) {
	sc.skip()
	if sc.i >= len(sc.s) {
		return genVal{}, false, false
	}
	if sc.s[sc.i] == '0' && sc.i+1 < len(sc.s) && (sc.s[sc.i+1] == 'x' || sc.s[sc.i+1] == 'X') {
		sc.i += 2
		start := sc.i
		for sc.i < len(sc.s) && isHex(sc.s[sc.i]) {
			sc.i++
		}
		raw, ok := decodeHex(sc.s[start:sc.i])
		if !ok {
			return genVal{}, false, true
		}
		return genVal{kind: genText, raw: raw, haveRaw: true, charset: "binary", text: string(raw), decoded: true}, true, true
	}
	if (sc.s[sc.i] == 'X' || sc.s[sc.i] == 'x') && sc.i+1 < len(sc.s) && sc.s[sc.i+1] == '\'' {
		sc.i += 2
		start := sc.i
		for sc.i < len(sc.s) && sc.s[sc.i] != '\'' {
			sc.i++
		}
		if sc.i >= len(sc.s) {
			return genVal{}, false, true
		}
		raw, ok := decodeHex(sc.s[start:sc.i])
		sc.i++
		if !ok {
			return genVal{}, false, true
		}
		return genVal{kind: genText, raw: raw, haveRaw: true, charset: "binary", text: string(raw), decoded: true}, true, true
	}
	return genVal{}, false, false
}

func isHex(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

func decodeHex(s string) ([]byte, bool) {
	if len(s)%2 != 0 {
		return nil, false
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(out); i++ {
		hi, ok1 := fromHex(s[i*2])
		lo, ok2 := fromHex(s[i*2+1])
		if !ok1 || !ok2 {
			return nil, false
		}
		out[i] = hi<<4 | lo
	}
	return out, true
}

func fromHex(b byte) (byte, bool) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', true
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, true
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, true
	default:
		return 0, false
	}
}

func (sc *sqlScan) skipCollate() {
	if sc.wordIs("COLLATE") {
		_, _ = sc.ident()
	}
}

func isNullWord(name string) bool { return strings.EqualFold(name, "NULL") }

func isBoolWord(name string) bool {
	return strings.EqualFold(name, "TRUE") || strings.EqualFold(name, "FALSE")
}

// bareSyntaxWord is a keyword that is not a column even when a column has that name.
// `DIV` in `a DIV 2` is the operator. A column of that name is written in backticks.
func bareSyntaxWord(name string) bool {
	switch strings.ToUpper(name) {
	case "AND", "OR", "XOR", "NOT", "DIV", "MOD", "IS", "IN", "LIKE", "RLIKE", "REGEXP",
		"BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END", "AS", "COLLATE", "USING",
		"FROM", "INTERVAL", "SEPARATOR":
		return true
	default:
		return false
	}
}

// generatedRefs lists column names the expression reads.
// ok is false when the expression text is not well-formed.
// A bare syntax keyword is not a column. Any other identifier that names a column is.
func generatedRefs(expr string, columns []string) (refs []string, ok bool) {
	known := map[string]struct{}{}
	for _, name := range columns {
		known[strings.ToLower(name)] = struct{}{}
	}
	sc := &sqlScan{s: expr}
	seen := map[string]struct{}{}
	for sc.i < len(sc.s) {
		sc.skip()
		if sc.i >= len(sc.s) {
			break
		}
		switch sc.s[sc.i] {
		case '\'', '"':
			if _, ok := sc.decodeQuoted(); !ok {
				return nil, false
			}
		default:
			if sc.s[sc.i] == '0' && sc.i+1 < len(sc.s) && (sc.s[sc.i+1] == 'x' || sc.s[sc.i+1] == 'X') {
				if _, _, parsed := sc.hexBinary(); !parsed {
					sc.i++
				}
				continue
			}
			if sc.s[sc.i] == 'X' || sc.s[sc.i] == 'x' {
				if _, _, parsed := sc.hexBinary(); parsed {
					continue
				}
			}
			if sc.s[sc.i] != '`' && !identStart(sc.s[sc.i]) {
				sc.i++
				continue
			}
			quoted := sc.s[sc.i] == '`'
			name, ok := sc.ident()
			if !ok {
				return nil, false
			}
			if !quoted && strings.HasPrefix(name, "_") {
				if _, _, parsed := sc.introducedLiteral(name); parsed {
					continue
				}
			}
			if sc.peekByte() == '(' {
				continue
			}
			if !quoted && bareSyntaxWord(name) {
				continue
			}
			key := strings.ToLower(name)
			if _, isCol := known[key]; !isCol {
				continue
			}
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			refs = append(refs, key)
		}
	}
	return refs, true
}

func parseLoggedValue(lit string) (genVal, bool) {
	s := strings.TrimSpace(lit)
	switch {
	case s == "":
		return genVal{}, false
	case strings.EqualFold(s, "NULL"):
		return genNullVal(), true
	case s[0] == '\'' || s[0] == '"':
		sc := &sqlScan{s: s}
		v, ok := sc.quotedText("utf8mb4")
		if !ok {
			return genVal{}, false
		}
		sc.skip()
		if sc.i != len(sc.s) {
			return genVal{}, false
		}
		return v, true
	case s[0] == '_' || s[0] == 'X' || s[0] == 'x' || strings.HasPrefix(strings.ToLower(s), "0x"):
		sc := &sqlScan{s: s}
		v, ok := parseGenAtom(sc, nil)
		if !ok {
			return genVal{}, false
		}
		sc.skip()
		if sc.i != len(sc.s) {
			return genVal{}, false
		}
		return v, true
	case strings.HasPrefix(strings.ToUpper(s), "JSON_") || strings.HasPrefix(strings.ToUpper(s), "CAST(") || strings.HasPrefix(strings.ToUpper(s), "CAST "):
		return parseJSONLogged(s)
	default:
		if v, ok := genNumVal(s); ok {
			return v, true
		}
		return genVal{}, false
	}
}

func loggedEnv(columns, values []string, meta ...[]schemaCol) map[string]genVal {
	var cols []schemaCol
	if len(meta) > 0 {
		cols = meta[0]
	}
	byName := make(map[string]schemaCol, len(cols))
	for _, col := range cols {
		byName[strings.ToLower(col.name)] = col
	}
	env := make(map[string]genVal, len(columns))
	for i, name := range columns {
		lit := "NULL"
		if i < len(values) {
			lit = values[i]
		}
		v, ok := parseLoggedValue(lit)
		if !ok {
			env[strings.ToLower(name)] = genVal{kind: genBad}
			continue
		}
		if col, found := byName[strings.ToLower(name)]; found {
			v = attachColMeta(v, col)
		}
		env[strings.ToLower(name)] = v
	}
	return env
}

func attachColMeta(v genVal, col schemaCol) genVal {
	v.base = col.base
	if len(col.members) > 0 {
		v.members = append([]string(nil), col.members...)
	}
	v.prec = col.prec
	v.scale = col.scale
	v.hasPrec = col.hasPrec
	if v.charset == "" && col.charset != "" {
		v.charset = col.charset
	}
	return v
}

// matchGenerated reports whether got is the logged literal after assignment to col.
// confident is false when the checker will not claim a match or a mismatch.
func matchGenerated(got genVal, lit string, col schemaCol) (match, confident bool) {
	logged, ok := parseLoggedValue(lit)
	if !ok {
		return false, false
	}
	if got.kind == genNull || logged.kind == genNull {
		return got.kind == genNull && logged.kind == genNull, true
	}
	if binaryAssign(col.base) && got.haveRaw && logged.haveRaw {
		return bytes.Equal(got.raw, logged.raw), true
	}
	if integerAssign(col.base) && (got.kind == genNum || logged.kind == genNum) {
		iv, ok := got.asInt()
		if !ok {
			text, tok := got.coerceText()
			if !tok {
				return false, false
			}
			iv, ok = textToInt(text)
			if !ok {
				return false, false
			}
		}
		return intValMatches(iv, literalNumber(lit)), true
	}
	if col.base == "json" && got.kind == genJSON && logged.kind == genJSON {
		return jsonEqual(got.j, logged.j), true
	}
	if stringAssign(col.base) {
		left, ok1 := assignText(got, col)
		right, ok2 := assignText(logged, col)
		if !ok1 || !ok2 {
			return false, false
		}
		return left == right, true
	}
	if got.kind == logged.kind && got.kind == genText {
		return got.text == logged.text, true
	}
	if got.kind == genJSON && logged.kind == genText {
		text, ok := jsonUnquote(got)
		if !ok || text.kind != genText {
			return false, false
		}
		return text.text == logged.text, true
	}
	return false, false
}

func literalNumber(lit string) string {
	return strings.TrimSpace(lit)
}

func textToInt(v genVal) (intVal, bool) {
	if v.kind != genText {
		return intVal{}, false
	}
	i, ok := new(big.Int).SetString(v.text, 10)
	if !ok {
		return intVal{}, false
	}
	return intVal{n: new(big.Rat).SetInt(i)}, true
}

func binaryAssign(base string) bool {
	switch base {
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return true
	default:
		return false
	}
}

func integerAssign(base string) bool {
	switch base {
	case "tinyint", "smallint", "mediumint", "int", "bigint", "year":
		return true
	default:
		return false
	}
}

func stringAssign(base string) bool {
	switch base {
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		return true
	default:
		return false
	}
}

func assignText(v genVal, col schemaCol) (string, bool) {
	src, ok := v.coerceText()
	if !ok || src.kind == genNull {
		return "", false
	}
	text := src.text
	if (col.base == "char" || col.base == "varchar") && col.hasPrec && utf8.RuneCountInString(text) > col.prec {
		// Non-strict sessions truncate. This checker does not know sql_mode, so a longer result is unverifiable.
		return "", false
	}
	if col.base == "char" && col.hasPrec {
		text = padChar(text, col.prec)
	}
	return text, true
}

func padChar(text string, width int) string {
	n := utf8.RuneCountInString(text)
	if n >= width {
		return text
	}
	return text + strings.Repeat(" ", width-n)
}

func (n *jNode) walk(steps []jStep) (*jNode, bool) {
	cur := n
	for _, step := range steps {
		if cur == nil {
			return nil, false
		}
		if step.isIdx {
			if cur.kind != jArr || step.index < 0 || step.index >= len(cur.arr) {
				return nil, false
			}
			cur = cur.arr[step.index]
			continue
		}
		if cur.kind != jObj {
			return nil, false
		}
		found := false
		for _, pair := range cur.obj {
			if pair.k == step.key {
				cur = pair.v
				found = true
				break
			}
		}
		if !found {
			return nil, false
		}
	}
	return cur, true
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

func parseJSONLogged(lit string) (genVal, bool) {
	node, ok := parseJSONSQLString(lit)
	if !ok {
		return genVal{}, false
	}
	return genVal{kind: genJSON, j: node}, true
}

func parseJSONSQLString(s string) (*jNode, bool) {
	sc := &sqlScan{s: s}
	node, ok := parseJSONNode(sc)
	if !ok {
		return nil, false
	}
	sc.skip()
	if sc.i != len(sc.s) {
		return nil, false
	}
	return node, true
}

func parseJSONNode(sc *sqlScan) (*jNode, bool) {
	sc.skip()
	if sc.i >= len(sc.s) {
		return nil, false
	}
	if sc.wordIs("JSON_OBJECT") {
		body, ok := sc.parenBody()
		if !ok {
			return nil, false
		}
		node := &jNode{kind: jObj}
		if strings.TrimSpace(body) == "" {
			return node, true
		}
		parts := splitComma(body)
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
	if sc.wordIs("JSON_ARRAY") {
		body, ok := sc.parenBody()
		if !ok {
			return nil, false
		}
		node := &jNode{kind: jArr}
		if strings.TrimSpace(body) == "" {
			return node, true
		}
		for _, part := range splitComma(body) {
			val, ok := parseJSONSQLString(part)
			if !ok {
				return nil, false
			}
			node.arr = append(node.arr, val)
		}
		return node, true
	}
	if sc.wordIs("CAST") {
		return parseJSONCast(sc)
	}
	if sc.wordIs("NULL") {
		return &jNode{kind: jNull}, true
	}
	if sc.wordIs("TRUE") {
		return &jNode{kind: jBool, b: true}, true
	}
	if sc.wordIs("FALSE") {
		return &jNode{kind: jBool, b: false}, true
	}
	if sc.s[sc.i] == '\'' || sc.s[sc.i] == '"' {
		text, ok := sc.decodeQuoted()
		if !ok {
			return nil, false
		}
		return &jNode{kind: jStr, str: text}, true
	}
	if sc.s[sc.i] == '-' || (sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9') {
		start := sc.i
		if sc.s[sc.i] == '-' {
			sc.i++
		}
		for sc.i < len(sc.s) && (isNumChar(sc.s[sc.i])) {
			sc.i++
		}
		text := sc.s[start:sc.i]
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

func parseJSONCast(sc *sqlScan) (*jNode, bool) {
	body, ok := sc.parenBody()
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
	sc := &sqlScan{s: body}
	depth := 0
	inString := byte(0)
	for sc.i < len(sc.s) {
		c := sc.s[sc.i]
		if inString != 0 {
			if c == inString {
				if sc.i+1 < len(sc.s) && sc.s[sc.i+1] == inString {
					sc.i += 2
					continue
				}
				inString = 0
			} else if c == '\\' && sc.i+1 < len(sc.s) {
				sc.i += 2
				continue
			}
			sc.i++
			continue
		}
		switch c {
		case '\'', '"', '`':
			inString = c
			sc.i++
		case '(':
			depth++
			sc.i++
		case ')':
			if depth > 0 {
				depth--
			}
			sc.i++
		default:
			if depth == 0 && identStart(c) {
				start := sc.i
				word := sc.bare()
				if strings.EqualFold(word, "AS") {
					return strings.TrimSpace(body[:start]), strings.TrimSpace(sc.s[sc.i:]), true
				}
				continue
			}
			sc.i++
		}
	}
	return "", "", false
}

func sqlConstString(s string) (string, bool) {
	return decodeQuotedSQL(strings.TrimSpace(s))
}

func decodeQuotedSQL(s string) (string, bool) {
	sc := &sqlScan{s: s}
	text, ok := sc.decodeQuoted()
	if !ok {
		return "", false
	}
	sc.skip()
	if sc.i != len(sc.s) {
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

func exampleImage(im loggedImage) string {
	parts := make([]string, 0, len(im.columns))
	for i, name := range im.columns {
		lit := "NULL"
		if i < len(im.values) {
			lit = im.values[i]
		}
		parts = append(parts, name+"="+exampleLiteral(lit))
	}
	return oneLine(strings.Join(parts, ", "), 180)
}

// exampleLiteral shows a logged SQL literal in a contradiction error.
// A text-charset introducer whose bytes are printable text becomes a quoted
// string. Binary bytes and any non-printable value stay in the original form.
func exampleLiteral(lit string) string {
	charset, raw, ok := introducerHex(strings.TrimSpace(lit))
	if !ok || charset == "binary" {
		return lit
	}
	text, ok := decodeTextCharset(charset, raw)
	if !ok || !printableText(text) {
		return lit
	}
	return quoteExampleText(text)
}

func introducerHex(lit string) (charset string, raw []byte, ok bool) {
	if len(lit) < 2 || lit[0] != '_' {
		return "", nil, false
	}
	i := 1
	for i < len(lit) {
		c := lit[i]
		if c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			i++
			continue
		}
		break
	}
	if i == 1 {
		return "", nil, false
	}
	raw, ok = hexIntroducerBody(strings.TrimSpace(lit[i:]))
	if !ok {
		return "", nil, false
	}
	return strings.ToLower(lit[1:i]), raw, true
}

func hexIntroducerBody(s string) ([]byte, bool) {
	if strings.HasPrefix(strings.ToLower(s), "0x") {
		return decodeHex(s[2:])
	}
	if len(s) >= 2 && (s[0] == 'X' || s[0] == 'x') && s[1] == '\'' && strings.HasSuffix(s, "'") {
		return decodeHex(s[2 : len(s)-1])
	}
	return nil, false
}

func decodeTextCharset(charset string, raw []byte) (string, bool) {
	switch charset {
	case "ascii":
		if !asciiBytes(raw) {
			return "", false
		}
		return string(raw), true
	case "utf8", "utf8mb3", "utf8mb4":
		if !utf8.Valid(raw) {
			return "", false
		}
		return string(raw), true
	case "ucs2":
		return decodeUCS2(raw)
	case "utf32":
		return decodeUTF32BE(raw)
	}
	if enc, ok := textCharsetEncoding(charset); ok {
		return decodeEncoding(enc, raw)
	}
	return asciiSupersetText(charset, raw)
}

func textCharsetEncoding(charset string) (encoding.Encoding, bool) {
	switch charset {
	case "latin1":
		return charmap.Windows1252, true
	case "latin2":
		return charmap.ISO8859_2, true
	case "latin5":
		return charmap.ISO8859_9, true
	case "latin7":
		return charmap.ISO8859_13, true
	case "greek":
		return charmap.ISO8859_7, true
	case "hebrew":
		return charmap.ISO8859_8, true
	case "cp850":
		return charmap.CodePage850, true
	case "cp852":
		return charmap.CodePage852, true
	case "cp866":
		return charmap.CodePage866, true
	case "cp1250":
		return charmap.Windows1250, true
	case "cp1251":
		return charmap.Windows1251, true
	case "cp1256":
		return charmap.Windows1256, true
	case "cp1257":
		return charmap.Windows1257, true
	case "koi8r":
		return charmap.KOI8R, true
	case "koi8u":
		return charmap.KOI8U, true
	case "macroman":
		return charmap.Macintosh, true
	case "gbk", "gb2312":
		return simplifiedchinese.GBK, true
	case "gb18030":
		return simplifiedchinese.GB18030, true
	case "big5":
		return traditionalchinese.Big5, true
	case "sjis", "cp932":
		return japanese.ShiftJIS, true
	case "ujis", "eucjpms":
		return japanese.EUCJP, true
	case "euckr":
		return korean.EUCKR, true
	case "utf16":
		return xunicode.UTF16(xunicode.BigEndian, xunicode.IgnoreBOM), true
	case "utf16le":
		return xunicode.UTF16(xunicode.LittleEndian, xunicode.IgnoreBOM), true
	default:
		return nil, false
	}
}

func decodeEncoding(enc encoding.Encoding, raw []byte) (string, bool) {
	out, err := enc.NewDecoder().Bytes(raw)
	if err != nil || bytes.ContainsRune(out, unicode.ReplacementChar) {
		return "", false
	}
	return string(out), true
}

// asciiSupersetText covers a text charset that has no exact decoder here.
// Letters, digits, and the punctuation those charsets do not remap are the
// same character. swe7, dec8, and hp8 remap ASCII punctuation, so they stay hex.
func asciiSupersetText(charset string, raw []byte) (string, bool) {
	switch charset {
	case "swe7", "dec8", "hp8":
		return "", false
	}
	for _, b := range raw {
		if b < 0x20 || b > 0x7E {
			return "", false
		}
	}
	return string(raw), true
}

func decodeUCS2(raw []byte) (string, bool) {
	if len(raw)%2 != 0 {
		return "", false
	}
	var b strings.Builder
	for i := 0; i < len(raw); i += 2 {
		r := rune(uint16(raw[i])<<8 | uint16(raw[i+1]))
		if r >= 0xD800 && r <= 0xDFFF {
			return "", false
		}
		b.WriteRune(r)
	}
	return b.String(), true
}

func decodeUTF32BE(raw []byte) (string, bool) {
	if len(raw)%4 != 0 {
		return "", false
	}
	var b strings.Builder
	for i := 0; i < len(raw); i += 4 {
		r := rune(uint32(raw[i])<<24 | uint32(raw[i+1])<<16 | uint32(raw[i+2])<<8 | uint32(raw[i+3]))
		if !utf8.ValidRune(r) {
			return "", false
		}
		b.WriteRune(r)
	}
	return b.String(), true
}

func printableText(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func quoteExampleText(s string) string {
	var b strings.Builder
	b.WriteByte('\'')
	for _, r := range s {
		if r == '\\' || r == '\'' {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('\'')
	return b.String()
}

func columnLit(im loggedImage, name string) (string, bool) {
	for i, col := range im.columns {
		if strings.EqualFold(col, name) && i < len(im.values) {
			return im.values[i], true
		}
	}
	return "", false
}

func refKey(im loggedImage, refs []string) (string, bool) {
	var b strings.Builder
	for _, name := range refs {
		lit, ok := columnLit(im, name)
		if !ok {
			return "", false
		}
		fmt.Fprintf(&b, "%d:%s\x00", len(lit), lit)
	}
	return b.String(), true
}

// generatedColumnOutcome checks one generated column against every logged image.
// mismatch is set when a value contradicts the expression. unverified is set when
// nothing contradicted it and at least one image could not be evaluated.
func generatedColumnOutcome(col schemaCol, rows []loggedImage, meta []schemaCol) (example string, unverified bool) {
	if strings.TrimSpace(col.expr) == "" || len(rows) == 0 {
		return "", true
	}
	var images []loggedImage
	names := map[string]struct{}{}
	var nameList []string
	for _, row := range rows {
		images = append(images, row)
		for _, name := range row.columns {
			key := strings.ToLower(name)
			if _, ok := names[key]; ok {
				continue
			}
			names[key] = struct{}{}
			nameList = append(nameList, name)
		}
	}
	unknown := false
	for _, im := range images {
		env := loggedEnv(im.columns, im.values, meta)
		got, ok := evalGenExpr(col.expr, env)
		if !ok {
			unknown = true
			continue
		}
		lit, found := columnLit(im, col.name)
		if !found {
			unknown = true
			continue
		}
		match, confident := matchGenerated(got, lit, col)
		if !confident {
			unknown = true
			continue
		}
		if !match {
			return exampleImage(im), false
		}
	}
	if clash := dependencyClash(col, images, nameList); clash != "" {
		return clash, false
	}
	return "", unknown
}

func dependencyClash(col schemaCol, images []loggedImage, names []string) string {
	refs, ok := generatedRefs(col.expr, names)
	if !ok {
		return ""
	}
	type hit struct {
		lit     string
		example string
	}
	seen := map[string]hit{}
	for _, im := range images {
		key, ok := refKey(im, refs)
		if !ok {
			continue
		}
		lit, found := columnLit(im, col.name)
		if !found {
			continue
		}
		if prev, ok := seen[key]; ok {
			if prev.lit != lit {
				return oneLine(prev.example+"; another image with the same inputs has "+col.name+"="+lit, 220)
			}
			continue
		}
		seen[key] = hit{lit: lit, example: exampleImage(im)}
	}
	return ""
}

func generatedMismatch(table string, col schemaCol, example string) error {
	return schemaMismatch(table, []string{i18n.Tf("error.flashbackSchemaGenerated", map[string]any{
		"Column":  col.name,
		"Expr":    exprText(col.expr),
		"Example": example,
	})})
}

func unverifiedGeneratedWarning(table string, col schemaCol) string {
	return i18n.Tf("warning.flashbackSchemaUnchecked", map[string]any{
		"Table":  table,
		"Column": col.name,
		"Expr":   exprText(col.expr),
	})
}

// unverifiedGeneratedComment is the script-header line. It stays English, like the other script comments.
func unverifiedGeneratedComment(table string, col schemaCol) string {
	return "-- WARNING: generated column " + table + "." + col.name + " not verified (" + exprText(col.expr) + "); it is left out of the script. The guard below checks that this column is generated on the target."
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

func (v genVal) enumAsText() (genVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	if !knownTextCharset(v.charset) {
		return genVal{}, false
	}
	idx, ok := v.exactInt()
	if !ok || idx < 0 || idx > int64(len(v.members)) {
		return genVal{}, false
	}
	if idx == 0 {
		return genTextVal("", textCharset(v.charset)), true
	}
	return genTextVal(v.members[idx-1], textCharset(v.charset)), true
}

func (v genVal) setAsText() (genVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	if !knownTextCharset(v.charset) || len(v.members) > 64 {
		return genVal{}, false
	}
	bits, ok := v.exactUint()
	if !ok {
		return genVal{}, false
	}
	if len(v.members) < 64 && bits>>uint(len(v.members)) != 0 {
		return genVal{}, false
	}
	var parts []string
	for i, member := range v.members {
		if bits&(uint64(1)<<uint(i)) != 0 {
			parts = append(parts, member)
		}
	}
	return genTextVal(strings.Join(parts, ","), textCharset(v.charset)), true
}

func (v genVal) decimalAsText() (genVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	if v.kind != genNum || v.n == nil {
		return genVal{}, false
	}
	scale := 0
	if v.hasPrec {
		scale = v.scale
	}
	if text, ok := decimalLiteralScale(v.numText, scale); ok {
		return genTextVal(text, "ascii"), true
	}
	return genTextVal(formatRatScale(v.n, scale), "ascii"), true
}

func decimalLiteralScale(text string, scale int) (string, bool) {
	text = strings.TrimSpace(text)
	if text == "" || strings.ContainsAny(text, "eE") {
		return "", false
	}
	body := text
	if strings.HasPrefix(body, "+") || strings.HasPrefix(body, "-") {
		body = body[1:]
	}
	intPart, frac, hasDot := strings.Cut(body, ".")
	if intPart == "" || strings.Contains(intPart, ".") {
		return "", false
	}
	for i := 0; i < len(intPart); i++ {
		if intPart[i] < '0' || intPart[i] > '9' {
			return "", false
		}
	}
	if hasDot {
		if len(frac) != scale {
			return "", false
		}
		for i := 0; i < len(frac); i++ {
			if frac[i] < '0' || frac[i] > '9' {
				return "", false
			}
		}
	} else if scale != 0 {
		return "", false
	}
	if strings.HasPrefix(text, "-") && strings.Trim(body, "0.") == "" {
		return strings.TrimPrefix(text, "-"), true
	}
	return text, true
}

func formatRatScale(r *big.Rat, scale int) string {
	if scale < 0 {
		scale = 0
	}
	abs := new(big.Rat).Abs(r)
	s := abs.FloatString(scale)
	if r.Sign() < 0 {
		return "-" + s
	}
	return s
}

func (v genVal) temporalAsText() (genVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	if v.kind != genText || !v.decoded {
		return genVal{}, false
	}
	fsp := 0
	if v.hasPrec {
		fsp = v.prec
	}
	text, ok := padFractional(v.text, fsp)
	if !ok {
		return genVal{}, false
	}
	return genTextVal(text, "ascii"), true
}

func padFractional(text string, fsp int) (string, bool) {
	if fsp < 0 || fsp > 6 {
		return "", false
	}
	base, frac, has := strings.Cut(text, ".")
	if !has {
		frac = ""
	}
	if fsp == 0 {
		if strings.Trim(frac, "0") != "" {
			return "", false
		}
		return base, true
	}
	if len(frac) > fsp {
		if strings.Trim(frac[fsp:], "0") != "" {
			return "", false
		}
		frac = frac[:fsp]
	}
	if len(frac) < fsp {
		frac += strings.Repeat("0", fsp-len(frac))
	}
	return base + "." + frac, true
}

func (v genVal) exactInt() (int64, bool) {
	if v.kind != genNum || v.n == nil || !v.n.IsInt() {
		return 0, false
	}
	num := v.n.Num()
	if !num.IsInt64() {
		return 0, false
	}
	return num.Int64(), true
}

func (v genVal) exactUint() (uint64, bool) {
	if v.kind != genNum || v.n == nil || !v.n.IsInt() || v.n.Sign() < 0 {
		return 0, false
	}
	num := v.n.Num()
	if !num.IsUint64() {
		return 0, false
	}
	return num.Uint64(), true
}

func knownTextCharset(charset string) bool {
	switch charset {
	case "", "utf8", "utf8mb4", "ascii", "latin1", "binary":
		return true
	default:
		return false
	}
}

func textCharset(charset string) string {
	switch charset {
	case "", "utf8", "utf8mb4":
		return "utf8mb4"
	default:
		return charset
	}
}
