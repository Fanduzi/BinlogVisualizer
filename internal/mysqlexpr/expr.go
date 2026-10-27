// Package mysqlexpr evaluates generated-column expressions against logged row images.
// input: the expression text from a schema file, and SQL literals from FULL row images.
// output: a match, a contradiction (with one example image), or unverified when the expression cannot be modelled exactly and the images do not contradict it. The example prints printable text for a text charset and keeps the hex literal for binary or non-printable bytes. ENUM and SET print the schema-file member labels when the file lists them; an index of 0 is an empty label, and a value with no member list stays the raw index or bitmask. UPPER/LOWER cover ascii, latin1's 1:1 map, and utf8mb4 Latin-1 plus µ. NULL propagates, except CONCAT_WS, which skips NULL arguments. Integer and DECIMAL columns share MySQL's division width (9 fractional digits for integer operands at the default div_precision_increment). DECIMAL assignment then rounds half away from zero to the column scale; integer assignment rounds the same way at scale 0. An intermediate product, sum, or quotient is not rounded to fit 30 fractional digits or 65 digits of precision. Past either limit a DECIMAL target is unverified, and an integer target rounds an exact product once. An unknown charset, an unmodelled type, or a value this checker will not claim is unverified, never a match and never a mismatch.
// pos: flashback-only helper. Analyze does not call it.
// note: if this file changes, update this header and module README.md.
package mysqlexpr

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
	"binlogviz/internal/model"
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

// GenVal is a logged literal or an expression result.
// text is Unicode when decoded is set. An undecoded charset is unverifiable.
// raw is the original bytes when known. base and the precision fields are the
// schema column the literal came from, so string functions can use that type.
type GenVal struct {
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

type LoggedImage struct {
	Columns []string
	Values  []string
}

func genNullVal() GenVal { return GenVal{kind: genNull} }

func genNumVal(text string) (GenVal, bool) {
	r, ok := new(big.Rat).SetString(text)
	if !ok {
		return GenVal{}, false
	}
	return GenVal{kind: genNum, n: r, numText: text}, true
}

func genTextVal(text, charset string) GenVal {
	if charset == "" {
		charset = "utf8mb4"
	}
	return GenVal{kind: genText, text: text, raw: []byte(text), haveRaw: true, charset: charset, decoded: true}
}

func (v GenVal) asInt() (intVal, bool) {
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

func applyGenAdd(left, right GenVal, op byte) (GenVal, bool) {
	li, ok1 := left.asInt()
	ri, ok2 := right.asInt()
	if !ok1 || !ok2 {
		return GenVal{}, false
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
	// MySQL rejects a signed BIGINT sum or difference (ERROR 1690). A value
	// past that range is unverified, not a result the checker may claim.
	if !fitsSignedBigint(n) {
		return GenVal{}, false
	}
	return GenVal{kind: genNum, n: n}, true
}

func (sc *Scan) startsArrow() bool {
	return sc.I+1 < len(sc.S) && sc.S[sc.I] == '-' && sc.S[sc.I+1] == '>'
}

func (sc *Scan) consumeArrow() (unquote, ok bool) {
	sc.Skip()
	if !sc.startsArrow() {
		return false, false
	}
	sc.I += 2
	if sc.I < len(sc.S) && sc.S[sc.I] == '>' {
		sc.I++
		return true, true
	}
	return false, true
}

func evalGenCall(name string, args []GenVal) (GenVal, bool) {
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
			return GenVal{}, false
		}
		li, ok1 := args[0].asInt()
		ri, ok2 := args[1].asInt()
		if !ok1 || !ok2 {
			return GenVal{}, false
		}
		next, ok := applyIntDiv(li, ri, true)
		if !ok {
			return GenVal{}, false
		}
		if next.null {
			return genNullVal(), true
		}
		return GenVal{kind: genNum, n: next.n}, true
	default:
		return GenVal{}, false
	}
}

func evalCaseFunc(args []GenVal, upper bool) (GenVal, bool) {
	if len(args) != 1 {
		return GenVal{}, false
	}
	v := args[0]
	if v.kind == genNull {
		return genNullVal(), true
	}
	src, ok := v.coerceText()
	if !ok {
		return GenVal{}, false
	}
	if src.kind == genNull {
		return genNullVal(), true
	}
	if src.charset == "binary" {
		return src, true
	}
	folded, ok := foldText(src.text, src.charset, upper)
	if !ok {
		return GenVal{}, false
	}
	return genTextVal(folded, src.charset), true
}

func evalConcat(args []GenVal) (GenVal, bool) {
	if len(args) == 0 {
		return GenVal{}, false
	}
	var b strings.Builder
	charset := "utf8mb4"
	for _, arg := range args {
		if arg.kind == genNull {
			return genNullVal(), true
		}
		piece, ok := arg.coerceText()
		if !ok || piece.kind == genNull {
			return GenVal{}, false
		}
		if piece.charset == "binary" || !singleCharset(charset, piece.charset) {
			return GenVal{}, false
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

func evalConcatWS(args []GenVal) (GenVal, bool) {
	if len(args) < 1 {
		return GenVal{}, false
	}
	if args[0].kind == genNull {
		return genNullVal(), true
	}
	sep, ok := args[0].coerceText()
	if !ok || sep.kind == genNull {
		return GenVal{}, false
	}
	var b strings.Builder
	n := 0
	for _, arg := range args[1:] {
		if arg.kind == genNull {
			continue
		}
		piece, ok := arg.coerceText()
		if !ok || piece.kind == genNull {
			return GenVal{}, false
		}
		if piece.charset == "binary" {
			return GenVal{}, false
		}
		if n > 0 {
			b.WriteString(sep.text)
		}
		b.WriteString(piece.text)
		n++
	}
	return genTextVal(b.String(), "utf8mb4"), true
}

func evalLength(args []GenVal, chars bool) (GenVal, bool) {
	if len(args) != 1 {
		return GenVal{}, false
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
		return GenVal{}, false
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
			return GenVal{}, false
		}
		n = len(src.raw)
	default:
		return GenVal{}, false
	}
	return genNumVal(strconv.Itoa(n))
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

func (v GenVal) coerceText() (GenVal, bool) {
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
		return GenVal{}, false
	case "time", "datetime":
		return v.temporalAsText()
	}
	switch v.kind {
	case genNull:
		return genNullVal(), true
	case genText:
		if !v.decoded {
			return GenVal{}, false
		}
		switch v.charset {
		case "binary", "utf8mb4", "utf8", "ascii", "latin1", "":
			return v, true
		default:
			return GenVal{}, false
		}
	case genNum:
		if v.n == nil || !v.n.IsInt() {
			return GenVal{}, false
		}
		return genTextVal(v.n.Num().String(), "ascii"), true
	default:
		return GenVal{}, false
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

func (sc *Scan) numberAtom() (GenVal, bool) {
	start := sc.I
	for sc.I < len(sc.S) && sc.S[sc.I] >= '0' && sc.S[sc.I] <= '9' {
		sc.I++
	}
	if sc.I < len(sc.S) && (sc.S[sc.I] == '.' || sc.S[sc.I] == 'e' || sc.S[sc.I] == 'E') {
		return GenVal{}, false
	}
	return genNumVal(sc.S[start:sc.I])
}

func (sc *Scan) quotedText(charset string) (GenVal, bool, bool) {
	text, ok := sc.decodeQuoted()
	if !ok {
		return GenVal{}, false, false
	}
	collated := sc.skipCollate()
	switch charset {
	case "latin1":
		raw := make([]byte, 0, len(text))
		for _, r := range text {
			if r > 0xFF {
				return GenVal{}, false, false
			}
			raw = append(raw, byte(r))
		}
		return GenVal{kind: genText, text: text, raw: raw, haveRaw: true, charset: "latin1", decoded: true}, true, collated
	case "binary":
		return GenVal{kind: genText, text: text, raw: []byte(text), haveRaw: true, charset: "binary", decoded: true}, true, collated
	case "utf8mb4", "utf8", "ascii", "":
		return genTextVal(text, "utf8mb4"), true, collated
	default:
		return GenVal{kind: genText, charset: charset}, true, collated
	}
}

func (sc *Scan) decodeQuoted() (string, bool) {
	if sc.I >= len(sc.S) {
		return "", false
	}
	q := sc.S[sc.I]
	if q != '\'' && q != '"' {
		return "", false
	}
	sc.I++
	var b strings.Builder
	for sc.I < len(sc.S) {
		c := sc.S[sc.I]
		if c == q {
			if sc.I+1 < len(sc.S) && sc.S[sc.I+1] == q {
				b.WriteByte(q)
				sc.I += 2
				continue
			}
			sc.I++
			return b.String(), true
		}
		if c == '\\' && sc.I+1 < len(sc.S) {
			sc.I++
			b.WriteByte(decodeSQLEscape(sc.S[sc.I]))
			sc.I++
			continue
		}
		b.WriteByte(c)
		sc.I++
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

func (sc *Scan) constString() (string, bool) {
	v, ok := sc.evalAtom()
	if !ok || v.kind != genText {
		return "", false
	}
	return v.text, true
}

func (sc *Scan) introducedLiteral(name string) (GenVal, bool, bool, bool) {
	charset := strings.ToLower(strings.TrimPrefix(name, "_"))
	save := *sc
	sc.Skip()
	if sc.I >= len(sc.S) {
		*sc = save
		return GenVal{}, false, false, false
	}
	if sc.S[sc.I] == '\'' || sc.S[sc.I] == '"' {
		v, ok, collated := sc.quotedText(normalizeIntroCharset(charset))
		return v, ok, true, collated
	}
	if v, ok, parsed := sc.hexBinary(); parsed {
		if !ok {
			return GenVal{}, false, true, false
		}
		return tagCharset(v, normalizeIntroCharset(charset)), true, true, sc.skipCollate()
	}
	*sc = save
	return GenVal{}, false, false, false
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

func tagCharset(v GenVal, charset string) GenVal {
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

func (sc *Scan) hexBinary() (GenVal, bool, bool) {
	sc.Skip()
	if sc.I >= len(sc.S) {
		return GenVal{}, false, false
	}
	if sc.S[sc.I] == '0' && sc.I+1 < len(sc.S) && (sc.S[sc.I+1] == 'x' || sc.S[sc.I+1] == 'X') {
		sc.I += 2
		start := sc.I
		for sc.I < len(sc.S) && isHex(sc.S[sc.I]) {
			sc.I++
		}
		raw, ok := decodeHex(sc.S[start:sc.I])
		if !ok {
			return GenVal{}, false, true
		}
		return GenVal{kind: genText, raw: raw, haveRaw: true, charset: "binary", text: string(raw), decoded: true}, true, true
	}
	if (sc.S[sc.I] == 'X' || sc.S[sc.I] == 'x') && sc.I+1 < len(sc.S) && sc.S[sc.I+1] == '\'' {
		sc.I += 2
		start := sc.I
		for sc.I < len(sc.S) && sc.S[sc.I] != '\'' {
			sc.I++
		}
		if sc.I >= len(sc.S) {
			return GenVal{}, false, true
		}
		raw, ok := decodeHex(sc.S[start:sc.I])
		sc.I++
		if !ok {
			return GenVal{}, false, true
		}
		return GenVal{kind: genText, raw: raw, haveRaw: true, charset: "binary", text: string(raw), decoded: true}, true, true
	}
	return GenVal{}, false, false
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

func (sc *Scan) skipCollate() bool {
	if sc.WordIs("COLLATE") {
		_, _ = sc.Ident()
		return true
	}
	return false
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
	sc := &Scan{S: expr}
	seen := map[string]struct{}{}
	for sc.I < len(sc.S) {
		sc.Skip()
		if sc.I >= len(sc.S) {
			break
		}
		switch sc.S[sc.I] {
		case '\'', '"':
			if _, ok := sc.decodeQuoted(); !ok {
				return nil, false
			}
		default:
			if sc.S[sc.I] == '0' && sc.I+1 < len(sc.S) && (sc.S[sc.I+1] == 'x' || sc.S[sc.I+1] == 'X') {
				if _, _, parsed := sc.hexBinary(); !parsed {
					sc.I++
				}
				continue
			}
			if sc.S[sc.I] == 'X' || sc.S[sc.I] == 'x' {
				if _, _, parsed := sc.hexBinary(); parsed {
					continue
				}
			}
			if sc.S[sc.I] != '`' && !identStart(sc.S[sc.I]) {
				sc.I++
				continue
			}
			quoted := sc.S[sc.I] == '`'
			name, ok := sc.Ident()
			if !ok {
				return nil, false
			}
			if !quoted && strings.HasPrefix(name, "_") {
				if _, _, parsed, _ := sc.introducedLiteral(name); parsed {
					continue
				}
			}
			if sc.PeekByte() == '(' {
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
	case s[0] == '_' || s[0] == 'X' || s[0] == 'x' || strings.HasPrefix(strings.ToLower(s), "0x"):
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
	if v.charset == "" && col.charset != "" {
		v.charset = col.charset
	}
	return v
}

// matchGenerated reports whether got is the logged literal after assignment to col.
// confident is false when the checker will not claim a match or a mismatch.
func matchGenerated(got GenVal, lit string, col SchemaCol) (match, confident bool) {
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
		if clip, clipped := integerClip(iv, col); clipped {
			// Non-strict sql_mode stores the type's endpoint. Matching that
			// endpoint would also accept a real column that happens to hold it.
			if intValMatches(intVal{n: new(big.Rat).SetInt(clip)}, literalNumber(lit)) {
				return false, false
			}
			return false, true
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

func textToInt(v GenVal) (intVal, bool) {
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

func assignText(v GenVal, col SchemaCol) (string, bool) {
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

func exampleImage(im LoggedImage, meta []SchemaCol) string {
	parts := make([]string, 0, len(im.Columns))
	for i, name := range im.Columns {
		lit := "NULL"
		if i < len(im.Values) {
			lit = im.Values[i]
		}
		col, found := schemaColByName(meta, name)
		parts = append(parts, name+"="+exampleCell(lit, col, found))
	}
	return OneLine(strings.Join(parts, ", "), 180)
}

func schemaColByName(meta []SchemaCol, name string) (SchemaCol, bool) {
	for _, col := range meta {
		if strings.EqualFold(col.Name, name) {
			return col, true
		}
	}
	return SchemaCol{}, false
}

func exampleCell(lit string, col SchemaCol, found bool) string {
	if found {
		if text, ok := exampleMember(lit, col); ok {
			return text
		}
	}
	return exampleLiteral(lit)
}

// exampleMember prints an ENUM index or SET bitmask as schema-file labels.
// ok is false when the column has no member list or the literal is not one.
func exampleMember(lit string, col SchemaCol) (string, bool) {
	base := strings.ToLower(col.base)
	if (base != "enum" && base != "set") || len(col.members) == 0 {
		return "", false
	}
	s := strings.TrimSpace(lit)
	if strings.EqualFold(s, "NULL") {
		return "NULL", true
	}
	if base == "enum" {
		idx, err := strconv.ParseInt(s, 10, 64)
		if err != nil || idx < 0 || idx > int64(len(col.members)) {
			return "", false
		}
		if idx == 0 {
			return quoteExampleText(""), true
		}
		return quoteExampleText(col.members[idx-1]), true
	}
	bits, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return "", false
	}
	if len(col.members) < 64 && bits>>uint(len(col.members)) != 0 {
		return "", false
	}
	var parts []string
	for i, member := range col.members {
		if bits&(uint64(1)<<uint(i)) != 0 {
			parts = append(parts, member)
		}
	}
	return quoteExampleText(strings.Join(parts, ",")), true
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

func columnLit(im LoggedImage, name string) (string, bool) {
	for i, col := range im.Columns {
		if strings.EqualFold(col, name) && i < len(im.Values) {
			return im.Values[i], true
		}
	}
	return "", false
}

func refKey(im LoggedImage, refs []string) (string, bool) {
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

// GenImages is one table's logged images. Each image's evaluation
// environment is built once and shared by every generated column and every
// div_precision_increment: it depends only on the logged values and the
// schema-file columns.
type GenImages struct {
	images []LoggedImage
	envs   []map[string]GenVal
	names  []string
	meta   []SchemaCol
}

func PrepareGenImages(rows []LoggedImage, meta []SchemaCol) *GenImages {
	g := &GenImages{images: rows, envs: make([]map[string]GenVal, len(rows)), meta: meta}
	seen := map[string]struct{}{}
	for i, row := range rows {
		g.envs[i] = loggedEnv(row.Columns, row.Values, meta)
		for _, name := range row.Columns {
			key := strings.ToLower(name)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			g.names = append(g.names, name)
		}
	}
	return g
}

// generatedColumnOutcome checks one generated column against every logged image.
// mismatch is set when a value contradicts the expression. unverified is set when
// nothing contradicted it and at least one image could not be evaluated.
func generatedColumnOutcome(col SchemaCol, rows []LoggedImage, meta []SchemaCol) (example string, unverified bool) {
	return PrepareGenImages(rows, meta).Outcome(col)
}

func (g *GenImages) Outcome(col SchemaCol) (example string, unverified bool) {
	if strings.TrimSpace(col.Expr) == "" || len(g.images) == 0 {
		return "", true
	}
	bad, unknown := g.checkInc(col, mysqlDefaultDivPrecIncrement, -1)
	if bad >= 0 {
		return exampleImage(g.images[bad], g.meta), false
	}
	if clash := g.dependencyClash(col); clash != "" {
		return clash, false
	}
	return "", unknown
}

// mysqlMaxDivPrecIncrement is the largest div_precision_increment MySQL accepts.
const mysqlMaxDivPrecIncrement = 30

// DivIncrementFit sorts the non-default div_precision_increment values for a
// column refused at the default. fit lists the increments at which every
// logged image is a confident match. unchecked lists the increments where no
// image contradicted the expression but at least one could not be evaluated,
// for example past 30 fractional digits or 65 digits of precision. The binlog
// does not record the variable, so a column that fits another increment
// points at the server setting, not at the dump. It is only a hint for the
// error message: the column is still refused.
func (g *GenImages) DivIncrementFit(col SchemaCol) (fit, unchecked []int) {
	if !strings.Contains(col.Expr, "/") || (col.base != "decimal" && !integerAssign(col.base)) {
		return nil, nil
	}
	if strings.TrimSpace(col.Expr) == "" || len(g.images) == 0 {
		return nil, nil
	}
	// Two images with the same inputs and different stored values fit no
	// increment. This does not depend on the increment, so check it once.
	if g.dependencyClash(col) != "" {
		return nil, nil
	}
	probe := -1
	for inc := 0; inc <= mysqlMaxDivPrecIncrement; inc++ {
		if inc == mysqlDefaultDivPrecIncrement {
			continue
		}
		bad, unknown := g.checkInc(col, inc, probe)
		switch {
		case bad >= 0:
			// Try the image that refused this increment first at the next one:
			// most non-fitting increments then stop after one evaluation.
			probe = bad
		case unknown:
			unchecked = append(unchecked, inc)
		default:
			fit = append(fit, inc)
		}
	}
	return fit, unchecked
}

// checkInc evaluates col at div_precision_increment inc. bad is the index of
// an image that contradicts the expression, or -1. unknown is set when no
// image contradicted it and at least one could not be evaluated. first, when
// not -1, is checked before the others; the result does not depend on order.
func (g *GenImages) checkInc(col SchemaCol, inc, first int) (bad int, unknown bool) {
	if first >= 0 && first < len(g.images) {
		res := g.checkImage(col, inc, first)
		if res == imageMismatch {
			return first, false
		}
		if res == imageUnknown {
			unknown = true
		}
	}
	for i := range g.images {
		if i == first {
			continue
		}
		switch g.checkImage(col, inc, i) {
		case imageMismatch:
			return i, false
		case imageUnknown:
			unknown = true
		}
	}
	return -1, unknown
}

type imageResult int

const (
	imageMatch imageResult = iota
	imageMismatch
	imageUnknown
)

func (g *GenImages) checkImage(col SchemaCol, inc, i int) imageResult {
	im := g.images[i]
	env := g.envs[i]
	lit, found := columnLit(im, col.Name)
	if !found {
		return imageUnknown
	}
	if col.base == "decimal" || integerAssign(col.base) {
		if dec, ok := EvalDecExprInc(col.Expr, env, inc); ok {
			// decOmit was parsed and must not fall through to a different
			// arithmetic. decIntOnly is an exact product whose scale is past
			// 30: a DECIMAL target must not claim it, and an integer target
			// rounds that product once.
			if dec.mode == decOmit || (dec.mode == decIntOnly && col.base == "decimal") {
				return imageUnknown
			}
			var match, confident bool
			if col.base == "decimal" {
				match, confident = matchDecLiteral(dec, lit, col)
			} else {
				match, confident = matchDecAsInt(dec, lit, col)
			}
			if !confident {
				return imageUnknown
			}
			if !match {
				return imageMismatch
			}
			return imageMatch
		}
	}
	got, ok := EvalGenExpr(col.Expr, env)
	if !ok {
		return imageUnknown
	}
	match, confident := matchGenerated(got, lit, col)
	if !confident {
		return imageUnknown
	}
	if !match {
		return imageMismatch
	}
	return imageMatch
}

func (g *GenImages) dependencyClash(col SchemaCol) string {
	refs, ok := generatedRefs(col.Expr, g.names)
	if !ok {
		return ""
	}
	type hit struct {
		lit string
		idx int
	}
	seen := map[string]hit{}
	for i, im := range g.images {
		key, ok := refKey(im, refs)
		if !ok {
			continue
		}
		lit, found := columnLit(im, col.Name)
		if !found {
			continue
		}
		if prev, ok := seen[key]; ok {
			if prev.lit != lit {
				return OneLine(exampleImage(g.images[prev.idx], g.meta)+i18n.T("error.flashbackSchemaSameInputs")+col.Name+"="+exampleCell(lit, col, true), 220)
			}
			continue
		}
		seen[key] = hit{lit: lit, idx: i}
	}
	return ""
}

func GeneratedMismatch(table string, col SchemaCol, example string) error {
	return schemaMismatch(table, []string{i18n.Tf("error.flashbackSchemaGenerated", map[string]any{
		"Column":  col.Name,
		"Expr":    exprText(col.Expr),
		"Example": example,
	})})
}

// DivIncrementMismatch replaces the usual schema-file advice: the dump is
// probably right and the server ran with a non-default div_precision_increment.
// unchecked increments are named so a partial fit does not read as exact.
// The long hand-restore note is not part of it; see DivIncrementHint.
func DivIncrementMismatch(table string, col SchemaCol, example string, fit, unchecked []int) error {
	var clause string
	switch {
	case len(unchecked) == 0:
		clause = i18n.Tf("error.flashbackDivIncFit", map[string]any{"Increments": intRanges(fit)})
	case len(fit) == 0:
		clause = i18n.Tf("error.flashbackDivIncUnchecked", map[string]any{"Unchecked": intRanges(unchecked)})
	default:
		clause = i18n.Tf("error.flashbackDivIncFitUnchecked", map[string]any{"Increments": intRanges(fit), "Unchecked": intRanges(unchecked)})
	}
	return fmt.Errorf("%s", i18n.Tf("error.flashbackSchemaDivInc", map[string]any{
		"Table":   table,
		"Column":  col.Name,
		"Expr":    exprText(col.Expr),
		"Fit":     clause,
		"Example": example,
	}))
}

// DivIncrementHint is the hand-restore note (SET SESSION, FLUSH TABLES and
// its cost, the fresh-connection route). It is printed once per run, on the
// first increment refusal line, not once per column (#246). name is that
// table as SQL (`db`.`tbl`).
func DivIncrementHint(name string, fit, unchecked []int) string {
	vals := fit
	if len(vals) == 0 {
		vals = unchecked
	}
	return i18n.Tf("error.flashbackSchemaDivIncHint", map[string]any{
		"N":    exampleIncrement(vals),
		"Name": name,
	})
}

// exampleIncrement picks the N shown in the hint: the start of the first run
// of two or more consecutive increments, else the first one, leaving out 0
// when anything else is listed: "0, 28-30" gives 28 and "0-3" gives 1, since
// an increment of 0 is an odd value to suggest.
func exampleIncrement(vals []int) int {
	if len(vals) > 1 && vals[0] == 0 {
		vals = vals[1:]
	}
	for i := 0; i+1 < len(vals); i++ {
		if vals[i+1] == vals[i]+1 {
			return vals[i]
		}
	}
	if len(vals) > 0 {
		return vals[0]
	}
	return 0
}

// SQLTableName is schema.table with backtick quoting, as an operator would
// type it after FLUSH TABLES.
func SQLTableName(schema, table string) string {
	q := func(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
	if schema == "" {
		return q(table)
	}
	return q(schema) + "." + q(table)
}

// intRanges prints sorted ints as "0-3, 5, 8-30".
func intRanges(vals []int) string {
	var parts []string
	for i := 0; i < len(vals); {
		j := i
		for j+1 < len(vals) && vals[j+1] == vals[j]+1 {
			j++
		}
		if j == i {
			parts = append(parts, strconv.Itoa(vals[i]))
		} else {
			parts = append(parts, strconv.Itoa(vals[i])+"-"+strconv.Itoa(vals[j]))
		}
		i = j + 1
	}
	return strings.Join(parts, ", ")
}

func UnverifiedGeneratedWarning(table string, col SchemaCol) model.FlashbackWarning {
	return model.NewFlashWarning(model.WarnGeneratedColumnUnverified, "warning.flashbackSchemaUnchecked", map[string]any{
		"Table":  table,
		"Column": col.Name,
		"Expr":   exprText(col.Expr),
	})
}

// UnverifiedGeneratedComment is the script-header line. It stays English, like the other script comments.
func UnverifiedGeneratedComment(table string, col SchemaCol) string {
	return "-- WARNING: generated column " + table + "." + col.Name + " not verified (" + exprText(col.Expr) + "); it is left out of the script. The guard below checks that this column is generated on the target."
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

func (v GenVal) enumAsText() (GenVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	if !knownTextCharset(v.charset) {
		return GenVal{}, false
	}
	idx, ok := v.exactInt()
	if !ok || idx < 0 || idx > int64(len(v.members)) {
		return GenVal{}, false
	}
	if idx == 0 {
		return genTextVal("", textCharset(v.charset)), true
	}
	return genTextVal(v.members[idx-1], textCharset(v.charset)), true
}

func (v GenVal) setAsText() (GenVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	if !knownTextCharset(v.charset) || len(v.members) > 64 {
		return GenVal{}, false
	}
	bits, ok := v.exactUint()
	if !ok {
		return GenVal{}, false
	}
	if len(v.members) < 64 && bits>>uint(len(v.members)) != 0 {
		return GenVal{}, false
	}
	var parts []string
	for i, member := range v.members {
		if bits&(uint64(1)<<uint(i)) != 0 {
			parts = append(parts, member)
		}
	}
	return genTextVal(strings.Join(parts, ","), textCharset(v.charset)), true
}

func (v GenVal) decimalAsText() (GenVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	if v.kind != genNum || v.n == nil {
		return GenVal{}, false
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

func (v GenVal) temporalAsText() (GenVal, bool) {
	if v.kind == genNull {
		return genNullVal(), true
	}
	if v.kind != genText || !v.decoded {
		return GenVal{}, false
	}
	fsp := 0
	if v.hasPrec {
		fsp = v.prec
	}
	text, ok := padFractional(v.text, fsp)
	if !ok {
		return GenVal{}, false
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

func (v GenVal) exactInt() (int64, bool) {
	if v.kind != genNum || v.n == nil || !v.n.IsInt() {
		return 0, false
	}
	num := v.n.Num()
	if !num.IsInt64() {
		return 0, false
	}
	return num.Int64(), true
}

func (v GenVal) exactUint() (uint64, bool) {
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

// mysqlDefaultDivPrecIncrement is MySQL's default div_precision_increment.
// The binlog does not record the variable. Integer / integer at this default
// is not "4 extra digits": decimal.cc rounds the fractional width up to a
// multiple of 9 (DIG_PER_DEC1) and cuts the rest off. 1/7 is 0.142857142,
// and DECIMAL(40,9) stores that truncation. Assignment then rounds half away
// from zero to the column scale, so DECIMAL(40,4) stores 0.1429.
const mysqlDefaultDivPrecIncrement = 4

const mysqlDecDigitsPerWord = 9

// mysqlDecMaxScale is DECIMAL's maximum scale. Column scales stay within it.
const mysqlDecMaxScale = 30

// mysqlDecMaxPrec is DECIMAL's maximum precision.
const mysqlDecMaxPrec = 65

const (
	decClaim uint8 = iota
	// decIntOnly is an exact product whose scale is above 30. Integer
	// assignment rounds it once. A DECIMAL target must not claim it.
	decIntOnly
	// decOmit means the expression was understood and is unverified.
	decOmit
)

// DecVal is one decimal result. scale is the fractional digits of an exact
// product or sum, or the division width MySQL cuts to. mode says whether a
// target may compare the value after one assignment round.
type DecVal struct {
	null  bool
	n     *big.Rat
	scale int
	mode  uint8
	// intKind is a signed integer type (an integer column or a BIGINT literal).
	// + - * of two intKind values use MySQL's integer arithmetic: a result
	// outside signed BIGINT is unverified. A DECIMAL operand clears it.
	intKind bool
}

func integerClip(v intVal, col SchemaCol) (*big.Int, bool) {
	if v.null || v.n == nil {
		return nil, false
	}
	min, max, ok := integerBounds(col)
	if !ok {
		return nil, false
	}
	rounded := roundRatHalfAway(v.n)
	if rounded.Cmp(min) >= 0 && rounded.Cmp(max) <= 0 {
		return nil, false
	}
	if rounded.Sign() < 0 {
		return min, true
	}
	return max, true
}

func integerBounds(col SchemaCol) (min, max *big.Int, ok bool) {
	bits := 0
	switch col.base {
	case "tinyint":
		bits = 8
	case "smallint":
		bits = 16
	case "mediumint":
		bits = 24
	case "int":
		bits = 32
	case "bigint":
		bits = 64
	default:
		return nil, nil, false
	}
	if col.unsigned {
		max = new(big.Int).Lsh(big.NewInt(1), uint(bits))
		max.Sub(max, big.NewInt(1))
		return new(big.Int), max, true
	}
	max = new(big.Int).Lsh(big.NewInt(1), uint(bits-1))
	min = new(big.Int).Neg(max)
	max = new(big.Int).Sub(max, big.NewInt(1))
	return min, max, true
}

// mysqlDivFrac is the fractional digit count of a / in MySQL 8.0.
// do_div_mod rounds each operand's scale up to a multiple of 9, spends
// div_precision_increment on that padding, then rounds the sum up again.
func mysqlDivFrac(frac1, frac2, inc int) int {
	if frac1 < 0 {
		frac1 = 0
	}
	if frac2 < 0 {
		frac2 = 0
	}
	pad1 := roundUpToWord(frac1)
	pad2 := roundUpToWord(frac2)
	extra := inc - (pad1 - frac1) - (pad2 - frac2)
	if extra < 0 {
		extra = 0
	}
	return roundUpToWord(pad1 + pad2 + extra)
}

func roundUpToWord(digits int) int {
	if digits <= 0 {
		return 0
	}
	words := (digits + mysqlDecDigitsPerWord - 1) / mysqlDecDigitsPerWord
	return words * mysqlDecDigitsPerWord
}

func roundRatToScale(r *big.Rat, scale int) *big.Rat {
	if r == nil {
		return new(big.Rat)
	}
	if scale <= 0 {
		return new(big.Rat).SetInt(roundRatHalfAway(r))
	}
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	scaled := new(big.Rat).Mul(r, new(big.Rat).SetInt(pow))
	return new(big.Rat).SetFrac(roundRatHalfAway(scaled), pow)
}

func truncRatToScale(r *big.Rat, scale int) *big.Rat {
	if r == nil {
		return new(big.Rat)
	}
	if scale < 0 {
		scale = 0
	}
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	num := new(big.Int).Mul(new(big.Int).Set(r.Num()), pow)
	q := new(big.Int).Quo(num, r.Denom())
	if scale == 0 {
		return new(big.Rat).SetInt(q)
	}
	return new(big.Rat).SetFrac(q, pow)
}

func ratAtScale(r *big.Rat, scale int) bool {
	if r == nil || scale < 0 {
		return false
	}
	if scale == 0 {
		return r.IsInt()
	}
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return new(big.Rat).Mul(r, new(big.Rat).SetInt(pow)).IsInt()
}

func decimalMax(prec, scale int) *big.Rat {
	num := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(prec)), nil)
	num.Sub(num, big.NewInt(1))
	den := big.NewInt(1)
	if scale > 0 {
		den = new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	}
	return new(big.Rat).SetFrac(num, den)
}

func decimalFits(r *big.Rat, prec, scale int) bool {
	if r == nil || prec <= 0 || scale < 0 || scale > prec {
		return false
	}
	abs := new(big.Rat).Abs(r)
	return abs.Cmp(decimalMax(prec, scale)) <= 0
}

func decimalClip(r *big.Rat, prec, scale int) *big.Rat {
	max := decimalMax(prec, scale)
	if r.Sign() < 0 {
		return new(big.Rat).Neg(max)
	}
	return max
}

// matchDecAsInt assigns a decimal result to an integer column.
// The quotient was already cut to MySQL's division width, so (a/b)*b is not
// brought back to a when that cut drops digits. 1/10000000000*10000000000 is 0.
func matchDecAsInt(v DecVal, lit string, col SchemaCol) (match, confident bool) {
	logged, ok := parseLoggedValue(lit)
	if !ok {
		return false, false
	}
	if v.null || logged.kind == genNull {
		return v.null && logged.kind == genNull, true
	}
	if logged.kind != genNum || logged.n == nil || v.n == nil {
		return false, false
	}
	iv := intVal{n: v.n}
	if clip, clipped := integerClip(iv, col); clipped {
		if intValMatches(intVal{n: new(big.Rat).SetInt(clip)}, literalNumber(lit)) {
			return false, false
		}
		return false, true
	}
	return intValMatches(iv, literalNumber(lit)), true
}

func matchDecLiteral(v DecVal, lit string, col SchemaCol) (match, confident bool) {
	logged, ok := parseLoggedValue(lit)
	if !ok {
		return false, false
	}
	if v.null || logged.kind == genNull {
		return v.null && logged.kind == genNull, true
	}
	if logged.kind != genNum || logged.n == nil || v.n == nil {
		return false, false
	}
	prec, scale := col.prec, col.scale
	if !col.hasPrec {
		prec, scale = 10, 0
	}
	rounded := roundRatToScale(v.n, scale)
	if !decimalFits(rounded, prec, scale) {
		if logged.n.Cmp(decimalClip(rounded, prec, scale)) == 0 {
			return false, false
		}
		return false, true
	}
	return rounded.Cmp(logged.n) == 0, true
}

func decFromGen(v GenVal) (DecVal, bool) {
	if v.kind == genNull {
		return DecVal{null: true}, true
	}
	if v.kind != genNum || v.n == nil {
		return DecVal{}, false
	}
	scale := 0
	switch v.base {
	case "":
		if !v.n.IsInt() {
			return DecVal{}, false
		}
	case "decimal":
		if v.hasPrec {
			scale = v.scale
		}
		if scale < 0 || scale > mysqlDecMaxScale || !ratAtScale(v.n, scale) {
			return DecVal{}, false
		}
	case "tinyint", "smallint", "mediumint", "int", "bigint", "year":
		if !v.n.IsInt() {
			return DecVal{}, false
		}
		return DecVal{n: new(big.Rat).Set(v.n), scale: scale, intKind: fitsSignedBigint(v.n)}, true
	default:
		return DecVal{}, false
	}
	return DecVal{n: new(big.Rat).Set(v.n), scale: scale}, true
}

func applyDecAdd(left, right DecVal, op byte) (DecVal, bool) {
	if left.null || right.null {
		return DecVal{null: true}, true
	}
	if left.mode != decClaim || right.mode != decClaim || left.n == nil || right.n == nil {
		return DecVal{mode: decOmit}, true
	}
	n := new(big.Rat)
	if op == '+' {
		n.Add(left.n, right.n)
	} else {
		n.Sub(left.n, right.n)
	}
	scale := left.scale
	if right.scale > scale {
		scale = right.scale
	}
	// Two signed integer operands use BIGINT arithmetic. MySQL rejects the
	// sum (ERROR 1690) instead of promoting it to DECIMAL.
	intKind := left.intKind && right.intKind
	if intKind && !fitsSignedBigint(n) {
		return DecVal{}, false
	}
	// The sum is exact at max(scale). MySQL does not round it here. A sum
	// that does not fit in 65 digits is unverified, not rounded down.
	return tagIntKind(placeDec(n, scale, false), intKind), true
}

func applyDecMul(left, right DecVal, op string, inc int) (DecVal, bool) {
	if left.null || right.null {
		return DecVal{null: true}, true
	}
	if left.mode == decOmit || right.mode == decOmit {
		return DecVal{mode: decOmit}, true
	}
	if left.n == nil || right.n == nil {
		return DecVal{}, false
	}
	switch op {
	case "*":
		if left.mode != decClaim || right.mode != decClaim {
			return DecVal{mode: decOmit}, true
		}
		// MySQL keeps this product and rounds once, on store. Rounding to
		// 30 places here and again on store can carry into the stored digit.
		scale := left.scale + right.scale
		prod := new(big.Rat).Mul(left.n, right.n)
		intKind := left.intKind && right.intKind
		if intKind && !fitsSignedBigint(prod) {
			return DecVal{}, false
		}
		return tagIntKind(placeDec(prod, scale, true), intKind), true
	case "/":
		if right.n.Sign() == 0 {
			if right.mode != decClaim {
				return DecVal{mode: decOmit}, true
			}
			return DecVal{null: true}, true
		}
		if left.mode != decClaim || right.mode != decClaim {
			return DecVal{mode: decOmit}, true
		}
		scale := mysqlDivFrac(left.scale, right.scale, inc)
		q := truncRatToScale(new(big.Rat).Quo(left.n, right.n), scale)
		// The cut to mysqlDivFrac is MySQL's division, not a second round.
		// A width past 30, or a quotient past 65 digits, is unverified.
		return placeDec(q, scale, false), true
	case "DIV":
		return decIntOp(left, right, false)
	case "%", "MOD":
		return decIntOp(left, right, true)
	default:
		return DecVal{}, false
	}
}

// placeDec accepts an exact result. product is set for multiplication: a
// scale above 30 stays exact so an integer target can round once, and a
// DECIMAL target does not claim it. Every other too-wide or too-long result
// is omitted.
func placeDec(n *big.Rat, scale int, product bool) DecVal {
	if n == nil || scale < 0 || !ratAtScale(n, scale) || decDigits(n, scale) > mysqlDecMaxPrec {
		return DecVal{mode: decOmit}
	}
	if scale > mysqlDecMaxScale {
		if product {
			return DecVal{n: n, scale: scale, mode: decIntOnly}
		}
		return DecVal{mode: decOmit}
	}
	return DecVal{n: n, scale: scale}
}

func tagIntKind(v DecVal, intKind bool) DecVal {
	if intKind && v.mode == decClaim {
		v.intKind = true
	}
	return v
}

// decDigits is the precision of n written with exactly scale fractional digits.
func decDigits(n *big.Rat, scale int) int {
	if n == nil || scale < 0 {
		return mysqlDecMaxPrec + 1
	}
	abs := new(big.Rat).Abs(n)
	ip := new(big.Int).Quo(new(big.Int).Set(abs.Num()), abs.Denom())
	digits := 0
	if ip.Sign() > 0 {
		digits = len(ip.String())
	}
	prec := digits + scale
	if prec < 1 {
		return 1
	}
	return prec
}
