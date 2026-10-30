// Package mysqlexpr evaluates generated-column expressions against logged row images.
// input: the expression text from a schema file, and SQL literals from FULL row images.
// output: a match, a contradiction (with one example image), or unverified when the expression cannot be modelled exactly and the images do not contradict it. The example prints printable text for a text charset and keeps the hex literal for binary or non-printable bytes. ENUM and SET print the schema-file member labels when the file lists them; an index of 0 is an empty label, and a value with no member list stays the raw index or bitmask. UPPER/LOWER cover ascii, latin1's 1:1 map, and utf8mb4 Latin-1 plus µ. NULL propagates, except CONCAT_WS, which skips NULL arguments. Integer and DECIMAL columns share MySQL's division width (9 fractional digits for integer operands at the default div_precision_increment). DECIMAL assignment then rounds half away from zero to the column scale; integer assignment rounds the same way at scale 0. TIME and DATETIME assignment rounds fractional seconds half away from zero to the column fsp; a carry into the seconds is unverified. A YEAR value coerced to text renders MySQL's 4 digits, so YEAR 0 is '0000'. A fixed BINARY column zero-pads the expression result to its width, and a longer result is unverified like a long CHAR. A _charset'..' quoted literal outside the dump-safe UTF family (utf8/utf8mb3/utf8mb4/ascii) is not evaluated: the dumped expression can carry '?' for a lost byte. A hex or binary literal names its bytes exactly, so it is evaluated in any charset. An intermediate product, sum, or quotient is not rounded to fit 30 fractional digits or 65 digits of precision. Past either limit a DECIMAL target is unverified, and an integer target rounds an exact product once. An unknown charset, an unmodelled type, or a value this checker will not claim is unverified, never a match and never a mismatch.
// pos: flashback-only helper. Analyze does not call it.
// note: if this file changes, update this header and module README.md.
package mysqlexpr

import (
	"bytes"
	"fmt"
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
	kind     genKind
	n        *big.Rat
	numText  string
	text     string
	raw      []byte
	haveRaw  bool
	charset  string
	decoded  bool
	j        *jNode
	base     string
	members  []string
	prec     int
	scale    int
	hasPrec  bool
	unsigned bool
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
		return intVal{n: v.n, unsigned: v.unsigned}, true
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
	unsigned := li.unsigned || ri.unsigned
	// MySQL rejects a signed BIGINT sum or difference (ERROR 1690); with an
	// UNSIGNED operand the arithmetic is unsigned, so a negative operand or
	// a result outside [0, 2^64-1] errors too. Either way the value is
	// unverified, not a result the checker may claim.
	if unsigned && (li.n.Sign() < 0 || ri.n.Sign() < 0 || !fitsUnsignedBigint(n)) {
		return GenVal{}, false
	}
	if !unsigned && !fitsSignedBigint(n) {
		return GenVal{}, false
	}
	return GenVal{kind: genNum, n: n, unsigned: unsigned}, true
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
	out := genTextVal(folded, src.charset)
	if src.charset == "latin1" {
		// foldText guarantees every rune is Latin-1, so raw holds the
		// charset's own bytes, not the UTF-8 form genTextVal assumes.
		raw := make([]byte, 0, len(folded))
		for _, r := range folded {
			raw = append(raw, byte(r))
		}
		out.raw = raw
	}
	return out, true
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
	case "year":
		return v.yearAsText()
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

func (sc *Scan) introducedLiteral(name string) (GenVal, bool, bool, bool, bool) {
	charset := strings.ToLower(strings.TrimPrefix(name, "_"))
	save := *sc
	sc.Skip()
	if sc.I >= len(sc.S) {
		*sc = save
		return GenVal{}, false, false, false, false
	}
	if sc.S[sc.I] == '\'' || sc.S[sc.I] == '"' {
		v, ok, collated := sc.quotedText(normalizeIntroCharset(charset))
		return v, ok, true, collated, false
	}
	if v, ok, parsed := sc.hexBinary(); parsed {
		if !ok {
			return GenVal{}, false, true, false, true
		}
		return tagCharset(v, normalizeIntroCharset(charset)), true, true, sc.skipCollate(), true
	}
	*sc = save
	return GenVal{}, false, false, false, false
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

// bitLiteral reads b'0101': the bit literal's numeric value, the same one a
// BIT column's logged literal carries. The cursor is on b or B followed by a
// quote; up to 64 bits.
func (sc *Scan) bitLiteral() (GenVal, bool) {
	sc.I += 2
	start := sc.I
	for sc.I < len(sc.S) && (sc.S[sc.I] == '0' || sc.S[sc.I] == '1') {
		sc.I++
	}
	if sc.I == start || sc.I-start > 64 || sc.I >= len(sc.S) || sc.S[sc.I] != '\'' {
		return GenVal{}, false
	}
	digits := sc.S[start:sc.I]
	sc.I++
	var bits uint64
	for i := 0; i < len(digits); i++ {
		bits = bits<<1 | uint64(digits[i]-'0')
	}
	return genNumVal(strconv.FormatUint(bits, 10))
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
				if _, _, parsed, _, _ := sc.introducedLiteral(name); parsed {
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
