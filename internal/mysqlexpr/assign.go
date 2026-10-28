// Package mysqlexpr: the column-assignment half of generated-column checking.
// input: the evaluated expression GenVal, the logged literal, and the column type.
// output: a match, a mismatch, or unverified when assignment semantics are not
// modelled (a non-strict truncation, a fraction carry into the seconds field).
// pos: comparison helpers for expr.go. Flashback-only module; analyze does not call it.
// note: if this file changes, update this header and module README.md.
package mysqlexpr

import (
	"bytes"
	"fmt"
	"math/big"
	"strings"
	"unicode/utf8"
)

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
		raw := got.raw
		if col.base == "binary" && col.hasPrec {
			if len(raw) > col.prec {
				// Longer than the column is a non-strict truncation; this
				// checker does not know sql_mode.
				return false, false
			}
			padded := make([]byte, col.prec)
			copy(padded, raw)
			raw = padded
		}
		return bytes.Equal(raw, logged.raw), true
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
	if temporalAssign(col.base) {
		left, ok1 := assignTemporalText(got, col)
		right, ok2 := assignTemporalText(logged, col)
		if !ok1 || !ok2 {
			return false, false
		}
		return left == right, true
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

func temporalAssign(base string) bool {
	switch base {
	case "time", "datetime":
		return true
	default:
		return false
	}
}

// assignTemporalText renders v as MySQL stores it in a TIME or DATETIME(fsp)
// column: fractional seconds past the column's fsp round half away from zero.
// A number into a temporal column, undecoded text, and a round-up that would
// carry into the seconds field are unverified.
func assignTemporalText(v GenVal, col SchemaCol) (string, bool) {
	if v.kind != genText || !v.decoded {
		return "", false
	}
	switch v.charset {
	case "binary", "utf8mb4", "utf8", "ascii", "latin1", "":
	default:
		return "", false
	}
	fsp := 0
	if col.hasPrec {
		fsp = col.prec
	}
	return roundFractional(v.text, fsp)
}

// roundFractional applies temporal fractional-second rounding: digits past
// fsp round the kept fraction half away from zero. Negative values keep the
// sign in base and round the magnitude. A non-digit fraction, and a round-up
// that carries into the seconds field, are unverified.
func roundFractional(text string, fsp int) (string, bool) {
	if fsp < 0 || fsp > 6 {
		return "", false
	}
	base, frac, has := strings.Cut(text, ".")
	if !has {
		frac = ""
	}
	for i := 0; i < len(frac); i++ {
		if frac[i] < '0' || frac[i] > '9' {
			return "", false
		}
	}
	if len(frac) <= fsp {
		if fsp == 0 {
			return base, true
		}
		return base + "." + frac + strings.Repeat("0", fsp-len(frac)), true
	}
	keep, drop := frac[:fsp], frac[fsp:]
	if drop[0] < '5' {
		if fsp == 0 {
			return base, true
		}
		return base + "." + keep, true
	}
	n, ok := new(big.Int).SetString(keep, 10)
	if !ok {
		return "", false
	}
	n.Add(n, big.NewInt(1))
	s := n.String()
	if len(s) > fsp {
		return "", false
	}
	return base + "." + strings.Repeat("0", fsp-len(s)) + s, true
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

// yearAsText renders a YEAR column's integer value in MySQL's 4-digit
// zero-padded display: YEAR 0 is '0000', not '0'.
func (v GenVal) yearAsText() (GenVal, bool) {
	if v.kind != genNum || v.n == nil || !v.n.IsInt() {
		return GenVal{}, false
	}
	num := v.n.Num()
	if !num.IsInt64() {
		return GenVal{}, false
	}
	i := num.Int64()
	if i < 0 || i > 9999 {
		return GenVal{}, false
	}
	return genTextVal(fmt.Sprintf("%04d", i), "ascii"), true
}
