// Package mysqlexpr: the exact-DECIMAL value domain.
// input: a logged SQL literal and the schema file's DECIMAL column bounds (precision, scale, div_precision_increment).
// output: the value MySQL's decimal.cc computes, or ok=false past 30 fractional digits or 65 digits of precision, where a DECIMAL target is unverified and an integer target rounds an exact product once. Division follows MySQL's per-operation fractional width: 9-digit words, DIV and % over a rounded /, then half-away-from-zero assignment rounding to the column scale.
// pos: value helpers for expr.go and eval.go. Flashback-only module; analyze does not call it.
// note: if this file changes, update this header and module README.md.
package mysqlexpr

import (
	"math/big"
)

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
