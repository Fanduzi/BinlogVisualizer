// Package mysqlexpr: the generated-column expression evaluator.
// input: a generated-column expression from a schema file, logged column values, and (for DECIMAL arithmetic) a div_precision_increment.
// output: the value MySQL would compute, or ok=false when the expression uses something this checker does not model. One grammar, parsed once per expression text into a tree (cached); one evaluator walks that tree in the general domain (integers, text, JSON; EvalGenExpr) or the exact-DECIMAL domain (EvalDecExprInc). The DECIMAL domain accepts only its subset: numbers, columns, NULL/TRUE/FALSE, + - * / DIV % MOD, MOD(), and parentheses; it follows MySQL's per-operation DECIMAL rounding, including DIV and % over a rounded / (#238). JSON paths autowrap a non-array for [0] (#237). A COLLATE clause is not modelled and returns ok=false (ADR 0020). Signed integer + - * DIV past BIGINT is unverified in both domains, because MySQL rejects it; an UNSIGNED operand switches to unsigned BIGINT, so a negative operand or a result outside [0, 2^64-1] (MySQL's ERROR 1690) is unverified too.
// pos: flashback-only module. Analyze does not call it. Value helpers (GenVal, DecVal, apply*) live in expr.go, expr_json.go, expr_decimal.go, and schema.go.
// note: if this file changes, update this header, CONTEXT.md (Expression evaluator) and docs/adr/0007-expression-evaluator.md.
package mysqlexpr

import (
	"math"
	"math/big"
	"strings"
	"sync"
)

type exprKind uint8

const (
	exAdd   exprKind = iota + 1 // l op r, op '+' or '-'
	exMul                       // l mulOp r, mulOp * / % DIV MOD
	exUnary                     // op x, op '+' or '-'
	exArrow                     // x -> path, x ->> path
	exLit                       // quoted text, X'..', or _charset introducer: lit
	exNum                       // unsigned integer literal: num
	exNull                      // NULL
	exBool                      // TRUE / FALSE: truth
	exCol                       // column reference: name (lower case)
	exCall                      // name(args)
)

// exprNode is one node of a parsed generated expression. Nodes are shared
// between evaluations and never modified after parsing.
type exprNode struct {
	kind    exprKind
	op      byte
	mulOp   string
	l, r, x *exprNode
	path    string
	unquote bool
	lit     GenVal
	num     string
	truth   bool
	name    string
	quoted  bool
	collate bool // a COLLATE clause followed the column name
	args    []*exprNode
	nparts  int // comma-separated parts in the call body
}

var exprCache sync.Map // expression text -> *exprNode (nil when it does not parse)

// parseExprTree parses expr once; later calls return the cached tree.
func parseExprTree(expr string) (*exprNode, bool) {
	if v, ok := exprCache.Load(expr); ok {
		n := v.(*exprNode)
		return n, n != nil
	}
	n, ok := parseExprText(expr)
	if !ok {
		n = nil
	}
	exprCache.Store(expr, n)
	return n, n != nil
}

func parseExprText(expr string) (*exprNode, bool) {
	sc := &Scan{S: expr}
	n, ok := pexAdd(sc)
	if !ok {
		return nil, false
	}
	sc.Skip()
	if sc.I != len(sc.S) {
		return nil, false
	}
	return n, true
}

func pexAdd(sc *Scan) (*exprNode, bool) {
	left, ok := pexMul(sc)
	if !ok {
		return nil, false
	}
	for {
		sc.Skip()
		if sc.I >= len(sc.S) || (sc.S[sc.I] != '+' && sc.S[sc.I] != '-') {
			return left, true
		}
		// `->` and `->>` belong to a primary. A following arrow is not subtraction.
		if sc.startsArrow() {
			return left, true
		}
		op := sc.S[sc.I]
		sc.I++
		right, ok := pexMul(sc)
		if !ok {
			return nil, false
		}
		left = &exprNode{kind: exAdd, op: op, l: left, r: right}
	}
}

func pexMul(sc *Scan) (*exprNode, bool) {
	left, ok := pexUnary(sc)
	if !ok {
		return nil, false
	}
	for {
		op, ok := consumeMulOp(sc)
		if !ok {
			return left, true
		}
		right, ok := pexUnary(sc)
		if !ok {
			return nil, false
		}
		left = &exprNode{kind: exMul, mulOp: op, l: left, r: right}
	}
}

func pexUnary(sc *Scan) (*exprNode, bool) {
	sc.Skip()
	if sc.I < len(sc.S) && (sc.S[sc.I] == '-' || sc.S[sc.I] == '+') && !sc.startsArrow() {
		op := sc.S[sc.I]
		sc.I++
		x, ok := pexUnary(sc)
		if !ok {
			return nil, false
		}
		return &exprNode{kind: exUnary, op: op, x: x}, true
	}
	return pexPrimary(sc)
}

func pexPrimary(sc *Scan) (*exprNode, bool) {
	n, ok := pexAtom(sc)
	if !ok {
		return nil, false
	}
	for {
		unquote, arrow := sc.consumeArrow()
		if !arrow {
			return n, true
		}
		path, ok := sc.constString()
		if !ok {
			return nil, false
		}
		n = &exprNode{kind: exArrow, x: n, path: path, unquote: unquote}
	}
}

func pexAtom(sc *Scan) (*exprNode, bool) {
	sc.Skip()
	if sc.I >= len(sc.S) {
		return nil, false
	}
	switch sc.S[sc.I] {
	case '(':
		body, ok := sc.ParenBody()
		if !ok {
			return nil, false
		}
		return parseExprText(body)
	case '\'', '"':
		v, ok, collated := sc.quotedText("utf8mb4")
		if !ok {
			return nil, false
		}
		return &exprNode{kind: exLit, lit: v, collate: collated}, true
	}
	if sc.S[sc.I] == 'X' || sc.S[sc.I] == 'x' {
		if v, ok, parsed := sc.hexBinary(); parsed {
			if !ok {
				return nil, false
			}
			return &exprNode{kind: exLit, lit: v, collate: sc.skipCollate()}, true
		}
	}
	if sc.S[sc.I] >= '0' && sc.S[sc.I] <= '9' {
		start := sc.I
		for sc.I < len(sc.S) && sc.S[sc.I] >= '0' && sc.S[sc.I] <= '9' {
			sc.I++
		}
		if sc.I < len(sc.S) && (sc.S[sc.I] == '.' || sc.S[sc.I] == 'e' || sc.S[sc.I] == 'E') {
			return nil, false
		}
		return &exprNode{kind: exNum, num: sc.S[start:sc.I]}, true
	}
	quoted := sc.S[sc.I] == '`'
	name, ok := sc.Ident()
	if !ok {
		return nil, false
	}
	if !quoted && strings.HasPrefix(name, "_") {
		if v, ok, parsed, collated := sc.introducedLiteral(name); parsed {
			if !ok {
				return nil, false
			}
			return &exprNode{kind: exLit, lit: v, collate: collated}, true
		}
	}
	if sc.PeekByte() == '(' {
		body, ok := sc.ParenBody()
		if !ok {
			return nil, false
		}
		call := &exprNode{kind: exCall, name: name, quoted: quoted}
		if strings.TrimSpace(body) != "" {
			parts := SplitComma(body)
			call.nparts = len(parts)
			for _, part := range parts {
				arg, ok := parseExprText(part)
				if !ok {
					return nil, false
				}
				call.args = append(call.args, arg)
			}
		}
		return call, true
	}
	collate := sc.skipCollate()
	if !quoted && isNullWord(name) {
		return &exprNode{kind: exNull, collate: collate}, true
	}
	if !quoted && isBoolWord(name) {
		return &exprNode{kind: exBool, truth: strings.EqualFold(name, "TRUE"), collate: collate}, true
	}
	return &exprNode{kind: exCol, name: strings.ToLower(name), collate: collate}, true
}

// EvalGenExpr evaluates expr with logged column values.
// ok is false when the expression uses something this checker does not evaluate.
func EvalGenExpr(expr string, env map[string]GenVal) (GenVal, bool) {
	n, ok := parseExprTree(expr)
	if !ok {
		return GenVal{}, false
	}
	v, ok := n.evalGen(env)
	if !ok {
		return GenVal{}, false
	}
	return v, true
}

func (n *exprNode) evalGen(env map[string]GenVal) (GenVal, bool) {
	// A COLLATE clause is not modelled. Returning a value would ignore a
	// collation that changes the result (for example Turkish UPPER of i).
	if n.collate {
		return GenVal{}, false
	}
	switch n.kind {
	case exAdd:
		left, ok := n.l.evalGen(env)
		if !ok {
			return GenVal{}, false
		}
		right, ok := n.r.evalGen(env)
		if !ok {
			return GenVal{}, false
		}
		return applyGenAdd(left, right, n.op)
	case exMul:
		left, ok := n.l.evalGen(env)
		if !ok {
			return GenVal{}, false
		}
		right, ok := n.r.evalGen(env)
		if !ok {
			return GenVal{}, false
		}
		li, ok1 := left.asInt()
		ri, ok2 := right.asInt()
		if !ok1 || !ok2 {
			return GenVal{}, false
		}
		next, ok := applyMulOp(li, ri, n.mulOp)
		if !ok {
			return GenVal{}, false
		}
		if next.null {
			return genNullVal(), true
		}
		return GenVal{kind: genNum, n: next.n}, true
	case exUnary:
		v, ok := n.x.evalGen(env)
		if !ok || v.kind == genNull || n.op == '+' {
			return v, ok
		}
		iv, ok := v.asInt()
		if !ok || iv.null {
			return v, ok
		}
		if iv.unsigned {
			// MySQL negates in unsigned BIGINT (wraps); not modelled.
			return GenVal{}, false
		}
		neg := new(big.Rat).Neg(iv.n)
		if !fitsSignedBigint(neg) {
			return GenVal{}, false
		}
		return GenVal{kind: genNum, n: neg, unsigned: iv.unsigned}, true
	case exArrow:
		v, ok := n.x.evalGen(env)
		if !ok {
			return GenVal{}, false
		}
		return applyJSONArrow(v, n.path, n.unquote)
	case exLit:
		return n.lit.clone(), true
	case exNum:
		return genNumVal(n.num)
	case exNull:
		return genNullVal(), true
	case exBool:
		if n.truth {
			return genNumVal("1")
		}
		return genNumVal("0")
	case exCol:
		v, ok := env[n.name]
		if !ok || v.kind == genBad {
			return GenVal{}, false
		}
		return v, true
	case exCall:
		var args []GenVal
		for _, arg := range n.args {
			v, ok := arg.evalGen(env)
			if !ok {
				return GenVal{}, false
			}
			args = append(args, v)
		}
		return evalGenCall(n.name, args)
	}
	return GenVal{}, false
}

// clone copies a parsed literal so an evaluation cannot change the tree.
func (v GenVal) clone() GenVal {
	if v.n != nil {
		v.n = new(big.Rat).Set(v.n)
	}
	if v.raw != nil {
		v.raw = append([]byte(nil), v.raw...)
	}
	if v.members != nil {
		v.members = append([]string(nil), v.members...)
	}
	return v
}

func evalDecExpr(expr string, env map[string]GenVal) (DecVal, bool) {
	return EvalDecExprInc(expr, env, mysqlDefaultDivPrecIncrement)
}

// EvalDecExprInc evaluates expr exactly as DECIMAL, with / at
// div_precision_increment inc.
func EvalDecExprInc(expr string, env map[string]GenVal, inc int) (DecVal, bool) {
	if inc+1 == 0 {
		inc = mysqlDefaultDivPrecIncrement
	}
	n, ok := parseExprTree(expr)
	if !ok {
		return DecVal{}, false
	}
	v, ok := n.evalDec(env, inc)
	if !ok {
		return DecVal{}, false
	}
	return v, true
}

func (n *exprNode) evalDec(env map[string]GenVal, inc int) (DecVal, bool) {
	switch n.kind {
	case exAdd:
		left, ok := n.l.evalDec(env, inc)
		if !ok {
			return DecVal{}, false
		}
		right, ok := n.r.evalDec(env, inc)
		if !ok {
			return DecVal{}, false
		}
		return decZeroScale(applyDecAdd(left, right, n.op))
	case exMul:
		left, ok := n.l.evalDec(env, inc)
		if !ok {
			return DecVal{}, false
		}
		right, ok := n.r.evalDec(env, inc)
		if !ok {
			return DecVal{}, false
		}
		if n.mulOp == "/" && decIsZero(left) && right.mode == decClaim && right.n != nil && right.n.Sign() != 0 {
			// MySQL's decimal_div returns a plain 0 for a zero dividend
			// before it sizes the quotient.
			return DecVal{n: new(big.Rat), scale: 0}, true
		}
		return decZeroScale(applyDecMul(left, right, n.mulOp, inc))
	case exUnary:
		v, ok := n.x.evalDec(env, inc)
		if !ok || v.null || n.op == '+' {
			return v, ok
		}
		if v.mode == decOmit || v.n == nil {
			if v.mode == decOmit {
				return DecVal{mode: decOmit}, true
			}
			return DecVal{}, false
		}
		if v.intKind && v.unsigned {
			// MySQL negates in unsigned BIGINT (wraps); not modelled.
			return DecVal{}, false
		}
		neg := new(big.Rat).Neg(v.n)
		if v.intKind && !fitsSignedBigint(neg) {
			return DecVal{}, false
		}
		return DecVal{n: neg, scale: v.scale, mode: v.mode, intKind: v.intKind, unsigned: v.unsigned}, true
	case exNum:
		i, ok := new(big.Int).SetString(n.num, 10)
		if !ok {
			return DecVal{}, false
		}
		r := new(big.Rat).SetInt(i)
		return DecVal{n: r, scale: 0, intKind: fitsSignedBigint(r)}, true
	case exNull:
		if n.collate {
			return DecVal{}, false
		}
		return DecVal{null: true}, true
	case exBool:
		if n.collate {
			return DecVal{}, false
		}
		if n.truth {
			return DecVal{n: big.NewRat(1, 1), scale: 0}, true
		}
		return DecVal{n: new(big.Rat), scale: 0}, true
	case exCol:
		if n.collate {
			return DecVal{}, false
		}
		v, ok := env[n.name]
		if !ok || v.kind == genBad {
			return DecVal{}, false
		}
		return decFromGen(v)
	case exCall:
		if n.quoted || !strings.EqualFold(n.name, "MOD") || n.nparts != 2 {
			return DecVal{}, false
		}
		left, ok := n.args[0].evalDec(env, inc)
		if !ok {
			return DecVal{}, false
		}
		right, ok := n.args[1].evalDec(env, inc)
		if !ok {
			return DecVal{}, false
		}
		return decZeroScale(decIntOp(left, right, true))
	}
	// exArrow, exLit: not DECIMAL arithmetic.
	return DecVal{}, false
}

// evalAtom reads one atom at sc (a literal, number, call, or parenthesis)
// and evaluates it with no columns. Used for JSON paths and logged literals.
func (sc *Scan) evalAtom() (GenVal, bool) {
	n, ok := pexAtom(sc)
	if !ok {
		return GenVal{}, false
	}
	return n.evalGen(nil)
}

// walk follows a constant JSON path. An array index on a value that is not
// an array autowraps it, as MySQL does: [0] is the value itself and any
// other index is missing (#237).
func (n *jNode) walk(steps []jStep) (*jNode, bool) {
	cur := n
	for _, step := range steps {
		if cur == nil {
			return nil, false
		}
		if step.isIdx {
			if cur.kind != jArr {
				if step.index != 0 {
					return nil, false
				}
				continue
			}
			if step.index < 0 || step.index >= len(cur.arr) {
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

// decIntOp is DIV (remainder false) or % / MOD (remainder true) on exact
// DECIMAL values. Operands with a fraction come from MySQL's own rounded
// arithmetic (for example a / already cut to its scale), so the result
// follows that rounding (#238). DIV truncates the exact quotient toward
// zero; a quotient outside BIGINT is not claimed. % is exact, takes the
// dividend's sign and the larger operand scale (scale 0 when it is zero). A zero divisor is NULL.
func decIntOp(left, right DecVal, remainder bool) (DecVal, bool) {
	if left.null || right.null {
		return DecVal{null: true}, true
	}
	if right.mode == decClaim && right.n != nil && right.n.Sign() == 0 {
		return DecVal{null: true}, true // whatever the dividend is
	}
	if left.scale == 0 && right.scale == 0 {
		if left.mode != decClaim || right.mode != decClaim || left.n == nil || right.n == nil {
			return DecVal{mode: decOmit}, true
		}
		next, ok := applyIntDiv(intVal{n: left.n}, intVal{n: right.n}, remainder)
		if !ok {
			return DecVal{}, false
		}
		if next.null {
			return DecVal{null: true}, true
		}
		return tagIntKind(placeDec(next.n, 0, false), true, left.unsigned || right.unsigned), true
	}
	// An operand this walk does not claim, or one near DECIMAL's 65 digits
	// (where MySQL's own arithmetic can overflow first), is declined as
	// before this was modelled.
	if left.mode != decClaim || right.mode != decClaim || left.n == nil || right.n == nil ||
		decDigits(left.n, left.scale) > decIntOpMaxDigits || decDigits(right.n, right.scale) > decIntOpMaxDigits {
		return DecVal{}, false
	}
	if (left.unsigned || right.unsigned) && (left.n.Sign() < 0 || right.n.Sign() < 0) {
		return DecVal{}, false
	}
	q := new(big.Rat).Quo(left.n, right.n)
	t := new(big.Int).Quo(q.Num(), q.Denom()) // toward zero
	if !remainder {
		if t.Cmp(maxInt64Big) > 0 || t.Cmp(minInt64Big) < 0 {
			return DecVal{mode: decOmit}, true
		}
		return tagIntKind(placeDec(new(big.Rat).SetInt(t), 0, false), true, left.unsigned || right.unsigned), true
	}
	rem := new(big.Rat).Sub(left.n, new(big.Rat).Mul(right.n, new(big.Rat).SetInt(t)))
	if rem.Sign() == 0 {
		// MySQL's decimal_mod makes a zero remainder a plain 0 (scale 0),
		// which a following / sees.
		return tagIntKind(placeDec(rem, 0, false), true, left.unsigned || right.unsigned), true
	}
	scale := left.scale
	if right.scale > scale {
		scale = right.scale
	}
	return placeDec(rem, scale, false), true
}

// decIntOpMaxDigits bounds the operands of a fractional DIV or %.
const decIntOpMaxDigits = 60

var (
	maxInt64Big = big.NewInt(math.MaxInt64)
	minInt64Big = big.NewInt(math.MinInt64)
)

// fitsSignedBigint reports whether n is an integer in signed BIGINT.
// MySQL's integer + - * DIV raise ERROR 1690 past that range.
func fitsSignedBigint(n *big.Rat) bool {
	if n == nil || !n.IsInt() {
		return false
	}
	i := n.Num()
	return i.Cmp(maxInt64Big) <= 0 && i.Cmp(minInt64Big) >= 0
}

var maxUint64Big = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 64), big.NewInt(1))

// fitsUnsignedBigint reports whether n is an integer in unsigned BIGINT.
// With an UNSIGNED operand MySQL's + - * raise ERROR 1690 below 0 or past
// 2^64-1, where a signed pair would have kept the value.
func fitsUnsignedBigint(n *big.Rat) bool {
	if n == nil || !n.IsInt() {
		return false
	}
	i := n.Num()
	return i.Sign() >= 0 && i.Cmp(maxUint64Big) <= 0
}

// decZeroScale applies MySQL's rule that an arithmetic result of zero is a
// plain 0 with scale 0 (decimal_make_zero), which a later / sees: for
// example 1.0000/((x - x) - 7) has 9 digits, not 9 + x's scale (#238). A
// literal zero keeps its scale; this is only applied to operator results.
func decZeroScale(v DecVal, ok bool) (DecVal, bool) {
	if ok && !v.null && v.mode == decClaim && v.n != nil && v.n.Sign() == 0 {
		v.scale = 0
	}
	return v, ok
}

func decIsZero(v DecVal) bool {
	return !v.null && v.mode == decClaim && v.n != nil && v.n.Sign() == 0
}
