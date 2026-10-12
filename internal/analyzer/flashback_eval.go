// Package analyzer: the generated-column expression evaluator.
// input: a generated-column expression from a schema file, logged column values, and (for DECIMAL arithmetic) a div_precision_increment.
// output: the value MySQL would compute, or ok=false when the expression uses something this checker does not model. One grammar, parsed once per expression text into a tree (cached); one evaluator walks that tree in the general domain (integers, text, JSON; evalGenExpr) or the exact-DECIMAL domain (evalDecExprInc). The DECIMAL domain accepts only its subset: numbers, columns, NULL/TRUE/FALSE, + - * / DIV % MOD, MOD(), and parentheses; it follows MySQL's per-operation DECIMAL rounding, including DIV and % over a rounded / (#238). JSON paths autowrap a non-array for [0] (#237).
// pos: flashback-only module. Analyze does not call it. Value helpers (genVal, decVal, apply*) live in flashback_expr.go and flashback_schema.go.
// note: if this file changes, update this header, CONTEXT.md (Expression evaluator) and docs/adr/0007-expression-evaluator.md.
package analyzer

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
	lit     genVal
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
	sc := &sqlScan{s: expr}
	n, ok := pexAdd(sc)
	if !ok {
		return nil, false
	}
	sc.skip()
	if sc.i != len(sc.s) {
		return nil, false
	}
	return n, true
}

func pexAdd(sc *sqlScan) (*exprNode, bool) {
	left, ok := pexMul(sc)
	if !ok {
		return nil, false
	}
	for {
		sc.skip()
		if sc.i >= len(sc.s) || (sc.s[sc.i] != '+' && sc.s[sc.i] != '-') {
			return left, true
		}
		// `->` and `->>` belong to a primary. A following arrow is not subtraction.
		if sc.startsArrow() {
			return left, true
		}
		op := sc.s[sc.i]
		sc.i++
		right, ok := pexMul(sc)
		if !ok {
			return nil, false
		}
		left = &exprNode{kind: exAdd, op: op, l: left, r: right}
	}
}

func pexMul(sc *sqlScan) (*exprNode, bool) {
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

func pexUnary(sc *sqlScan) (*exprNode, bool) {
	sc.skip()
	if sc.i < len(sc.s) && (sc.s[sc.i] == '-' || sc.s[sc.i] == '+') && !sc.startsArrow() {
		op := sc.s[sc.i]
		sc.i++
		x, ok := pexUnary(sc)
		if !ok {
			return nil, false
		}
		return &exprNode{kind: exUnary, op: op, x: x}, true
	}
	return pexPrimary(sc)
}

func pexPrimary(sc *sqlScan) (*exprNode, bool) {
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

func pexAtom(sc *sqlScan) (*exprNode, bool) {
	sc.skip()
	if sc.i >= len(sc.s) {
		return nil, false
	}
	switch sc.s[sc.i] {
	case '(':
		body, ok := sc.parenBody()
		if !ok {
			return nil, false
		}
		return parseExprText(body)
	case '\'', '"':
		v, ok := sc.quotedText("utf8mb4")
		if !ok {
			return nil, false
		}
		return &exprNode{kind: exLit, lit: v}, true
	}
	if sc.s[sc.i] == 'X' || sc.s[sc.i] == 'x' {
		if v, ok, parsed := sc.hexBinary(); parsed {
			if !ok {
				return nil, false
			}
			return &exprNode{kind: exLit, lit: v}, true
		}
	}
	if sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9' {
		start := sc.i
		for sc.i < len(sc.s) && sc.s[sc.i] >= '0' && sc.s[sc.i] <= '9' {
			sc.i++
		}
		if sc.i < len(sc.s) && (sc.s[sc.i] == '.' || sc.s[sc.i] == 'e' || sc.s[sc.i] == 'E') {
			return nil, false
		}
		return &exprNode{kind: exNum, num: sc.s[start:sc.i]}, true
	}
	quoted := sc.s[sc.i] == '`'
	name, ok := sc.ident()
	if !ok {
		return nil, false
	}
	if !quoted && strings.HasPrefix(name, "_") {
		if v, ok, parsed := sc.introducedLiteral(name); parsed {
			if !ok {
				return nil, false
			}
			return &exprNode{kind: exLit, lit: v}, true
		}
	}
	if sc.peekByte() == '(' {
		body, ok := sc.parenBody()
		if !ok {
			return nil, false
		}
		call := &exprNode{kind: exCall, name: name, quoted: quoted}
		if strings.TrimSpace(body) != "" {
			parts := splitComma(body)
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
	save := sc.i
	sc.skipCollate()
	collate := sc.i != save
	if !quoted && isNullWord(name) {
		return &exprNode{kind: exNull, collate: collate}, true
	}
	if !quoted && isBoolWord(name) {
		return &exprNode{kind: exBool, truth: strings.EqualFold(name, "TRUE"), collate: collate}, true
	}
	return &exprNode{kind: exCol, name: strings.ToLower(name), collate: collate}, true
}

// evalGenExpr evaluates expr with logged column values.
// ok is false when the expression uses something this checker does not evaluate.
func evalGenExpr(expr string, env map[string]genVal) (genVal, bool) {
	n, ok := parseExprTree(expr)
	if !ok {
		return genVal{}, false
	}
	v, ok := n.evalGen(env)
	if !ok {
		return genVal{}, false
	}
	return v, true
}

func (n *exprNode) evalGen(env map[string]genVal) (genVal, bool) {
	switch n.kind {
	case exAdd:
		left, ok := n.l.evalGen(env)
		if !ok {
			return genVal{}, false
		}
		right, ok := n.r.evalGen(env)
		if !ok {
			return genVal{}, false
		}
		return applyGenAdd(left, right, n.op)
	case exMul:
		left, ok := n.l.evalGen(env)
		if !ok {
			return genVal{}, false
		}
		right, ok := n.r.evalGen(env)
		if !ok {
			return genVal{}, false
		}
		li, ok1 := left.asInt()
		ri, ok2 := right.asInt()
		if !ok1 || !ok2 {
			return genVal{}, false
		}
		next, ok := applyMulOp(li, ri, n.mulOp)
		if !ok {
			return genVal{}, false
		}
		if next.null {
			return genNullVal(), true
		}
		return genVal{kind: genNum, n: next.n}, true
	case exUnary:
		v, ok := n.x.evalGen(env)
		if !ok || v.kind == genNull || n.op == '+' {
			return v, ok
		}
		iv, ok := v.asInt()
		if !ok || iv.null {
			return v, ok
		}
		return genVal{kind: genNum, n: new(big.Rat).Neg(iv.n)}, true
	case exArrow:
		v, ok := n.x.evalGen(env)
		if !ok {
			return genVal{}, false
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
			return genVal{}, false
		}
		return v, true
	case exCall:
		var args []genVal
		for _, arg := range n.args {
			v, ok := arg.evalGen(env)
			if !ok {
				return genVal{}, false
			}
			args = append(args, v)
		}
		return evalGenCall(n.name, args)
	}
	return genVal{}, false
}

// clone copies a parsed literal so an evaluation cannot change the tree.
func (v genVal) clone() genVal {
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

func evalDecExpr(expr string, env map[string]genVal) (decVal, bool) {
	return evalDecExprInc(expr, env, mysqlDefaultDivPrecIncrement)
}

// evalDecExprInc evaluates expr exactly as DECIMAL, with / at
// div_precision_increment inc.
func evalDecExprInc(expr string, env map[string]genVal, inc int) (decVal, bool) {
	if inc+1 == 0 {
		inc = mysqlDefaultDivPrecIncrement
	}
	n, ok := parseExprTree(expr)
	if !ok {
		return decVal{}, false
	}
	v, ok := n.evalDec(env, inc)
	if !ok {
		return decVal{}, false
	}
	return v, true
}

func (n *exprNode) evalDec(env map[string]genVal, inc int) (decVal, bool) {
	switch n.kind {
	case exAdd:
		left, ok := n.l.evalDec(env, inc)
		if !ok {
			return decVal{}, false
		}
		right, ok := n.r.evalDec(env, inc)
		if !ok {
			return decVal{}, false
		}
		return decZeroScale(applyDecAdd(left, right, n.op))
	case exMul:
		left, ok := n.l.evalDec(env, inc)
		if !ok {
			return decVal{}, false
		}
		right, ok := n.r.evalDec(env, inc)
		if !ok {
			return decVal{}, false
		}
		if n.mulOp == "/" && decIsZero(left) && right.mode == decClaim && right.n != nil && right.n.Sign() != 0 {
			// MySQL's decimal_div returns a plain 0 for a zero dividend
			// before it sizes the quotient.
			return decVal{n: new(big.Rat), scale: 0}, true
		}
		return decZeroScale(applyDecMul(left, right, n.mulOp, inc))
	case exUnary:
		v, ok := n.x.evalDec(env, inc)
		if !ok || v.null || n.op == '+' {
			return v, ok
		}
		if v.mode == decOmit || v.n == nil {
			if v.mode == decOmit {
				return decVal{mode: decOmit}, true
			}
			return decVal{}, false
		}
		return decVal{n: new(big.Rat).Neg(v.n), scale: v.scale, mode: v.mode}, true
	case exNum:
		i, ok := new(big.Int).SetString(n.num, 10)
		if !ok {
			return decVal{}, false
		}
		return decVal{n: new(big.Rat).SetInt(i), scale: 0}, true
	case exNull:
		if n.collate {
			return decVal{}, false
		}
		return decVal{null: true}, true
	case exBool:
		if n.collate {
			return decVal{}, false
		}
		if n.truth {
			return decVal{n: big.NewRat(1, 1), scale: 0}, true
		}
		return decVal{n: new(big.Rat), scale: 0}, true
	case exCol:
		if n.collate {
			return decVal{}, false
		}
		v, ok := env[n.name]
		if !ok || v.kind == genBad {
			return decVal{}, false
		}
		return decFromGen(v)
	case exCall:
		if n.quoted || !strings.EqualFold(n.name, "MOD") || n.nparts != 2 {
			return decVal{}, false
		}
		left, ok := n.args[0].evalDec(env, inc)
		if !ok {
			return decVal{}, false
		}
		right, ok := n.args[1].evalDec(env, inc)
		if !ok {
			return decVal{}, false
		}
		return decZeroScale(decIntOp(left, right, true))
	}
	// exArrow, exLit: not DECIMAL arithmetic.
	return decVal{}, false
}

// evalAtom reads one atom at sc (a literal, number, call, or parenthesis)
// and evaluates it with no columns. Used for JSON paths and logged literals.
func (sc *sqlScan) evalAtom() (genVal, bool) {
	n, ok := pexAtom(sc)
	if !ok {
		return genVal{}, false
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
func decIntOp(left, right decVal, remainder bool) (decVal, bool) {
	if left.null || right.null {
		return decVal{null: true}, true
	}
	if right.mode == decClaim && right.n != nil && right.n.Sign() == 0 {
		return decVal{null: true}, true // whatever the dividend is
	}
	if left.scale == 0 && right.scale == 0 {
		if left.mode != decClaim || right.mode != decClaim || left.n == nil || right.n == nil {
			return decVal{mode: decOmit}, true
		}
		next, ok := applyIntDiv(intVal{n: left.n}, intVal{n: right.n}, remainder)
		if !ok {
			return decVal{}, false
		}
		if next.null {
			return decVal{null: true}, true
		}
		return placeDec(next.n, 0, false), true
	}
	// An operand this walk does not claim, or one near DECIMAL's 65 digits
	// (where MySQL's own arithmetic can overflow first), is declined as
	// before this was modelled.
	if left.mode != decClaim || right.mode != decClaim || left.n == nil || right.n == nil ||
		decDigits(left.n, left.scale) > decIntOpMaxDigits || decDigits(right.n, right.scale) > decIntOpMaxDigits {
		return decVal{}, false
	}
	q := new(big.Rat).Quo(left.n, right.n)
	t := new(big.Int).Quo(q.Num(), q.Denom()) // toward zero
	if !remainder {
		if t.Cmp(maxInt64Big) > 0 || t.Cmp(minInt64Big) < 0 {
			return decVal{mode: decOmit}, true
		}
		return placeDec(new(big.Rat).SetInt(t), 0, false), true
	}
	rem := new(big.Rat).Sub(left.n, new(big.Rat).Mul(right.n, new(big.Rat).SetInt(t)))
	if rem.Sign() == 0 {
		// MySQL's decimal_mod makes a zero remainder a plain 0 (scale 0),
		// which a following / sees.
		return placeDec(rem, 0, false), true
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

// decZeroScale applies MySQL's rule that an arithmetic result of zero is a
// plain 0 with scale 0 (decimal_make_zero), which a later / sees: for
// example 1.0000/((x - x) - 7) has 9 digits, not 9 + x's scale (#238). A
// literal zero keeps its scale; this is only applied to operator results.
func decZeroScale(v decVal, ok bool) (decVal, bool) {
	if ok && !v.null && v.mode == decClaim && v.n != nil && v.n.Sign() == 0 {
		v.scale = 0
	}
	return v, ok
}

func decIsZero(v decVal) bool {
	return !v.null && v.mode == decClaim && v.n != nil && v.n.Sign() == 0
}
