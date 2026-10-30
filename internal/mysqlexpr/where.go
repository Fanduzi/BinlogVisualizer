// Package mysqlexpr: the --where row predicate (ADR 0024).
// input: predicate text compiled once per run, then the emitted literals of
// one row image (before for DELETE/UPDATE, after for INSERT).
// output: whether the row is provably selected. The evaluator is
// three-valued; a row whose predicate is NULL, false, or unverifiable is not
// emitted — the same "unverified is never a match" rule the generated-column
// checker applies. Scalar operands reuse eval.go's grammar; the boolean layer
// here covers = <=> <> != < <= > >=, IS [NOT] NULL/TRUE/FALSE, [NOT] IN,
// AND/OR/XOR/NOT, and parentheses. A function outside the generated-column
// whitelist, a COLLATE clause, or malformed text fails to compile, which the
// CLI reports as a usage error.
// pos: beside eval.go; the comparison and three-valued layer is new.
// note: if this file changes, update this header and module README.md.
package mysqlexpr

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"binlogviz/internal/model"
)

type whereKind uint8

const (
	wAnd whereKind = iota + 1
	wOr
	wXor
	wNot
	wCmp    // scalar op scalar: op
	wIs     // scalar IS [NOT] NULL/TRUE/FALSE: isOp, neg
	wIn     // scalar [NOT] IN (args): neg
	wScalar // bare scalar truth test
)

// whereNode is one node of a compiled predicate. Nodes are shared between
// evaluations and never modified after compiling.
type whereNode struct {
	kind   whereKind
	l, r   *whereNode
	x      *whereNode
	scalar *exprNode
	rhs    *exprNode
	args   []*exprNode
	op     string // = <=> <> != < <= > >=
	isOp   string // NULL TRUE FALSE
	neg    bool
}

// WhereExpr is a compiled --where predicate. refs holds the column names it
// reads, lower-cased; a row event missing one refuses where_column before
// any row is evaluated.
type WhereExpr struct {
	root *whereNode
	refs map[string]struct{}
}

var errWhereSyntax = errors.New("syntax error")

// CompileWhere parses one --where expression. An error means the predicate
// is malformed or outside the supported subset — a usage error, decided
// before any input is read.
func CompileWhere(expr string) (*WhereExpr, error) {
	root, err := compileWhereBody(expr)
	if err != nil {
		return nil, err
	}
	w := &WhereExpr{root: root, refs: map[string]struct{}{}}
	if err := w.collect(root); err != nil {
		return nil, err
	}
	return w, nil
}

func compileWhereBody(body string) (*whereNode, error) {
	sc := &Scan{S: body}
	n, err := pWhereOr(sc)
	if err != nil {
		return nil, err
	}
	sc.Skip()
	if sc.I != len(sc.S) {
		return nil, errWhereSyntax
	}
	return n, nil
}

// MySQL boolean precedence: NOT below comparisons but above AND, then XOR,
// then OR.
func pWhereOr(sc *Scan) (*whereNode, error) {
	left, err := pWhereXor(sc)
	if err != nil {
		return nil, err
	}
	for sc.WordIs("OR") {
		right, err := pWhereXor(sc)
		if err != nil {
			return nil, err
		}
		left = &whereNode{kind: wOr, l: left, r: right}
	}
	return left, nil
}

func pWhereXor(sc *Scan) (*whereNode, error) {
	left, err := pWhereAnd(sc)
	if err != nil {
		return nil, err
	}
	for sc.WordIs("XOR") {
		right, err := pWhereAnd(sc)
		if err != nil {
			return nil, err
		}
		left = &whereNode{kind: wXor, l: left, r: right}
	}
	return left, nil
}

func pWhereAnd(sc *Scan) (*whereNode, error) {
	left, err := pWhereNot(sc)
	if err != nil {
		return nil, err
	}
	for sc.WordIs("AND") {
		right, err := pWhereNot(sc)
		if err != nil {
			return nil, err
		}
		left = &whereNode{kind: wAnd, l: left, r: right}
	}
	return left, nil
}

func pWhereNot(sc *Scan) (*whereNode, error) {
	sc.Skip()
	if sc.WordIs("NOT") {
		x, err := pWhereNot(sc)
		if err != nil {
			return nil, err
		}
		return &whereNode{kind: wNot, x: x}, nil
	}
	return pWherePred(sc)
}

// pWherePred parses one predicate: a scalar optionally followed by a
// comparator, IS test, or IN list; a bare scalar is a truth test. When the
// scalar grammar fails on a parenthesized boolean group ((a AND b)), the
// whole body inside the parens is compiled as a nested predicate.
func pWherePred(sc *Scan) (*whereNode, error) {
	save := *sc
	if left, ok := pexAdd(sc); ok {
		return pWhereTail(sc, left)
	}
	*sc = save
	if sc.PeekByte() != '(' {
		return nil, errWhereSyntax
	}
	body, ok := sc.ParenBody()
	if !ok {
		return nil, errWhereSyntax
	}
	return compileWhereBody(body)
}

func pWhereTail(sc *Scan, left *exprNode) (*whereNode, error) {
	sc.Skip()
	if sc.WordIs("IS") {
		neg := sc.WordIs("NOT")
		op := strings.ToUpper(sc.Word())
		if op == "UNKNOWN" {
			op = "NULL"
		}
		switch op {
		case "NULL", "TRUE", "FALSE":
			return &whereNode{kind: wIs, scalar: left, isOp: op, neg: neg}, nil
		}
		return nil, errWhereSyntax
	}
	save := *sc
	neg := sc.WordIs("NOT")
	if sc.WordIs("IN") {
		body, ok := sc.ParenBody()
		if !ok {
			return nil, errWhereSyntax
		}
		var args []*exprNode
		for _, part := range SplitComma(body) {
			arg, ok := parseExprText(part)
			if !ok {
				return nil, errWhereSyntax
			}
			args = append(args, arg)
		}
		if len(args) == 0 {
			return nil, errWhereSyntax
		}
		return &whereNode{kind: wIn, scalar: left, args: args, neg: neg}, nil
	}
	*sc = save
	if op, ok := consumeCmpOp(sc); ok {
		rhs, ok := pexAdd(sc)
		if !ok {
			return nil, errWhereSyntax
		}
		return &whereNode{kind: wCmp, scalar: left, op: op, rhs: rhs}, nil
	}
	return &whereNode{kind: wScalar, scalar: left}, nil
}

// consumeCmpOp reads a run of comparison operator characters. Anything
// outside the supported set leaves the cursor alone.
func consumeCmpOp(sc *Scan) (string, bool) {
	sc.Skip()
	start := sc.I
	for sc.I < len(sc.S) && strings.IndexByte("<>=!", sc.S[sc.I]) >= 0 {
		sc.I++
	}
	switch sc.S[start:sc.I] {
	case "=", "<=>", "<>", "!=", "<", "<=", ">", ">=":
		return sc.S[start:sc.I], true
	}
	sc.I = start
	return "", false
}

// whereFunction is the generated-column function whitelist the predicate
// shares (ADR 0024). Anything else is a compile error, not a per-row miss.
func whereFunction(name string) bool {
	switch strings.ToUpper(name) {
	case "UPPER", "LOWER", "CONCAT", "CONCAT_WS",
		"LENGTH", "OCTET_LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH",
		"JSON_EXTRACT", "JSON_UNQUOTE", "MOD":
		return true
	default:
		return false
	}
}

// collect validates the compiled tree and records referenced column names.
func (w *WhereExpr) collect(n *whereNode) error {
	switch n.kind {
	case wAnd, wOr, wXor:
		if err := w.collect(n.l); err != nil {
			return err
		}
		return w.collect(n.r)
	case wNot:
		return w.collect(n.x)
	}
	scalars := append([]*exprNode{n.scalar, n.rhs}, n.args...)
	for _, s := range scalars {
		if err := w.checkScalar(s); err != nil {
			return err
		}
	}
	return nil
}

func (w *WhereExpr) checkScalar(n *exprNode) error {
	if n == nil {
		return nil
	}
	if n.collate {
		return errors.New("COLLATE is not supported in --where")
	}
	switch n.kind {
	case exCol:
		w.refs[n.name] = struct{}{}
	case exCall:
		if n.quoted || !whereFunction(n.name) {
			return fmt.Errorf("function %s is not supported in --where", strings.ToUpper(n.name))
		}
	}
	for _, c := range []*exprNode{n.l, n.r, n.x} {
		if err := w.checkScalar(c); err != nil {
			return err
		}
	}
	for _, a := range n.args {
		if err := w.checkScalar(a); err != nil {
			return err
		}
	}
	return nil
}

// Refs is the sorted, lower-cased column name set the predicate reads.
func (w *WhereExpr) Refs() []string {
	out := make([]string, 0, len(w.refs))
	for name := range w.refs {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// References reports whether the predicate reads this column name.
func (w *WhereExpr) References(name string) bool {
	_, ok := w.refs[strings.ToLower(name)]
	return ok
}

// MissingRef is the first referenced column absent from the row event's
// column set, or "" — the where_column refusal the planner raises (ADR 0024).
func (w *WhereExpr) MissingRef(columns []string) string {
	have := make(map[string]struct{}, len(columns))
	for _, name := range columns {
		have[strings.ToLower(name)] = struct{}{}
	}
	for _, ref := range w.Refs() {
		if _, ok := have[ref]; !ok {
			return ref
		}
	}
	return ""
}

// LiteralCell parses one emitted literal — a FlashRow image cell or an
// EmitLiteral output — and tags it with the column's resolved facts, so enum
// labels, unsigned arithmetic, and charset handling apply to comparisons.
// An unparseable literal is genBad: unverifiable, never a match.
func LiteralCell(lit string, col model.FlashCol) GenVal {
	v, ok := parseLoggedValue(lit)
	if !ok {
		return GenVal{kind: genBad}
	}
	return attachFlashMeta(v, col)
}

// attachFlashMeta is attachColMeta for the row-image column descriptor:
// base type, member list, precision, signedness, and the charset the cell's
// own literal did not already declare.
func attachFlashMeta(v GenVal, col model.FlashCol) GenVal {
	v.base = col.Base
	if len(col.Members) > 0 {
		v.members = append([]string(nil), col.Members...)
	}
	if col.HasPrec {
		v.prec, v.scale, v.hasPrec = col.Prec, col.Scale, true
	}
	v.unsigned = col.Unsigned
	if col.Binary {
		v.charset = "binary"
	} else if v.charset == "" {
		v.charset = col.Charset
	}
	return v
}

// wTruth is a three-valued predicate result: false, unknown (NULL or
// unverifiable), true. Only wTrue keeps the row.
type wTruth int8

const (
	wFalse   wTruth = -1
	wUnknown wTruth = 0
	wTrue    wTruth = 1
)

// Match evaluates the predicate on one image env. The env holds the
// referenced columns of the image the row's operation matches on (ADR 0024).
func (w *WhereExpr) Match(env map[string]GenVal) bool {
	return w != nil && w.root != nil && w.root.eval(env) == wTrue
}

func (n *whereNode) eval(env map[string]GenVal) wTruth {
	switch n.kind {
	case wAnd:
		l, r := n.l.eval(env), n.r.eval(env)
		if l == wFalse || r == wFalse {
			return wFalse
		}
		if l == wUnknown || r == wUnknown {
			return wUnknown
		}
		return wTrue
	case wOr:
		l, r := n.l.eval(env), n.r.eval(env)
		if l == wTrue || r == wTrue {
			return wTrue
		}
		if l == wUnknown || r == wUnknown {
			return wUnknown
		}
		return wFalse
	case wXor:
		l, r := n.l.eval(env), n.r.eval(env)
		if l == wUnknown || r == wUnknown {
			return wUnknown
		}
		if l == r {
			return wFalse
		}
		return wTrue
	case wNot:
		switch n.x.eval(env) {
		case wTrue:
			return wFalse
		case wFalse:
			return wTrue
		default:
			return wUnknown
		}
	case wCmp:
		left, ok := n.scalar.evalGen(env)
		if !ok {
			return wUnknown
		}
		right, ok := n.rhs.evalGen(env)
		if !ok {
			return wUnknown
		}
		sign, ok := compareGen(left, right, n.op == "<=>")
		if !ok {
			return wUnknown
		}
		switch n.op {
		case "=", "<=>":
			return wTruthOf(sign == 0)
		case "<>", "!=":
			return wTruthOf(sign != 0)
		case "<":
			return wTruthOf(sign < 0)
		case "<=":
			return wTruthOf(sign <= 0)
		case ">":
			return wTruthOf(sign > 0)
		default: // ">="
			return wTruthOf(sign >= 0)
		}
	case wIs:
		v, ok := n.scalar.evalGen(env)
		if !ok {
			return wUnknown
		}
		var truth wTruth
		switch n.isOp {
		case "NULL":
			truth = wTruthOf(v.kind == genNull)
		default: // TRUE, FALSE
			want := wTrue
			if n.isOp == "FALSE" {
				want = wFalse
			}
			truth = wTruthOf(v.truthValue() == want)
		}
		if n.neg {
			return truth.negate()
		}
		return truth
	case wIn:
		base, ok := n.scalar.evalGen(env)
		if !ok {
			return wUnknown
		}
		unknown := false
		for _, arg := range n.args {
			v, ok := arg.evalGen(env)
			if !ok {
				unknown = true
				continue
			}
			sign, ok := compareGen(base, v, false)
			if !ok {
				unknown = true
				continue
			}
			if sign == 0 {
				return wTruthOf(!n.neg)
			}
		}
		if unknown {
			return wUnknown
		}
		return wTruthOf(n.neg)
	default: // wScalar
		v, ok := n.scalar.evalGen(env)
		if !ok {
			return wUnknown
		}
		return v.truthValue()
	}
}

func wTruthOf(b bool) wTruth {
	if b {
		return wTrue
	}
	return wFalse
}

func (t wTruth) negate() wTruth { return -t }

// truthValue is MySQL's boolean coercion of one value: nonzero numbers are
// true, non-numeric text is 0 (false), NULL and unverifiable are unknown.
func (v GenVal) truthValue() wTruth {
	switch v.kind {
	case genNum:
		if v.n == nil {
			return wUnknown
		}
		return wTruthOf(v.n.Sign() != 0)
	case genText:
		if !v.decoded {
			return wUnknown
		}
		if r, ok := strictNumText(v.text); ok {
			return wTruthOf(r.Sign() != 0)
		}
		return wFalse
	default:
		return wUnknown
	}
}

// compareGen orders two values the way a WHERE comparison can claim exactly
// (ADR 0024): ok=false is unknown, never a match. nullSafe is the <=> rule —
// the only operator that decides NULL operands.
func compareGen(l, r GenVal, nullSafe bool) (int, bool) {
	if l.kind == genBad || r.kind == genBad {
		return 0, false
	}
	if l.kind == genNull || r.kind == genNull {
		if !nullSafe {
			return 0, false
		}
		switch {
		case l.kind == genNull && r.kind == genNull:
			return 0, true
		case l.kind == genNull:
			return -1, true
		default:
			return 1, true
		}
	}
	// An ENUM/SET literal is its index or bitmask; against text the member
	// label is what MySQL compares.
	if l.kind == genNum && len(l.members) > 0 && r.kind == genText {
		if lt, ok := l.coerceText(); ok {
			return compareTextClaim(lt, r)
		}
		return 0, false
	}
	if r.kind == genNum && len(r.members) > 0 && l.kind == genText {
		if rt, ok := r.coerceText(); ok {
			sign, ok := compareTextClaim(rt, l)
			return -sign, ok
		}
		return 0, false
	}
	if l.kind == genNum && r.kind == genNum {
		if l.n == nil || r.n == nil {
			return 0, false
		}
		return l.n.Cmp(r.n), true
	}
	if l.kind == genText && r.kind == genText {
		return compareTextClaim(l, r)
	}
	if l.kind == genNum && r.kind == genText {
		if !r.decoded {
			return 0, false
		}
		n, ok := strictNumText(r.text)
		if !ok || l.n == nil {
			return 0, false
		}
		return l.n.Cmp(n), true
	}
	if l.kind == genText && r.kind == genNum {
		if !l.decoded {
			return 0, false
		}
		n, ok := strictNumText(l.text)
		if !ok || r.n == nil {
			return 0, false
		}
		return n.Cmp(r.n), true
	}
	if l.kind == genJSON && r.kind == genJSON {
		if l.j == nil || r.j == nil || !jsonEqual(l.j, r.j) {
			return 0, false
		}
		return 0, true
	}
	return 0, false
}

// compareTextClaim orders two text values only when byte order and
// case-folded order agree — what holds under _bin and _ci collations alike.
// The binlog carries the charset, never the collation, so a disagreement is
// unknown rather than guessed (ADR 0024).
func compareTextClaim(l, r GenVal) (int, bool) {
	if !l.decoded || !r.decoded {
		return 0, false
	}
	if l.charset == "binary" || r.charset == "binary" {
		if !l.haveRaw || !r.haveRaw {
			return 0, false
		}
		return bytes.Compare(l.raw, r.raw), true
	}
	lf, ok := foldText(l.text, l.charset, false)
	if !ok {
		return 0, false
	}
	rf, ok := foldText(r.text, r.charset, false)
	if !ok {
		return 0, false
	}
	// PAD SPACE collations ignore trailing spaces and NO PAD ones do not; a
	// pair that differs only there orders differently under the two families,
	// so the claim is unverifiable rather than the byte answer.
	if lt, rt := strings.TrimRight(l.text, " "), strings.TrimRight(r.text, " "); lt == rt && l.text != r.text {
		return 0, false
	}
	plain := strings.Compare(l.text, r.text)
	folded := strings.Compare(lf, rf)
	if signOf(plain) != signOf(folded) {
		return 0, false
	}
	return plain, true
}

func signOf(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

// strictNumText parses a full-string decimal literal — no leading-prefix
// coercion. Anything else is unverifiable, not 0-with-a-warning.
func strictNumText(text string) (*big.Rat, bool) {
	s := strings.TrimSpace(text)
	if !numericText(s) {
		return nil, false
	}
	return new(big.Rat).SetString(s)
}

func numericText(s string) bool {
	i := 0
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		i++
	}
	digits := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
		digits++
	}
	if i < len(s) && s[i] == '.' {
		i++
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
			digits++
		}
	}
	if digits == 0 {
		return false
	}
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			i++
		}
		exp := 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
			exp++
		}
		if exp == 0 {
			return false
		}
	}
	return i == len(s)
}
