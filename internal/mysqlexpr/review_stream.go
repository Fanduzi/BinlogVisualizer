package mysqlexpr

import (
	"strings"

	"binlogviz/internal/i18n"
)

// StreamReview checks one table's generated columns without retaining images.
// Observe is the first pass (default div_precision_increment and column names).
// Finish computes which columns need a clash check. NoteClash is the second
// pass. NoteDiv is the third, and only for a column that mismatched at the
// default, contains "/", and did not clash. Results match GenImages.
type StreamReview struct {
	meta  []SchemaCol
	names []string
	seen  map[string]struct{}
	cols  []streamCol
	n     int
}

type streamCol struct {
	col        SchemaCol
	emptyExpr  bool
	div        bool
	example    string
	unknown    bool
	mismatched bool
	needClash  bool
	refs       []string
	sawClash   bool
	clashMsg   string
	inc        []divInc
	divDone    bool
}

type divInc struct {
	seen    bool
	unknown bool
	bad     bool
}

// NewStreamReview prepares a review for meta. Images are not stored.
func NewStreamReview(meta []SchemaCol) *StreamReview {
	r := &StreamReview{meta: meta, seen: map[string]struct{}{}}
	for _, col := range meta {
		if !col.Generated {
			continue
		}
		empty := strings.TrimSpace(col.Expr) == ""
		r.cols = append(r.cols, streamCol{
			col:       col,
			emptyExpr: empty,
			div:       !empty && strings.Contains(col.Expr, "/") && (col.base == "decimal" || integerAssign(col.base)),
		})
	}
	return r
}

// Observe evaluates im at the default increment and discards it.
func (r *StreamReview) Observe(im LoggedImage) {
	if r == nil {
		return
	}
	r.n++
	for _, name := range im.Columns {
		key := strings.ToLower(name)
		if _, ok := r.seen[key]; ok {
			continue
		}
		if r.seen == nil {
			r.seen = map[string]struct{}{}
		}
		r.seen[key] = struct{}{}
		r.names = append(r.names, name)
	}
	env := loggedEnv(im.Columns, im.Values, r.meta)
	for i := range r.cols {
		c := &r.cols[i]
		if c.emptyExpr || c.mismatched {
			continue
		}
		switch checkGeneratedImage(c.col, mysqlDefaultDivPrecIncrement, im, env) {
		case imageMismatch:
			c.mismatched = true
			c.example = exampleImage(im, r.meta)
			c.unknown = false
		case imageUnknown:
			c.unknown = true
		}
	}
}

// Finish freezes the column-name set and decides which columns need a clash check.
func (r *StreamReview) Finish() {
	if r == nil {
		return
	}
	for i := range r.cols {
		c := &r.cols[i]
		if c.emptyExpr || r.n == 0 {
			c.unknown = true
			c.example = ""
			c.mismatched = false
			c.needClash = false
			continue
		}
		refs, ok := generatedRefs(c.col.Expr, r.names)
		c.refs = refs
		c.needClash = ok && (!c.mismatched || c.div)
	}
}

// NeedsClash reports whether any column still needs dependency records.
func (r *StreamReview) NeedsClash() bool {
	if r == nil {
		return false
	}
	for i := range r.cols {
		if r.cols[i].needClash {
			return true
		}
	}
	return false
}

// NoteClash writes one dependency record per column that needs it.
// seq is this image's index in the review, the same order Observe used.
func (r *StreamReview) NoteClash(seq uint64, im LoggedImage, write func(col int, seq uint64, key, lit string) error) error {
	if r == nil {
		return nil
	}
	for i := range r.cols {
		c := &r.cols[i]
		if !c.needClash {
			continue
		}
		key, ok := refKey(im, c.refs)
		if !ok {
			continue
		}
		lit, found := columnLit(im, c.col.Name)
		if !found {
			continue
		}
		if err := write(i, seq, key, lit); err != nil {
			return err
		}
	}
	return nil
}

// NeedsExactClash reports whether column i needs the clash message, not only a boolean.
func (r *StreamReview) NeedsExactClash(i int) bool {
	if r == nil || i < 0 || i >= len(r.cols) {
		return false
	}
	c := &r.cols[i]
	return c.needClash && !c.mismatched
}

// NeedsDiv reports whether column i still needs the increment sweep.
func (r *StreamReview) NeedsDiv(i int) bool {
	if r == nil || i < 0 || i >= len(r.cols) {
		return false
	}
	c := &r.cols[i]
	return c.div && c.mismatched && !c.sawClash && !c.emptyExpr && r.n > 0 && !c.divDone
}

// NeedsAnyDiv reports whether NoteDiv has work to do.
func (r *StreamReview) NeedsAnyDiv() bool {
	if r == nil {
		return false
	}
	for i := range r.cols {
		if r.NeedsDiv(i) {
			return true
		}
	}
	return false
}

// NoteDiv evaluates increments other than the default. Order does not change the sets.
func (r *StreamReview) NoteDiv(im LoggedImage) {
	if r == nil {
		return
	}
	env := loggedEnv(im.Columns, im.Values, r.meta)
	for i := range r.cols {
		if !r.NeedsDiv(i) {
			continue
		}
		c := &r.cols[i]
		if c.inc == nil {
			c.inc = make([]divInc, mysqlMaxDivPrecIncrement+1)
		}
		for inc := 0; inc <= mysqlMaxDivPrecIncrement; inc++ {
			if inc == mysqlDefaultDivPrecIncrement || c.inc[inc].bad {
				continue
			}
			switch checkGeneratedImage(c.col, inc, im, env) {
			case imageMismatch:
				c.inc[inc].bad = true
			case imageUnknown:
				c.inc[inc].unknown = true
				c.inc[inc].seen = true
			default:
				c.inc[inc].seen = true
			}
		}
	}
}

// MarkDivDone freezes the increment sweep.
func (r *StreamReview) MarkDivDone() {
	if r == nil {
		return
	}
	for i := range r.cols {
		if r.cols[i].div && r.cols[i].mismatched && !r.cols[i].sawClash {
			r.cols[i].divDone = true
		}
	}
}

// Len is the number of generated columns, in schema order.
func (r *StreamReview) Len() int {
	if r == nil {
		return 0
	}
	return len(r.cols)
}

// Col returns generated column i.
func (r *StreamReview) Col(i int) SchemaCol { return r.cols[i].col }

// Example is the one-line image GenImages uses in a mismatch or clash.
func (r *StreamReview) Example(im LoggedImage) string {
	if r == nil {
		return ""
	}
	return exampleImage(im, r.meta)
}

// SetSawClash records that some increment-independent contradiction exists.
func (r *StreamReview) SetSawClash(col int) {
	if r == nil || col < 0 || col >= len(r.cols) {
		return
	}
	r.cols[col].sawClash = true
}

// SetExactClash records the clash message for a column that did not already mismatch.
func (r *StreamReview) SetExactClash(col int, example, lit string) {
	if r == nil || col < 0 || col >= len(r.cols) {
		return
	}
	c := &r.cols[col]
	c.sawClash = true
	c.clashMsg = OneLine(example+i18n.T("error.flashbackSchemaSameInputs")+c.col.Name+"="+exampleCell(lit, c.col, true), 220)
}

// Outcome matches (*GenImages).Outcome for column i.
func (r *StreamReview) Outcome(i int) (example string, unknown bool) {
	c := &r.cols[i]
	if c.emptyExpr || r.n == 0 {
		return "", true
	}
	if c.mismatched {
		return c.example, false
	}
	if c.sawClash {
		return c.clashMsg, false
	}
	return "", c.unknown
}

// DivIncrementFit matches (*GenImages).DivIncrementFit for column i.
func (r *StreamReview) DivIncrementFit(i int) (fit, unchecked []int) {
	c := &r.cols[i]
	if !c.div || c.emptyExpr || r.n == 0 || c.sawClash || !c.divDone || c.inc == nil {
		return nil, nil
	}
	for inc := 0; inc <= mysqlMaxDivPrecIncrement; inc++ {
		if inc == mysqlDefaultDivPrecIncrement {
			continue
		}
		st := c.inc[inc]
		switch {
		case st.bad:
		case st.unknown:
			unchecked = append(unchecked, inc)
		case st.seen:
			fit = append(fit, inc)
		}
	}
	return fit, unchecked
}

func checkGeneratedImage(col SchemaCol, inc int, im LoggedImage, env map[string]GenVal) imageResult {
	g := &GenImages{images: []LoggedImage{im}, envs: []map[string]GenVal{env}}
	return g.checkImage(col, inc, 0)
}
