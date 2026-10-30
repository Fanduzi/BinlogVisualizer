// Package redo keeps staged output records on disk, not in memory.
// input: boundary, statement, comment, and row records in stream order.
// output: the same records read forward, with binds and column lists restored from small intern tables.
// pos: storage behind Planner (ADR 0022). Resident memory is one record; the staged file is unlinked at creation.
// note: if this file changes, update this header and module README.md.
package redo

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/shopspring/decimal"

	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

// Staged record kinds, in the order they are checked at emit.
const (
	recGTID    byte = 1 // group-head comment: gtid, path, pos
	recBegin   byte = 2 // a BEGIN was logged for key
	recClose   byte = 3 // group close verb in text (COMMIT, ROLLBACK)
	recStmt    byte = 4 // verbatim statement: sql in text, schema, xaVerb flag
	recComment byte = 5 // annotation comment: text (one logical line)
	recXAPair  byte = 6 // XA_END + XA PREPARE pair for a marker event: xid in text
	recRows    byte = 7 // row events: rows with binds and seqs
)

// stagedRecord is one unit of staged output. Which fields are populated
// depends on kind; rows records carry rows, binds and seqs, all aligned.
type stagedRecord struct {
	kind   byte
	key    string
	gtid   string
	schema string // default database of the statement, for USE tracking
	path   string
	pos    int64
	text   string
	xaVerb bool
	rows   []model.RedoRow
	binds  []redoBind
	seqs   []int64
}

// redoSpill is an append-only temp file of staged records. Each record is
// len(4) payload len(4), matching the undo spill's framing. The file is
// unlinked as soon as it is created, so it never outlives the process.
type redoSpill struct {
	f      *os.File
	w      *bufio.Writer
	size   int64
	count  int
	binds  []redoBind
	bindID map[string]uint64
	cols   [][]model.FlashCol
	colsID map[string]uint64
	buf    []byte
}

func newRedoSpill() (*redoSpill, error) {
	f, err := os.CreateTemp("", "binlogviz-redo-*")
	if err != nil {
		return nil, err
	}
	_ = os.Remove(f.Name())
	return &redoSpill{f: f, w: bufio.NewWriterSize(f, 1<<20), bindID: map[string]uint64{}, colsID: map[string]uint64{}}, nil
}

func (s *redoSpill) close() {
	if s == nil || s.f == nil {
		return
	}
	name := s.f.Name()
	_ = s.f.Close()
	_ = os.Remove(name)
	s.f = nil
}

// redoBind is the table definition in effect for one staged row. It mirrors
// the undo bind: gen is the generated-name set used to omit columns, cols is
// the resolved definition that supplies MINIMAL names and type facts, and
// the defer/pending flags say the resolution may still be revised.
type redoBind struct {
	gen         map[string]struct{}
	cols        []mysqlexpr.SchemaCol
	deferCheck  bool
	fromPending bool
}

func (s *redoSpill) internBind(b redoBind) uint64 {
	names := make([]string, 0, len(b.gen))
	for name := range b.gen {
		names = append(names, name)
	}
	sort.Strings(names)
	key := fmt.Sprintf("%t|%t|%t|%q|%#v", b.deferCheck, b.fromPending, b.gen != nil, names, b.cols)
	if id, ok := s.bindID[key]; ok {
		return id
	}
	id := uint64(len(s.binds))
	s.binds = append(s.binds, b)
	s.bindID[key] = id
	return id
}

// internCols stores each distinct column descriptor list once. Every row of
// a table carries the same list, so this keeps the spill near the image size.
func (s *redoSpill) internCols(cols []model.FlashCol) uint64 {
	var e recEnc
	if cols == nil {
		e.uint(0)
	} else {
		e.uint(uint64(len(cols)) + 1)
		for _, c := range cols {
			e.str(c.Base)
			e.bool(c.Unsigned)
			e.bool(c.HasSign)
			e.str(c.Charset)
			e.strs(c.Members)
			e.int(int64(c.Prec))
			e.int(int64(c.Scale))
			e.bool(c.HasPrec)
			e.int(int64(c.FSP))
			e.bool(c.HasFSP)
			e.uint(uint64(c.Typ))
			e.uint(uint64(c.Meta))
			e.bool(c.Binary)
		}
	}
	key := string(e.b)
	if id, ok := s.colsID[key]; ok {
		return id
	}
	id := uint64(len(s.cols))
	s.cols = append(s.cols, cols)
	s.colsID[key] = id
	return id
}

func (s *redoSpill) append(rec *stagedRecord) error {
	e := recEnc{b: s.buf[:0]}
	e.uint(uint64(rec.kind))
	e.str(rec.key)
	e.str(rec.gtid)
	e.str(rec.schema)
	e.str(rec.path)
	e.int(rec.pos)
	e.str(rec.text)
	e.bool(rec.xaVerb)
	e.uint(uint64(len(rec.rows)))
	for i := range rec.rows {
		var bind redoBind
		if i < len(rec.binds) {
			bind = rec.binds[i]
		}
		e.uint(s.internBind(bind))
		var seq int64
		if i < len(rec.seqs) {
			seq = rec.seqs[i]
		}
		e.int(seq)
		e.uint(s.internCols(rec.rows[i].Cols))
		e.row(&rec.rows[i])
	}
	s.buf = e.b
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(e.b)))
	for _, part := range [][]byte{n[:], e.b, n[:]} {
		if _, err := s.w.Write(part); err != nil {
			return err
		}
	}
	s.size += int64(len(e.b)) + 8
	s.count++
	return nil
}

func (s *redoSpill) flush() error {
	if s == nil {
		return nil
	}
	return s.w.Flush()
}

func (s *redoSpill) read(off int64, n uint32) (*stagedRecord, error) {
	if cap(s.buf) < int(n) {
		s.buf = make([]byte, n)
	}
	buf := s.buf[:n]
	if _, err := s.f.ReadAt(buf, off); err != nil {
		return nil, err
	}
	return s.decode(buf)
}

func (s *redoSpill) lenAt(off int64) (uint32, error) {
	var n [4]byte
	if _, err := s.f.ReadAt(n[:], off); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(n[:]), nil
}

// forward calls fn for each staged record in stream order.
func (s *redoSpill) forward(fn func(*stagedRecord) error) error {
	if s == nil || s.count == 0 {
		return nil
	}
	if err := s.flush(); err != nil {
		return err
	}
	for off := int64(0); off < s.size; {
		n, err := s.lenAt(off)
		if err != nil {
			return err
		}
		rec, err := s.read(off+4, n)
		if err != nil {
			return err
		}
		if err := fn(rec); err != nil {
			return err
		}
		off += int64(n) + 8
	}
	return nil
}

func (s *redoSpill) decode(b []byte) (*stagedRecord, error) {
	d := recDec{b: b}
	rec := &stagedRecord{
		kind:   byte(d.uint()),
		key:    d.str(),
		gtid:   d.str(),
		schema: d.str(),
		path:   d.str(),
		pos:    d.int(),
		text:   d.str(),
		xaVerb: d.bool(),
	}
	n := int(d.uint())
	if d.err != nil || n < 0 || n > len(b) {
		return nil, errRedoSpillCorrupt
	}
	rec.rows = make([]model.RedoRow, n)
	rec.binds = make([]redoBind, n)
	rec.seqs = make([]int64, n)
	for i := 0; i < n; i++ {
		id := d.uint()
		if id >= uint64(len(s.binds)) {
			return nil, errRedoSpillCorrupt
		}
		rec.binds[i] = s.binds[id]
		rec.seqs[i] = d.int()
		cid := d.uint()
		if cid >= uint64(len(s.cols)) {
			return nil, errRedoSpillCorrupt
		}
		d.row(&rec.rows[i])
		rec.rows[i].Cols = s.cols[cid]
	}
	if d.err != nil {
		return nil, d.err
	}
	return rec, nil
}

var errRedoSpillCorrupt = errors.New("redo spill file is corrupt")

type recEnc struct{ b []byte }

func (e *recEnc) uint(v uint64) { e.b = binary.AppendUvarint(e.b, v) }
func (e *recEnc) int(v int64)   { e.b = binary.AppendVarint(e.b, v) }
func (e *recEnc) bool(v bool) {
	if v {
		e.b = append(e.b, 1)
	} else {
		e.b = append(e.b, 0)
	}
}
func (e *recEnc) str(v string) { e.uint(uint64(len(v))); e.b = append(e.b, v...) }

// strs keeps nil apart from empty: callers test slices for nil.
func (e *recEnc) strs(v []string) {
	if v == nil {
		e.uint(0)
		return
	}
	e.uint(uint64(len(v)) + 1)
	for _, s := range v {
		e.str(s)
	}
}

func (e *recEnc) ints(v []int) {
	if v == nil {
		e.uint(0)
		return
	}
	e.uint(uint64(len(v)) + 1)
	for _, p := range v {
		e.int(int64(p))
	}
}

// docs encodes per-column binary JSON documents: nil slice vs len, then a
// presence byte plus bytes for each cell.
func (e *recEnc) docs(v [][]byte) {
	if v == nil {
		e.uint(0)
		return
	}
	e.uint(uint64(len(v)) + 1)
	for _, doc := range v {
		e.bool(doc != nil)
		if doc != nil {
			e.uint(uint64(len(doc)))
			e.b = append(e.b, doc...)
		}
	}
}

// Cell value tags for the image codec. The decode must return the same Go
// shape the emitter's helpers assert on.
const (
	cellNil byte = iota
	cellInt
	cellUint
	cellFloat32
	cellFloat64
	cellString
	cellBytes
	cellDecimal
	cellJSONDiff
	cellOpaque
)

// opaqueCell marks a decoded value type the codec could not represent. It
// fails every type assertion in the emitter, which refuses it by type name.
type opaqueCell struct{}

func (e *recEnc) cell(v any) {
	switch typed := v.(type) {
	case nil:
		e.b = append(e.b, cellNil)
	case int:
		e.b = append(e.b, cellInt)
		e.int(int64(typed))
	case int8:
		e.b = append(e.b, cellInt)
		e.int(int64(typed))
	case int16:
		e.b = append(e.b, cellInt)
		e.int(int64(typed))
	case int32:
		e.b = append(e.b, cellInt)
		e.int(int64(typed))
	case int64:
		e.b = append(e.b, cellInt)
		e.int(typed)
	case uint64:
		e.b = append(e.b, cellUint)
		e.uint(typed)
	case float32:
		e.b = append(e.b, cellFloat32)
		var f [4]byte
		binary.LittleEndian.PutUint32(f[:], math.Float32bits(typed))
		e.b = append(e.b, f[:]...)
	case float64:
		e.b = append(e.b, cellFloat64)
		var f [8]byte
		binary.LittleEndian.PutUint64(f[:], math.Float64bits(typed))
		e.b = append(e.b, f[:]...)
	case string:
		e.b = append(e.b, cellString)
		e.str(typed)
	case []byte:
		e.b = append(e.b, cellBytes)
		e.uint(uint64(len(typed)))
		e.b = append(e.b, typed...)
	case decimal.Decimal:
		e.b = append(e.b, cellDecimal)
		e.str(typed.String())
	case *replication.JsonDiff:
		e.b = append(e.b, cellJSONDiff)
	default:
		e.b = append(e.b, cellOpaque)
	}
}

func (e *recEnc) image(v []any) {
	if v == nil {
		e.uint(0)
		return
	}
	e.uint(uint64(len(v)) + 1)
	for _, cell := range v {
		e.cell(cell)
	}
}

func (e *recEnc) row(r *model.RedoRow) {
	e.str(r.Schema)
	e.str(r.Table)
	e.str(r.Op)
	e.strs(r.Columns)
	e.str(r.Names)
	e.image(r.Before)
	e.image(r.After)
	e.docs(r.BeforeDocs)
	e.docs(r.AfterDocs)
	e.ints(r.PK)
	e.bool(r.NoPK)
	e.bool(r.NonStrict)
	e.bool(r.ZeroDate)
	e.str(r.ProblemKind)
	e.str(r.ProblemColumn)
	e.str(r.ProblemType)
}

type recDec struct {
	b   []byte
	err error
}

func (d *recDec) uint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errRedoSpillCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *recDec) int() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errRedoSpillCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *recDec) bool() bool {
	if d.err != nil || len(d.b) == 0 {
		d.err = errRedoSpillCorrupt
		return false
	}
	v := d.b[0] == 1
	d.b = d.b[1:]
	return v
}

func (d *recDec) str() string {
	n := d.uint()
	if d.err != nil || n > uint64(len(d.b)) {
		d.err = errRedoSpillCorrupt
		return ""
	}
	v := string(d.b[:n])
	d.b = d.b[n:]
	return v
}

func (d *recDec) strs() []string {
	n := d.uint()
	if d.err != nil {
		return nil
	}
	if n == 0 {
		return nil
	}
	out := make([]string, int(n)-1)
	for i := range out {
		out[i] = d.str()
	}
	return out
}

func (d *recDec) ints() []int {
	n := d.uint()
	if d.err != nil {
		return nil
	}
	if n == 0 {
		return nil
	}
	out := make([]int, int(n)-1)
	for i := range out {
		out[i] = int(d.int())
	}
	return out
}

func (d *recDec) docs() [][]byte {
	n := d.uint()
	if d.err != nil {
		return nil
	}
	if n == 0 {
		return nil
	}
	out := make([][]byte, int(n)-1)
	for i := range out {
		if !d.bool() {
			continue
		}
		m := d.uint()
		if d.err != nil || m > uint64(len(d.b)) {
			d.err = errRedoSpillCorrupt
			return nil
		}
		out[i] = append([]byte(nil), d.b[:m]...)
		d.b = d.b[m:]
	}
	return out
}

func (d *recDec) cell() any {
	if d.err != nil || len(d.b) == 0 {
		d.err = errRedoSpillCorrupt
		return nil
	}
	tag := d.b[0]
	d.b = d.b[1:]
	switch tag {
	case cellNil:
		return nil
	case cellInt:
		return d.int()
	case cellUint:
		return d.uint()
	case cellFloat32:
		if len(d.b) < 4 {
			d.err = errRedoSpillCorrupt
			return nil
		}
		v := math.Float32frombits(binary.LittleEndian.Uint32(d.b[:4]))
		d.b = d.b[4:]
		return v
	case cellFloat64:
		if len(d.b) < 8 {
			d.err = errRedoSpillCorrupt
			return nil
		}
		v := math.Float64frombits(binary.LittleEndian.Uint64(d.b[:8]))
		d.b = d.b[8:]
		return v
	case cellString:
		return d.str()
	case cellBytes:
		m := d.uint()
		if d.err != nil || m > uint64(len(d.b)) {
			d.err = errRedoSpillCorrupt
			return nil
		}
		v := append([]byte(nil), d.b[:m]...)
		d.b = d.b[m:]
		return v
	case cellDecimal:
		text := d.str()
		v, err := decimal.NewFromString(text)
		if err != nil {
			d.err = errRedoSpillCorrupt
			return nil
		}
		return v
	case cellJSONDiff:
		return &replication.JsonDiff{}
	case cellOpaque:
		return opaqueCell{}
	default:
		d.err = errRedoSpillCorrupt
		return nil
	}
}

func (d *recDec) image() []any {
	n := d.uint()
	if d.err != nil {
		return nil
	}
	if n == 0 {
		return nil
	}
	out := make([]any, int(n)-1)
	for i := range out {
		out[i] = d.cell()
	}
	return out
}

func (d *recDec) row(r *model.RedoRow) {
	r.Schema = d.str()
	r.Table = d.str()
	r.Op = d.str()
	r.Columns = d.strs()
	r.Names = d.str()
	r.Before = d.image()
	r.After = d.image()
	r.BeforeDocs = d.docs()
	r.AfterDocs = d.docs()
	r.PK = d.ints()
	r.NoPK = d.bool()
	r.NonStrict = d.bool()
	r.ZeroDate = d.bool()
	r.ProblemKind = d.str()
	r.ProblemColumn = d.str()
	r.ProblemType = d.str()
}
