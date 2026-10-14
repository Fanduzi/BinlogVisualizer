// Package analyzer keeps flashback row images on disk, not in memory.
// input: finished flashback transaction groups, in binlog order.
// output: the same groups read forward or last-to-first, with binds restored from a small intern table.
// pos: storage behind Analyzer flashback (ADR 0008). Resident memory is one group plus one distinct bind per table definition.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"binlogviz/internal/model"
)

// flashSpill is an append-only temp file of groups. Each record is
// len(4) payload len(4), so it can be read in either direction with no index.
// The file is unlinked as soon as it is created, so it never outlives the process.
type flashSpill struct {
	f      *os.File
	w      *bufio.Writer
	size   int64
	count  int
	binds  []flashBind
	bindID map[string]uint64
	cols   [][]model.FlashCol
	colsID map[string]uint64
	buf    []byte
}

func newFlashSpill() (*flashSpill, error) {
	f, err := os.CreateTemp("", "binlogviz-flashback-*")
	if err != nil {
		return nil, err
	}
	_ = os.Remove(f.Name())
	return &flashSpill{f: f, w: bufio.NewWriterSize(f, 1<<20), bindID: map[string]uint64{}, colsID: map[string]uint64{}}, nil
}

func (s *flashSpill) close() {
	if s == nil || s.f == nil {
		return
	}
	name := s.f.Name()
	_ = s.f.Close()
	_ = os.Remove(name)
	s.f = nil
}

func (s *flashSpill) internBind(b flashBind) uint64 {
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

// internCols stores each distinct column type list once. Every row of a
// table carries the same list, so this keeps the spill near the image size.
func (s *flashSpill) internCols(cols []model.FlashCol) uint64 {
	var e spillEnc
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

func (s *flashSpill) append(g *flashGroup) error {
	e := spillEnc{b: s.buf[:0]}
	e.str(g.key)
	e.str(g.gtid)
	e.str(g.path)
	e.int(g.pos)
	e.int(g.id)
	e.uint(uint64(g.chunk))
	e.uint(uint64(len(g.rows)))
	for i := range g.rows {
		var bind flashBind
		if i < len(g.binds) {
			bind = g.binds[i]
		}
		e.uint(s.internBind(bind))
		var seq int64
		if i < len(g.seqs) {
			seq = g.seqs[i]
		}
		e.int(seq)
		e.uint(s.internCols(g.rows[i].Cols))
		e.row(&g.rows[i])
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

func (s *flashSpill) flush() error {
	if s == nil {
		return nil
	}
	return s.w.Flush()
}

func (s *flashSpill) read(off int64, n uint32) (*flashGroup, error) {
	if cap(s.buf) < int(n) {
		s.buf = make([]byte, n)
	}
	buf := s.buf[:n]
	if _, err := s.f.ReadAt(buf, off); err != nil {
		return nil, err
	}
	return s.decode(buf)
}

func (s *flashSpill) lenAt(off int64) (uint32, error) {
	var n [4]byte
	if _, err := s.f.ReadAt(n[:], off); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(n[:]), nil
}

// forward calls fn for each spilled group in binlog order.
func (s *flashSpill) forward(fn func(*flashGroup) error) error {
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
		g, err := s.read(off+4, n)
		if err != nil {
			return err
		}
		if err := fn(g); err != nil {
			return err
		}
		off += int64(n) + 8
	}
	return nil
}

// backward calls fn for each spilled group, last first.
func (s *flashSpill) backward(fn func(*flashGroup) error) error {
	if s == nil || s.count == 0 {
		return nil
	}
	if err := s.flush(); err != nil {
		return err
	}
	for end := s.size; end > 0; {
		n, err := s.lenAt(end - 4)
		if err != nil {
			return err
		}
		start := end - 4 - int64(n)
		g, err := s.read(start, n)
		if err != nil {
			return err
		}
		if err := fn(g); err != nil {
			return err
		}
		end = start - 4
	}
	return nil
}

func (s *flashSpill) decode(b []byte) (*flashGroup, error) {
	d := spillDec{b: b}
	g := &flashGroup{key: d.str(), gtid: d.str(), path: d.str(), pos: d.int(), id: d.int(), chunk: int(d.uint())}
	n := int(d.uint())
	if d.err != nil || n < 0 || n > len(b) {
		return nil, errSpillCorrupt
	}
	g.rows = make([]model.FlashRow, n)
	g.binds = make([]flashBind, n)
	g.seqs = make([]int64, n)
	for i := 0; i < n; i++ {
		id := d.uint()
		if id >= uint64(len(s.binds)) {
			return nil, errSpillCorrupt
		}
		g.binds[i] = s.binds[id]
		g.seqs[i] = d.int()
		cid := d.uint()
		if cid >= uint64(len(s.cols)) {
			return nil, errSpillCorrupt
		}
		d.row(&g.rows[i])
		g.rows[i].Cols = s.cols[cid]
	}
	if d.err != nil {
		return nil, d.err
	}
	return g, nil
}

var errSpillCorrupt = errors.New("flashback spill file is corrupt")

type spillEnc struct{ b []byte }

func (e *spillEnc) uint(v uint64) { e.b = binary.AppendUvarint(e.b, v) }
func (e *spillEnc) int(v int64)   { e.b = binary.AppendVarint(e.b, v) }
func (e *spillEnc) bool(v bool) {
	if v {
		e.b = append(e.b, 1)
	} else {
		e.b = append(e.b, 0)
	}
}
func (e *spillEnc) str(v string) { e.uint(uint64(len(v))); e.b = append(e.b, v...) }

// strs keeps nil apart from empty: callers test Before != nil.
func (e *spillEnc) strs(v []string) {
	if v == nil {
		e.uint(0)
		return
	}
	e.uint(uint64(len(v)) + 1)
	for _, s := range v {
		e.str(s)
	}
}

func (e *spillEnc) row(r *model.FlashRow) {
	e.str(r.Schema)
	e.str(r.Table)
	e.str(r.Op)
	e.strs(r.Columns)
	e.strs(r.Before)
	e.strs(r.After)
	if r.PK == nil {
		e.uint(0)
	} else {
		e.uint(uint64(len(r.PK)) + 1)
		for _, p := range r.PK {
			e.int(int64(p))
		}
	}
	e.bool(r.NoPK)
	e.bool(r.NonStrict)
	e.bool(r.ZeroDate)
	e.str(r.ProblemKind)
	e.str(r.ProblemColumn)
	e.str(r.ProblemType)
}

type spillDec struct {
	b   []byte
	err error
}

func (d *spillDec) uint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errSpillCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *spillDec) int() int64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.err = errSpillCorrupt
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *spillDec) bool() bool {
	if d.err != nil || len(d.b) == 0 {
		d.err = errSpillCorrupt
		return false
	}
	v := d.b[0] == 1
	d.b = d.b[1:]
	return v
}

func (d *spillDec) str() string {
	n := d.uint()
	if d.err != nil || n > uint64(len(d.b)) {
		d.err = errSpillCorrupt
		return ""
	}
	s := strings.Clone(string(d.b[:n]))
	d.b = d.b[n:]
	return s
}

func (d *spillDec) strs() []string {
	n := d.uint()
	if d.err != nil || n == 0 {
		return nil
	}
	if n-1 > uint64(len(d.b)) {
		d.err = errSpillCorrupt
		return nil
	}
	out := make([]string, n-1)
	for i := range out {
		out[i] = d.str()
	}
	return out
}

func (d *spillDec) row(r *model.FlashRow) {
	r.Schema = d.str()
	r.Table = d.str()
	r.Op = d.str()
	r.Columns = d.strs()
	r.Before = d.strs()
	r.After = d.strs()
	if n := d.uint(); n > 0 && d.err == nil {
		if n-1 > uint64(len(d.b)) {
			d.err = errSpillCorrupt
			return
		}
		r.PK = make([]int, n-1)
		for i := range r.PK {
			r.PK[i] = int(d.int())
		}
	}
	r.NoPK = d.bool()
	r.NonStrict = d.bool()
	r.ZeroDate = d.bool()
	r.ProblemKind = d.str()
	r.ProblemColumn = d.str()
	r.ProblemType = d.str()
}

// writerFunc lets the renderer target a hash or the output with one code path.
type writerFunc func(string) error

func toWriter(w io.Writer) writerFunc {
	return func(s string) error {
		_, err := io.WriteString(w, s)
		return err
	}
}
