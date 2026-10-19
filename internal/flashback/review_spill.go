package flashback

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"io"
	"os"
	"sort"
)

// Clash records are sorted in chunks of this many encoded bytes, then merged
// with a fixed fan-in. Resident memory is one chunk plus one record per open run.
const (
	clashChunkBytes = 256 << 10
	clashFanIn      = 16
)

type clashRec struct {
	bucket uint32
	col    uint32
	seq    uint64
	key    string
	lit    string
}

// clashHit is the contradiction GenImages reports for one column:
// firstSeq is the earliest image of that key, lit is the later value.
type clashHit struct {
	firstSeq uint64
	clashSeq uint64
	lit      string
}

type clashSpill struct {
	cur      []clashRec
	curBytes int
	files    []*os.File
	budget   func(total int64) error
	used     int64
}

func (s *clashSpill) add(r clashRec) error {
	n := 20 + len(r.key) + len(r.lit)
	if s.curBytes > 0 && s.curBytes+n > clashChunkBytes {
		if err := s.flush(); err != nil {
			return err
		}
	}
	s.cur = append(s.cur, r)
	s.curBytes += n
	return nil
}

func (s *clashSpill) flush() error {
	if len(s.cur) == 0 {
		return nil
	}
	sort.SliceStable(s.cur, func(i, j int) bool { return clashLess(s.cur[i], s.cur[j]) })
	f, n, err := writeClashRun(s.cur)
	if err != nil {
		return err
	}
	if err := s.charge(n); err != nil {
		_ = f.Close()
		return err
	}
	s.files = append(s.files, f)
	s.cur = nil
	s.curBytes = 0
	return nil
}

func (s *clashSpill) charge(n int64) error {
	if s.budget != nil {
		if err := s.budget(s.used + n); err != nil {
			return err
		}
	}
	s.used += n
	return nil
}

func (s *clashSpill) close() {
	for _, f := range s.files {
		closeClash(f)
	}
	s.files = nil
}

// hits returns, for each bucket and column, the clash GenImages would report.
func (s *clashSpill) hits() (map[uint32]map[uint32]clashHit, error) {
	if err := s.flush(); err != nil {
		return nil, err
	}
	files := s.files
	s.files = nil
	defer func() { closeClashAll(files) }()
	for len(files) > clashFanIn {
		next := make([]*os.File, 0, (len(files)+clashFanIn-1)/clashFanIn)
		for i := 0; i < len(files); i += clashFanIn {
			j := i + clashFanIn
			if j > len(files) {
				j = len(files)
			}
			out, n, err := mergeClashRun(files[i:j])
			if err != nil {
				closeClash(out)
				closeClashAll(next)
				return nil, err
			}
			if err := s.charge(n); err != nil {
				closeClash(out)
				closeClashAll(next)
				return nil, err
			}
			next = append(next, out)
		}
		closeClashAll(files)
		files = next
	}
	return mergeClashHits(files)
}

func writeClashRun(recs []clashRec) (*os.File, int64, error) {
	f, err := os.CreateTemp("", "binlogviz-flashback-review-*")
	if err != nil {
		return nil, 0, err
	}
	_ = os.Remove(f.Name())
	w := bufio.NewWriterSize(f, 1<<16)
	var scratch [4]byte
	var n int64
	for _, r := range recs {
		if err := writeClash(w, scratch[:], r); err != nil {
			_ = f.Close()
			return nil, 0, err
		}
		n += int64(20 + len(r.key) + len(r.lit))
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	return f, n, nil
}

func mergeClashRun(in []*os.File) (*os.File, int64, error) {
	f, err := os.CreateTemp("", "binlogviz-flashback-review-*")
	if err != nil {
		return nil, 0, err
	}
	_ = os.Remove(f.Name())
	w := bufio.NewWriterSize(f, 1<<16)
	var scratch [4]byte
	var n int64
	err = mergeClash(in, func(r clashRec) error {
		if werr := writeClash(w, scratch[:], r); werr != nil {
			return werr
		}
		n += int64(20 + len(r.key) + len(r.lit))
		return nil
	})
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	return f, n, nil
}

func mergeClashHits(in []*os.File) (map[uint32]map[uint32]clashHit, error) {
	out := map[uint32]map[uint32]clashHit{}
	var (
		have         bool
		bucket, col  uint32
		key          string
		firstLit     string
		firstSeq     uint64
		groupClashed bool
	)
	err := mergeClash(in, func(r clashRec) error {
		if !have || r.bucket != bucket || r.col != col || r.key != key {
			have = true
			bucket, col, key = r.bucket, r.col, r.key
			firstLit, firstSeq = r.lit, r.seq
			groupClashed = false
			return nil
		}
		if groupClashed || r.lit == firstLit {
			return nil
		}
		groupClashed = true
		byCol := out[bucket]
		if byCol == nil {
			byCol = map[uint32]clashHit{}
			out[bucket] = byCol
		}
		prev, ok := byCol[col]
		if !ok || r.seq < prev.clashSeq {
			byCol[col] = clashHit{firstSeq: firstSeq, clashSeq: r.seq, lit: r.lit}
		}
		return nil
	})
	return out, err
}

func closeClash(f *os.File) {
	if f == nil {
		return
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
}

func closeClashAll(files []*os.File) {
	for _, f := range files {
		closeClash(f)
	}
}

func clashLess(a, b clashRec) bool {
	if a.bucket != b.bucket {
		return a.bucket < b.bucket
	}
	if a.col != b.col {
		return a.col < b.col
	}
	if a.key != b.key {
		return a.key < b.key
	}
	return a.seq < b.seq
}

func writeClash(w io.Writer, scratch []byte, r clashRec) error {
	binary.LittleEndian.PutUint32(scratch[:4], r.bucket)
	if _, err := w.Write(scratch[:4]); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(scratch[:4], r.col)
	if _, err := w.Write(scratch[:4]); err != nil {
		return err
	}
	var seq [8]byte
	binary.LittleEndian.PutUint64(seq[:], r.seq)
	if _, err := w.Write(seq[:]); err != nil {
		return err
	}
	if err := writeClashStr(w, scratch, r.key); err != nil {
		return err
	}
	return writeClashStr(w, scratch, r.lit)
}

func writeClashStr(w io.Writer, scratch []byte, s string) error {
	binary.LittleEndian.PutUint32(scratch[:4], uint32(len(s)))
	if _, err := w.Write(scratch[:4]); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

type clashSrc struct {
	r   *bufio.Reader
	rec clashRec
	ok  bool
	err error
}

func openClashSrc(f *os.File) (*clashSrc, error) {
	s := &clashSrc{r: bufio.NewReaderSize(f, 1<<16)}
	if err := s.next(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *clashSrc) next() error {
	rec, err := readClash(s.r)
	if err == io.EOF {
		s.ok = false
		return nil
	}
	if err != nil {
		s.ok = false
		s.err = err
		return err
	}
	s.rec = rec
	s.ok = true
	return nil
}

func readClash(r io.Reader) (clashRec, error) {
	var hdr [16]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return clashRec{}, err
	}
	rec := clashRec{
		bucket: binary.LittleEndian.Uint32(hdr[0:4]),
		col:    binary.LittleEndian.Uint32(hdr[4:8]),
		seq:    binary.LittleEndian.Uint64(hdr[8:16]),
	}
	var err error
	if rec.key, err = readClashStr(r); err != nil {
		return clashRec{}, err
	}
	rec.lit, err = readClashStr(r)
	return rec, err
}

func readClashStr(r io.Reader) (string, error) {
	var nbuf [4]byte
	if _, err := io.ReadFull(r, nbuf[:]); err != nil {
		return "", err
	}
	n := binary.LittleEndian.Uint32(nbuf[:])
	if n == 0 {
		return "", nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

type clashHeap []*clashSrc

func (h clashHeap) Len() int { return len(h) }
func (h clashHeap) Less(i, j int) bool {
	return clashLess(h[i].rec, h[j].rec)
}
func (h clashHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *clashHeap) Push(x any)   { *h = append(*h, x.(*clashSrc)) }
func (h *clashHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func mergeClash(in []*os.File, fn func(clashRec) error) error {
	h := &clashHeap{}
	for _, f := range in {
		if f == nil {
			continue
		}
		src, err := openClashSrc(f)
		if err != nil {
			return err
		}
		if src.ok {
			heap.Push(h, src)
		}
	}
	for h.Len() > 0 {
		src := heap.Pop(h).(*clashSrc)
		if err := fn(src.rec); err != nil {
			return err
		}
		if err := src.next(); err != nil {
			return err
		}
		if src.ok {
			heap.Push(h, src)
		}
	}
	return nil
}
