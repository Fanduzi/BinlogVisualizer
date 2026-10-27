// Package analyzer spills the per-(table, minute) series once it no longer fits a small report.
// input: one row or DDL event at a time, from the table aggregator and the minute aggregator, which share one store.
// output: per-minute table row counts and per-table activity points, either copied onto the result or read back from an unlinked temp file. Analysis only appends sorted runs. A full buffer is written and cleared; a revisit after that is a new record in the next run. When more than 8 runs exist they are merged in order and duplicate cells are summed. The high water mark stays within 12× points×68, and after truncate the file is one record per point. The render reads one merged run.
// pos: the (table, minute) term of analyze and flashback. Minute totals stay in memory. This series does not.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"binlogviz/internal/model"
	"golang.org/x/sys/unix"
)

// activityInlineLimit is how many (table, minute) points stay in memory.
// Past that, the series is an unlinked temp file and the result reads it
// back one minute (or one displayed table) at a time. 8192 points is a
// small report: the E2 template's large file is about 20k and spills, and
// its small file stays inline so the bytes of a small report do not change.
// A temp file that cannot be created falls back to memory, as apply-delay does.
var activityInlineLimit = 8192

// SetActivityInlineLimitForTest sets how many (table, minute) points stay
// on the result. The returned function restores the previous limit.
func SetActivityInlineLimitForTest(n int) func() {
	prev := activityInlineLimit
	activityInlineLimit = n
	return func() { activityInlineLimit = prev }
}

// actRec is one table in one minute. 4 + 8*8 bytes on disk, after the minute.
type actRec struct {
	id                                      uint32
	tableRows, rows, insertRows, updateRows uint64
	deleteRows, events, ddl, bytes          uint64
}

const actRecSize = 68

// diskRecSize is the on-disk record: the minute, then actRec.
const diskRecSize = 8 + actRecSize

// spillResidentBytes is the in-memory buffer of cell bytes. A full buffer
// is written out as one sorted run and cleared. Increments, including a
// revisit of a minute already written, only touch this buffer. ponytail:
// 320KiB. WideActivityHeap's 2MiB gap is the ceiling and wants about 1MiB
// free on both sides. This is not an eviction window: nothing is read back
// to make room.
var spillResidentBytes = 320 << 10

// SetSpillResidentBytesForTest sets the buffer budget. The returned
// function restores the previous budget.
func SetSpillResidentBytesForTest(n int) func() {
	prev := spillResidentBytes
	spillResidentBytes = n
	return func() { spillResidentBytes = prev }
}

// spillFileFactor is the spill file's high water mark divided by points×68.
// ponytail: 12. A record is 76 bytes, and a merge keeps at most 8 runs plus
// the merged copy before it truncates. 10×76 is 760 bytes per point and
// 12×68 is 816, so a correct merge fits and a run list that is not merged
// does not.
const spillFileFactor = 12

// spillFinalFactor is the file after that truncate, divided by points×68.
// One 76-byte record per point is under 2×68. Skipping the truncate, or
// keeping duplicate cells, is not.
const spillFinalFactor = 2

// spillIOFactor bounds bytes read plus bytes written against the final file.
// A reload of every minute on each switch moves orders of magnitude more.
const spillIOFactor = 64

// spillRunK is how many sorted runs may sit in the file. One more, and they
// are merged into one. ponytail: 8. Past that the file grows with every
// flush instead of with the points.
const spillRunK = 8

// spillReserve is free space left on the spill filesystem for temps that
// are not optional (flashback SQL, the DuckDB store). Past this the series
// stays in memory instead of filling the disk.
const spillReserve int64 = 32 << 20

// SpillReadError is a failed read of the spill file. The command maps it
// to its own kind; it is not a failure to write the report.
type SpillReadError struct{ Err error }

func (e *SpillReadError) Error() string {
	if e == nil || e.Err == nil {
		return "read spilled activity"
	}
	return "read spilled activity: " + e.Err.Error()
}

func (e *SpillReadError) Unwrap() error { return e.Err }

// spillDiskAvail, when set, replaces the free-space check. Tests use it
// as a small tmpfs. A false ok means the check itself failed.
var spillDiskAvail func() (avail uint64, ok bool)

// SetSpillDiskAvailForTest installs the free-space check. The returned
// function restores the previous one.
func SetSpillDiskAvailForTest(fn func() (uint64, bool)) func() {
	prev := spillDiskAvail
	spillDiskAvail = fn
	return func() { spillDiskAvail = prev }
}

// cellKey is one (minute, table) point. The minute is UTC unix nanos.
type cellKey struct {
	min int64
	id  uint32
}

// run is one sorted spill of the buffer, or a merge of earlier runs.
type run struct {
	off int64
	n   int
}

// span is one minute inside the final merged run.
type span struct {
	min int64
	off int64
	n   int
}

// activityStore is the per-(table, minute) series for one analysis.
// The buffer accumulates every increment. Full buffers become sorted runs.
// Runs merge when there are more than spillRunK of them, and once more at
// the end. The parse never reads a run in order to update it.
type activityStore struct {
	limit int
	n     int
	ids   map[tableIdentity]uint32
	names []tableIdentity

	buf   map[cellKey]*actRec
	runs  []run
	spans []span
	mem   map[int64][]actRec

	off      int64
	file     *os.File
	path     string
	unlinked bool
	noSpill  bool
	// failWrites, when non-zero, fails that many spill writes and keeps the
	// series in memory. Tests use it as a full disk.
	failWrites int
	// ops counts writeAt and readAt calls. written and readBytes are the
	// bytes those calls moved. peak is the largest logical file size.
	ops       int
	written   int64
	readBytes int64
	peak      int64
	err       error
	closed    bool
}

func newActivityStore() *activityStore {
	s := &activityStore{
		limit: activityInlineLimit,
		ids:   make(map[tableIdentity]uint32),
		buf:   make(map[cellKey]*actRec),
	}
	runtime.SetFinalizer(s, (*activityStore).Close)
	return s
}

var errNoRoom = errors.New("activity spill would fill the temp filesystem")

func (s *activityStore) addTable(id tableIdentity, minute time.Time, ev model.NormalizedEvent, isDDL bool) {
	rec := s.touch(minute, id)
	if rec == nil {
		return
	}
	rec.events++
	rec.bytes += uint64(ev.BinlogBytes)
	if isDDL {
		rec.ddl++
		return
	}
	rec.rows += uint64(ev.RowCount)
	switch ev.Operation {
	case "INSERT":
		rec.insertRows += uint64(ev.RowCount)
	case "UPDATE":
		rec.updateRows += uint64(ev.RowCount)
	case "DELETE":
		rec.deleteRows += uint64(ev.RowCount)
	}
}

func (s *activityStore) addRows(id tableIdentity, minute time.Time, rows int) {
	if rows <= 0 {
		return
	}
	rec := s.touch(minute, id)
	if rec == nil {
		return
	}
	rec.tableRows += uint64(rows)
}

func (s *activityStore) touch(minute time.Time, id tableIdentity) *actRec {
	if s.err != nil {
		return nil
	}
	key := cellKey{min: minute.UTC().UnixNano(), id: s.idOf(id)}
	if rec := s.buf[key]; rec != nil {
		return rec
	}
	if !s.noSpill && len(s.buf)*actRecSize > spillResidentBytes {
		if err := s.flushBuffer(); err != nil {
			if isSpillRead(err) {
				s.err = err
				return nil
			}
			s.keepInMemory()
			if s.err != nil {
				return nil
			}
		}
	}
	rec := &actRec{id: key.id}
	s.buf[key] = rec
	s.n++
	return rec
}

func (s *activityStore) idOf(id tableIdentity) uint32 {
	if tid, ok := s.ids[id]; ok {
		return tid
	}
	tid := uint32(len(s.names))
	s.names = append(s.names, id)
	s.ids[id] = tid
	return tid
}

func (s *activityStore) flushAll() {
	if s.err != nil {
		return
	}
	if s.noSpill {
		if err := s.recall(); err != nil {
			s.err = err
		}
		return
	}
	if err := s.flushBuffer(); err != nil {
		if isSpillRead(err) {
			s.err = err
			return
		}
		s.keepInMemory()
		return
	}
	if len(s.runs) > 1 {
		if err := s.mergeRuns(); err != nil {
			if isSpillRead(err) {
				s.err = err
				return
			}
			s.keepInMemory()
			return
		}
	}
	if len(s.runs) == 1 {
		// The merged run has one record per point. Counting buffer inserts
		// would count a revisit again.
		s.n = s.runs[0].n
		if err := s.indexRun(); err != nil {
			s.err = err
		}
	}
}

func (s *activityStore) keepInMemory() {
	s.noSpill = true
	if err := s.recall(); err != nil {
		s.err = err
	}
}

func (s *activityStore) flushBuffer() error {
	if s.noSpill || len(s.buf) == 0 {
		return nil
	}
	keys := make([]cellKey, 0, len(s.buf))
	for k := range s.buf {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].min != keys[j].min {
			return keys[i].min < keys[j].min
		}
		return keys[i].id < keys[j].id
	})
	buf := make([]byte, len(keys)*diskRecSize)
	for i, k := range keys {
		putDisk(buf[i*diskRecSize:], k.min, *s.buf[k])
	}
	if err := s.writeNew(buf); err != nil {
		return err
	}
	s.runs = append(s.runs, run{off: s.off - int64(len(buf)), n: len(keys)})
	clear(s.buf)
	if len(s.runs) > spillRunK {
		return s.mergeRuns()
	}
	return nil
}

// mergeRuns writes every run, summing a cell that appears more than once,
// then copies that output to the front and truncates.
func (s *activityStore) mergeRuns() error {
	if len(s.runs) <= 1 {
		return nil
	}
	cs := make([]runReader, len(s.runs))
	for i, r := range s.runs {
		cs[i] = runReader{base: r.off, n: r.n}
		if err := cs[i].pull(s); err != nil {
			return err
		}
	}
	out := make([]byte, 0, 64*1024)
	var outN int
	var have bool
	var key cellKey
	var acc actRec
	flushOut := func() error {
		if len(out) == 0 {
			return nil
		}
		if err := s.writeNew(out); err != nil {
			return err
		}
		outN += len(out) / diskRecSize
		out = out[:0]
		return nil
	}
	emit := func(k cellKey, rec actRec) error {
		if len(out)+diskRecSize > cap(out) {
			if err := flushOut(); err != nil {
				return err
			}
		}
		n := len(out)
		out = out[:n+diskRecSize]
		putDisk(out[n:], k.min, rec)
		return nil
	}
	for {
		i := pickReader(cs)
		if i < 0 {
			break
		}
		k := cellKey{min: cs[i].min, id: cs[i].rec.id}
		rec := cs[i].rec
		if err := cs[i].pull(s); err != nil {
			return err
		}
		if have && k == key {
			addAct(&acc, rec)
			continue
		}
		if have {
			if err := emit(key, acc); err != nil {
				return err
			}
		}
		key, acc, have = k, rec, true
	}
	if have {
		if err := emit(key, acc); err != nil {
			return err
		}
	}
	if err := flushOut(); err != nil {
		return err
	}
	from := s.off - int64(outN)*diskRecSize
	if err := s.rewind(from, int64(outN)*diskRecSize); err != nil {
		// The copy may have overwritten the inputs. The merged bytes at
		// the end are complete, so a fallback reads those and not the front.
		s.runs = []run{{off: from, n: outN}}
		return err
	}
	s.runs = []run{{off: 0, n: outN}}
	s.spans = nil
	return nil
}

func (s *activityStore) rewind(from, n int64) error {
	if from < n {
		return errors.New("merged activity run overlaps its inputs")
	}
	const chunk = 64 * 1024
	var off int64
	for off < n {
		m := chunk
		if int64(m) > n-off {
			m = int(n - off)
		}
		buf, err := s.readAt(from+off, m)
		if err != nil {
			return err
		}
		if err := s.writeAt(buf, off); err != nil {
			return err
		}
		off += int64(len(buf))
	}
	if err := s.file.Truncate(n); err != nil {
		return err
	}
	s.off = n
	return nil
}

func (s *activityStore) indexRun() error {
	r := s.runs[0]
	s.spans = nil
	const chunkRecs = (1 << 20) / diskRecSize
	var prev int64
	var have bool
	for at := 0; at < r.n; {
		n := r.n - at
		if n > chunkRecs {
			n = chunkRecs
		}
		raw, err := s.readAt(r.off+int64(at)*diskRecSize, n*diskRecSize)
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			min := int64(binary.LittleEndian.Uint64(raw[i*diskRecSize:]))
			if !have || min != prev {
				s.spans = append(s.spans, span{
					min: min,
					off: r.off + int64(at+i)*diskRecSize,
					n:   1,
				})
				prev = min
				have = true
				continue
			}
			s.spans[len(s.spans)-1].n++
		}
		at += n
	}
	return nil
}

// recall sums every run back into the buffer. A failed write uses it so a
// run already on disk is not dropped.
func (s *activityStore) recall() error {
	for _, r := range s.runs {
		c := runReader{base: r.off, n: r.n}
		if err := c.pull(s); err != nil {
			return err
		}
		for c.ok {
			k := cellKey{min: c.min, id: c.rec.id}
			if dst := s.buf[k]; dst != nil {
				addAct(dst, c.rec)
			} else {
				rec := c.rec
				s.buf[k] = &rec
			}
			if err := c.pull(s); err != nil {
				return err
			}
		}
	}
	s.runs = nil
	s.spans = nil
	return nil
}

func (s *activityStore) writeNew(p []byte) error {
	if len(p) == 0 {
		return nil
	}
	if !s.canGrow(int64(len(p))) {
		return errNoRoom
	}
	if err := s.ensureFile(); err != nil {
		return err
	}
	if err := s.writeAt(p, s.off); err != nil {
		return err
	}
	s.off += int64(len(p))
	if s.off > s.peak {
		s.peak = s.off
	}
	return nil
}

type runReader struct {
	base int64
	n    int
	i    int
	buf  []byte
	pos  int
	min  int64
	rec  actRec
	ok   bool
}

func (c *runReader) pull(s *activityStore) error {
	if c.i >= c.n {
		c.ok = false
		return nil
	}
	if c.pos >= len(c.buf)/diskRecSize {
		want := c.n - c.i
		if maxN := (64 * 1024) / diskRecSize; want > maxN {
			want = maxN
		}
		raw, err := s.readAt(c.base+int64(c.i)*diskRecSize, want*diskRecSize)
		if err != nil {
			return err
		}
		c.buf = raw
		c.pos = 0
	}
	b := c.buf[c.pos*diskRecSize:]
	c.min = int64(binary.LittleEndian.Uint64(b[:8]))
	c.rec = takeRec(b[8:])
	c.pos++
	c.i++
	c.ok = true
	return nil
}

func pickReader(cs []runReader) int {
	best := -1
	for i := range cs {
		if !cs[i].ok {
			continue
		}
		if best < 0 || cs[i].min < cs[best].min || (cs[i].min == cs[best].min && cs[i].rec.id < cs[best].rec.id) {
			best = i
		}
	}
	return best
}

func addAct(dst *actRec, src actRec) {
	dst.tableRows += src.tableRows
	dst.rows += src.rows
	dst.insertRows += src.insertRows
	dst.updateRows += src.updateRows
	dst.deleteRows += src.deleteRows
	dst.events += src.events
	dst.ddl += src.ddl
	dst.bytes += src.bytes
}

func (s *activityStore) canGrow(n int64) bool {
	if n <= 0 {
		return true
	}
	avail, ok := s.diskFree()
	if !ok {
		return true
	}
	return avail >= uint64(n)+uint64(spillReserve)
}

func (s *activityStore) diskFree() (uint64, bool) {
	if spillDiskAvail != nil {
		return spillDiskAvail()
	}
	dir := os.TempDir()
	if s.path != "" {
		dir = filepath.Dir(s.path)
	}
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, false
	}
	return st.Bavail * uint64(st.Bsize), true
}

func (s *activityStore) ensureFile() error {
	if s.file != nil {
		return nil
	}
	f, err := os.CreateTemp("", "binlogviz-activity-*")
	if err != nil {
		return err
	}
	s.file = f
	s.path = f.Name()
	s.unlinked = os.Remove(s.path) == nil
	return nil
}

func (s *activityStore) writeAt(p []byte, off int64) error {
	if s.failWrites > 0 {
		s.failWrites--
		return errors.New("activity spill write failed")
	}
	s.ops++
	n, err := s.file.WriteAt(p, off)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrShortWrite
	}
	s.written += int64(n)
	return nil
}

func (s *activityStore) readAt(off int64, n int) ([]byte, error) {
	if n == 0 {
		return nil, nil
	}
	if s.file == nil {
		return nil, &SpillReadError{Err: errors.New("activity series was spilled to a file that is closed")}
	}
	s.ops++
	buf := make([]byte, n)
	if _, err := s.file.ReadAt(buf, off); err != nil {
		return nil, &SpillReadError{Err: err}
	}
	s.readBytes += int64(n)
	return buf, nil
}

func (s *activityStore) spillIO() int64 {
	if s == nil {
		return 0
	}
	return s.written + s.readBytes
}

func (s *activityStore) peakSpill() int64 {
	if s == nil {
		return 0
	}
	if s.peak > s.spillBytes() {
		return s.peak
	}
	return s.spillBytes()
}

// spillBytes is the temp file's size. Zero when the series is in memory.
func (s *activityStore) spillBytes() int64 {
	if s == nil || s.file == nil {
		return 0
	}
	info, err := s.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// inline reports whether Finalize should copy the series onto the result.
// A small series, and a series that could not spill, is copied. A spilled
// series stays on the file and the result reads it through MinuteTables.
func (s *activityStore) inline() bool {
	if s == nil {
		return true
	}
	s.flushAll()
	if s.err != nil {
		return false
	}
	if s.noSpill || s.file == nil || s.n <= s.limit {
		if err := s.loadMem(); err != nil {
			s.err = err
			return false
		}
		s.dropFile()
		return true
	}
	return false
}

func (s *activityStore) loadMem() error {
	if err := s.recall(); err != nil {
		return err
	}
	if len(s.buf) == 0 {
		return nil
	}
	if s.mem == nil {
		s.mem = make(map[int64][]actRec, len(s.buf))
	}
	for k, rec := range s.buf {
		s.mem[k.min] = append(s.mem[k.min], *rec)
	}
	for min, recs := range s.mem {
		sort.Slice(recs, func(i, j int) bool { return recs[i].id < recs[j].id })
		s.mem[min] = recs
	}
	s.buf = map[cellKey]*actRec{}
	return nil
}

func (s *activityStore) dropFile() {
	if s.file == nil {
		return
	}
	_ = s.file.Close()
	if !s.unlinked {
		_ = os.Remove(s.path)
	}
	s.file = nil
	s.runs = nil
	s.spans = nil
	s.off = 0
}

// Err is a spill read that failed. Finalize and report writes surface it.
func (s *activityStore) Err() error {
	if s == nil {
		return nil
	}
	return s.err
}

// TableRows is the per-table row counts for one minute. Nil when the minute
// has no positive row counts. The map is a copy.
func (s *activityStore) TableRows(minute time.Time) map[string]int {
	recs, err := s.records(minute)
	if err != nil {
		if s.err == nil {
			s.err = err
		}
		return nil
	}
	var out map[string]int
	for _, rec := range recs {
		if rec.tableRows == 0 {
			continue
		}
		if out == nil {
			out = make(map[string]int)
		}
		out[s.names[rec.id].String()] = int(rec.tableRows)
	}
	return out
}

// Activity is one table's points, oldest minute first. Nil when the table
// has no activity fields (a minute-only row count is not an activity point).
func (s *activityStore) Activity(schema, table string) []model.TableActivityPoint {
	tid, ok := s.ids[tableIdentity{schema: schema, table: table}]
	if !ok {
		return nil
	}
	var points []model.TableActivityPoint
	take := func(at time.Time, recs []actRec) {
		for _, rec := range recs {
			if rec.id != tid || !rec.hasActivity() {
				continue
			}
			points = append(points, model.TableActivityPoint{
				Minute:      at,
				Rows:        int(rec.rows),
				InsertRows:  int(rec.insertRows),
				UpdateRows:  int(rec.updateRows),
				DeleteRows:  int(rec.deleteRows),
				EventCount:  int(rec.events),
				BinlogBytes: int64(rec.bytes),
				DDLCount:    int(rec.ddl),
			})
		}
	}
	if err := s.eachMinute(take); err != nil {
		if s.err == nil {
			s.err = err
		}
		return nil
	}
	sort.Slice(points, func(i, j int) bool {
		return points[i].Minute.Before(points[j].Minute)
	})
	return points
}

func (s *activityStore) eachMinute(take func(time.Time, []actRec)) error {
	if len(s.mem) > 0 {
		keys := make([]int64, 0, len(s.mem))
		for k := range s.mem {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for _, k := range keys {
			take(time.Unix(0, k).UTC(), s.mem[k])
		}
		return nil
	}
	for _, sp := range s.spans {
		recs, err := s.readSpan(sp)
		if err != nil {
			return err
		}
		take(time.Unix(0, sp.min).UTC(), recs)
	}
	return nil
}

// Close releases the spill file. It is safe to call more than once.
// The result owns a spilled store until it calls Close.
func (s *activityStore) Close() {
	if s == nil || s.closed {
		return
	}
	s.closed = true
	runtime.SetFinalizer(s, nil)
	s.dropFile()
	s.buf = nil
	s.mem = nil
	s.names = nil
	s.ids = nil
}

func (s *activityStore) records(minute time.Time) ([]actRec, error) {
	if s.closed {
		return nil, errors.New("activity series is closed")
	}
	key := minute.UTC().UnixNano()
	if recs, ok := s.mem[key]; ok {
		return recs, nil
	}
	i := sort.Search(len(s.spans), func(i int) bool { return s.spans[i].min >= key })
	if i >= len(s.spans) || s.spans[i].min != key {
		return nil, nil
	}
	return s.readSpan(s.spans[i])
}

func (s *activityStore) readSpan(sp span) ([]actRec, error) {
	raw, err := s.readAt(sp.off, sp.n*diskRecSize)
	if err != nil {
		return nil, err
	}
	recs := make([]actRec, sp.n)
	for i := 0; i < sp.n; i++ {
		recs[i] = takeRec(raw[i*diskRecSize+8:])
	}
	return recs, nil
}

func (r actRec) hasActivity() bool {
	return r.rows|r.insertRows|r.updateRows|r.deleteRows|r.events|r.ddl|r.bytes != 0
}

func isSpillRead(err error) bool {
	var spill *SpillReadError
	return errors.As(err, &spill)
}

func putDisk(b []byte, min int64, r actRec) {
	binary.LittleEndian.PutUint64(b[0:8], uint64(min))
	putRec(b[8:], r)
}

func putRec(b []byte, r actRec) {
	binary.LittleEndian.PutUint32(b[0:4], r.id)
	binary.LittleEndian.PutUint64(b[4:12], r.tableRows)
	binary.LittleEndian.PutUint64(b[12:20], r.rows)
	binary.LittleEndian.PutUint64(b[20:28], r.insertRows)
	binary.LittleEndian.PutUint64(b[28:36], r.updateRows)
	binary.LittleEndian.PutUint64(b[36:44], r.deleteRows)
	binary.LittleEndian.PutUint64(b[44:52], r.events)
	binary.LittleEndian.PutUint64(b[52:60], r.ddl)
	binary.LittleEndian.PutUint64(b[60:68], r.bytes)
}

func takeRec(b []byte) actRec {
	return actRec{
		id:         binary.LittleEndian.Uint32(b[0:4]),
		tableRows:  binary.LittleEndian.Uint64(b[4:12]),
		rows:       binary.LittleEndian.Uint64(b[12:20]),
		insertRows: binary.LittleEndian.Uint64(b[20:28]),
		updateRows: binary.LittleEndian.Uint64(b[28:36]),
		deleteRows: binary.LittleEndian.Uint64(b[36:44]),
		events:     binary.LittleEndian.Uint64(b[44:52]),
		ddl:        binary.LittleEndian.Uint64(b[52:60]),
		bytes:      binary.LittleEndian.Uint64(b[60:68]),
	}
}
