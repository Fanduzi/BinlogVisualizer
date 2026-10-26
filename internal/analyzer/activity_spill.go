// Package analyzer spills the per-(table, minute) series once it no longer fits a small report.
// input: one row or DDL event at a time, from the table aggregator and the minute aggregator, which share one store.
// output: per-minute table row counts and per-table activity points, either copied onto the result or read back from an unlinked temp file. The file is one 68-byte record per (table, minute). A few recent minutes stay resident; a flush writes each dirty run with one write, and a read is one read per run.
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

// actRec is one table in one minute. 4 + 8*8 bytes on disk.
type actRec struct {
	id                                      uint32
	tableRows, rows, insertRows, updateRows uint64
	deleteRows, events, ddl, bytes          uint64
}

const actRecSize = 68

// spillResident is how many minutes stay in memory after the series spills.
// Two channels a few minutes apart, or a transaction crossing into the next
// minute, keep hitting the same minutes. Holding those avoids a read and a
// write on every switch. ponytail: 4 is enough for that pair plus one
// neighbor; a workload that keeps dozens of minutes hot would raise it.
const spillResident = 4

// spillReserve is free space left on the spill filesystem for temps that
// are not optional (flashback SQL, the DuckDB store). Past this the series
// stays in memory instead of filling the disk.
const spillReserve int64 = 32 << 20

// span is a contiguous run of records. A minute is one or more runs.
type span struct {
	off int64
	n   int
}

// cell is one table in the minute being updated.
type cell struct {
	rec   actRec
	off   int64 // -1 when the cell has no disk slot yet
	dirty bool
}

// minuteState is one minute, either resident or about to be written.
type minuteState struct {
	at    time.Time
	cells map[uint32]*cell
	spans []span
}

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

// activityStore is the per-(table, minute) series for one analysis.
// Up to activityInlineLimit points live in mem. Past that, the last
// spillResident minutes stay in memory and the rest are contiguous runs
// in an unlinked temp file. A later event for an earlier minute reloads
// that minute, because the event timestamp is not a high-water mark.
// Dirty cells are written back as one write per contiguous run.
type activityStore struct {
	limit int
	n     int
	ids   map[tableIdentity]uint32
	names []tableIdentity

	live   *minuteState
	recent []*minuteState

	mem   map[time.Time][]actRec
	index map[time.Time][]span
	off   int64

	file     *os.File
	path     string
	unlinked bool
	noSpill  bool
	// failWrites, when non-zero, fails that many spill writes and keeps the
	// minute in memory. Tests use it as a full disk.
	failWrites int
	// ops counts writeAt and readAt calls. A test budgets it.
	ops    int
	err    error
	closed bool
}

func newActivityStore() *activityStore {
	s := &activityStore{
		limit: activityInlineLimit,
		ids:   make(map[tableIdentity]uint32),
		mem:   make(map[time.Time][]actRec),
		index: make(map[time.Time][]span),
	}
	runtime.SetFinalizer(s, (*activityStore).Close)
	return s
}

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
	s.useMinute(minute)
	if s.err != nil || s.live == nil {
		return nil
	}
	tid := s.idOf(id)
	c := s.live.cells[tid]
	if c == nil {
		c = &cell{off: -1, dirty: true}
		c.rec.id = tid
		s.live.cells[tid] = c
		s.n++
		if s.spillOK() {
			s.noteSpill()
		}
	}
	c.dirty = true
	return &c.rec
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

func (s *activityStore) spillOK() bool {
	return s != nil && !s.noSpill && s.n > s.limit
}

func (s *activityStore) useMinute(minute time.Time) {
	if s.live != nil && s.live.at.Equal(minute) {
		return
	}
	if s.live != nil {
		s.park(s.live)
		s.live = nil
	}
	if s.err != nil {
		return
	}
	if m := s.takeRecent(minute); m != nil {
		s.live = m
		return
	}
	if spans, ok := s.index[minute]; ok {
		cells, err := s.readSpans(spans)
		if err != nil {
			s.err = err
			return
		}
		delete(s.index, minute)
		s.live = &minuteState{at: minute, cells: cells, spans: append([]span(nil), spans...)}
		return
	}
	if recs, ok := s.mem[minute]; ok {
		delete(s.mem, minute)
		s.live = minuteFromRecs(minute, recs)
		return
	}
	s.live = &minuteState{at: minute, cells: make(map[uint32]*cell)}
}

func (s *activityStore) takeRecent(at time.Time) *minuteState {
	for i, m := range s.recent {
		if m.at.Equal(at) {
			s.recent = append(s.recent[:i], s.recent[i+1:]...)
			return m
		}
	}
	return nil
}

func (s *activityStore) park(m *minuteState) {
	if m == nil || len(m.cells) == 0 {
		return
	}
	if s.spillOK() {
		s.recent = append(s.recent, m)
		s.trimRecent()
		return
	}
	s.mem[m.at] = cellsToRecs(m)
}

// noteSpill moves minutes that were parked before the series crossed the
// limit into the resident set, then evicts down to spillResident.
func (s *activityStore) noteSpill() {
	if !s.spillOK() {
		return
	}
	for at, recs := range s.mem {
		s.recent = append(s.recent, minuteFromRecs(at, recs))
		delete(s.mem, at)
	}
	s.trimRecent()
}

func (s *activityStore) trimRecent() {
	for len(s.recent) > spillResident && s.spillOK() {
		s.evictOldest()
	}
}

func (s *activityStore) evictOldest() {
	m := s.recent[0]
	s.recent = s.recent[1:]
	if err := s.writeMinute(m); err != nil {
		// The minute is no longer resident. Put it back in memory. Dropping
		// it here loses table_rows on a full disk.
		s.noSpill = true
		s.mem[m.at] = cellsToRecs(m)
	}
}

func (s *activityStore) flushAll() {
	if s.live != nil {
		s.park(s.live)
		s.live = nil
	}
	for len(s.recent) > 0 && s.spillOK() {
		s.evictOldest()
	}
	for _, m := range s.recent {
		s.mem[m.at] = cellsToRecs(m)
	}
	s.recent = nil
}

// writeMinute overwrites dirty cells that already have a slot, one write
// per contiguous run, and appends cells that have no slot yet as one run.
func (s *activityStore) writeMinute(m *minuteState) error {
	var dirty, fresh []*cell
	for _, c := range m.cells {
		if !c.dirty {
			continue
		}
		if c.off < 0 {
			fresh = append(fresh, c)
		} else {
			dirty = append(dirty, c)
		}
	}
	if len(fresh) > 0 && !s.canGrow(int64(len(fresh))*actRecSize) {
		return errors.New("activity spill would fill the disk")
	}
	if len(dirty) > 0 {
		sort.Slice(dirty, func(i, j int) bool { return dirty[i].off < dirty[j].off })
		if err := s.ensureFile(); err != nil {
			return err
		}
		for i := 0; i < len(dirty); {
			j := i + 1
			buf := make([]byte, (j-i)*actRecSize)
			for k := i; k < j; k++ {
				putRec(buf[(k-i)*actRecSize:], dirty[k].rec)
			}
			if err := s.writeAt(buf, dirty[i].off); err != nil {
				return err
			}
			for k := i; k < j; k++ {
				dirty[k].dirty = false
			}
			i = j
		}
	}
	if len(fresh) > 0 {
		sort.Slice(fresh, func(i, j int) bool { return fresh[i].rec.id < fresh[j].rec.id })
		if err := s.ensureFile(); err != nil {
			return err
		}
		buf := make([]byte, len(fresh)*actRecSize)
		for i, c := range fresh {
			putRec(buf[i*actRecSize:], c.rec)
		}
		off := s.off
		if err := s.writeAt(buf, off); err != nil {
			return err
		}
		s.off += int64(len(buf))
		m.spans = append(m.spans, span{off: off, n: len(fresh)})
		for i, c := range fresh {
			c.off = off + int64(i)*actRecSize
			c.dirty = false
		}
	}
	if len(m.spans) > 0 {
		s.index[m.at] = append([]span(nil), m.spans...)
	}
	return nil
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
	if err := unix.Statfs(dir, &st); err != nil || st.Bsize <= 0 {
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
	return buf, nil
}

func (s *activityStore) readSpans(spans []span) (map[uint32]*cell, error) {
	cells := make(map[uint32]*cell)
	for _, sp := range spans {
		buf, err := s.readAt(sp.off, sp.n*actRecSize)
		if err != nil {
			return nil, err
		}
		for i := 0; i < sp.n; i++ {
			rec := takeRec(buf[i*actRecSize:])
			cells[rec.id] = &cell{rec: rec, off: sp.off + int64(i)*actRecSize}
		}
	}
	return cells, nil
}

// spillBytes is the temp file's logical size. Zero when the series is in memory.
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
	if s.file == nil || s.noSpill {
		return true
	}
	return s.n <= s.limit
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
	if s.live != nil {
		take(s.live.at, cellsToRecs(s.live))
	}
	for _, m := range s.recent {
		take(m.at, cellsToRecs(m))
	}
	for at, recs := range s.mem {
		take(at, recs)
	}
	for at, spans := range s.index {
		cells, err := s.readSpans(spans)
		if err != nil {
			return err
		}
		take(at, cellsToRecs(&minuteState{cells: cells}))
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
	if s.file != nil {
		_ = s.file.Close()
		if !s.unlinked {
			_ = os.Remove(s.path)
		}
		s.file = nil
	}
	s.mem = nil
	s.live = nil
	s.recent = nil
	s.index = nil
}

func (s *activityStore) records(minute time.Time) ([]actRec, error) {
	if s.closed {
		return nil, errors.New("activity series is closed")
	}
	if s.live != nil && s.live.at.Equal(minute) {
		return cellsToRecs(s.live), nil
	}
	for _, m := range s.recent {
		if m.at.Equal(minute) {
			return cellsToRecs(m), nil
		}
	}
	if recs, ok := s.mem[minute]; ok {
		return recs, nil
	}
	spans, ok := s.index[minute]
	if !ok {
		return nil, nil
	}
	cells, err := s.readSpans(spans)
	if err != nil {
		return nil, err
	}
	return cellsToRecs(&minuteState{cells: cells}), nil
}

func minuteFromRecs(at time.Time, recs []actRec) *minuteState {
	m := &minuteState{at: at, cells: make(map[uint32]*cell, len(recs))}
	for i := range recs {
		rec := recs[i]
		m.cells[rec.id] = &cell{rec: rec, off: -1, dirty: true}
	}
	return m
}

func cellsToRecs(m *minuteState) []actRec {
	if m == nil || len(m.cells) == 0 {
		return nil
	}
	recs := make([]actRec, 0, len(m.cells))
	for id, c := range m.cells {
		rec := c.rec
		rec.id = id
		recs = append(recs, rec)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].id < recs[j].id })
	return recs
}

func (r actRec) hasActivity() bool {
	return r.rows|r.insertRows|r.updateRows|r.deleteRows|r.events|r.ddl|r.bytes != 0
}

func compactRecs(live map[uint32]*actRec) []actRec {
	if len(live) == 0 {
		return nil
	}
	recs := make([]actRec, 0, len(live))
	for id, rec := range live {
		cp := *rec
		cp.id = id
		recs = append(recs, cp)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].id < recs[j].id })
	return recs
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
