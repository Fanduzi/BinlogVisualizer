// Package analyzer spills the per-(table, minute) series once it no longer fits a small report.
// input: one row or DDL event at a time, from the table aggregator and the minute aggregator, which share one store.
// output: per-minute table row counts and per-table activity points, either copied onto the result or read back from an unlinked temp file. Each spilled minute is one contiguous region of 68-byte records. A region that cannot hold a flush is rewritten at the end at the next power of two slots. Resident cells are capped by bytes. A load is one read of the region, and a flush is one write per dirty run inside it.
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

// spillResidentBytes is how many bytes of cells stay in memory after the
// series spills. A count of minutes evicts the 5th hot minute; interleaved
// channels share the same bytes instead. ponytail: 320KiB of 68-byte cells,
// six channels of 800 tables or twelve of 400. WideActivityHeap's 2MiB gap
// is the ceiling and wants about 1MiB free on both sides. 800KiB left the
// flashback side 150KB under that cap; 336KiB left about 1MiB with nothing
// to spare. Raise this only while both extras keep that room.
var spillResidentBytes = 320 << 10

// SetSpillResidentBytesForTest sets the resident cell budget. The returned
// function restores the previous budget.
func SetSpillResidentBytesForTest(n int) func() {
	prev := spillResidentBytes
	spillResidentBytes = n
	return func() { spillResidentBytes = prev }
}

// spillFileFactor is the spill file's logical size divided by points×68.
// Geometric spare and a hole left by a relocate stay under this once dead
// regions are reused or the file is packed.
const spillFileFactor = 2

// spillReserve is free space left on the spill filesystem for temps that
// are not optional (flashback SQL, the DuckDB store). Past this the series
// stays in memory instead of filling the disk.
const spillReserve int64 = 32 << 20

// region is one minute's bytes. n is the used slots, cap the slots reserved.
type region struct {
	off int64
	n   int
	cap int
}

// minuteState is one minute. recs is append order, which is the disk order.
// ids/pos is a sorted id index into recs. off is -1 until the minute has a region.
type minuteState struct {
	at    time.Time
	recs  []actRec
	ids   []uint32
	pos   []int
	dirty []bool
	off   int64
	cap   int
	nDisk int // prefix of recs already stored at off
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
// Up to activityInlineLimit points live in mem. Past that, spilled minutes
// are regions in an unlinked temp file and spillResidentBytes of cells stay
// in memory. A later event for an earlier minute reloads that minute,
// because the event timestamp is not a high-water mark.
type activityStore struct {
	limit int
	n     int
	ids   map[tableIdentity]uint32
	names []tableIdentity

	live   *minuteState
	recent []*minuteState
	hold   *minuteState

	mem   map[time.Time][]actRec
	index map[time.Time]region
	free  map[int][]int64
	off   int64

	file     *os.File
	path     string
	unlinked bool
	noSpill  bool
	// failWrites, when non-zero, fails that many spill writes and keeps the
	// minute in memory. Tests use it as a full disk.
	failWrites int
	// ops counts writeAt and readAt calls. written is the bytes those writes
	// accepted. Tests budget both.
	ops     int
	written int64
	err     error
	closed  bool
}

func newActivityStore() *activityStore {
	s := &activityStore{
		limit: activityInlineLimit,
		ids:   make(map[tableIdentity]uint32),
		mem:   make(map[time.Time][]actRec),
		index: make(map[time.Time]region),
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
	m := s.live
	tid := s.idOf(id)
	if slot, _, ok := m.lookup(tid); ok {
		m.dirty[slot] = true
		return &m.recs[slot]
	}
	_, at, _ := m.lookup(tid)
	slot := m.appendRec(tid, at)
	s.n++
	if s.spillOK() {
		s.noteSpill()
	}
	return &m.recs[slot]
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
		m := s.live
		s.live = nil
		s.park(m)
	}
	if s.err != nil {
		return
	}
	if m := s.takeRecent(minute); m != nil {
		s.live = m
		return
	}
	if r, ok := s.index[minute]; ok {
		m, err := s.readRegion(r)
		if err != nil {
			s.err = err
			return
		}
		delete(s.index, minute)
		m.at = minute
		s.live = m
		return
	}
	if recs, ok := s.mem[minute]; ok {
		delete(s.mem, minute)
		s.live = minuteFromRecs(minute, recs)
		return
	}
	s.live = &minuteState{at: minute, off: -1}
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
	if m == nil || len(m.recs) == 0 {
		return
	}
	if s.spillOK() {
		s.recent = append(s.recent, m)
		s.trimRecent()
		return
	}
	s.mem[m.at] = recsOf(m)
}

// noteSpill moves minutes that were parked before the series crossed the
// limit into the resident set, then evicts down to the byte budget.
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

func (s *activityStore) residentBytes() int {
	n := 0
	if s.live != nil {
		n += len(s.live.recs) * actRecSize
	}
	for _, m := range s.recent {
		n += len(m.recs) * actRecSize
	}
	return n
}

func (s *activityStore) trimRecent() {
	for len(s.recent) > 4 && s.spillOK() {
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
		s.mem[m.at] = recsOf(m)
	}
}

func (s *activityStore) flushAll() {
	if s.live != nil {
		m := s.live
		s.live = nil
		s.park(m)
	}
	for len(s.recent) > 0 && s.spillOK() {
		s.evictOldest()
	}
	for _, m := range s.recent {
		s.mem[m.at] = recsOf(m)
	}
	s.recent = nil
	if s.err == nil && s.spillOK() && s.file != nil && s.n > 0 {
		limit := int64(spillFileFactor) * int64(s.n) * actRecSize
		if s.off > limit || s.spillBytes() > limit {
			_ = s.compact()
		}
	}
}

// writeMinute keeps the minute in one region. Dirty runs inside that region
// are overwritten in place. A flush that does not fit is rewritten at the
// end with geometric capacity. Dirty bits are cleared only after the write
// succeeds, so a second flush of an unchanged minute writes nothing.
func (s *activityStore) writeMinute(m *minuteState) error {
	if m == nil || len(m.recs) == 0 {
		return nil
	}
	if m.off >= 0 && len(m.recs) <= m.cap {
		if err := s.writeDirtyRuns(m); err != nil {
			return err
		}
	} else if err := s.relocate(m); err != nil {
		return err
	}
	for i := range m.dirty {
		m.dirty[i] = false
	}
	m.nDisk = len(m.recs)
	s.index[m.at] = region{off: m.off, n: len(m.recs), cap: m.cap}
	return nil
}

func (s *activityStore) writeDirtyRuns(m *minuteState) error {
	if err := s.ensureFile(); err != nil {
		return err
	}
	for i := 0; i < len(m.recs); {
		if !m.dirty[i] {
			i++
			continue
		}
		j := i + 1
		for j < len(m.recs) && m.dirty[j] {
			j++
		}
		buf := make([]byte, (j-i)*actRecSize)
		for k := i; k < j; k++ {
			putRec(buf[(k-i)*actRecSize:], m.recs[k])
		}
		if err := s.writeAt(buf, m.off+int64(i)*actRecSize); err != nil {
			return err
		}
		i = j
	}
	return nil
}

func (s *activityStore) relocate(m *minuteState) error {
	slots := nextPow2(len(m.recs))
	s.hold = m
	off, err := s.alloc(slots)
	s.hold = nil
	if err != nil {
		return err
	}
	buf := make([]byte, len(m.recs)*actRecSize)
	for i := range m.recs {
		putRec(buf[i*actRecSize:], m.recs[i])
	}
	if err := s.writeAt(buf, off); err != nil {
		s.undoAlloc(off, slots)
		return err
	}
	if m.off >= 0 {
		s.freeRegion(m.cap, m.off)
	}
	m.off = off
	m.cap = slots
	return nil
}

func (s *activityStore) alloc(slots int) (int64, error) {
	if holes := s.free[slots]; len(holes) > 0 {
		off := holes[len(holes)-1]
		s.free[slots] = holes[:len(holes)-1]
		return off, nil
	}
	nbytes := int64(slots) * actRecSize
	if s.n > 0 && s.off+nbytes > int64(spillFileFactor)*int64(s.n)*actRecSize {
		if err := s.compact(); err != nil {
			return 0, err
		}
	}
	if !s.canGrow(nbytes) {
		return 0, errors.New("activity spill would fill the disk")
	}
	if err := s.ensureFile(); err != nil {
		return 0, err
	}
	off := s.off
	s.off += nbytes
	return off, nil
}

func (s *activityStore) undoAlloc(off int64, slots int) {
	nbytes := int64(slots) * actRecSize
	if off >= 0 && off+nbytes == s.off {
		s.off = off
		return
	}
	s.freeRegion(slots, off)
}

func (s *activityStore) freeRegion(slots int, off int64) {
	if slots <= 0 || off < 0 {
		return
	}
	if s.free == nil {
		s.free = make(map[int][]int64)
	}
	s.free[slots] = append(s.free[slots], off)
}

// placed is one live region, in disk order, for packing the file.
type placed struct {
	off int64
	n   int
	set func(off int64, n int)
}

func (s *activityStore) placed() []placed {
	var out []placed
	add := func(m *minuteState) {
		// Only the prefix already on disk moves. Cells appended since the
		// last flush are still just in this minute.
		if m == nil || m.off < 0 || m.nDisk == 0 {
			return
		}
		out = append(out, placed{off: m.off, n: m.nDisk, set: func(off int64, n int) {
			m.off = off
			m.cap = n
			m.nDisk = n
		}})
	}
	add(s.live)
	if s.hold != nil && s.hold != s.live {
		add(s.hold)
	}
	for _, m := range s.recent {
		if m != s.hold && m != s.live {
			add(m)
		}
	}
	for at, r := range s.index {
		at, r := at, r
		out = append(out, placed{off: r.off, n: r.n, set: func(off int64, n int) {
			s.index[at] = region{off: off, n: n, cap: n}
		}})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].off < out[j].off })
	return out
}

// compact packs live regions to the front of the file, one minute at a time.
func (s *activityStore) compact() error {
	regs := s.placed()
	if s.file == nil {
		return nil
	}
	var packed int64
	for _, r := range regs {
		buf, err := s.readAt(r.off, r.n*actRecSize)
		if err != nil {
			s.err = err
			return err
		}
		if packed != r.off {
			if err := s.writeAt(buf, packed); err != nil {
				s.err = err
				return err
			}
		}
		r.set(packed, r.n)
		packed += int64(r.n) * actRecSize
	}
	s.off = packed
	s.free = nil
	if err := s.file.Truncate(packed); err != nil {
		s.err = err
		return err
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
	return buf, nil
}

// readRegion loads one minute with one read.
func (s *activityStore) readRegion(r region) (*minuteState, error) {
	buf, err := s.readAt(r.off, r.n*actRecSize)
	if err != nil {
		return nil, err
	}
	m := &minuteState{off: r.off, cap: r.cap, nDisk: r.n, recs: make([]actRec, r.n), dirty: make([]bool, r.n)}
	for i := 0; i < r.n; i++ {
		m.recs[i] = takeRec(buf[i*actRecSize:])
	}
	m.reindex()
	return m, nil
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
		take(s.live.at, recsOf(s.live))
	}
	for _, m := range s.recent {
		take(m.at, recsOf(m))
	}
	for at, recs := range s.mem {
		take(at, recs)
	}
	for at, r := range s.index {
		m, err := s.readRegion(r)
		if err != nil {
			return err
		}
		take(at, recsOf(m))
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
	s.hold = nil
	s.index = nil
	s.free = nil
}

func (s *activityStore) records(minute time.Time) ([]actRec, error) {
	if s.closed {
		return nil, errors.New("activity series is closed")
	}
	if s.live != nil && s.live.at.Equal(minute) {
		return recsOf(s.live), nil
	}
	for _, m := range s.recent {
		if m.at.Equal(minute) {
			return recsOf(m), nil
		}
	}
	if recs, ok := s.mem[minute]; ok {
		return recs, nil
	}
	r, ok := s.index[minute]
	if !ok {
		return nil, nil
	}
	m, err := s.readRegion(r)
	if err != nil {
		return nil, err
	}
	return recsOf(m), nil
}

func (m *minuteState) lookup(id uint32) (slot, at int, ok bool) {
	i := sort.Search(len(m.ids), func(i int) bool { return m.ids[i] >= id })
	if i < len(m.ids) && m.ids[i] == id {
		return m.pos[i], i, true
	}
	return 0, i, false
}

func (m *minuteState) appendRec(id uint32, at int) int {
	m.recs = append(m.recs, actRec{id: id})
	m.dirty = append(m.dirty, true)
	slot := len(m.recs) - 1
	m.ids = append(m.ids, 0)
	copy(m.ids[at+1:], m.ids[at:])
	m.ids[at] = id
	m.pos = append(m.pos, 0)
	copy(m.pos[at+1:], m.pos[at:])
	m.pos[at] = slot
	return slot
}

func (m *minuteState) reindex() {
	order := make([]int, len(m.recs))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return m.recs[order[i]].id < m.recs[order[j]].id })
	m.ids = make([]uint32, len(order))
	m.pos = make([]int, len(order))
	for i, p := range order {
		m.ids[i] = m.recs[p].id
		m.pos[i] = p
	}
}

func minuteFromRecs(at time.Time, recs []actRec) *minuteState {
	m := &minuteState{
		at:    at,
		off:   -1,
		recs:  append([]actRec(nil), recs...),
		dirty: make([]bool, len(recs)),
	}
	for i := range m.dirty {
		m.dirty[i] = true
	}
	m.reindex()
	return m
}

func recsOf(m *minuteState) []actRec {
	if m == nil || len(m.recs) == 0 {
		return nil
	}
	recs := append([]actRec(nil), m.recs...)
	sort.Slice(recs, func(i, j int) bool { return recs[i].id < recs[j].id })
	return recs
}

func (r actRec) hasActivity() bool {
	return r.rows|r.insertRows|r.updateRows|r.deleteRows|r.events|r.ddl|r.bytes != 0
}

func nextPow2(n int) int {
	c := 1
	for c < n {
		c <<= 1
		if c <= 0 {
			return n
		}
	}
	return c
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
