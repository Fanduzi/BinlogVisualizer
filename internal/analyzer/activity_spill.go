// Package analyzer spills the per-(table, minute) series once it no longer fits a small report.
// input: one row or DDL event at a time, from the table aggregator and the minute aggregator, which share one store.
// output: per-minute table row counts and per-table activity points, either copied onto the result or read back from an unlinked temp file.
// pos: the (table, minute) term of analyze and flashback. Minute totals stay in memory. This series does not.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"time"

	"binlogviz/internal/model"
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

type actBlob struct {
	off int64
	n   uint32
}

// activityStore is the per-(table, minute) series for one analysis.
// Up to activityInlineLimit points live in mem. Past that, finished minutes
// are appended to an unlinked temp file and mem is cleared. The minute
// being updated is the only one held expanded. A later event for an earlier
// minute reloads that minute, because the event timestamp is not a high-water mark.
type activityStore struct {
	limit int
	n     int
	ids   map[tableIdentity]uint32
	names []tableIdentity

	liveOn bool
	liveAt time.Time
	live   map[uint32]*actRec

	mem   map[time.Time][]actRec
	index map[time.Time]actBlob
	off   int64

	file     *os.File
	path     string
	unlinked bool
	noSpill  bool
	err      error
	closed   bool
}

func newActivityStore() *activityStore {
	s := &activityStore{
		limit: activityInlineLimit,
		ids:   make(map[tableIdentity]uint32),
		mem:   make(map[time.Time][]actRec),
		index: make(map[time.Time]actBlob),
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
	if s.err != nil {
		return nil
	}
	tid := s.idOf(id)
	rec := s.live[tid]
	if rec == nil {
		rec = &actRec{}
		s.live[tid] = rec
		s.n++
	}
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

func (s *activityStore) useMinute(minute time.Time) {
	if s.liveOn && s.liveAt == minute {
		return
	}
	if s.liveOn {
		s.flushLive()
		if s.err != nil {
			return
		}
	}
	s.liveAt = minute
	s.liveOn = true
	if s.live == nil {
		s.live = make(map[uint32]*actRec)
	}
	if recs, ok := s.mem[minute]; ok {
		delete(s.mem, minute)
		s.adopt(recs)
		return
	}
	blob, ok := s.index[minute]
	if !ok {
		return
	}
	recs, err := s.readBlob(blob)
	if err != nil {
		s.err = err
		return
	}
	delete(s.index, minute)
	s.adopt(recs)
}

func (s *activityStore) adopt(recs []actRec) {
	for i := range recs {
		rec := recs[i]
		s.live[rec.id] = &rec
	}
}

func (s *activityStore) flushLive() {
	if !s.liveOn {
		return
	}
	recs := compactRecs(s.live)
	s.live = nil
	at := s.liveAt
	s.liveOn = false
	if len(recs) == 0 {
		return
	}
	if s.file != nil {
		if err := s.writeBlob(at, recs); err != nil {
			s.noSpill = true
			s.mem[at] = recs
		}
		return
	}
	s.mem[at] = recs
	if !s.noSpill && s.n > s.limit {
		s.dumpMem()
	}
}

func (s *activityStore) dumpMem() {
	for at, recs := range s.mem {
		if err := s.writeBlob(at, recs); err != nil {
			s.noSpill = true
			return
		}
		delete(s.mem, at)
	}
}

func (s *activityStore) writeBlob(at time.Time, recs []actRec) error {
	if s.file == nil {
		f, err := os.CreateTemp("", "binlogviz-activity-*")
		if err != nil {
			return err
		}
		s.file = f
		s.path = f.Name()
		s.unlinked = os.Remove(s.path) == nil
	}
	buf := make([]byte, len(recs)*actRecSize)
	for i, rec := range recs {
		putRec(buf[i*actRecSize:], rec)
	}
	if _, err := s.file.WriteAt(buf, s.off); err != nil {
		return err
	}
	s.index[at] = actBlob{off: s.off, n: uint32(len(recs))}
	s.off += int64(len(buf))
	return nil
}

// inline reports whether Finalize should copy the series onto the result.
// A small series, and a series that could not spill, is copied. A spilled
// series stays on the file and the result reads it through MinuteTables.
func (s *activityStore) inline() bool {
	if s == nil {
		return true
	}
	s.flushLive()
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
	if s.liveOn {
		take(s.liveAt, compactRecs(s.live))
	}
	for at, recs := range s.mem {
		take(at, recs)
	}
	for at, blob := range s.index {
		recs, err := s.readBlob(blob)
		if err != nil {
			if s.err == nil {
				s.err = err
			}
			return nil
		}
		take(at, recs)
	}
	sort.Slice(points, func(i, j int) bool {
		return points[i].Minute.Before(points[j].Minute)
	})
	return points
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
	s.index = nil
}

func (s *activityStore) records(minute time.Time) ([]actRec, error) {
	if s.closed {
		return nil, errors.New("activity series is closed")
	}
	if s.liveOn && s.liveAt == minute {
		return compactRecs(s.live), nil
	}
	if recs, ok := s.mem[minute]; ok {
		return recs, nil
	}
	blob, ok := s.index[minute]
	if !ok {
		return nil, nil
	}
	return s.readBlob(blob)
}

func (s *activityStore) readBlob(blob actBlob) ([]actRec, error) {
	if blob.n == 0 {
		return nil, nil
	}
	if s.file == nil {
		return nil, errors.New("activity series was spilled to a file that is closed")
	}
	buf := make([]byte, int(blob.n)*actRecSize)
	if _, err := io.ReadFull(io.NewSectionReader(s.file, blob.off, int64(len(buf))), buf); err != nil {
		return nil, fmt.Errorf("read spilled activity: %w", err)
	}
	recs := make([]actRec, blob.n)
	for i := range recs {
		recs[i] = takeRec(buf[i*actRecSize:])
	}
	return recs, nil
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
