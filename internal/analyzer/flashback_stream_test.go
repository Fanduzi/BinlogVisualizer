package analyzer

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

// flashTxns feeds n committed transactions, each with one UPDATE row whose
// images carry pad bytes, and finalizes the analyzer.
func flashTxns(t testing.TB, a *Analyzer, n, pad int) {
	t.Helper()
	base := time.Date(2026, 10, 14, 6, 0, 0, 0, time.UTC)
	pos := int64(4)
	ev := func(e model.NormalizedEvent) {
		e.Timestamp = base
		e.BinlogPath = "mysql-bin.000001"
		e.PositionStart = pos
		pos += 40
		e.PositionEnd = pos
		if err := a.Consume(e); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < n; i++ {
		// Distinct images, as a real binlog has: nothing is shared.
		before := fmt.Sprintf("'%d%s'", i, strings.Repeat("b", pad))
		after := fmt.Sprintf("'%d%s'", i, strings.Repeat("a", pad))
		ev(model.NormalizedEvent{EventType: "GTID", GTID: fmt.Sprintf("3e11fa47-71ca-11e1-9e33-c80aa9429562:%d", i+1)})
		ev(model.NormalizedEvent{EventType: "BEGIN"})
		ev(model.NormalizedEvent{EventType: "ROWS", Operation: "UPDATE", Schema: "shop", Table: "orders", RowCount: 1,
			FlashRows: []model.FlashRow{{Schema: "shop", Table: "orders", Op: "UPDATE", Columns: []string{"id", "note"},
				Before: []string{fmt.Sprint(i), before}, After: []string{fmt.Sprint(i), after}, PK: []int{0}}}})
		ev(model.NormalizedEvent{EventType: "XID"})
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
}

func heapAfterGC() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// #258: flashback must not hold every selected row image in memory. The
// heap that flashback adds on top of analyze must not grow with the binlog.
func TestFlashbackMemoryDoesNotGrowWithBinlog(t *testing.T) {
	retained := func(flashback bool, n int) int64 {
		before := heapAfterGC()
		a := New(Options{Flashback: flashback})
		flashTxns(t, a, n, 2048)
		after := heapAfterGC()
		runtime.KeepAlive(a)
		return int64(after) - int64(before)
	}
	const n = 10000
	extra := retained(true, n) - retained(false, n)
	// Kept in memory, 10k transactions hold about 40 MB of row images.
	if extra > 4<<20 {
		t.Fatalf("flashback retains %d KB for %d transactions", extra>>10, n)
	}
	a := New(Options{Flashback: true})
	flashTxns(t, a, n, 64)
	if rows, err := a.WriteFlashback(&countWriter{}); err != nil || rows != n {
		t.Fatalf("rows %d err %v", rows, err)
	}
}

// The streamed script is the same script FlashbackSQL renders.
func TestWriteFlashbackMatchesFlashbackSQL(t *testing.T) {
	a := New(Options{Flashback: true})
	flashTxns(t, a, 50, 8)
	sql, err := a.FlashbackSQL()
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	rows, err := a.WriteFlashback(&b)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 50 || b.String() != sql || !strings.Contains(sql, "-- gtid: 3e11fa47-71ca-11e1-9e33-c80aa9429562:50") {
		t.Fatalf("rows %d, same %v", rows, b.String() == sql)
	}
	if strings.Index(sql, ":50\n") > strings.Index(sql, ":1\n") {
		t.Fatal("last transaction must come first")
	}
}

type countWriter struct{ n int }

func (w *countWriter) Write(p []byte) (int, error) { w.n += len(p); return len(p), nil }

// One transaction larger than a spill chunk is still one block, rows last first,
// and its rows do not stay in memory either.
func TestFlashbackLargeTransactionIsOneBlock(t *testing.T) {
	a := New(Options{Flashback: true})
	base := time.Date(2026, 10, 14, 6, 0, 0, 0, time.UTC)
	pos := int64(4)
	ev := func(e model.NormalizedEvent) {
		e.Timestamp, e.BinlogPath, e.PositionStart = base, "mysql-bin.000001", pos
		pos += 40
		e.PositionEnd = pos
		if err := a.Consume(e); err != nil {
			t.Fatal(err)
		}
	}
	const n = 3*flashChunkRows + 7
	ev(model.NormalizedEvent{EventType: "GTID", GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:9"})
	ev(model.NormalizedEvent{EventType: "BEGIN"})
	for i := 0; i < n; i++ {
		ev(model.NormalizedEvent{EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "orders", RowCount: 1,
			FlashRows: []model.FlashRow{{Schema: "shop", Table: "orders", Op: "DELETE", Columns: []string{"id"},
				Before: []string{fmt.Sprint(i)}, PK: []int{0}}}})
	}
	if a.flashCur != nil && len(a.flashCur.rows) >= flashChunkRows {
		t.Fatalf("%d rows of the open transaction are in memory", len(a.flashCur.rows))
	}
	ev(model.NormalizedEvent{EventType: "XID"})
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	rows, err := a.WriteFlashback(&b)
	if err != nil || rows != n {
		t.Fatalf("rows %d err %v", rows, err)
	}
	sql := b.String()
	if strings.Count(sql, "START TRANSACTION;") != 1 || strings.Count(sql, "COMMIT;") != 1 {
		t.Fatalf("blocks: %d starts, %d commits", strings.Count(sql, "START TRANSACTION;"), strings.Count(sql, "COMMIT;"))
	}
	last := strings.Index(sql, fmt.Sprintf("VALUES (%d);", n-1))
	first := strings.Index(sql, "VALUES (0);")
	if last < 0 || first < 0 || last > first || strings.Index(sql, "COMMIT;") < first {
		t.Fatal("rows must be undone last first inside one block")
	}
}
