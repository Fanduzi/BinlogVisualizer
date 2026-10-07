// Package analyzer keeps a bounded ranking of primary keys touched by UPDATE and DELETE.
// input: retained UPDATE and DELETE events whose primary key came from FULL row metadata.
// output: the busiest keys, plus tables whose key is not in the binlog.
// pos: streaming aggregator used by Analyzer; the map never grows past HotRowTrackLimit.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"container/heap"
	"sort"
	"time"

	"binlogviz/internal/model"
)

// HotRowTrackLimit is the most primary keys analyze keeps while reading a binlog.
// A 1 GB file can name far more keys than this. When a new key arrives after the
// table is full, it replaces the least-touched key. Its touch count starts at
// the dropped count plus one, and the row is marked approximate. A key that
// stays in the table keeps an exact touch count, transaction count, and
// first/last transaction. Distinct transactions are counted from the stream
// order (one open group at a time), not from a set of every GTID.
const HotRowTrackLimit = 8192

type hotKey struct {
	schema string
	table  string
	pk     string
}

type hotAcc struct {
	key       hotKey
	touches   int
	inherited int
	txns      int
	lastTxn   string
	firstAt   time.Time
	lastAt    time.Time
	firstGTID string
	lastGTID  string
	firstFile string
	lastFile  string
	firstPos  int64
	lastPos   int64
	heapIdx   int
}

type hotHeap []*hotAcc

func (h hotHeap) Len() int { return len(h) }

func (h hotHeap) Less(i, j int) bool {
	if h[i].touches != h[j].touches {
		return h[i].touches < h[j].touches
	}
	return hotKeyLess(h[i].key, h[j].key)
}

func (h hotHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}

func (h *hotHeap) Push(x any) {
	acc := x.(*hotAcc)
	acc.heapIdx = len(*h)
	*h = append(*h, acc)
}

func (h *hotHeap) Pop() any {
	old := *h
	n := len(old)
	acc := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return acc
}

func hotKeyLess(a, b hotKey) bool {
	if a.schema != b.schema {
		return a.schema < b.schema
	}
	if a.table != b.table {
		return a.table < b.table
	}
	return a.pk < b.pk
}

type hotRowAggregator struct {
	limit       int
	byKey       map[hotKey]*hotAcc
	items       hotHeap
	overflow    bool
	ranked      map[string]struct{}
	unavailable map[string]model.HotRowGap
}

func newHotRowAggregator(limit int) *hotRowAggregator {
	if limit <= 0 {
		limit = HotRowTrackLimit
	}
	return &hotRowAggregator{
		limit:       limit,
		byKey:       make(map[hotKey]*hotAcc),
		ranked:      make(map[string]struct{}),
		unavailable: make(map[string]model.HotRowGap),
	}
}

func (a *hotRowAggregator) Consume(ev model.NormalizedEvent) {
	if a == nil || (ev.Operation != "UPDATE" && ev.Operation != "DELETE") {
		return
	}
	if ev.Schema == "" || ev.Table == "" {
		return
	}
	switch ev.KeyStatus {
	case model.KeyStatusNoPK:
		return
	case model.KeyStatusHasPK:
		added := 0
		for _, key := range ev.RowKeys {
			if key == "" {
				continue
			}
			a.observe(ev, key)
			added++
		}
		if added == 0 {
			a.noteUnavailable(ev.Schema, ev.Table, model.HotRowReasonValues)
		}
	default:
		a.noteUnavailable(ev.Schema, ev.Table, model.HotRowReasonMetadata)
	}
}

func (a *hotRowAggregator) observe(ev model.NormalizedEvent, pk string) {
	a.clearUnavailable(ev.Schema, ev.Table)
	key := hotKey{schema: ev.Schema, table: ev.Table, pk: pk}
	if acc, ok := a.byKey[key]; ok {
		acc.touches++
		if ev.TxnKey == "" || ev.TxnKey != acc.lastTxn {
			acc.txns++
			acc.lastTxn = ev.TxnKey
		}
		acc.lastAt = ev.Timestamp
		acc.lastGTID = ev.TxnGTID
		acc.lastFile = ev.TxnStartPath
		acc.lastPos = ev.TxnStartPos
		heap.Fix(&a.items, acc.heapIdx)
		return
	}
	acc := &hotAcc{
		key:       key,
		touches:   1,
		txns:      1,
		lastTxn:   ev.TxnKey,
		firstAt:   ev.Timestamp,
		lastAt:    ev.Timestamp,
		firstGTID: ev.TxnGTID,
		lastGTID:  ev.TxnGTID,
		firstFile: ev.TxnStartPath,
		lastFile:  ev.TxnStartPath,
		firstPos:  ev.TxnStartPos,
		lastPos:   ev.TxnStartPos,
	}
	if len(a.byKey) < a.limit {
		a.byKey[key] = acc
		heap.Push(&a.items, acc)
		return
	}
	a.overflow = true
	victim := a.items[0]
	delete(a.byKey, victim.key)
	acc.touches = victim.touches + 1
	acc.inherited = victim.touches
	*victim = *acc
	victim.heapIdx = 0
	a.byKey[key] = victim
	heap.Fix(&a.items, 0)
}

func tableGapID(schema, table string) string {
	return schema + "\x00" + table
}

func (a *hotRowAggregator) noteUnavailable(schema, table, reason string) {
	id := tableGapID(schema, table)
	if _, ok := a.ranked[id]; ok {
		return
	}
	if _, ok := a.unavailable[id]; ok {
		return
	}
	a.unavailable[id] = model.HotRowGap{Schema: schema, Table: table, Reason: reason}
}

func (a *hotRowAggregator) clearUnavailable(schema, table string) {
	id := tableGapID(schema, table)
	a.ranked[id] = struct{}{}
	delete(a.unavailable, id)
}

func (a *hotRowAggregator) Snapshot() model.HotRowReport {
	if a == nil {
		return model.HotRowReport{TrackLimit: HotRowTrackLimit}
	}
	rows := make([]model.HotRow, 0, len(a.byKey))
	for _, acc := range a.byKey {
		rows = append(rows, model.HotRow{
			Schema:       acc.key.schema,
			Table:        acc.key.table,
			PrimaryKey:   acc.key.pk,
			Touches:      acc.touches,
			Transactions: acc.txns,
			FirstTime:    acc.firstAt,
			LastTime:     acc.lastAt,
			FirstGTID:    acc.firstGTID,
			FirstFile:    acc.firstFile,
			FirstPos:     acc.firstPos,
			LastGTID:     acc.lastGTID,
			LastFile:     acc.lastFile,
			LastPos:      acc.lastPos,
			Approximate:  acc.inherited > 0,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Touches != rows[j].Touches {
			return rows[i].Touches > rows[j].Touches
		}
		if rows[i].Transactions != rows[j].Transactions {
			return rows[i].Transactions > rows[j].Transactions
		}
		if rows[i].Schema != rows[j].Schema {
			return rows[i].Schema < rows[j].Schema
		}
		if rows[i].Table != rows[j].Table {
			return rows[i].Table < rows[j].Table
		}
		return rows[i].PrimaryKey < rows[j].PrimaryKey
	})
	gaps := make([]model.HotRowGap, 0, len(a.unavailable))
	for _, gap := range a.unavailable {
		gaps = append(gaps, gap)
	}
	sort.Slice(gaps, func(i, j int) bool {
		if gaps[i].Schema != gaps[j].Schema {
			return gaps[i].Schema < gaps[j].Schema
		}
		return gaps[i].Table < gaps[j].Table
	})
	return model.HotRowReport{
		Rows:        rows,
		Unavailable: gaps,
		TrackLimit:  a.limit,
		Overflow:    a.overflow,
	}
}
