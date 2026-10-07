// Package analyzer ranks replica apply delay from MySQL 8 commit timestamps.
// input: retained transactions that already passed table, schema, time, position, GTID, and DML filters.
// output: max, nearest-rank p95, peak minute, and a bounded list of the slowest transactions. Absent timestamps stay absent.
// pos: report-aggregator helper for replica apply delay.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"sort"
	"time"

	"binlogviz/internal/model"
)

// applyDelayTracker keeps one signed delay per counted transaction and a
// bounded ranking. The int64 slice is the p95 input; a histogram would only
// matter if a single analyze held tens of millions of transactions.
type applyDelayTracker struct {
	values       []int64
	maxUs        int64
	maxImmediate time.Time
	has          bool
	top          []model.ApplyDelayTxn
}

func (t *applyDelayTracker) add(txn model.Transaction, limit int) {
	if t == nil || txn.TotalRows <= 0 {
		return
	}
	delay, ok := txn.CommitDelay()
	if !ok {
		return
	}
	delayUs := delay.Microseconds()
	immediate := model.UnixMicroTime(txn.ImmediateCommitUs)
	t.values = append(t.values, delayUs)
	if !t.has || delayUs > t.maxUs || (delayUs == t.maxUs && immediate.Before(t.maxImmediate)) {
		t.has = true
		t.maxUs = delayUs
		t.maxImmediate = immediate
	}
	t.top = insertApplyDelay(t.top, model.ApplyDelayTxn{
		GTID:            txn.GTID,
		TxnStartPath:    txn.TxnStartPath,
		TxnStartPos:     txn.TxnStartPos,
		OriginalCommit:  model.UnixMicroTime(txn.OriginalCommitUs),
		ImmediateCommit: immediate,
		Delay:           delay,
		Tables:          cloneDelayTables(txn.Tables),
	}, limit)
}

func (t applyDelayTracker) snapshot() *model.ApplyDelay {
	if len(t.values) == 0 {
		return nil
	}
	sorted := append([]int64(nil), t.values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	origin := model.ApplyDelaySource
	for _, us := range t.values {
		if us != 0 {
			origin = model.ApplyDelayReplica
			break
		}
	}
	out := &model.ApplyDelay{
		Origin:     origin,
		Max:        time.Duration(t.maxUs) * time.Microsecond,
		P95:        time.Duration(percentile95(sorted)) * time.Microsecond,
		PeakMinute: t.maxImmediate.UTC().Truncate(time.Minute),
	}
	if origin == model.ApplyDelayReplica {
		out.Transactions = append([]model.ApplyDelayTxn(nil), t.top...)
	}
	return out
}

// percentile95 is the nearest-rank 95th percentile: the 1-based rank
// ceil(0.95*n), which is an observed delay. Integer arithmetic avoids a
// float landing just off an exact boundary.
func percentile95(sorted []int64) int64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := (95*n + 99) / 100
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}

func insertApplyDelay(current []model.ApplyDelayTxn, txn model.ApplyDelayTxn, limit int) []model.ApplyDelayTxn {
	if limit == 0 {
		limit = len(current) + 1
	}
	insertAt := len(current)
	for index := range current {
		if applyDelayBetter(txn, current[index]) {
			insertAt = index
			break
		}
	}
	if insertAt == len(current) {
		if len(current) < limit {
			return append(current, txn)
		}
		return current
	}
	if len(current) < limit {
		current = append(current, model.ApplyDelayTxn{})
	}
	copy(current[insertAt+1:], current[insertAt:])
	current[insertAt] = txn
	if len(current) > limit {
		current = current[:limit]
	}
	return current
}

func applyDelayBetter(left, right model.ApplyDelayTxn) bool {
	if left.Delay != right.Delay {
		return left.Delay > right.Delay
	}
	if !left.ImmediateCommit.Equal(right.ImmediateCommit) {
		return left.ImmediateCommit.Before(right.ImmediateCommit)
	}
	if left.GTID != right.GTID {
		return left.GTID < right.GTID
	}
	if left.TxnStartPos != right.TxnStartPos {
		return left.TxnStartPos < right.TxnStartPos
	}
	return left.TxnStartPath < right.TxnStartPath
}

func cloneDelayTables(src map[string]int) map[string]int {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]int, len(src))
	for name, rows := range src {
		if name == "" || rows == 0 {
			continue
		}
		dst[name] = rows
	}
	if len(dst) == 0 {
		return nil
	}
	return dst
}
