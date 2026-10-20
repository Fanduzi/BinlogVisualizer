// Package analyzer ranks replica apply delay from MySQL 8 commit timestamps.
// input: retained transactions that already passed table, schema, time, position, GTID, and DML filters.
// output: max, nearest-rank p95, peak minute, and a bounded list of the slowest transactions. Absent timestamps stay absent.
// pos: report-aggregator helper for replica apply delay.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"time"

	"binlogviz/internal/model"
)

// applyDelayTracker keeps one signed delay per counted transaction and a
// bounded ranking. The delays are the p95 input; delayValues keeps them in
// bounded memory and spills the rest to disk (#364).
type applyDelayTracker struct {
	values       delayValues
	maxUs        int64
	maxImmediate time.Time
	has          bool
	top          *topN[model.ApplyDelayTxn]
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
	t.values.add(delayUs)
	if !t.has || delayUs > t.maxUs || (delayUs == t.maxUs && immediate.Before(t.maxImmediate)) {
		t.has = true
		t.maxUs = delayUs
		t.maxImmediate = immediate
	}
	if t.top == nil {
		t.top = newTopN(limit, applyDelayBetter)
	}
	t.top.add(model.ApplyDelayTxn{
		GTID:            txn.GTID,
		TxnStartPath:    txn.TxnStartPath,
		TxnStartPos:     txn.TxnStartPos,
		OriginalCommit:  model.UnixMicroTime(txn.OriginalCommitUs),
		ImmediateCommit: immediate,
		Delay:           delay,
		Tables:          cloneDelayTables(txn.Tables),
	})
}

func (t *applyDelayTracker) snapshot() *model.ApplyDelay {
	if t.values.n == 0 {
		return nil
	}
	p95, err := t.values.p95()
	if err != nil {
		return nil
	}
	origin := model.ApplyDelaySource
	if t.values.nonZero {
		origin = model.ApplyDelayReplica
	}
	out := &model.ApplyDelay{
		Origin:     origin,
		Max:        time.Duration(t.maxUs) * time.Microsecond,
		P95:        time.Duration(p95) * time.Microsecond,
		PeakMinute: t.maxImmediate.UTC().Truncate(time.Minute),
	}
	if origin == model.ApplyDelayReplica {
		out.Transactions = append([]model.ApplyDelayTxn(nil), t.top.sorted()...)
	}
	return out
}

// err reports a failure to read spilled delays back; snapshot then has no
// apply-delay section, and the analyzer fails instead of printing it.
func (t *applyDelayTracker) err() error { return t.values.err }

func (t *applyDelayTracker) close() { t.values.close() }

// percentile95 is the nearest-rank 95th percentile: the 1-based rank
// ceil(0.95*n), which is an observed delay. Integer arithmetic avoids a
// float landing just off an exact boundary.
func percentile95(sorted []int64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[nearestRank95(len(sorted))-1]
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
