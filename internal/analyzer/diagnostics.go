// Package analyzer builds DBA-oriented findings and evidence strings from analyzer outputs.
// input: alerts, minute buckets, completed transactions, and DDL timeline entries.
// output: model.Finding slices with deterministic evidence references.
// pos: analyzer-side diagnostics enrichment layer consumed by later report assembly.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"fmt"
	"time"

	"binlogviz/internal/model"
)

// HotIntervalEvidence formats bucket and DDL context for one hot minute.
func HotIntervalEvidence(bucket model.MinuteBucket, ddlEvents []model.DDLEvent) []string {
	return hotIntervalEvidenceWithDDLs(bucket, matchingDDLEventsForMinute(bucket.Minute, ddlEvents))
}

func hotIntervalEvidenceWithDDLs(bucket model.MinuteBucket, ddlEvents []model.DDLEvent) []string {
	evidence := []string{
		fmt.Sprintf("rows=%d", bucket.TotalRows),
		fmt.Sprintf("txns=%d", bucket.TxnCount),
		fmt.Sprintf("events=%d", bucket.EventCount),
		fmt.Sprintf("binlog_bytes=%d", bucket.BinlogBytes),
	}
	for _, ddl := range ddlEvents {
		evidence = append(evidence, fmt.Sprintf("ddl=%s %s.%s @ %s", ddl.Operation, ddl.Schema, ddl.Table, ddl.BinlogPath))
	}
	return evidence
}

func matchingDDLEventsForMinute(minute time.Time, ddlEvents []model.DDLEvent) []model.DDLEvent {
	if len(ddlEvents) == 0 {
		return nil
	}
	minute = truncateToMinute(minute)
	var out []model.DDLEvent
	for _, ddl := range ddlEvents {
		if truncateToMinute(ddl.Timestamp) == minute {
			out = append(out, ddl)
		}
	}
	return out
}

// BuildFindingsFromAlerts converts persisted alerts into richer finding payloads.
func BuildFindingsFromAlerts(alerts []model.Alert, minutes []model.MinuteBucket, txns []model.Transaction, ddlEvents []model.DDLEvent) []model.Finding {
	if len(alerts) == 0 {
		return nil
	}

	txnByKey := indexTransactionsByKey(txns)
	minuteByTime := indexMinutesByTime(minutes)
	ddlByMinute := indexDDLEventsByMinute(ddlEvents)

	findings := make([]model.Finding, 0, len(alerts))
	for _, alert := range alerts {
		finding := model.Finding{
			Kind:     alert.Type,
			Severity: alert.Severity,
			Message:  alert.Message,
			TxnKey:   alert.TxnKey,
			Minute:   alert.Minute,
		}
		if alert.TxnKey != "" {
			if txn, ok := txnByKey[alert.TxnKey]; ok {
				finding.EvidenceRefs = transactionEvidence(txn)
			}
		} else if !alert.Minute.IsZero() {
			minute := truncateToMinute(alert.Minute)
			if bucket, ok := minuteByTime[minute]; ok {
				finding.EvidenceRefs = hotIntervalEvidenceWithDDLs(bucket, ddlByMinute[minute])
			}
		}
		findings = append(findings, finding)
	}

	return findings
}

func indexTransactionsByKey(transactions []model.Transaction) map[string]model.Transaction {
	if len(transactions) == 0 {
		return nil
	}
	out := make(map[string]model.Transaction, len(transactions))
	for _, txn := range transactions {
		if txn.TxnKey != "" {
			out[txn.TxnKey] = txn
		}
	}
	return out
}

func indexMinutesByTime(minutes []model.MinuteBucket) map[time.Time]model.MinuteBucket {
	if len(minutes) == 0 {
		return nil
	}
	out := make(map[time.Time]model.MinuteBucket, len(minutes))
	for _, bucket := range minutes {
		out[truncateToMinute(bucket.Minute)] = bucket
	}
	return out
}

func indexDDLEventsByMinute(ddlEvents []model.DDLEvent) map[time.Time][]model.DDLEvent {
	if len(ddlEvents) == 0 {
		return nil
	}
	out := make(map[time.Time][]model.DDLEvent)
	for _, ddl := range ddlEvents {
		minute := truncateToMinute(ddl.Timestamp)
		out[minute] = append(out[minute], ddl)
	}
	return out
}

// SelectDiagnosticTransactions returns the top N transactions ranked by rows and duration.
func SelectDiagnosticTransactions(txns []model.Transaction, limit int) ([]model.Transaction, []model.Transaction) {
	if limit <= 0 || len(txns) == 0 {
		return nil, nil
	}

	byRows := topTransactions(txns, limit, func(left, right model.Transaction) bool {
		if left.TotalRows != right.TotalRows {
			return left.TotalRows > right.TotalRows
		}
		if left.BinlogBytes != right.BinlogBytes {
			return left.BinlogBytes > right.BinlogBytes
		}
		return left.TxnKey < right.TxnKey
	})

	byDuration := topTransactions(txns, limit, func(left, right model.Transaction) bool {
		if left.Duration != right.Duration {
			return left.Duration > right.Duration
		}
		if left.TotalRows != right.TotalRows {
			return left.TotalRows > right.TotalRows
		}
		return left.TxnKey < right.TxnKey
	})
	return byRows, byDuration
}

// SelectHotIntervals returns the top N minute buckets by total row volume.
func SelectHotIntervals(minutes []model.MinuteBucket, limit int) []model.MinuteBucket {
	if limit <= 0 || len(minutes) == 0 {
		return nil
	}

	return topMinutes(minutes, limit, func(left, right model.MinuteBucket) bool {
		if left.TotalRows != right.TotalRows {
			return left.TotalRows > right.TotalRows
		}
		if left.EventCount != right.EventCount {
			return left.EventCount > right.EventCount
		}
		return left.Minute.Before(right.Minute)
	})
}

// SelectWidestTransactions returns the top N transactions ranked by number of distinct tables touched.
func SelectWidestTransactions(txns []model.Transaction, limit int) []model.Transaction {
	if limit <= 0 || len(txns) == 0 {
		return nil
	}

	return topTransactions(txns, limit, func(left, right model.Transaction) bool {
		wi := len(left.Tables)
		wj := len(right.Tables)
		if wi != wj {
			return wi > wj
		}
		if left.TotalRows != right.TotalRows {
			return left.TotalRows > right.TotalRows
		}
		return left.TxnKey < right.TxnKey
	})
}

func topTransactions(txns []model.Transaction, limit int, better func(left, right model.Transaction) bool) []model.Transaction {
	if limit > len(txns) {
		limit = len(txns)
	}
	out := make([]model.Transaction, 0, limit)
	for _, txn := range txns {
		insertAt := len(out)
		for index := range out {
			if better(txn, out[index]) {
				insertAt = index
				break
			}
		}
		if insertAt == len(out) {
			if len(out) < limit {
				out = append(out, txn)
			}
			continue
		}
		if len(out) < limit {
			out = append(out, model.Transaction{})
		}
		copy(out[insertAt+1:], out[insertAt:])
		out[insertAt] = txn
		if len(out) > limit {
			out = out[:limit]
		}
	}
	return out
}

func topMinutes(minutes []model.MinuteBucket, limit int, better func(left, right model.MinuteBucket) bool) []model.MinuteBucket {
	if limit > len(minutes) {
		limit = len(minutes)
	}
	out := make([]model.MinuteBucket, 0, limit)
	for _, minute := range minutes {
		insertAt := len(out)
		for index := range out {
			if better(minute, out[index]) {
				insertAt = index
				break
			}
		}
		if insertAt == len(out) {
			if len(out) < limit {
				out = append(out, minute)
			}
			continue
		}
		if len(out) < limit {
			out = append(out, model.MinuteBucket{})
		}
		copy(out[insertAt+1:], out[insertAt:])
		out[insertAt] = minute
		if len(out) > limit {
			out = out[:limit]
		}
	}
	return out
}

// BuildFileSegments groups minute buckets into time-based segments showing binlog generation throughput.
// segmentSize controls how many consecutive minutes are grouped into each segment.
func BuildFileSegments(minutes []model.MinuteBucket, segmentSize int) []model.FileSegment {
	if len(minutes) == 0 || segmentSize <= 0 {
		return nil
	}

	var segments []model.FileSegment
	for i := 0; i < len(minutes); i += segmentSize {
		end := i + segmentSize
		if end > len(minutes) {
			end = len(minutes)
		}
		batch := minutes[i:end]

		var seg model.FileSegment
		seg.StartTime = batch[0].Minute
		seg.EndTime = batch[len(batch)-1].Minute
		for _, b := range batch {
			seg.BinlogBytes += b.BinlogBytes
			seg.Rows += b.TotalRows
			seg.Events += b.EventCount
		}
		segments = append(segments, seg)
	}
	return segments
}

func transactionByKey(transactions []model.Transaction, txnKey string) (model.Transaction, bool) {
	for _, txn := range transactions {
		if txn.TxnKey == txnKey {
			return txn, true
		}
	}
	return model.Transaction{}, false
}

func minuteByTimestamp(minutes []model.MinuteBucket, ts time.Time) (model.MinuteBucket, bool) {
	minute := truncateToMinute(ts)
	for _, bucket := range minutes {
		if bucket.Minute.Equal(minute) {
			return bucket, true
		}
	}
	return model.MinuteBucket{}, false
}

func transactionEvidence(txn model.Transaction) []string {
	evidence := []string{
		fmt.Sprintf("rows=%d", txn.TotalRows),
		fmt.Sprintf("events=%d", txn.EventCount),
		fmt.Sprintf("duration_ms=%d", txn.Duration/time.Millisecond),
		fmt.Sprintf("binlog_bytes=%d", txn.BinlogBytes),
	}
	if txn.BinlogPathStart != "" || txn.BinlogPathEnd != "" || txn.PositionStart != 0 || txn.PositionEnd != 0 {
		evidence = append(evidence, fmt.Sprintf(
			"binlog_span=%s:%d-%s:%d",
			txn.BinlogPathStart,
			txn.PositionStart,
			txn.BinlogPathEnd,
			txn.PositionEnd,
		))
	}
	return evidence
}
