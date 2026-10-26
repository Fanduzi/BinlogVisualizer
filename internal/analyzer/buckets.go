// Package analyzer aggregates row activity into minute buckets that can later be persisted and queried.
// input: normalized row events with timestamps, transaction keys, and schema/table identifiers.
// output: deterministic minute-level aggregates and per-table minute rows for spike detection and activity reporting.
// pos: live minute map keyed by truncated minute. The analyzer keeps every minute until Finalize, so an earlier timestamp updates the existing bucket. Memory is one bucket per minute, not per transaction. Per-table row counts live in the shared activity store when one is set.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"sort"
	"time"

	"binlogviz/internal/model"
)

// MinuteAggregator aggregates row activity into minute buckets.
// It tracks total rows, distinct transactions, and per-table row counts per minute.
type MinuteAggregator struct {
	buckets     map[time.Time]*minuteBucket // key: truncated minute
	series      *activityStore
	includeRows bool
}

type minuteBucket struct {
	minute       time.Time
	totalRows    int
	eventCount   int
	binlogBytes  int64
	ddlCount     int
	insertEvents int
	updateEvents int
	deleteEvents int
	// txnCount counts distinct transactions with rows in this minute. A
	// transaction's events are contiguous, so a change of key is a new one.
	txnCount   int
	lastTxnKey string
	tableRows  map[tableIdentity]int
}

// NewMinuteAggregator creates a new MinuteAggregator.
func NewMinuteAggregator() *MinuteAggregator {
	return &MinuteAggregator{
		buckets: make(map[time.Time]*minuteBucket),
	}
}

// Consume processes a normalized event and updates minute statistics.
// Only events with Schema, Table, AND RowCount > 0 are counted as row events.
// Non-row events (BEGIN, COMMIT, XID) don't have schema/table.
// TABLE_MAP events have schema/table but RowCount is 0, so they're filtered out.
func (a *MinuteAggregator) Consume(ev model.NormalizedEvent) {
	isDDL := ev.EventType == "DDL"
	isRowEvent := ev.Schema != "" && ev.Table != "" && ev.RowCount > 0
	if !isDDL && !isRowEvent {
		return
	}

	minute := truncateToMinute(ev.Timestamp)
	bucket, exists := a.buckets[minute]
	if !exists {
		bucket = &minuteBucket{minute: minute}
		if a.series == nil {
			bucket.tableRows = make(map[tableIdentity]int)
		}
		a.buckets[minute] = bucket
	}

	bucket.eventCount++
	bucket.binlogBytes += ev.BinlogBytes
	if isDDL {
		bucket.ddlCount++
	}

	// Accumulate row count
	if isRowEvent {
		bucket.totalRows += ev.RowCount
		switch ev.Operation {
		case "INSERT":
			bucket.insertEvents++
		case "UPDATE":
			bucket.updateEvents++
		case "DELETE":
			bucket.deleteEvents++
		}
		if ev.TxnKey != "" && ev.TxnKey != bucket.lastTxnKey {
			bucket.txnCount++
			bucket.lastTxnKey = ev.TxnKey
		}
	}

	// Track per-table rows
	if isRowEvent {
		id := newTableIdentity(ev.Schema, ev.Table)
		if a.series != nil {
			a.series.addRows(id, minute, ev.RowCount)
		} else {
			bucket.tableRows[id] += ev.RowCount
		}
	}
}

// Snapshot returns all minute buckets sorted by time ascending.
// Returns defensive copies of TableRows to prevent external mutations
// from polluting internal aggregator state.
func (a *MinuteAggregator) Snapshot() []model.MinuteBucket {
	result := make([]model.MinuteBucket, 0, len(a.buckets))
	for _, bucket := range a.buckets {
		result = append(result, a.exportBucket(bucket))
	}

	// Sort by time ascending for deterministic output
	sort.Slice(result, func(i, j int) bool {
		return result[i].Minute.Before(result[j].Minute)
	})

	return result
}

// DrainBefore returns and removes buckets older than cutoffMinute.
// Streaming analysis does not call this. The event timestamp is not a
// high-water mark: a later event can belong to an earlier minute, and
// dropping that minute would open a second bucket for it.
func (a *MinuteAggregator) DrainBefore(cutoffMinute time.Time) []model.MinuteBucket {
	if len(a.buckets) == 0 {
		return nil
	}
	var result []model.MinuteBucket
	for minute, bucket := range a.buckets {
		if !minute.Before(cutoffMinute) {
			continue
		}
		if result == nil {
			result = make([]model.MinuteBucket, 0, len(a.buckets))
		}
		result = append(result, a.exportBucket(bucket))
		delete(a.buckets, minute)
	}
	if len(result) > 1 {
		sort.Slice(result, func(i, j int) bool {
			return result[i].Minute.Before(result[j].Minute)
		})
	}
	return result
}

// DrainAll returns and clears all remaining minute buckets.
func (a *MinuteAggregator) DrainAll() []model.MinuteBucket {
	if len(a.buckets) == 0 {
		return nil
	}
	result := make([]model.MinuteBucket, 0, len(a.buckets))
	for minute, bucket := range a.buckets {
		result = append(result, a.exportBucket(bucket))
		delete(a.buckets, minute)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Minute.Before(result[j].Minute)
	})
	return result
}

// truncateToMinute truncates a timestamp to the start of its minute.
func truncateToMinute(t time.Time) time.Time {
	return t.Truncate(time.Minute)
}

func (a *MinuteAggregator) exportBucket(bucket *minuteBucket) model.MinuteBucket {
	return minuteBucketSnapshot(bucket, a.tableRowsOf(bucket))
}

func (a *MinuteAggregator) tableRowsOf(bucket *minuteBucket) map[string]int {
	if a.series == nil {
		return exportTableRows(bucket.tableRows)
	}
	if !a.includeRows {
		return nil
	}
	return a.series.TableRows(bucket.minute)
}

func minuteBucketSnapshot(bucket *minuteBucket, tableRows map[string]int) model.MinuteBucket {
	return model.MinuteBucket{
		Minute:      bucket.minute,
		TotalRows:   bucket.totalRows,
		TxnCount:    bucket.txnCount,
		EventCount:  bucket.eventCount,
		BinlogBytes: bucket.binlogBytes,
		DDLCount:    bucket.ddlCount,
		TableRows:   tableRows,
	}
}

func exportTableRows(src map[tableIdentity]int) map[string]int {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]int, len(src))
	for key, rows := range src {
		dst[key.String()] = rows
	}
	return dst
}

// OperationCounts returns per-minute operation event counts for timeseries projection.
func (a *MinuteAggregator) OperationCounts() map[time.Time]operationMinuteStats {
	if len(a.buckets) == 0 {
		return nil
	}
	result := make(map[time.Time]operationMinuteStats, len(a.buckets))
	for minute, bucket := range a.buckets {
		result[minute] = operationMinuteStats{
			insertEvents: bucket.insertEvents,
			updateEvents: bucket.updateEvents,
			deleteEvents: bucket.deleteEvents,
			ddlEvents:    bucket.ddlCount,
		}
	}
	return result
}
