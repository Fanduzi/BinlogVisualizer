package analyzer

import (
	"sort"
	"time"

	"binlogviz/internal/model"
)

// MinuteAggregator aggregates row activity into minute buckets.
// It tracks total rows, distinct transactions, and per-table row counts per minute.
type MinuteAggregator struct {
	buckets map[time.Time]*minuteBucket // key: truncated minute
}

type minuteBucket struct {
	minute    time.Time
	totalRows int
	txnSet    map[string]struct{} // distinct transactions
	tableRows map[string]int // "schema.table" -> row count
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
	// Only process events that have schema, table, AND actual rows
	if ev.Schema == "" || ev.Table == "" || ev.RowCount == 0 {
		return
	}

	minute := truncateToMinute(ev.Timestamp)
	bucket, exists := a.buckets[minute]
	if !exists {
		bucket = &minuteBucket{
			minute:    minute,
			txnSet:    make(map[string]struct{}),
			tableRows: make(map[string]int),
		}
		a.buckets[minute] = bucket
	}

	// Accumulate row count
	bucket.totalRows += ev.RowCount

	// Track distinct transaction
	if ev.TxnKey != "" {
		bucket.txnSet[ev.TxnKey] = struct{}{}
	}

	// Track per-table rows
	tableKey := ev.Schema + "." + ev.Table
	bucket.tableRows[tableKey] += ev.RowCount
}

// Snapshot returns all minute buckets sorted by time ascending.
// Returns defensive copies of TableRows to prevent external mutations
// from polluting internal aggregator state.
func (a *MinuteAggregator) Snapshot() []model.MinuteBucket {
	result := make([]model.MinuteBucket, 0, len(a.buckets))
	for _, bucket := range a.buckets {
		// Create defensive copy of tableRows to prevent external mutation
		tableRowsCopy := make(map[string]int, len(bucket.tableRows))
		for k, v := range bucket.tableRows {
			tableRowsCopy[k] = v
		}

		result = append(result, model.MinuteBucket{
			Minute:    bucket.minute,
			TotalRows: bucket.totalRows,
			TxnCount:  len(bucket.txnSet),
			TableRows: tableRowsCopy,
		})
	}

	// Sort by time ascending for deterministic output
	sort.Slice(result, func(i, j int) bool {
		return result[i].Minute.Before(result[j].Minute)
	})

	return result
}

// truncateToMinute truncates a timestamp to the start of its minute.
func truncateToMinute(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
}
