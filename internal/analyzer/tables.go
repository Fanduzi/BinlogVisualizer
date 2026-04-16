// Package analyzer aggregates per-table write statistics for final reports.
// input: normalized row and DDL events streamed from the analyzer pipeline.
// output: table-level totals, operation counts, and activity metadata for reporting.
// pos: analyzer sub-aggregator between normalized events and model.TableStats output.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"sort"
	"time"

	"binlogviz/internal/model"
)

// TableAggregator tracks per-table write statistics.
type TableAggregator struct {
	tables map[string]*tableStats // key: "schema.table"
}

type tableStats struct {
	schema        string
	table         string
	totalRows     int
	insertRows    int
	updateRows    int
	deleteRows    int
	eventCount    int
	binlogBytes   int64
	ddlCount      int
	lastChangedAt time.Time
	txnSet        map[string]struct{} // distinct transactions that touched this table
	activity      map[time.Time]*tableActivityPoint
}

type tableActivityPoint struct {
	minute      time.Time
	rows        int
	insertRows  int
	updateRows  int
	deleteRows  int
	eventCount  int
	binlogBytes int64
	ddlCount    int
}

// NewTableAggregator creates a new TableAggregator.
func NewTableAggregator() *TableAggregator {
	return &TableAggregator{
		tables: make(map[string]*tableStats),
	}
}

// Consume processes a normalized event and updates table statistics.
// Only row mutation events (INSERT, UPDATE, DELETE) are counted.
// Non-row events like TABLE_MAP, BEGIN, COMMIT are ignored.
func (a *TableAggregator) Consume(ev model.NormalizedEvent) {
	if ev.Schema == "" || ev.Table == "" {
		return
	}

	if !isRowMutation(ev.Operation) && ev.EventType != "DDL" {
		return
	}

	key := ev.Schema + "." + ev.Table
	ts, exists := a.tables[key]
	if !exists {
		ts = &tableStats{
			schema:   ev.Schema,
			table:    ev.Table,
			txnSet:   make(map[string]struct{}),
			activity: make(map[time.Time]*tableActivityPoint),
		}
		a.tables[key] = ts
	}

	ts.eventCount++
	ts.binlogBytes += ev.BinlogBytes
	if ts.lastChangedAt.IsZero() || ev.Timestamp.After(ts.lastChangedAt) {
		ts.lastChangedAt = ev.Timestamp
	}

	if ev.EventType == "DDL" {
		ts.ddlCount++
	} else {
		ts.totalRows += ev.RowCount
		switch ev.Operation {
		case "INSERT":
			ts.insertRows += ev.RowCount
		case "UPDATE":
			ts.updateRows += ev.RowCount
		case "DELETE":
			ts.deleteRows += ev.RowCount
		}
	}

	minute := truncateToMinute(ev.Timestamp)
	point, exists := ts.activity[minute]
	if !exists {
		point = &tableActivityPoint{minute: minute}
		ts.activity[minute] = point
	}
	point.eventCount++
	point.binlogBytes += ev.BinlogBytes
	if ev.EventType == "DDL" {
		point.ddlCount++
	} else {
		point.rows += ev.RowCount
		switch ev.Operation {
		case "INSERT":
			point.insertRows += ev.RowCount
		case "UPDATE":
			point.updateRows += ev.RowCount
		case "DELETE":
			point.deleteRows += ev.RowCount
		}
	}

	// Track distinct transaction
	if ev.TxnKey != "" {
		ts.txnSet[ev.TxnKey] = struct{}{}
	}
}

// Snapshot returns all table statistics sorted for reporting.
// Sort order: TotalRows DESC, Schema ASC, Table ASC
func (a *TableAggregator) Snapshot() []model.TableStats {
	result := make([]model.TableStats, 0, len(a.tables))
	for _, ts := range a.tables {
		activity := make([]model.TableActivityPoint, 0, len(ts.activity))
		for _, point := range ts.activity {
			activity = append(activity, model.TableActivityPoint{
				Minute:      point.minute,
				Rows:        point.rows,
				InsertRows:  point.insertRows,
				UpdateRows:  point.updateRows,
				DeleteRows:  point.deleteRows,
				EventCount:  point.eventCount,
				BinlogBytes: point.binlogBytes,
				DDLCount:    point.ddlCount,
			})
		}
		sort.Slice(activity, func(i, j int) bool {
			return activity[i].Minute.Before(activity[j].Minute)
		})

		result = append(result, model.TableStats{
			Schema:        ts.schema,
			Table:         ts.table,
			TotalRows:     ts.totalRows,
			InsertRows:    ts.insertRows,
			UpdateRows:    ts.updateRows,
			DeleteRows:    ts.deleteRows,
			TxnCount:      len(ts.txnSet),
			EventCount:    ts.eventCount,
			BinlogBytes:   ts.binlogBytes,
			DDLCount:      ts.ddlCount,
			LastChangedAt: ts.lastChangedAt,
			Activity:      activity,
		})
	}

	// Sort for deterministic output
	sort.Slice(result, func(i, j int) bool {
		if result[i].TotalRows != result[j].TotalRows {
			return result[i].TotalRows > result[j].TotalRows // DESC
		}
		if result[i].Schema != result[j].Schema {
			return result[i].Schema < result[j].Schema // ASC
		}
		return result[i].Table < result[j].Table // ASC
	})

	return result
}

// isRowMutation returns true if the operation represents a row mutation.
func isRowMutation(op string) bool {
	switch op {
	case "INSERT", "UPDATE", "DELETE":
		return true
	default:
		return false
	}
}
