package analyzer

import (
	"sort"

	"binlogviz/internal/model"
)

// TableAggregator tracks per-table write statistics.
type TableAggregator struct {
	tables map[string]*tableStats // key: "schema.table"
}

type tableStats struct {
	schema     string
	table      string
	totalRows  int
	insertRows int
	updateRows int
	deleteRows int
	txnSet     map[string]struct{} // distinct transactions that touched this table
}

// NewTableAggregator creates a new TableAggregator.
func NewTableAggregator() *TableAggregator {
	return &TableAggregator{
		tables: make(map[string]*tableStats),
	}
}

// Consume processes a normalized event and updates table statistics.
func (a *TableAggregator) Consume(ev model.NormalizedEvent) {
	if ev.Schema == "" || ev.Table == "" {
		return
	}

	key := ev.Schema + "." + ev.Table
	ts, exists := a.tables[key]
	if !exists {
		ts = &tableStats{
			schema: ev.Schema,
			table:  ev.Table,
			txnSet: make(map[string]struct{}),
		}
		a.tables[key] = ts
	}

	ts.totalRows += ev.RowCount
	switch ev.Operation {
	case "INSERT":
		ts.insertRows += ev.RowCount
	case "UPDATE":
		ts.updateRows += ev.RowCount
	case "DELETE":
		ts.deleteRows += ev.RowCount
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
		result = append(result, model.TableStats{
			Schema:     ts.schema,
			Table:      ts.table,
			TotalRows:  ts.totalRows,
			InsertRows: ts.insertRows,
			UpdateRows: ts.updateRows,
			DeleteRows: ts.deleteRows,
			TxnCount:   len(ts.txnSet),
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
