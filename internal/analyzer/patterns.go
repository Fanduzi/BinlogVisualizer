// Package analyzer derives deterministic workload patterns from finalized transactions.
// input: reconstructed model.Transaction slices with table, operation, row, and optional query-summary context.
// output: sorted pattern aggregates with structural identity keys and operator-readable labels.
// pos: post-transaction aggregation layer used during analyze Finalize before rendering.
package analyzer

import (
	"fmt"
	"sort"
	"strings"

	"binlogviz/internal/model"
)

// BuildPatterns groups finalized transactions into repeated workload shapes.
func BuildPatterns(txns []model.Transaction) []model.PatternStats {
	if len(txns) == 0 {
		return []model.PatternStats{}
	}

	totalRows := 0
	totalTxns := len(txns)
	union := make(map[string]*model.PatternStats)
	for _, txn := range txns {
		totalRows += txn.TotalRows
		key, label := patternIdentity(txn)
		p := union[key]
		if p == nil {
			p = &model.PatternStats{
				PatternKey: key,
				Label:      label,
				Tables:     make(map[string]int),
				Operations: make(map[string]int),
			}
			union[key] = p
		}

		p.TotalRows += txn.TotalRows
		p.TxnCount++
		p.EventCount += txn.EventCount
		for k, v := range txn.Tables {
			p.Tables[k] += v
		}
		for k, v := range txn.Operations {
			p.Operations[k] += v
		}
		if p.SampleQuerySummary == "" && strings.TrimSpace(txn.QuerySummary) != "" {
			p.SampleQuerySummary = txn.QuerySummary
		}
	}

	result := make([]model.PatternStats, 0, len(union))
	for _, p := range union {
		if p.TxnCount > 0 {
			p.AvgRowsPerTxn = float64(p.TotalRows) / float64(p.TxnCount)
			p.ShareOfTransactions = float64(p.TxnCount) / float64(totalTxns)
		}
		if totalRows > 0 {
			p.ShareOfRows = float64(p.TotalRows) / float64(totalRows)
		}
		result = append(result, *p)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].TotalRows != result[j].TotalRows {
			return result[i].TotalRows > result[j].TotalRows
		}
		if result[i].TxnCount != result[j].TxnCount {
			return result[i].TxnCount > result[j].TxnCount
		}
		return result[i].PatternKey < result[j].PatternKey
	})
	return result
}

func patternIdentity(txn model.Transaction) (string, string) {
	tables := sortedMapKeys(txn.Tables)
	ops := sortedMapKeys(txn.Operations)
	shape := rowsPerEventBucket(txn.TotalRows, txn.EventCount)

	key := fmt.Sprintf("tables=%s|ops=%s|shape=%s", strings.Join(tables, "+"), strings.Join(ops, "+"), shape)
	if len(tables) == 0 && len(ops) == 0 {
		return key, "unknown workload shape"
	}

	label := fmt.Sprintf("%s / %s / %s batch", humanJoin(tables), humanJoin(ops), shape)
	return key, label
}

func sortedMapKeys(m map[string]int) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func rowsPerEventBucket(totalRows, eventCount int) string {
	if eventCount <= 0 {
		if totalRows <= 0 {
			return "tiny"
		}
		return "large"
	}

	rowsPerEvent := float64(totalRows) / float64(eventCount)
	switch {
	case rowsPerEvent < 2:
		return "tiny"
	case rowsPerEvent <= 5:
		return "small"
	case rowsPerEvent <= 20:
		return "medium"
	default:
		return "large"
	}
}

func humanJoin(parts []string) string {
	if len(parts) == 0 {
		return "unknown"
	}
	return strings.Join(parts, " + ")
}
