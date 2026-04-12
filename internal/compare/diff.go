// Package compare computes stable comparison results from validated input reports.
// input: two validated InputReport values representing current and baseline analyses.
// output: deterministic CompareResult values for text, JSON, and HTML renderers.
// pos: compare pipeline core between report loading and output rendering.
package compare

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

func BuildCompareResult(current, baseline InputReport) CompareResult {
	result := CompareResult{
		Summary: SummaryDelta{
			CurrentTotalRows:          current.Summary.TotalRows,
			BaselineTotalRows:         baseline.Summary.TotalRows,
			TotalRowsDelta:            current.Summary.TotalRows - baseline.Summary.TotalRows,
			CurrentTotalTransactions:  current.Summary.TotalTransactions,
			BaselineTotalTransactions: baseline.Summary.TotalTransactions,
			TotalTransactionsDelta:    current.Summary.TotalTransactions - baseline.Summary.TotalTransactions,
			CurrentWarnings:           current.Warnings,
			BaselineWarnings:          baseline.Warnings,
		},
		TableChanges:     buildTableChanges(current.Tables, baseline.Tables),
		PatternChanges:   buildPatternChanges(current.Patterns, baseline.Patterns),
		OperationMix:     buildOperationMix(current.Tables, baseline.Tables),
		AlertChanges:     buildAlertChanges(current.Alerts, baseline.Alerts),
		CurrentLabel:     compareLabel(current.Snapshot, "current"),
		BaselineLabel:    compareLabel(baseline.Snapshot, "baseline"),
		CurrentSnapshot:  current.Snapshot,
		BaselineSnapshot: baseline.Snapshot,
	}
	result.KeyFindings = buildKeyFindings(result)
	buildCompareEvidenceRefs(&result)
	result.Recommendations = buildCompareRecommendations(result)
	result.PatternDrilldowns = buildComparePatternDrilldowns(result)
	return result
}

func compareLabel(snapshot *InputSnapshot, fallback string) string {
	if snapshot == nil {
		return fallback
	}

	label := strings.TrimSpace(snapshot.Label)
	name := strings.TrimSpace(snapshot.Name)

	switch {
	case label != "" && name != "" && label != name:
		return fmt.Sprintf("%s (%s)", label, name)
	case label != "":
		return label
	case name != "":
		return name
	default:
		return fallback
	}
}

func buildTableChanges(current, baseline []InputTable) []TableChange {
	type tableKey struct {
		schema string
		table  string
	}

	merged := make(map[tableKey]TableChange, len(current)+len(baseline))

	for _, item := range baseline {
		key := tableKey{schema: item.Schema, table: item.Table}
		merged[key] = TableChange{
			Schema:       item.Schema,
			Table:        item.Table,
			BaselineRows: item.TotalRows,
		}
	}

	for _, item := range current {
		key := tableKey{schema: item.Schema, table: item.Table}
		change := merged[key]
		change.Schema = item.Schema
		change.Table = item.Table
		change.CurrentRows = item.TotalRows
		merged[key] = change
	}

	result := make([]TableChange, 0, len(merged))
	for _, item := range merged {
		item.DeltaRows = item.CurrentRows - item.BaselineRows
		if item.BaselineRows > 0 {
			item.DeltaPercent = (float64(item.DeltaRows) / float64(item.BaselineRows)) * 100
		}
		result = append(result, item)
	}

	sort.Slice(result, func(i, j int) bool {
		left := math.Abs(float64(result[i].DeltaRows))
		right := math.Abs(float64(result[j].DeltaRows))
		if left == right {
			return fmt.Sprintf("%s.%s", result[i].Schema, result[i].Table) < fmt.Sprintf("%s.%s", result[j].Schema, result[j].Table)
		}
		return left > right
	})

	return result
}

func buildPatternChanges(current, baseline []InputPattern) []PatternChange {
	merged := make(map[string]PatternChange, len(current)+len(baseline))

	for _, item := range baseline {
		merged[item.PatternKey] = PatternChange{
			PatternKey:         item.PatternKey,
			Label:              item.Label,
			BaselineRows:       item.TotalRows,
			BaselineTxnCount:   item.TxnCount,
			Tables:             cloneIntMap(item.Tables),
			Operations:         cloneIntMap(item.Operations),
			SampleQuerySummary: item.SampleQuerySummary,
		}
	}

	for _, item := range current {
		change := merged[item.PatternKey]
		change.PatternKey = item.PatternKey
		change.Label = preferredPatternLabel(item.Label, change.Label)
		change.CurrentRows = item.TotalRows
		change.CurrentTxnCount = item.TxnCount
		change.Tables = cloneIntMap(item.Tables)
		change.Operations = cloneIntMap(item.Operations)
		if strings.TrimSpace(item.SampleQuerySummary) != "" {
			change.SampleQuerySummary = item.SampleQuerySummary
		}
		merged[item.PatternKey] = change
	}

	result := make([]PatternChange, 0, len(merged))
	for _, item := range merged {
		item.DeltaRows = item.CurrentRows - item.BaselineRows
		item.DeltaTxnCount = item.CurrentTxnCount - item.BaselineTxnCount
		if item.BaselineRows > 0 {
			item.DeltaPercent = (float64(item.DeltaRows) / float64(item.BaselineRows)) * 100
		}
		result = append(result, item)
	}

	sort.Slice(result, func(i, j int) bool {
		leftRows := math.Abs(float64(result[i].DeltaRows))
		rightRows := math.Abs(float64(result[j].DeltaRows))
		if leftRows != rightRows {
			return leftRows > rightRows
		}

		leftTxns := math.Abs(float64(result[i].DeltaTxnCount))
		rightTxns := math.Abs(float64(result[j].DeltaTxnCount))
		if leftTxns != rightTxns {
			return leftTxns > rightTxns
		}

		return result[i].PatternKey < result[j].PatternKey
	})

	return result
}

func buildOperationMix(current, baseline []InputTable) []OperationDelta {
	currentInsert, currentUpdate, currentDelete := sumOperations(current)
	baselineInsert, baselineUpdate, baselineDelete := sumOperations(baseline)

	return []OperationDelta{
		{Operation: "INSERT", Current: currentInsert, Baseline: baselineInsert, Delta: currentInsert - baselineInsert},
		{Operation: "UPDATE", Current: currentUpdate, Baseline: baselineUpdate, Delta: currentUpdate - baselineUpdate},
		{Operation: "DELETE", Current: currentDelete, Baseline: baselineDelete, Delta: currentDelete - baselineDelete},
	}
}

func sumOperations(tables []InputTable) (int, int, int) {
	var inserts, updates, deletes int
	for _, table := range tables {
		inserts += table.InsertRows
		updates += table.UpdateRows
		deletes += table.DeleteRows
	}
	return inserts, updates, deletes
}

func cloneIntMap(src map[string]int) map[string]int {
	if len(src) == 0 {
		return map[string]int{}
	}

	dst := make(map[string]int, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func preferredPatternLabel(current, fallback string) string {
	if strings.TrimSpace(current) != "" {
		return current
	}
	return fallback
}

func buildAlertChanges(current, baseline []InputAlert) AlertDelta {
	baselineSet := make(map[string]InputAlert, len(baseline))
	currentSet := make(map[string]InputAlert, len(current))

	for _, alert := range baseline {
		baselineSet[alertKey(alert)] = alert
	}
	for _, alert := range current {
		currentSet[alertKey(alert)] = alert
	}

	result := AlertDelta{}
	for key, alert := range currentSet {
		if _, ok := baselineSet[key]; !ok {
			result.Added = append(result.Added, alert)
		}
		delete(baselineSet, key)
	}
	for _, alert := range baselineSet {
		result.Removed = append(result.Removed, alert)
	}

	sort.Slice(result.Added, func(i, j int) bool {
		return alertKey(result.Added[i]) < alertKey(result.Added[j])
	})
	sort.Slice(result.Removed, func(i, j int) bool {
		return alertKey(result.Removed[i]) < alertKey(result.Removed[j])
	})

	return result
}

func alertKey(alert InputAlert) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s", alert.Type, alert.Severity, alert.Message, alert.TxnKey, alert.Minute)
}
