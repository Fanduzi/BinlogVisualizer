// Package compare computes stable comparison results from validated input reports.
// input: two validated InputReport values representing current and baseline analyses.
// output: deterministic CompareResult values for text, JSON, and HTML renderers, including current replay evidence.
// pos: compare pipeline core between report loading and output rendering.
// note: if this file changes, keep internal/compare/README.md synchronized.
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
		AlertChanges:     buildAlertChangesFromReports(current, baseline),
		CurrentLabel:     compareLabel(current.Snapshot, "current"),
		BaselineLabel:    compareLabel(baseline.Snapshot, "baseline"),
		CurrentSnapshot:  current.Snapshot,
		BaselineSnapshot: baseline.Snapshot,
		DiagnosticsDelta: buildDiagnosticsDelta(current, baseline),
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
		item.DeltaPercent = deltaPercent(item.CurrentRows, item.BaselineRows)
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
		item.DeltaPercent = deltaPercent(item.CurrentRows, item.BaselineRows)
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
	return buildAlertChangesWithTxns(current, baseline, nil, nil)
}

func buildAlertChangesFromReports(current, baseline InputReport) AlertDelta {
	return buildAlertChangesWithTxns(current.Alerts, baseline.Alerts, indexReportTransactions(current), indexReportTransactions(baseline))
}

func buildAlertChangesWithTxns(current, baseline []InputAlert, currentTxns, baselineTxns map[string]InputTransaction) AlertDelta {
	baselineSet := make(map[string]InputAlert, len(baseline))
	currentSet := make(map[string]InputAlert, len(current))

	for _, alert := range baseline {
		baselineSet[alertIdentity(alert, baselineTxns)] = alert
	}
	for _, alert := range current {
		currentSet[alertIdentity(alert, currentTxns)] = alert
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
		return alertIdentity(result.Added[i], currentTxns) < alertIdentity(result.Added[j], currentTxns)
	})
	sort.Slice(result.Removed, func(i, j int) bool {
		return alertIdentity(result.Removed[i], baselineTxns) < alertIdentity(result.Removed[j], baselineTxns)
	})

	return result
}

func alertIdentity(alert InputAlert, txns map[string]InputTransaction) string {
	if alert.Type == "large_transaction" {
		return "large_transaction|" + largeTxnContentID(alert, txns)
	}
	return fmt.Sprintf("%s|%s|%s|%s|%s", alert.Type, alert.Severity, alert.Message, alert.TxnKey, alert.Minute)
}

func buildDiagnosticsDelta(current, baseline InputReport) DiagnosticsDelta {
	currentDiagnostics := current.Diagnostics
	baselineDiagnostics := baseline.Diagnostics
	if len(currentDiagnostics.LargestTransactions) == 0 {
		currentDiagnostics.LargestTransactions = current.Transactions
	}
	if len(currentDiagnostics.LongestTransactions) == 0 {
		currentDiagnostics.LongestTransactions = current.Transactions
	}
	if len(baselineDiagnostics.LargestTransactions) == 0 {
		baselineDiagnostics.LargestTransactions = baseline.Transactions
	}
	if len(baselineDiagnostics.LongestTransactions) == 0 {
		baselineDiagnostics.LongestTransactions = baseline.Transactions
	}
	return DiagnosticsDelta{
		DDLChanges:       buildDDLChangeDelta(current.Diagnostics.DDLEvents, baseline.Diagnostics.DDLEvents),
		TxnDiagnostics:   buildTxnDiagnosticDelta(currentDiagnostics, baselineDiagnostics),
		HotIntervalDelta: buildHotIntervalDelta(current.Diagnostics.HotIntervals, baseline.Diagnostics.HotIntervals),
		EventMixDelta:    buildEventMixDelta(current.Timeseries, baseline.Timeseries),
	}
}

func buildDDLChangeDelta(current, baseline []InputDDLEvent) DDLChangeDelta {
	delta := DDLChangeDelta{
		BaselineCount: len(baseline),
		CurrentCount:  len(current),
		Delta:         len(current) - len(baseline),
	}

	baselineSet := make(map[string]bool, len(baseline))
	for _, evt := range baseline {
		baselineSet[ddlEventKey(evt)] = true
	}
	currentSet := make(map[string]bool, len(current))
	for _, evt := range current {
		currentSet[ddlEventKey(evt)] = true
	}

	for _, evt := range current {
		if !baselineSet[ddlEventKey(evt)] {
			delta.Added = append(delta.Added, DDLEventItem{
				Timestamp: evt.Timestamp,
				Schema:    evt.Schema,
				Table:     evt.Table,
				Operation: evt.Operation,
				Statement: evt.Statement,
			})
		}
	}
	for _, evt := range baseline {
		if !currentSet[ddlEventKey(evt)] {
			delta.Removed = append(delta.Removed, DDLEventItem{
				Timestamp: evt.Timestamp,
				Schema:    evt.Schema,
				Table:     evt.Table,
				Operation: evt.Operation,
				Statement: evt.Statement,
			})
		}
	}

	sort.Slice(delta.Added, func(i, j int) bool {
		return delta.Added[i].Timestamp < delta.Added[j].Timestamp
	})
	sort.Slice(delta.Removed, func(i, j int) bool {
		return delta.Removed[i].Timestamp < delta.Removed[j].Timestamp
	})

	return delta
}

func ddlEventKey(evt InputDDLEvent) string {
	return fmt.Sprintf("%s|%s|%s|%s", evt.Timestamp, evt.Schema, evt.Table, evt.Operation)
}

func buildTxnDiagnosticDelta(current, baseline InputDiagnostics) TxnDiagnosticDelta {
	return TxnDiagnosticDelta{
		LargestTxnDelta: buildTxnSizeCompare(
			firstTxn(baseline.LargestTransactions),
			firstTxn(current.LargestTransactions),
		),
		LongestTxnDelta: buildTxnDurationCompare(
			baseline.LongestTransactions,
			current.LongestTransactions,
		),
	}
}

func buildTxnSizeCompare(baseline, current InputTransaction) TxnSizeCompare {
	cmp := TxnSizeCompare{
		BaselineRows:    baseline.TotalRows,
		BaselineKey:     baseline.TxnKey,
		CurrentRows:     current.TotalRows,
		CurrentKey:      current.TxnKey,
		DeltaRows:       current.TotalRows - baseline.TotalRows,
		BaselineTable:   dominantMapKey(baseline.Tables),
		CurrentTable:    dominantMapKey(current.Tables),
		BaselineOp:      dominantMapKey(baseline.Operations),
		CurrentOp:       dominantMapKey(current.Operations),
		CurrentEvidence: TransactionEvidenceFor(current),
	}
	if !txnContentEqual(baseline, current) && !txnIsEmpty(current) {
		cmp.IdentityNew = true
	}
	return cmp
}

func firstTxn(txns []InputTransaction) InputTransaction {
	if len(txns) == 0 {
		return InputTransaction{}
	}
	return txns[0]
}

func firstTxnKey(txns []InputTransaction) string {
	if len(txns) == 0 {
		return ""
	}
	return txns[0].TxnKey
}

func buildTxnDurationCompare(baseline, current []InputTransaction) TxnDurationCompare {
	currentTxn := firstTxn(current)
	return TxnDurationCompare{
		BaselineDuration: firstTxnDuration(baseline),
		BaselineKey:      firstTxnKey(baseline),
		CurrentDuration:  firstTxnDuration(current),
		CurrentKey:       firstTxnKey(current),
		CurrentEvidence:  TransactionEvidenceFor(currentTxn),
	}
}

func firstTxnDuration(txns []InputTransaction) string {
	if len(txns) == 0 {
		return ""
	}
	return txns[0].Duration
}

func buildHotIntervalDelta(current, baseline []InputHotInterval) HotIntervalDelta {
	delta := HotIntervalDelta{
		BaselineCount: len(baseline),
		CurrentCount:  len(current),
	}

	if len(baseline) > 0 {
		delta.BaselineTopRows = baseline[0].TotalRows
	}
	if len(current) > 0 {
		delta.CurrentTopRows = current[0].TotalRows
	}
	delta.DeltaTopRows = delta.CurrentTopRows - delta.BaselineTopRows

	limit := 5
	for i, item := range current {
		if i >= limit {
			break
		}
		delta.TopItems = append(delta.TopItems, HotIntervalItem{
			Minute:      item.Minute,
			Source:      "current",
			TotalRows:   item.TotalRows,
			TxnCount:    item.TxnCount,
			BinlogBytes: item.BinlogBytes,
		})
	}
	for i, item := range baseline {
		if i >= limit {
			break
		}
		delta.TopItems = append(delta.TopItems, HotIntervalItem{
			Minute:      item.Minute,
			Source:      "baseline",
			TotalRows:   item.TotalRows,
			TxnCount:    item.TxnCount,
			BinlogBytes: item.BinlogBytes,
		})
	}

	sort.SliceStable(delta.TopItems, func(i, j int) bool {
		if delta.TopItems[i].TotalRows != delta.TopItems[j].TotalRows {
			return delta.TopItems[i].TotalRows > delta.TopItems[j].TotalRows
		}
		return delta.TopItems[i].Minute < delta.TopItems[j].Minute
	})

	return delta
}

func buildEventMixDelta(current, baseline InputTimeseries) EventMixDelta {
	return EventMixDelta{
		InsertDelta: sumSeriesValues(current.InsertEventSeries) - sumSeriesValues(baseline.InsertEventSeries),
		UpdateDelta: sumSeriesValues(current.UpdateEventSeries) - sumSeriesValues(baseline.UpdateEventSeries),
		DeleteDelta: sumSeriesValues(current.DeleteEventSeries) - sumSeriesValues(baseline.DeleteEventSeries),
		DDLDelta:    sumSeriesValues(current.DDLEventSeries) - sumSeriesValues(baseline.DDLEventSeries),
	}
}

func sumSeriesValues(points []InputTimeseriesPoint) int {
	var total float64
	for _, p := range points {
		total += p.Value
	}
	return int(total)
}
