// Package compare renders human-readable text reports for compare results.
// input: deterministic CompareResult values with structured comparability evidence produced by the compare diff engine.
// output: fixed-section text compare reports leading with any narrative guard, visible identity/provenance/object-selector evidence, and named byte coverage, rendered through report.text.compare.* i18n keys.
// pos: compare renderer used by the compare command text output path.
// note: if this file changes, update this header and module README.md.
package compare

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"binlogviz/internal/displaytz"
	"binlogviz/internal/i18n"
)

func signedDelta(n int) string {
	return fmt.Sprintf("%+d", n)
}

func RenderText(result CompareResult) (string, error) {
	var b strings.Builder

	fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.summary"))
	guarded := hasComparabilityGuard(result.Comparability)
	if guarded {
		fmt.Fprintf(&b, "%s [%s]\n", ComparabilityGuardTitle(), result.Comparability.Verdict)
		fmt.Fprintf(&b, "%s\n", ComparabilityGuardSummary(result.Comparability.Verdict))
		fmt.Fprintf(&b, "%s %s\n", ComparabilityReasonCodesLabel(), strings.Join(result.Comparability.ReasonCodes, ", "))
		for _, evidence := range result.Comparability.Evidence {
			fmt.Fprintf(&b, "- %s\n", FormatComparabilityEvidence(evidence))
		}
		fmt.Fprint(&b, "\n")
	}
	fmt.Fprint(&b, i18n.Tf("report.text.compare.currentLabel", map[string]any{"Value": result.CurrentLabel}))
	fmt.Fprint(&b, i18n.Tf("report.text.compare.baselineLabel", map[string]any{"Value": result.BaselineLabel}))
	if window := formatSnapshotWindow(result.CurrentSnapshot); window != "" {
		fmt.Fprint(&b, i18n.Tf("report.text.compare.currentWindow", map[string]any{"Value": window}))
	}
	if window := formatSnapshotWindow(result.BaselineSnapshot); window != "" {
		fmt.Fprint(&b, i18n.Tf("report.text.compare.baselineWindow", map[string]any{"Value": window}))
	}
	if inputMode := formatSnapshotInputMode(result.CurrentSnapshot); inputMode != "" {
		fmt.Fprint(&b, i18n.Tf("report.text.compare.currentInputMode", map[string]any{"Value": inputMode}))
	}
	if inputMode := formatSnapshotInputMode(result.BaselineSnapshot); inputMode != "" {
		fmt.Fprint(&b, i18n.Tf("report.text.compare.baselineInputMode", map[string]any{"Value": inputMode}))
	}
	if source := formatSnapshotSource(result.CurrentSnapshot); source != "" {
		fmt.Fprint(&b, i18n.Tf("report.text.compare.currentSource", map[string]any{"Value": source}))
	}
	if source := formatSnapshotSource(result.BaselineSnapshot); source != "" {
		fmt.Fprint(&b, i18n.Tf("report.text.compare.baselineSource", map[string]any{"Value": source}))
	}
	if filters := formatSnapshotFilters(result.CurrentSnapshot); filters != "" {
		fmt.Fprint(&b, i18n.Tf("report.text.compare.currentFilters", map[string]any{"Value": filters}))
	}
	if filters := formatSnapshotFilters(result.BaselineSnapshot); filters != "" {
		fmt.Fprint(&b, i18n.Tf("report.text.compare.baselineFilters", map[string]any{"Value": filters}))
	}
	fmt.Fprint(&b, i18n.Tf("report.text.compare.rows", map[string]any{"Baseline": result.Summary.BaselineTotalRows, "Current": result.Summary.CurrentTotalRows, "Delta": signedDelta(result.Summary.TotalRowsDelta)}))
	fmt.Fprint(&b, i18n.Tf("report.text.compare.transactions", map[string]any{"Baseline": result.Summary.BaselineTotalTransactions, "Current": result.Summary.CurrentTotalTransactions, "Delta": signedDelta(result.Summary.TotalTransactionsDelta)}))
	fmt.Fprint(&b, i18n.Tf("report.text.compare.warnings", map[string]any{"Baseline": result.Summary.BaselineWarnings, "Current": result.Summary.CurrentWarnings, "Delta": signedDelta(result.Summary.CurrentWarnings - result.Summary.BaselineWarnings)}))
	fmt.Fprint(&b, i18n.Tf("report.text.compare.inputFileSize", map[string]any{"BaselineLabel": result.BaselineLabel, "BaselineSize": formatOptionalCompareFileSize(result.DiagnosticsDelta.BaselineInputFileBytes), "CurrentLabel": result.CurrentLabel, "CurrentSize": formatOptionalCompareFileSize(result.DiagnosticsDelta.CurrentInputFileBytes)}))
	fmt.Fprint(&b, i18n.Tf("report.text.compare.countedEventBytes", map[string]any{"BaselineLabel": result.BaselineLabel, "BaselineSize": formatCompareFileSize(result.DiagnosticsDelta.BaselineCountedEventBytes), "CurrentLabel": result.CurrentLabel, "CurrentSize": formatCompareFileSize(result.DiagnosticsDelta.CurrentCountedEventBytes)}))
	if line := formatLargestTxnLine(result.DiagnosticsDelta.TxnDiagnostics.LargestTxnDelta); line != "" {
		fmt.Fprintf(&b, "%s\n", line)
	}
	fmt.Fprint(&b, "\n")

	if len(result.KeyFindings) > 0 && !guarded {
		fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.keyFindings"))
		for i, finding := range result.KeyFindings {
			fmt.Fprintf(&b, "%d. [%s] %s\n", i+1, finding.Kind, finding.Summary)
			if len(finding.EvidenceRefs) > 0 {
				labels := make([]string, 0, len(finding.EvidenceRefs))
				for _, ref := range finding.EvidenceRefs {
					labels = append(labels, ref.Label)
				}
				fmt.Fprint(&b, i18n.Tf("report.text.compare.evidence", map[string]any{"Labels": strings.Join(labels, ", ")}))
			}
		}
		fmt.Fprintf(&b, "\n")
	}

	if len(result.Recommendations) > 0 {
		fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.recommendedNextChecks"))
		for i, rec := range result.Recommendations {
			fmt.Fprintf(&b, "%d. [%s] %s\n", i+1, rec.Priority, rec.Title)
			fmt.Fprintf(&b, "   %s\n", rec.Summary)
			if len(rec.EvidenceRefs) > 0 {
				labels := make([]string, 0, len(rec.EvidenceRefs))
				for _, ref := range rec.EvidenceRefs {
					labels = append(labels, ref.Label)
				}
				fmt.Fprint(&b, i18n.Tf("report.text.compare.evidence", map[string]any{"Labels": strings.Join(labels, ", ")}))
			}
		}
		fmt.Fprintf(&b, "\n")
	}

	fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.topTableChanges"))
	if len(result.TableChanges) == 0 {
		fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.noneList"))
	} else {
		for _, change := range result.TableChanges {
			fmt.Fprintf(&b, "- %s.%s: %d -> %d (%+d, %s)\n", change.Schema, change.Table, change.BaselineRows, change.CurrentRows, change.DeltaRows, formatDeltaPercent(change.CurrentRows, change.BaselineRows, change.DeltaPercent))
		}
	}

	fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.topPatternChanges"))
	if len(result.PatternChanges) == 0 {
		fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.noneList"))
	} else {
		drilldownMap := make(map[string]PatternDrilldown, len(result.PatternDrilldowns))
		for _, d := range result.PatternDrilldowns {
			drilldownMap[d.PatternKey] = d
		}
		for _, change := range result.PatternChanges {
			fmt.Fprintf(
				&b,
				"- %s: %d -> %d (%+d, %s), txns %d -> %d (%+d)\n",
				change.Label,
				change.BaselineRows,
				change.CurrentRows,
				change.DeltaRows,
				formatDeltaPercent(change.CurrentRows, change.BaselineRows, change.DeltaPercent),
				change.BaselineTxnCount,
				change.CurrentTxnCount,
				change.DeltaTxnCount,
			)
			if strings.TrimSpace(change.SampleQuerySummary) != "" {
				fmt.Fprint(&b, i18n.Tf("report.text.compare.queryLine", map[string]any{"Query": change.SampleQuerySummary}))
			}
			if dd, ok := drilldownMap[change.PatternKey]; ok {
				fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.drilldown"))
				fmt.Fprint(&b, i18n.Tf("report.text.compare.whyLine", map[string]any{"Why": dd.WhySelected}))
				for _, kp := range dd.KeyPoints {
					fmt.Fprintf(&b, "    %s: %s\n", kp.Label, kp.Summary)
				}
			}
		}
	}

	fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.operationMix"))
	if len(result.OperationMix) == 0 {
		fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.noneList"))
	} else {
		for _, change := range result.OperationMix {
			fmt.Fprintf(&b, "- %s: %d -> %d (%+d)\n", change.Operation, change.Baseline, change.Current, change.Delta)
		}
	}

	fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.alertChanges"))
	fmt.Fprint(&b, i18n.Tf("report.text.compare.addedAlerts", map[string]any{"Count": len(result.AlertChanges.Added)}))
	if len(result.AlertChanges.Added) == 0 {
		fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.noneList"))
	} else {
		for _, alert := range result.AlertChanges.Added {
			fmt.Fprintf(&b, "- [%s] %s\n", strings.ToUpper(alert.Type), alert.Message)
		}
	}
	fmt.Fprint(&b, i18n.Tf("report.text.compare.removedAlerts", map[string]any{"Count": len(result.AlertChanges.Removed)}))
	if len(result.AlertChanges.Removed) == 0 {
		fmt.Fprintf(&b, "%s", i18n.T("report.text.compare.noneList"))
	} else {
		for _, alert := range result.AlertChanges.Removed {
			fmt.Fprintf(&b, "- [%s] %s\n", strings.ToUpper(alert.Type), alert.Message)
		}
	}

	return b.String(), nil
}

func formatLargestTxnLine(delta TxnSizeCompare) string {
	if txnSizeCompareEmpty(delta) {
		return ""
	}
	line := i18n.Tf("report.text.compare.largestTxn", map[string]any{
		"Baseline": formatTxnSide(delta.BaselineTable, delta.BaselineOp, delta.BaselineRows),
		"Current":  formatTxnSide(delta.CurrentTable, delta.CurrentOp, delta.CurrentRows),
	})
	if delta.IdentityNew {
		line += i18n.T("report.text.compare.newSuffix")
	}
	return line
}

func txnSizeCompareEmpty(delta TxnSizeCompare) bool {
	return delta.BaselineRows == 0 && delta.CurrentRows == 0 &&
		delta.BaselineKey == "" && delta.CurrentKey == "" &&
		delta.BaselineTable == "" && delta.CurrentTable == ""
}

func formatTxnSide(table, op string, rows int) string {
	parts := make([]string, 0, 3)
	if table != "" {
		parts = append(parts, table)
	}
	if op != "" {
		parts = append(parts, op)
	}
	parts = append(parts, fmt.Sprintf("%d", rows))
	return strings.Join(parts, " ")
}

func formatSnapshotWindow(snapshot *InputSnapshot) string {
	return formatSnapshotWindowIn(snapshot, nil)
}

func formatSnapshotWindowIn(snapshot *InputSnapshot, loc *time.Location) string {
	if snapshot == nil {
		return ""
	}

	start := displaytz.Stored(strings.TrimSpace(snapshot.Window.StartTime), loc)
	end := displaytz.Stored(strings.TrimSpace(snapshot.Window.EndTime), loc)

	switch {
	case start != "" && end != "":
		return fmt.Sprintf("%s -> %s", start, end)
	case start != "":
		return start
	case end != "":
		return end
	default:
		return ""
	}
}

func formatSnapshotInputMode(snapshot *InputSnapshot) string {
	if snapshot == nil {
		return ""
	}
	return strings.TrimSpace(snapshot.InputMode)
}

func formatSnapshotSource(snapshot *InputSnapshot) string {
	if snapshot == nil {
		return ""
	}

	parts := make([]string, 0, 3)
	if count := len(snapshot.Input.Files); count > 0 {
		parts = append(parts, fmt.Sprintf("files=%d", count))
	}
	if fromDir := strings.TrimSpace(snapshot.Input.FromDir); fromDir != "" {
		parts = append(parts, "from_dir="+fromDir)
	}
	if prefix := strings.TrimSpace(snapshot.Input.Prefix); prefix != "" {
		parts = append(parts, "prefix="+prefix)
	}
	return strings.Join(parts, " ")
}

func formatSnapshotFilters(snapshot *InputSnapshot) string {
	if snapshot == nil {
		return ""
	}
	return formatScopeFilters(snapshot.Filters)
}

func formatScopeFilters(filters InputSnapshotFilters) string {
	parts := make([]string, 0, 4)
	if len(filters.IncludeSchemas) > 0 {
		parts = append(parts, "include_schema="+strings.Join(filters.IncludeSchemas, ","))
	}
	if len(filters.ExcludeSchemas) > 0 {
		parts = append(parts, "exclude_schema="+strings.Join(filters.ExcludeSchemas, ","))
	}
	if len(filters.IncludeTables) > 0 {
		parts = append(parts, "include_table="+strings.Join(filters.IncludeTables, ","))
	}
	if len(filters.ExcludeTables) > 0 {
		parts = append(parts, "exclude_table="+strings.Join(filters.ExcludeTables, ","))
	}
	slices.Sort(parts)
	return strings.Join(parts, " ")
}

func hasComparabilityGuard(comparability Comparability) bool {
	return comparability.Verdict == VerdictNotComparable || comparability.Verdict == VerdictUnknown
}

// FormatComparabilityEvidence renders one compact operator-visible evidence line.
func FormatComparabilityEvidence(evidence ComparabilityEvidence) string {
	parts := []string{evidence.Role}
	if evidence.Name != "" {
		parts = append(parts, "name="+evidence.Name)
	}
	if evidence.ReportVersion != nil {
		parts = append(parts, fmt.Sprintf("report_version=%d", *evidence.ReportVersion))
	} else {
		parts = append(parts, "report_version=legacy")
	}
	if evidence.WorkloadID != "" {
		parts = append(parts, "workload_id="+evidence.WorkloadID)
	} else {
		parts = append(parts, "workload_id=unknown")
	}
	if len(evidence.ServerIDs) > 0 {
		parts = append(parts, fmt.Sprintf("server_ids=%v", evidence.ServerIDs))
	}
	if len(evidence.ServerVersions) > 0 {
		parts = append(parts, "server_versions="+strings.Join(evidence.ServerVersions, ","))
	}
	if len(evidence.ServerFlavors) > 0 {
		parts = append(parts, "server_flavors="+strings.Join(evidence.ServerFlavors, ","))
	}
	if evidence.MixedProducers {
		parts = append(parts, "mixed_producers=true")
	}
	if len(evidence.Schemas) > 0 {
		parts = append(parts, "schemas="+strings.Join(evidence.Schemas, ","))
	}
	if evidence.Scope != nil {
		scope := formatScopeFilters(*evidence.Scope)
		if scope == "" {
			scope = "all"
		}
		parts = append(parts, "scope="+scope)
	} else {
		parts = append(parts, "scope=unknown")
	}
	if evidence.Selection != nil {
		parts = append(parts, "selection="+formatSelection(*evidence.Selection))
	}
	parts = append(parts, fmt.Sprintf("total_transactions=%d", evidence.TotalTransactions))
	parts = append(parts, "partial_transactions="+formatComparabilityCount(evidence.PartialTransactions))
	parts = append(parts, "unknown_transactions="+formatComparabilityCount(evidence.UnknownTransactions))
	return strings.Join(parts, " ")
}

func formatSelection(selection InputSelection) string {
	parts := make([]string, 0, 8)
	if selection.RequestedStartPosition != nil {
		parts = append(parts, fmt.Sprintf("requested_start_position=%d", *selection.RequestedStartPosition))
	}
	if selection.RequestedStopPosition != nil {
		parts = append(parts, fmt.Sprintf("requested_stop_position=%d", *selection.RequestedStopPosition))
	}
	if selection.EffectiveStartPosition != nil {
		parts = append(parts, fmt.Sprintf("effective_start_position=%d", *selection.EffectiveStartPosition))
	}
	if selection.EffectiveStopPosition != nil {
		parts = append(parts, fmt.Sprintf("effective_stop_position=%d", *selection.EffectiveStopPosition))
	}
	if len(selection.IncludeGTIDs) > 0 {
		parts = append(parts, "include_gtids="+strings.Join(selection.IncludeGTIDs, ","))
	}
	if len(selection.ExcludeGTIDs) > 0 {
		parts = append(parts, "exclude_gtids="+strings.Join(selection.ExcludeGTIDs, ","))
	}
	if selection.ResolvedGTIDFlavor != "" {
		parts = append(parts, "resolved_gtid_flavor="+selection.ResolvedGTIDFlavor)
	}
	if len(selection.MatchedGTIDs) > 0 {
		parts = append(parts, "matched_gtids="+strings.Join(selection.MatchedGTIDs, ","))
	}
	if len(selection.MatchedGTIDSet) > 0 {
		parts = append(parts, "matched_gtid_set="+strings.Join(selection.MatchedGTIDSet, ","))
	}
	if selection.MatchedTransactions != nil {
		parts = append(parts, fmt.Sprintf("matched_transactions=%d", *selection.MatchedTransactions))
	}
	if len(parts) == 0 {
		return "explicit"
	}
	return strings.Join(parts, ",")
}

func formatComparabilityCount(value *int) string {
	if value == nil {
		return "unknown"
	}
	return fmt.Sprintf("%d", *value)
}
