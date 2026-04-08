// Package compare renders human-readable text reports for compare results.
// input: deterministic CompareResult values produced by the compare diff engine.
// output: fixed-section text compare reports for terminal and file output.
// pos: compare renderer used by the compare command text output path.
package compare

import (
	"fmt"
	"slices"
	"strings"
)

func RenderText(result CompareResult) (string, error) {
	var b strings.Builder

	fmt.Fprintf(&b, "Compare Summary\n")
	fmt.Fprintf(&b, "Current Label: %s\n", result.CurrentLabel)
	fmt.Fprintf(&b, "Baseline Label: %s\n", result.BaselineLabel)
	if window := formatSnapshotWindow(result.CurrentSnapshot); window != "" {
		fmt.Fprintf(&b, "Current Window: %s\n", window)
	}
	if window := formatSnapshotWindow(result.BaselineSnapshot); window != "" {
		fmt.Fprintf(&b, "Baseline Window: %s\n", window)
	}
	if inputMode := formatSnapshotInputMode(result.CurrentSnapshot); inputMode != "" {
		fmt.Fprintf(&b, "Current Input Mode: %s\n", inputMode)
	}
	if inputMode := formatSnapshotInputMode(result.BaselineSnapshot); inputMode != "" {
		fmt.Fprintf(&b, "Baseline Input Mode: %s\n", inputMode)
	}
	if source := formatSnapshotSource(result.CurrentSnapshot); source != "" {
		fmt.Fprintf(&b, "Current Source: %s\n", source)
	}
	if source := formatSnapshotSource(result.BaselineSnapshot); source != "" {
		fmt.Fprintf(&b, "Baseline Source: %s\n", source)
	}
	if filters := formatSnapshotFilters(result.CurrentSnapshot); filters != "" {
		fmt.Fprintf(&b, "Current Filters: %s\n", filters)
	}
	if filters := formatSnapshotFilters(result.BaselineSnapshot); filters != "" {
		fmt.Fprintf(&b, "Baseline Filters: %s\n", filters)
	}
	fmt.Fprintf(&b, "Rows: %d -> %d (%+d)\n", result.Summary.BaselineTotalRows, result.Summary.CurrentTotalRows, result.Summary.TotalRowsDelta)
	fmt.Fprintf(&b, "Transactions: %d -> %d (%+d)\n", result.Summary.BaselineTotalTransactions, result.Summary.CurrentTotalTransactions, result.Summary.TotalTransactionsDelta)
	fmt.Fprintf(&b, "Warnings: %d -> %d (%+d)\n\n", result.Summary.BaselineWarnings, result.Summary.CurrentWarnings, result.Summary.CurrentWarnings-result.Summary.BaselineWarnings)

	fmt.Fprintf(&b, "Top Table Changes\n")
	if len(result.TableChanges) == 0 {
		fmt.Fprintf(&b, "- none\n")
	} else {
		for _, change := range result.TableChanges {
			fmt.Fprintf(&b, "- %s.%s: %d -> %d (%+d, %.1f%%)\n", change.Schema, change.Table, change.BaselineRows, change.CurrentRows, change.DeltaRows, change.DeltaPercent)
		}
	}

	fmt.Fprintf(&b, "\nTop Pattern Changes\n")
	if len(result.PatternChanges) == 0 {
		fmt.Fprintf(&b, "- none\n")
	} else {
		for _, change := range result.PatternChanges {
			fmt.Fprintf(
				&b,
				"- %s: %d -> %d (%+d, %.1f%%), txns %d -> %d (%+d)\n",
				change.Label,
				change.BaselineRows,
				change.CurrentRows,
				change.DeltaRows,
				change.DeltaPercent,
				change.BaselineTxnCount,
				change.CurrentTxnCount,
				change.DeltaTxnCount,
			)
			if strings.TrimSpace(change.SampleQuerySummary) != "" {
				fmt.Fprintf(&b, "  query: %s\n", change.SampleQuerySummary)
			}
		}
	}

	fmt.Fprintf(&b, "\nOperation Mix\n")
	if len(result.OperationMix) == 0 {
		fmt.Fprintf(&b, "- none\n")
	} else {
		for _, change := range result.OperationMix {
			fmt.Fprintf(&b, "- %s: %d -> %d (%+d)\n", change.Operation, change.Baseline, change.Current, change.Delta)
		}
	}

	fmt.Fprintf(&b, "\nAlert Changes\n")
	fmt.Fprintf(&b, "Added Alerts (%d)\n", len(result.AlertChanges.Added))
	if len(result.AlertChanges.Added) == 0 {
		fmt.Fprintf(&b, "- none\n")
	} else {
		for _, alert := range result.AlertChanges.Added {
			fmt.Fprintf(&b, "- [%s] %s\n", strings.ToUpper(alert.Type), alert.Message)
		}
	}
	fmt.Fprintf(&b, "Removed Alerts (%d)\n", len(result.AlertChanges.Removed))
	if len(result.AlertChanges.Removed) == 0 {
		fmt.Fprintf(&b, "- none\n")
	} else {
		for _, alert := range result.AlertChanges.Removed {
			fmt.Fprintf(&b, "- [%s] %s\n", strings.ToUpper(alert.Type), alert.Message)
		}
	}

	return b.String(), nil
}

func formatSnapshotWindow(snapshot *InputSnapshot) string {
	if snapshot == nil {
		return ""
	}

	start := strings.TrimSpace(snapshot.Window.StartTime)
	end := strings.TrimSpace(snapshot.Window.EndTime)

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

	parts := make([]string, 0, 4)
	if len(snapshot.Filters.IncludeSchemas) > 0 {
		parts = append(parts, "include_schema="+strings.Join(snapshot.Filters.IncludeSchemas, ","))
	}
	if len(snapshot.Filters.ExcludeSchemas) > 0 {
		parts = append(parts, "exclude_schema="+strings.Join(snapshot.Filters.ExcludeSchemas, ","))
	}
	if len(snapshot.Filters.IncludeTables) > 0 {
		parts = append(parts, "include_table="+strings.Join(snapshot.Filters.IncludeTables, ","))
	}
	if len(snapshot.Filters.ExcludeTables) > 0 {
		parts = append(parts, "exclude_table="+strings.Join(snapshot.Filters.ExcludeTables, ","))
	}
	slices.Sort(parts)
	return strings.Join(parts, " ")
}
