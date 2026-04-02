// Package compare renders human-readable text reports for compare results.
// input: deterministic CompareResult values produced by the compare diff engine.
// output: fixed-section text compare reports for terminal and file output.
// pos: compare renderer used by the compare command text output path.
package compare

import (
	"fmt"
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
