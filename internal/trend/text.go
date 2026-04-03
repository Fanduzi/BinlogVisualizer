package trend

import (
	"fmt"
	"strings"
)

func RenderText(result Result) (string, error) {
	var b strings.Builder

	fmt.Fprintf(&b, "Trend Summary\n")
	fmt.Fprintf(&b, "Input Mode: %s\n", result.InputMode)
	if result.BaselineSnapshot != nil {
		fmt.Fprintf(&b, "Baseline Snapshot: %s\n", formatSnapshotDisplay(*result.BaselineSnapshot))
	}
	if len(result.Points) > 0 {
		first := result.Points[0]
		last := result.Points[len(result.Points)-1]
		fmt.Fprintf(&b, "Rows: %d -> %d (%+d)\n", first.Summary.TotalRows, last.Summary.TotalRows, result.Insights.RowsDelta)
		fmt.Fprintf(&b, "Transactions: %d -> %d (%+d)\n", first.Summary.TotalTransactions, last.Summary.TotalTransactions, result.Insights.TxnsDelta)
		fmt.Fprintf(&b, "Events: %d -> %d (%+d)\n", first.Summary.TotalEvents, last.Summary.TotalEvents, result.Insights.EventsDelta)
		fmt.Fprintf(&b, "Alerts: %d -> %d (%+d)\n", first.AlertCount, last.AlertCount, result.Insights.AlertCountDelta)
	}

	fmt.Fprintf(&b, "\nOrdered Points\n")
	for _, point := range result.Points {
		fmt.Fprintf(&b, "- %s | %s | rows=%d txns=%d events=%d alerts=%d",
			point.Window.StartTime,
			formatSnapshotDisplay(point.Snapshot),
			point.Summary.TotalRows,
			point.Summary.TotalTransactions,
			point.Summary.TotalEvents,
			point.AlertCount,
		)
		if point.BaselineDelta != nil {
			fmt.Fprintf(&b, " | vs baseline rows=%+d txns=%+d events=%+d alerts=%+d",
				point.BaselineDelta.RowsDelta,
				point.BaselineDelta.TransactionsDelta,
				point.BaselineDelta.EventsDelta,
				point.BaselineDelta.AlertDelta,
			)
		}
		fmt.Fprintln(&b)
	}

	fmt.Fprintf(&b, "\nOperation Mix\n")
	for _, point := range result.Points {
		fmt.Fprintf(&b, "- %s: insert=%d (%.1f%%) update=%d (%.1f%%) delete=%d (%.1f%%)\n",
			point.Snapshot.Name,
			point.Operations.Inserts, point.Operations.InsertShare,
			point.Operations.Updates, point.Operations.UpdateShare,
			point.Operations.Deletes, point.Operations.DeleteShare,
		)
	}

	fmt.Fprintf(&b, "\nTop Table Movement\n")
	if len(result.TableTrends) == 0 {
		fmt.Fprintln(&b, "- none")
	} else {
		limit := len(result.TableTrends)
		if limit > 8 {
			limit = 8
		}
		for _, trend := range result.TableTrends[:limit] {
			fmt.Fprintf(&b, "- %s.%s: %d -> %d (%+d)\n", trend.Schema, trend.Table, trend.FirstRows, trend.LastRows, trend.DeltaRows)
		}
	}

	fmt.Fprintf(&b, "\nAggregate Insights\n")
	fmt.Fprintf(&b, "- First snapshot: %s\n", result.Insights.FirstSnapshot)
	fmt.Fprintf(&b, "- Last snapshot: %s\n", result.Insights.LastSnapshot)
	fmt.Fprintf(&b, "- Rows delta: %+d\n", result.Insights.RowsDelta)
	fmt.Fprintf(&b, "- Transactions delta: %+d\n", result.Insights.TxnsDelta)
	fmt.Fprintf(&b, "- Events delta: %+d\n", result.Insights.EventsDelta)
	fmt.Fprintf(&b, "- Alert delta: %+d\n", result.Insights.AlertCountDelta)

	return b.String(), nil
}

func formatSnapshotDisplay(snapshot SnapshotMeta) string {
	label := strings.TrimSpace(snapshot.Label)
	name := strings.TrimSpace(snapshot.Name)
	switch {
	case label != "" && name != "" && label != name:
		return fmt.Sprintf("%s (%s)", label, name)
	case name != "":
		return name
	case label != "":
		return label
	default:
		return ""
	}
}
