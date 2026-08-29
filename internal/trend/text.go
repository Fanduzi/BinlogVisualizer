// Package trend renders human-readable trend reports with raw series and gated narratives.
// input: deterministic Result values with structured comparability evidence and ordered numeric point data.
// output: text reports leading with any comparability guard before raw deltas, tables, patterns, and diagnostics.
// pos: terminal renderer for the trend pipeline after series-wide comparability assessment.
// note: if this file changes, update this header and internal/trend/README.md.
package trend

import (
	"fmt"
	"strings"

	comparepkg "binlogviz/internal/compare"
)

func RenderText(result Result) (string, error) {
	var b strings.Builder

	fmt.Fprintf(&b, "Trend Summary\n")
	guarded := hasTrendComparabilityGuard(result.Comparability)
	if guarded {
		fmt.Fprintf(&b, "%s [%s]\n", comparepkg.ComparabilityGuardTitle(), result.Comparability.Verdict)
		fmt.Fprintf(&b, "%s\n", comparepkg.ComparabilityGuardSummary(result.Comparability.Verdict))
		fmt.Fprintf(&b, "%s %s\n", comparepkg.ComparabilityReasonCodesLabel(), strings.Join(result.Comparability.ReasonCodes, ", "))
		for _, evidence := range result.Comparability.Evidence {
			fmt.Fprintf(&b, "- %s\n", comparepkg.FormatComparabilityEvidence(evidence))
		}
		fmt.Fprint(&b, "\n")
	}
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

	if len(result.TrendSummary) > 0 && !guarded {
		fmt.Fprintf(&b, "\nKey Findings\n")
		for i, finding := range result.TrendSummary {
			fmt.Fprintf(&b, "%d. [%s] %s\n", i+1, finding.Kind, finding.Summary)
			if len(finding.EvidenceRefs) > 0 {
				labels := make([]string, 0, len(finding.EvidenceRefs))
				for _, ref := range finding.EvidenceRefs {
					labels = append(labels, ref.Label)
				}
				fmt.Fprintf(&b, "   evidence: %s\n", strings.Join(labels, ", "))
			}
		}
	}

	if len(result.Recommendations) > 0 {
		fmt.Fprintf(&b, "\nRecommended Next Checks\n")
		for i, rec := range result.Recommendations {
			fmt.Fprintf(&b, "%d. [%s] %s\n", i+1, rec.Priority, rec.Title)
			fmt.Fprintf(&b, "   %s\n", rec.Summary)
			if len(rec.EvidenceRefs) > 0 {
				labels := make([]string, 0, len(rec.EvidenceRefs))
				for _, ref := range rec.EvidenceRefs {
					labels = append(labels, ref.Label)
				}
				fmt.Fprintf(&b, "   evidence: %s\n", strings.Join(labels, ", "))
			}
		}
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

	fmt.Fprintf(&b, "\nTop Pattern Trends\n")
	if len(result.PatternTrends) == 0 {
		fmt.Fprintln(&b, "- none")
	} else {
		drilldownMap := make(map[string]PatternDrilldown, len(result.PatternDrilldowns))
		for _, d := range result.PatternDrilldowns {
			drilldownMap[d.PatternKey] = d
		}
		for _, trend := range result.PatternTrends {
			fmt.Fprintf(&b, "- %s: share=%.1f%% -> %.1f%% (%+.1f%%) | rows=%d -> %d (%+d)\n",
				formatPatternTrendDisplay(trend),
				trend.FirstShareOfRows*100,
				trend.LastShareOfRows*100,
				trend.DeltaShareOfRows*100,
				trend.FirstRows,
				trend.LastRows,
				trend.DeltaRows,
			)
			if dd, ok := drilldownMap[trend.PatternKey]; ok {
				fmt.Fprintf(&b, "  drilldown:\n")
				fmt.Fprintf(&b, "    why: %s\n", dd.WhySelected)
				for _, kp := range dd.KeyPoints {
					fmt.Fprintf(&b, "    %s: %s\n", kp.Label, kp.Summary)
				}
			}
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

func hasTrendComparabilityGuard(comparability Comparability) bool {
	return comparability.Verdict == comparepkg.VerdictNotComparable || comparability.Verdict == comparepkg.VerdictUnknown
}

func formatPatternTrendDisplay(trend PatternTrend) string {
	key := strings.TrimSpace(trend.PatternKey)
	label := strings.TrimSpace(trend.Label)
	switch {
	case key != "" && label != "" && key != label:
		return fmt.Sprintf("%s (%s)", label, key)
	case label != "":
		return label
	case key != "":
		return key
	default:
		return "pattern"
	}
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
