// Package trend renders human-readable trend reports with raw series and gated narratives.
// input: deterministic Result values with structured comparability evidence and ordered numeric point data.
// output: text reports leading with any comparability guard before raw deltas, tables, patterns, and diagnostics, rendered through report.text.trend.* i18n keys.
// pos: terminal renderer for the trend pipeline after series-wide comparability assessment.
// note: if this file changes, update this header and internal/trend/README.md.
package trend

import (
	"fmt"
	"strings"

	comparepkg "binlogviz/internal/compare"
	"binlogviz/internal/i18n"
)

func signedTrendDelta(n int) string {
	return fmt.Sprintf("%+d", n)
}

func RenderText(result Result) (string, error) {
	var b strings.Builder

	fmt.Fprintf(&b, "%s", i18n.T("report.text.trend.summary"))
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
	fmt.Fprint(&b, i18n.Tf("report.text.trend.inputMode", map[string]any{"Value": result.InputMode}))
	if result.BaselineSnapshot != nil {
		fmt.Fprint(&b, i18n.Tf("report.text.trend.baselineSnapshot", map[string]any{"Value": formatSnapshotDisplay(*result.BaselineSnapshot)}))
	}
	if len(result.Points) > 0 {
		first := result.Points[0]
		last := result.Points[len(result.Points)-1]
		fmt.Fprint(&b, i18n.Tf("report.text.trend.rows", map[string]any{"First": first.Summary.TotalRows, "Last": last.Summary.TotalRows, "Delta": signedTrendDelta(result.Insights.RowsDelta)}))
		fmt.Fprint(&b, i18n.Tf("report.text.trend.transactions", map[string]any{"First": first.Summary.TotalTransactions, "Last": last.Summary.TotalTransactions, "Delta": signedTrendDelta(result.Insights.TxnsDelta)}))
		fmt.Fprint(&b, i18n.Tf("report.text.trend.events", map[string]any{"First": first.Summary.TotalEvents, "Last": last.Summary.TotalEvents, "Delta": signedTrendDelta(result.Insights.EventsDelta)}))
		fmt.Fprint(&b, i18n.Tf("report.text.trend.alerts", map[string]any{"First": first.AlertCount, "Last": last.AlertCount, "Delta": signedTrendDelta(result.Insights.AlertCountDelta)}))
	}

	if len(result.TrendSummary) > 0 && !guarded {
		fmt.Fprintf(&b, "%s", i18n.T("report.text.trend.keyFindings"))
		for i, finding := range result.TrendSummary {
			fmt.Fprintf(&b, "%d. [%s] %s\n", i+1, finding.Kind, finding.Summary)
			if len(finding.EvidenceRefs) > 0 {
				labels := make([]string, 0, len(finding.EvidenceRefs))
				for _, ref := range finding.EvidenceRefs {
					labels = append(labels, ref.Label)
				}
				fmt.Fprint(&b, i18n.Tf("report.text.trend.evidence", map[string]any{"Labels": strings.Join(labels, ", ")}))
			}
		}
	}

	if len(result.Recommendations) > 0 {
		fmt.Fprintf(&b, "%s", i18n.T("report.text.trend.recommendedNextChecks"))
		for i, rec := range result.Recommendations {
			fmt.Fprintf(&b, "%d. [%s] %s\n", i+1, rec.Priority, rec.Title)
			fmt.Fprintf(&b, "   %s\n", rec.Summary)
			if len(rec.EvidenceRefs) > 0 {
				labels := make([]string, 0, len(rec.EvidenceRefs))
				for _, ref := range rec.EvidenceRefs {
					labels = append(labels, ref.Label)
				}
				fmt.Fprint(&b, i18n.Tf("report.text.trend.evidence", map[string]any{"Labels": strings.Join(labels, ", ")}))
			}
		}
	}

	fmt.Fprintf(&b, "%s", i18n.T("report.text.trend.orderedPoints"))
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
			fmt.Fprint(&b, i18n.Tf("report.text.trend.pointBaselineDelta", map[string]any{
				"Rows":   signedTrendDelta(point.BaselineDelta.RowsDelta),
				"Txns":   signedTrendDelta(point.BaselineDelta.TransactionsDelta),
				"Events": signedTrendDelta(point.BaselineDelta.EventsDelta),
				"Alerts": signedTrendDelta(point.BaselineDelta.AlertDelta),
			}))
		}
		fmt.Fprintln(&b)
	}

	fmt.Fprintf(&b, "%s", i18n.T("report.text.trend.operationMix"))
	for _, point := range result.Points {
		fmt.Fprintf(&b, "- %s: insert=%d (%.1f%%) update=%d (%.1f%%) delete=%d (%.1f%%)\n",
			point.Snapshot.Name,
			point.Operations.Inserts, point.Operations.InsertShare,
			point.Operations.Updates, point.Operations.UpdateShare,
			point.Operations.Deletes, point.Operations.DeleteShare,
		)
	}

	fmt.Fprintf(&b, "%s", i18n.T("report.text.trend.topTableMovement"))
	if len(result.TableTrends) == 0 {
		fmt.Fprintf(&b, "%s", i18n.T("report.text.trend.noneList"))
	} else {
		limit := len(result.TableTrends)
		if limit > 8 {
			limit = 8
		}
		for _, trend := range result.TableTrends[:limit] {
			fmt.Fprintf(&b, "- %s.%s: %d -> %d (%+d)\n", trend.Schema, trend.Table, trend.FirstRows, trend.LastRows, trend.DeltaRows)
		}
	}

	fmt.Fprintf(&b, "%s", i18n.T("report.text.trend.topPatternTrends"))
	if len(result.PatternTrends) == 0 {
		fmt.Fprintf(&b, "%s", i18n.T("report.text.trend.noneList"))
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
				fmt.Fprintf(&b, "%s", i18n.T("report.text.trend.drilldown"))
				fmt.Fprint(&b, i18n.Tf("report.text.trend.whyLine", map[string]any{"Why": dd.WhySelected}))
				for _, kp := range dd.KeyPoints {
					fmt.Fprintf(&b, "    %s: %s\n", kp.Label, kp.Summary)
				}
			}
		}
	}

	fmt.Fprintf(&b, "%s", i18n.T("report.text.trend.aggregateInsights"))
	fmt.Fprint(&b, i18n.Tf("report.text.trend.firstSnapshot", map[string]any{"Value": result.Insights.FirstSnapshot}))
	fmt.Fprint(&b, i18n.Tf("report.text.trend.lastSnapshot", map[string]any{"Value": result.Insights.LastSnapshot}))
	fmt.Fprint(&b, i18n.Tf("report.text.trend.rowsDelta", map[string]any{"Delta": signedTrendDelta(result.Insights.RowsDelta)}))
	fmt.Fprint(&b, i18n.Tf("report.text.trend.txnsDelta", map[string]any{"Delta": signedTrendDelta(result.Insights.TxnsDelta)}))
	fmt.Fprint(&b, i18n.Tf("report.text.trend.eventsDelta", map[string]any{"Delta": signedTrendDelta(result.Insights.EventsDelta)}))
	fmt.Fprint(&b, i18n.Tf("report.text.trend.alertDelta", map[string]any{"Delta": signedTrendDelta(result.Insights.AlertCountDelta)}))

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
		return i18n.T("report.text.trend.patternFallback")
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
