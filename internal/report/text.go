// Package report renders human-readable text reports from bounded analysis results.
// input: analyzer-produced AnalysisResult values plus optional SQL context presentation controls.
// output: stable six-section text reports with configurable transaction SQL display.
// pos: text renderer for the CLI output path after analyzer Finalize.
// note: if this file changes, update this header and module README.md.
package report

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// RenderText renders an AnalysisResult as human-readable text.
// Sections are always rendered in a fixed order, even if empty.
func RenderText(result model.AnalysisResult) (string, error) {
	return RenderTextWithOptions(result, DefaultOptions())
}

// RenderTextWithOptions renders an AnalysisResult with explicit presentation controls.
func RenderTextWithOptions(result model.AnalysisResult, opts Options) (string, error) {
	opts = normalizeOptions(opts)
	var buf strings.Builder

	// Section 1: Workload Summary
	renderWorkloadSummary(&buf, result.Summary, result.Warnings)

	// Section 2: Top Tables
	renderTopTables(&buf, result.Tables)

	// Section 3: Top Transactions
	renderTopTransactions(&buf, result.Transactions, opts.SQLContextMode)

	// Section 4: Top Patterns
	renderTopPatterns(&buf, result.Patterns, result.PatternDrilldowns)

	// Section 5: Minute Activity
	renderMinuteActivity(&buf, result.Minutes)

	// Section 6: Alerts
	renderAlerts(&buf, result.Alerts)

	return buf.String(), nil
}

func renderWorkloadSummary(buf *strings.Builder, summary model.WorkloadSummary, warnings int) {
	buf.WriteString("=== " + i18n.T("report.section.workload") + " ===\n")
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalTransactions"), summary.TotalTransactions))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalRows"), summary.TotalRows))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalEvents"), summary.TotalEvents))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.warnings"), warnings))
	buf.WriteString(fmt.Sprintf("  %s: %s - %s\n", i18n.T("report.label.timeRange"), formatTime(summary.StartTime), formatTime(summary.EndTime)))
	buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.label.duration"), formatDuration(summary.Duration)))
	buf.WriteString("\n")
}

func renderTopTables(buf *strings.Builder, tables []model.TableStats) {
	buf.WriteString("=== " + i18n.T("report.section.tables") + " ===\n")
	if len(tables) == 0 {
		buf.WriteString("  " + i18n.T("report.placeholder.noTableActivity") + "\n")
	} else {
		for _, t := range tables {
			buf.WriteString("  " + i18n.Tf("report.format.rowsSummary", map[string]any{
				"Schema":      t.Schema,
				"Table":       t.Table,
				"TotalRows":   t.TotalRows,
				"InsertRows":  t.InsertRows,
				"UpdateRows":  t.UpdateRows,
				"DeleteRows":  t.DeleteRows,
				"TxnCount":    t.TxnCount,
			}) + "\n")
		}
	}
	buf.WriteString("\n")
}

func renderTopTransactions(buf *strings.Builder, transactions []model.Transaction, mode SQLContextMode) {
	buf.WriteString("=== " + i18n.T("report.section.transactions") + " ===\n")
	if len(transactions) == 0 {
		buf.WriteString("  " + i18n.T("report.placeholder.noTransactions") + "\n")
	} else {
		// Sort by TotalRows descending, with TxnKey ascending as tie-breaker for determinism
		sorted := make([]model.Transaction, len(transactions))
		copy(sorted, transactions)
		sort.Slice(sorted, func(i, j int) bool {
			if sorted[i].TotalRows != sorted[j].TotalRows {
				return sorted[i].TotalRows > sorted[j].TotalRows
			}
			return sorted[i].TxnKey < sorted[j].TxnKey
		})

		for _, txn := range sorted {
			buf.WriteString("  " + i18n.Tf("report.format.transactionSummary", map[string]any{
				"TxnKey":     txn.TxnKey,
				"TotalRows":  txn.TotalRows,
				"Duration":   formatDuration(txn.Duration),
				"EventCount": txn.EventCount,
			}) + "\n")
			if queryLine := transactionTextQuery(txn, mode); queryLine != "" {
				buf.WriteString(fmt.Sprintf("    %s: %s\n", i18n.T("report.label.query"), queryLine))
			}
		}
	}
	buf.WriteString("\n")
}

func renderTopPatterns(buf *strings.Builder, patterns []model.PatternStats, drilldowns []model.PatternDrilldown) {
	buf.WriteString("=== " + i18n.T("report.section.patterns") + " ===\n")
	if len(patterns) == 0 {
		buf.WriteString("  " + i18n.T("report.placeholder.noPatterns") + "\n")
		buf.WriteString("\n")
		return
	}

	// Build drilldown lookup by pattern_key
	ddMap := make(map[string]model.PatternDrilldown, len(drilldowns))
	for _, d := range drilldowns {
		ddMap[d.PatternKey] = d
	}

	for _, p := range patterns {
		buf.WriteString(fmt.Sprintf("  %s: rows=%d txns=%d avg_rows_per_txn=%.1f\n", p.Label, p.TotalRows, p.TxnCount, p.AvgRowsPerTxn))
		if strings.TrimSpace(p.SampleQuerySummary) != "" {
			buf.WriteString(fmt.Sprintf("    %s: %s\n", i18n.T("report.label.query"), p.SampleQuerySummary))
		}

		// Render drilldown block if this pattern was selected
		if dd, ok := ddMap[p.PatternKey]; ok {
			renderDrilldownBlock(buf, dd)
		}
	}
	buf.WriteString("\n")
}

func renderDrilldownBlock(buf *strings.Builder, dd model.PatternDrilldown) {
	buf.WriteString("    drilldown:\n")
	buf.WriteString(fmt.Sprintf("      why: %s\n", dd.WhySelected))

	for i, m := range dd.BusiestMinutes {
		if i >= 2 {
			break
		}
		buf.WriteString(fmt.Sprintf("      peak minute: %s rows=%d txns=%d\n",
			m.Minute.Format("2006-01-02 15:04"), m.TotalRows, m.TxnCount))
	}

	for i, txn := range dd.RepresentativeTransactions {
		if i >= 2 {
			break
		}
		line := fmt.Sprintf("rep txn: %s rows=%d", txn.TxnKey, txn.TotalRows)
		if txn.Duration > 0 {
			line += fmt.Sprintf(" dur=%s", formatDuration(txn.Duration))
		}
		buf.WriteString("      " + line + "\n")
	}
}

func transactionTextQuery(txn model.Transaction, mode SQLContextMode) string {
	switch mode {
	case SQLContextOff:
		return ""
	case SQLContextFull:
		if txn.QueryContext != nil {
			return txn.QueryContext.SQL
		}
		return ""
	case SQLContextSummary:
		fallthrough
	default:
		return txn.QuerySummary
	}
}

func renderMinuteActivity(buf *strings.Builder, minutes []model.MinuteBucket) {
	buf.WriteString("=== " + i18n.T("report.section.minutes") + " ===\n")
	if len(minutes) == 0 {
		buf.WriteString("  " + i18n.T("report.placeholder.noMinuteActivity") + "\n")
	} else {
		for _, m := range minutes {
			buf.WriteString("  " + i18n.Tf("report.format.minuteActivity", map[string]any{
				"Minute":    m.Minute.Format("2006-01-02 15:04"),
				"TotalRows": m.TotalRows,
				"TxnCount":  m.TxnCount,
			}) + "\n")
		}
	}
	buf.WriteString("\n")
}

func renderAlerts(buf *strings.Builder, alerts []model.Alert) {
	buf.WriteString("=== " + i18n.T("report.section.alerts") + " ===\n")
	if len(alerts) == 0 {
		buf.WriteString("  " + i18n.T("report.placeholder.noAlerts") + "\n")
	} else {
		for _, a := range alerts {
			buf.WriteString("  " + i18n.Tf("report.format.alertLine", map[string]any{
				"Severity": strings.ToUpper(a.Severity),
				"Type":     a.Type,
				"Message":  a.Message,
			}) + "\n")
		}
	}
	buf.WriteString("\n")
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return i18n.T("time.notAvailable")
	}
	return t.Format("2006-01-02 15:04:05")
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return d.String()
}

// RenderTextTo writes the text report to the specified writer.
func RenderTextTo(result model.AnalysisResult, w io.Writer) error {
	return RenderTextToWithOptions(result, w, DefaultOptions())
}

// RenderTextToWithOptions writes the text report with explicit presentation controls.
func RenderTextToWithOptions(result model.AnalysisResult, w io.Writer, opts Options) error {
	text, err := RenderTextWithOptions(result, opts)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(w, text)
	return err
}

// RenderTextToStdout writes the text report to stdout.
func RenderTextToStdout(result model.AnalysisResult) error {
	return RenderTextTo(result, os.Stdout)
}

// RenderTextToStdoutWithOptions writes the text report with explicit presentation controls.
func RenderTextToStdoutWithOptions(result model.AnalysisResult, opts Options) error {
	return RenderTextToWithOptions(result, os.Stdout, opts)
}
