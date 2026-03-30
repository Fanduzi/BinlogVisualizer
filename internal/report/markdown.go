// Package report renders Markdown reports from bounded analysis results.
// input: analyzer-produced AnalysisResult values plus optional SQL context presentation controls.
// output: GitHub-flavored Markdown with tables, sections, and alert callouts.
// pos: Markdown renderer for the CLI output path after analyzer Finalize.
// note: if this file changes, update this header and module README.md.
package report

import (
	"fmt"
	"io"
	"os"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// RenderMarkdown renders an AnalysisResult as GitHub-flavored Markdown.
func RenderMarkdown(result model.AnalysisResult) (string, error) {
	return RenderMarkdownWithOptions(result, DefaultOptions())
}

// RenderMarkdownWithOptions renders an AnalysisResult as Markdown with explicit presentation controls.
func RenderMarkdownWithOptions(result model.AnalysisResult, opts Options) (string, error) {
	opts = normalizeOptions(opts)
	var buf strings.Builder

	buf.WriteString("# BinlogViz Report\n\n")

	mdWorkloadSummary(&buf, result.Summary)
	mdTopTables(&buf, result.Tables)
	mdTopTransactions(&buf, result.Transactions, opts.SQLContextMode)
	mdMinuteActivity(&buf, result.Minutes)
	mdAlerts(&buf, result.Alerts)

	return buf.String(), nil
}

func mdWorkloadSummary(buf *strings.Builder, summary model.WorkloadSummary) {
	buf.WriteString("## " + i18n.T("report.section.workload") + "\n\n")
	buf.WriteString("| Field | Value |\n")
	buf.WriteString("|---|---|\n")
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.totalTransactions"), formatInt(summary.TotalTransactions)))
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.totalRows"), formatInt(summary.TotalRows)))
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.totalEvents"), formatInt(summary.TotalEvents)))
	buf.WriteString(fmt.Sprintf("| %s | %s — %s |\n", i18n.T("report.label.timeRange"), formatTime(summary.StartTime), formatTime(summary.EndTime)))
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.duration"), formatDuration(summary.Duration)))
	buf.WriteString("\n")
}

func mdTopTables(buf *strings.Builder, tables []model.TableStats) {
	buf.WriteString("## " + i18n.T("report.section.tables") + "\n\n")
	if len(tables) == 0 {
		buf.WriteString("_" + i18n.T("report.placeholder.noTables") + "_\n\n")
		return
	}
	buf.WriteString("| Schema | Table | Total Rows | Inserts | Updates | Deletes | Transactions |\n")
	buf.WriteString("|---|---|---:|---:|---:|---:|---:|\n")
	for _, t := range tables {
		buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s |\n",
			escapeMD(t.Schema),
			escapeMD(t.Table),
			formatInt(t.TotalRows),
			formatInt(t.InsertRows),
			formatInt(t.UpdateRows),
			formatInt(t.DeleteRows),
			formatInt(t.TxnCount),
		))
	}
	buf.WriteString("\n")
}

func mdTopTransactions(buf *strings.Builder, transactions []model.Transaction, mode SQLContextMode) {
	buf.WriteString("## " + i18n.T("report.section.transactions") + "\n\n")
	if len(transactions) == 0 {
		buf.WriteString("_" + i18n.T("report.placeholder.noTransactions") + "_\n\n")
		return
	}
	buf.WriteString("| # | Rows | Duration | Tables | Operations |\n")
	buf.WriteString("|---|---:|---|---|---|\n")
	for i, t := range transactions {
		tables := joinMapKeys(t.Tables)
		ops := joinMapKeys(t.Operations)
		buf.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %s |\n",
			i+1,
			formatInt(t.TotalRows),
			formatDuration(t.Duration),
			escapeMD(tables),
			escapeMD(ops),
		))
		if mode != SQLContextOff && t.QuerySummary != "" {
			buf.WriteString(fmt.Sprintf("> `%s`\n", escapeMD(t.QuerySummary)))
		}
	}
	buf.WriteString("\n")
}

func mdMinuteActivity(buf *strings.Builder, minutes []model.MinuteBucket) {
	buf.WriteString("## " + i18n.T("report.section.minutes") + "\n\n")
	if len(minutes) == 0 {
		buf.WriteString("_" + i18n.T("report.placeholder.noActivity") + "_\n\n")
		return
	}
	buf.WriteString("| Time | Rows | Transactions |\n")
	buf.WriteString("|---|---:|---:|\n")
	for _, m := range minutes {
		buf.WriteString(fmt.Sprintf("| %s | %s | %s |\n",
			formatTime(m.Minute),
			formatInt(m.TotalRows),
			formatInt(m.TxnCount),
		))
	}
	buf.WriteString("\n")
}

func mdAlerts(buf *strings.Builder, alerts []model.Alert) {
	if len(alerts) == 0 {
		return
	}
	buf.WriteString("## " + i18n.T("report.section.alerts") + "\n\n")
	for _, a := range alerts {
		prefix := ">"
		switch a.Severity {
		case "critical":
			prefix = "> **[CRITICAL]**"
		case "warning":
			prefix = "> **[WARNING]**"
		default:
			prefix = "> **[INFO]**"
		}
		buf.WriteString(fmt.Sprintf("%s %s\n\n", prefix, escapeMD(a.Message)))
	}
}

// escapeMD escapes pipe characters that would break Markdown tables.
func escapeMD(s string) string {
	return strings.ReplaceAll(s, "|", `\|`)
}

// joinMapKeys returns a comma-separated list of map keys, sorted for stability.
func joinMapKeys(m map[string]int) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// simple insertion sort — maps are small
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return strings.Join(keys, ", ")
}

// formatInt formats an integer with thousands separators.
func formatInt(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	s := fmt.Sprintf("%d", n)
	result := make([]byte, 0, len(s)+(len(s)-1)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	if neg {
		return "-" + string(result)
	}
	return string(result)
}

// RenderMarkdownTo writes the Markdown report to the specified writer.
func RenderMarkdownTo(result model.AnalysisResult, w io.Writer) error {
	return RenderMarkdownToWithOptions(result, w, DefaultOptions())
}

// RenderMarkdownToWithOptions writes the Markdown report with explicit presentation controls.
func RenderMarkdownToWithOptions(result model.AnalysisResult, w io.Writer, opts Options) error {
	text, err := RenderMarkdownWithOptions(result, opts)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(w, text)
	return err
}

// RenderMarkdownToStdout writes the Markdown report to stdout.
func RenderMarkdownToStdout(result model.AnalysisResult) error {
	return RenderMarkdownTo(result, os.Stdout)
}

// RenderMarkdownToStdoutWithOptions writes the Markdown report with explicit presentation controls.
func RenderMarkdownToStdoutWithOptions(result model.AnalysisResult, opts Options) error {
	return RenderMarkdownToWithOptions(result, os.Stdout, opts)
}
