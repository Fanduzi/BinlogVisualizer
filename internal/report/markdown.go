// Package report renders Markdown reports from complete analysis results.
// input: analyzer-produced AnalysisResult values plus optional SQL context presentation controls.
// output: GitHub-flavored Markdown with completeness-aware tables, trusted replay evidence, DDL timeline, and findings.
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

	mdWorkloadSummary(&buf, result.Summary, result.Diagnostics)
	mdTopTables(&buf, result.Tables, opts.TopTables)
	mdTopTransactions(&buf, result.Transactions, opts.SQLContextMode, result.Diagnostics.ServerVersion)
	mdMinuteActivity(&buf, result.Minutes)
	mdDDLTimeline(&buf, result.Diagnostics.DDLEvents)
	mdFindings(&buf, result.Diagnostics.Findings, result.Alerts)

	return buf.String(), nil
}

func mdWorkloadSummary(buf *strings.Builder, summary model.WorkloadSummary, diagnostics model.Diagnostics) {
	buf.WriteString("## " + i18n.T("report.section.workload") + "\n\n")
	buf.WriteString("| Field | Value |\n")
	buf.WriteString("|---|---|\n")
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.totalTransactions"), formatInt(summary.TotalTransactions)))
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.partialTransactions"), formatInt(summary.PartialTransactions)))
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.unknownTransactions"), formatInt(summary.UnknownTransactions)))
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.totalRows"), formatInt(summary.TotalRows)))
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.totalEvents"), formatInt(summary.TotalEvents)))
	buf.WriteString(fmt.Sprintf("| %s | %s — %s |\n", i18n.T("report.label.timeRange"), formatTime(summary.StartTime), formatTime(summary.EndTime)))
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.duration"), formatDuration(summary.Duration)))
	format := diagnostics.InputFormatGuess
	if format == "" {
		format = i18n.T("time.notAvailable")
	}
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.format"), escapeMD(format)))
	buf.WriteString(fmt.Sprintf("| %s | %s |\n", i18n.T("report.label.ignoredQueryDML"), formatInt(diagnostics.IgnoredQueryDMLEvents)))
	buf.WriteString("\n")
}

func mdTopTables(buf *strings.Builder, tables []model.TableStats, topN int) {
	buf.WriteString("## " + i18n.T("report.section.tables") + "\n\n")
	if len(tables) == 0 {
		buf.WriteString("_" + i18n.T("report.placeholder.noTableActivity") + "_\n\n")
		return
	}
	displayedTables, omittedTables := limitTablesForDisplay(tables, topN)
	buf.WriteString("| Schema | Table | Total Rows | Inserts | Updates | Deletes | Transactions |\n")
	buf.WriteString("|---|---|---:|---:|---:|---:|---:|\n")
	for _, t := range displayedTables {
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
	if omittedTables > 0 {
		buf.WriteString("_" + omittedTablesLabel(omittedTables) + "_\n")
	}
	buf.WriteString("\n")
}

func mdDDLTimeline(buf *strings.Builder, events []model.DDLEvent) {
	if len(events) == 0 {
		return
	}
	buf.WriteString("## " + i18n.T("report.html.analyze.ddlTimeline") + "\n\n")
	buf.WriteString("| Time | Operation | Object | Statement | File:Position |\n")
	buf.WriteString("|---|---|---|---|---|\n")
	for _, event := range events {
		object := strings.Trim(strings.TrimSpace(event.Schema+"."+event.Table), ".")
		if object == "" {
			object = event.Object
		}
		statement := model.MakeQuerySummary(event.Statement)
		location := formatBinlogLocation(event.BinlogPath, event.PositionStart, event.PositionEnd)
		if location == "" {
			location = i18n.T("time.notAvailable")
		}
		if statement == "" {
			statement = i18n.T("time.notAvailable")
		}
		buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s |\n",
			formatTime(event.Timestamp),
			mdCell(event.Operation),
			mdCell(object),
			mdCell(statement),
			mdCell(location),
		))
	}
	buf.WriteString("\n")
}

func mdTopTransactions(buf *strings.Builder, transactions []model.Transaction, mode SQLContextMode, serverVersion string) {
	buf.WriteString("## " + i18n.T("report.section.transactions") + "\n\n")
	if len(transactions) == 0 {
		buf.WriteString("_" + i18n.T("report.placeholder.noTransactions") + "_\n\n")
		return
	}
	buf.WriteString("| # | Txn Key | Completeness | Full Replay | Rows | Bytes | Duration | File:Position | Tables | Operations |\n")
	buf.WriteString("|---|---|---|---|---:|---:|---|---|---|---|\n")
	for i, t := range transactions {
		tables := joinMapKeys(t.Tables)
		ops := joinMapKeys(t.Operations)
		span := FormatBinlogSpan(t)
		if span == "" {
			span = i18n.T("time.notAvailable")
		}
		replay := "no"
		if txnReplayAvailable(t) {
			replay = "yes"
		}
		buf.WriteString(fmt.Sprintf("| %d | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			i+1,
			mdCell(t.TxnKey),
			t.EffectiveCompleteness(),
			replay,
			formatInt(t.TotalRows),
			formatInt64(t.BinlogBytes),
			formatDuration(t.Duration),
			mdCell(span),
			escapeMD(tables),
			escapeMD(ops),
		))
	}
	buf.WriteString("\n")
	for _, t := range transactions {
		if cmd := FormatReplayCommand(t, serverVersion); cmd != "" {
			mdReplayCommand(buf, t.TxnKey, cmd)
		}
		if mode != SQLContextOff && t.QuerySummary != "" {
			buf.WriteString(fmt.Sprintf("> `%s`\n\n", escapeMD(t.QuerySummary)))
		}
	}
	buf.WriteString("\n")
}

func mdReplayCommand(buf *strings.Builder, txnKey, cmd string) {
	buf.WriteString("`mysqlbinlog_cmd`")
	if txnKey != "" {
		buf.WriteString(" for `" + escapeMD(txnKey) + "`")
	}
	buf.WriteString(":\n\n```text\n")
	buf.WriteString(cmd)
	if !strings.HasSuffix(cmd, "\n") {
		buf.WriteByte('\n')
	}
	buf.WriteString("```\n\n")
}

func mdMinuteActivity(buf *strings.Builder, minutes []model.MinuteBucket) {
	buf.WriteString("## " + i18n.T("report.section.minutes") + "\n\n")
	if len(minutes) == 0 {
		buf.WriteString("_" + i18n.T("report.placeholder.noMinuteActivity") + "_\n\n")
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

func mdFindings(buf *strings.Builder, findings []model.Finding, alerts []model.Alert) {
	if len(findings) == 0 && len(alerts) == 0 {
		return
	}
	allFindings := append([]model.Finding(nil), findings...)
	for _, alert := range alerts {
		duplicate := false
		for _, finding := range allFindings {
			if finding.Kind == alert.Type &&
				finding.Severity == alert.Severity &&
				finding.Message == alert.Message &&
				finding.TxnKey == alert.TxnKey &&
				finding.Minute.Equal(alert.Minute) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			allFindings = append(allFindings, model.Finding{
				Kind:     alert.Type,
				Severity: alert.Severity,
				Message:  alert.Message,
				TxnKey:   alert.TxnKey,
				Minute:   alert.Minute,
			})
		}
	}
	findings = allFindings

	buf.WriteString("## " + i18n.T("report.section.findings") + "\n\n")
	buf.WriteString("| Severity | Kind | Message | Txn Key | Time | Evidence |\n")
	buf.WriteString("|---|---|---|---|---|---|\n")
	for _, finding := range findings {
		evidence := strings.Join(finding.EvidenceRefs, ", ")
		if evidence == "" {
			evidence = i18n.T("time.notAvailable")
		}
		txnKey := finding.TxnKey
		if txnKey == "" {
			txnKey = i18n.T("time.notAvailable")
		}
		buf.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s |\n",
			mdCell(finding.Severity),
			mdCell(finding.Kind),
			mdCell(finding.Message),
			mdCell(txnKey),
			formatTime(finding.Minute),
			mdCell(evidence),
		))
	}
	buf.WriteString("\n")
}

// escapeMD escapes characters that would break Markdown table rows.
func escapeMD(s string) string {
	s = strings.ReplaceAll(s, "\r\n", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, "|", `\|`)
}

func mdCell(s string) string {
	if s == "" {
		return i18n.T("time.notAvailable")
	}
	return escapeMD(s)
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
	return formatInt64(int64(n))
}

func formatInt64(n int64) string {
	if n == 0 {
		return "0"
	}
	s := fmt.Sprintf("%d", n)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
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
