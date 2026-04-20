// Package report renders human-readable text reports from bounded analysis results.
// input: analyzer-produced AnalysisResult values plus optional SQL context presentation controls.
// output: concise diagnostic-first text reports with opt-in minute and write-pattern detail sections.
// pos: text renderer for the CLI output path after analyzer Finalize.
// note: if this file changes, update this header and module README.md.
package report

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// RenderText renders an AnalysisResult as human-readable text.
func RenderText(result model.AnalysisResult) (string, error) {
	return RenderTextWithOptions(result, DefaultOptions())
}

// RenderTextWithOptions renders an AnalysisResult with explicit presentation controls.
func RenderTextWithOptions(result model.AnalysisResult, opts Options) (string, error) {
	opts = normalizeOptions(opts)
	var buf strings.Builder

	renderDiagnosticSummary(&buf, result)
	renderTopFindings(&buf, result, opts)
	renderActivitySection(&buf, result)
	renderTopTablesTable(&buf, result.Tables, opts.TopN)
	renderTopTransactions(&buf, result, opts.TopN)
	renderNextActions(&buf, result)

	if opts.ShowMinutes {
		renderMinuteDetails(&buf, result.Minutes, opts.TopN)
	}
	if opts.ShowPatterns {
		renderWriteShapePatterns(&buf, result.Patterns, result.PatternDrilldowns, opts.TopN)
	}

	return buf.String(), nil
}

func renderDiagnosticSummary(buf *strings.Builder, result model.AnalysisResult) {
	summary := result.Summary
	buf.WriteString("=== " + i18n.T("report.text.summary") + " ===\n")
	buf.WriteString(fmt.Sprintf("  %s: %s - %s\n", i18n.T("report.label.timeRange"), formatTime(summary.StartTime), formatTime(summary.EndTime)))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalTransactions"), summary.TotalTransactions))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalRows"), summary.TotalRows))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalEvents"), summary.TotalEvents))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.html.analyze.ddlTimeline"), len(result.Diagnostics.DDLEvents)))
	buf.WriteString("\n")
}

func renderActivitySection(buf *strings.Builder, result model.AnalysisResult) {
	buf.WriteString("=== " + i18n.T("report.text.activity") + " ===\n")
	tpsSpark := formatSparkline(result.Timeseries.TPSSeries)
	rowsSpark := formatSparkline(result.Timeseries.RowsSeries)
	resolution := formatSparklineResolution(len(result.Timeseries.TPSSeries))
	buf.WriteString(fmt.Sprintf("  %-8s %s  %s  %s\n", i18n.T("report.text.tpsShort")+":", tpsSpark, formatPeakSeries(result.Timeseries.TPSSeries), resolution))
	buf.WriteString(fmt.Sprintf("  %-8s %s  %s\n", i18n.T("report.text.rowsPerMinuteShort")+":", rowsSpark, formatPeakSeries(result.Timeseries.RowsSeries)))
	buf.WriteString("\n")
}

func formatSparklineResolution(pointCount int) string {
	if pointCount <= 50 {
		return ""
	}
	minPerBin := (pointCount + 49) / 50
	return fmt.Sprintf("(%d min/bar)", minPerBin)
}

func renderTopFindings(buf *strings.Builder, result model.AnalysisResult, opts Options) {
	buf.WriteString("=== " + i18n.T("report.text.topFindings") + " ===\n")

	lines := make([]string, 0, opts.TopN)
	for _, finding := range result.Diagnostics.Findings {
		lines = append(lines, fmt.Sprintf("  [%s] %s", finding.Severity, finding.Message))
		if len(lines) >= opts.TopN {
			break
		}
	}

	if len(lines) < opts.TopN && len(result.Diagnostics.HotIntervals) > 0 {
		hot := result.Diagnostics.HotIntervals[0]
		lines = append(lines, fmt.Sprintf("  [critical] %s at %s: rows=%d, txns=%d",
			i18n.T("report.text.writeSpike"), hot.Minute.Format("2006-01-02 15:04"), hot.TotalRows, hot.TxnCount))
	}

	if len(lines) < opts.TopN && len(result.Diagnostics.LongestTransactions) > 0 {
		txn := result.Diagnostics.LongestTransactions[0]
		lines = append(lines, fmt.Sprintf("  [warning] %s: %s, rows=%d, tables=%d, file=%s",
			i18n.T("report.text.longestTransaction"),
			formatDuration(txn.Duration),
			txn.TotalRows,
			len(txn.Tables),
			formatSuspiciousLocation(txn),
		))
	}

	if len(lines) < opts.TopN && len(result.Diagnostics.DDLEvents) > 0 {
		ddl := result.Diagnostics.DDLEvents[0]
		target := strings.Trim(strings.TrimSpace(ddl.Schema+"."+ddl.Table), ".")
		if target == "" {
			target = ddl.Object
		}
		lines = append(lines, fmt.Sprintf("  [warning] %s: %s %s at %s",
			i18n.T("report.text.ddlDetected"),
			ddl.Operation,
			target,
			ddl.Timestamp.Format("2006-01-02 15:04"),
		))
	}

	if len(lines) == 0 {
		buf.WriteString("  " + i18n.T("report.text.noFindings") + "\n\n")
		return
	}
	for _, line := range lines {
		buf.WriteString(line + "\n")
	}
	buf.WriteString("\n")
}

func renderTopTablesTable(buf *strings.Builder, tables []model.TableStats, topN int) {
	buf.WriteString("=== " + i18n.T("report.text.topTables") + " ===\n")
	if len(tables) == 0 {
		buf.WriteString("  " + i18n.T("report.text.noTableActivity") + "\n\n")
		return
	}

	limit := minInt(topN, len(tables))
	totalRows := 0
	for _, table := range tables {
		totalRows += table.TotalRows
	}

	buf.WriteString("  #  Table                         Rows       Txns     Events    Share\n")
	for i := 0; i < limit; i++ {
		table := tables[i]
		name := table.Schema + "." + table.Table
		share := 0.0
		if totalRows > 0 {
			share = float64(table.TotalRows) * 100 / float64(totalRows)
		}
		buf.WriteString(fmt.Sprintf("  %-2d %-28s %10d %8d %8d %6.1f%%\n",
			i+1, name, table.TotalRows, table.TxnCount, table.EventCount, share))
	}
	buf.WriteString("\n")
}

func renderTopTransactions(buf *strings.Builder, result model.AnalysisResult, topN int) {
	buf.WriteString("=== " + i18n.T("report.text.topTransactions") + " ===\n")

	limit := minInt(1, topN)
	lines := make([]string, 0, limit*3)
	for _, txn := range limitTransactions(result.Diagnostics.LargestTransactions, limit) {
		lines = append(lines, fmt.Sprintf("  %s: %s rows=%d tables=%d file=%s",
			i18n.T("report.text.largestTransaction"), txn.TxnKey, txn.TotalRows, len(txn.Tables), formatSuspiciousLocation(txn)))
	}
	for _, txn := range limitTransactions(result.Diagnostics.LongestTransactions, limit) {
		lines = append(lines, fmt.Sprintf("  %s: %s dur=%s rows=%d file=%s",
			i18n.T("report.text.longestTransaction"), txn.TxnKey, formatDuration(txn.Duration), txn.TotalRows, formatSuspiciousLocation(txn)))
	}
	for _, txn := range limitTransactions(result.Diagnostics.WidestTransactions, limit) {
		lines = append(lines, fmt.Sprintf("  %s: %s tables=%d rows=%d file=%s",
			i18n.T("report.text.widestTransaction"), txn.TxnKey, len(txn.Tables), txn.TotalRows, formatSuspiciousLocation(txn)))
	}

	if len(lines) == 0 {
		buf.WriteString("  " + i18n.T("report.placeholder.noTransactions") + "\n\n")
		return
	}
	for _, line := range lines {
		buf.WriteString(line + "\n")
	}
	buf.WriteString("\n")
}

func renderNextActions(buf *strings.Builder, result model.AnalysisResult) {
	buf.WriteString("=== " + i18n.T("report.text.nextActions") + " ===\n")
	buf.WriteString("  " + i18n.T("report.text.openHTML") + "\n")
	if location := firstSuspiciousLocation(result); location != "" {
		buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.text.firstSuspiciousPosition"), location))
	}
	buf.WriteString("\n")
}

func renderMinuteDetails(buf *strings.Builder, minutes []model.MinuteBucket, topN int) {
	buf.WriteString("=== " + i18n.T("report.text.minuteDetails") + " ===\n")
	if len(minutes) == 0 {
		buf.WriteString("  " + i18n.T("report.placeholder.noMinuteActivity") + "\n\n")
		return
	}
	for i, minute := range minutes {
		if i >= topN {
			break
		}
		buf.WriteString("  " + i18n.Tf("report.format.minuteActivity", map[string]any{
			"Minute":    minute.Minute.Format("2006-01-02 15:04"),
			"TotalRows": minute.TotalRows,
			"TxnCount":  minute.TxnCount,
		}) + "\n")
	}
	buf.WriteString("\n")
}

func renderWriteShapePatterns(buf *strings.Builder, patterns []model.PatternStats, drilldowns []model.PatternDrilldown, topN int) {
	buf.WriteString("=== " + i18n.T("report.text.writeShapePatterns") + " ===\n")
	if len(patterns) == 0 {
		buf.WriteString("  " + i18n.T("report.placeholder.noPatterns") + "\n\n")
		return
	}

	ddMap := make(map[string]model.PatternDrilldown, len(drilldowns))
	for _, drilldown := range drilldowns {
		ddMap[drilldown.PatternKey] = drilldown
	}

	limit := minInt(topN, len(patterns))
	for i := 0; i < limit; i++ {
		pattern := patterns[i]
		buf.WriteString(fmt.Sprintf("  %s: rows=%d txns=%d avg_rows_per_txn=%.1f\n",
			pattern.Label, pattern.TotalRows, pattern.TxnCount, pattern.AvgRowsPerTxn))
		if strings.TrimSpace(pattern.SampleQuerySummary) != "" {
			buf.WriteString(fmt.Sprintf("    %s: %s\n", i18n.T("report.label.query"), pattern.SampleQuerySummary))
		}
		if drilldown, ok := ddMap[pattern.PatternKey]; ok {
			renderDrilldownBlock(buf, drilldown)
		}
	}
	buf.WriteString("\n")
}

func renderDrilldownBlock(buf *strings.Builder, dd model.PatternDrilldown) {
	buf.WriteString("    drilldown:\n")
	buf.WriteString(fmt.Sprintf("      why: %s\n", dd.WhySelected))

	for i, minute := range dd.BusiestMinutes {
		if i >= 2 {
			break
		}
		buf.WriteString(fmt.Sprintf("      workload minute: %s rows=%d txns=%d\n",
			minute.Minute.Format("2006-01-02 15:04"), minute.TotalRows, minute.TxnCount))
	}

	for i, txn := range dd.RepresentativeTransactions {
		if i >= 2 {
			break
		}
		line := fmt.Sprintf("workload txn: %s rows=%d", txn.TxnKey, txn.TotalRows)
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

func formatPeakSeries(points []model.TimeseriesPoint) string {
	if len(points) == 0 {
		return i18n.T("time.notAvailable")
	}
	peak := points[0]
	for _, point := range points[1:] {
		if point.Value > peak.Value || (point.Value == peak.Value && point.Minute.Before(peak.Minute)) {
			peak = point
		}
	}
	return fmt.Sprintf("%.1f at %s", peak.Value, peak.Minute.Format("2006-01-02 15:04"))
}

func formatSparkline(points []model.TimeseriesPoint) string {
	if len(points) == 0 {
		return i18n.T("time.notAvailable")
	}
	const maxBins = 50
	downsampled := downsampleSeries(points, maxBins)
	const blocks = "▁▂▃▄▅▆▇█"
	minVal := downsampled[0].Value
	maxVal := downsampled[0].Value
	for _, point := range downsampled[1:] {
		if point.Value < minVal {
			minVal = point.Value
		}
		if point.Value > maxVal {
			maxVal = point.Value
		}
	}
	if maxVal <= minVal {
		return strings.Repeat("▁", len(downsampled))
	}
	var b strings.Builder
	for _, point := range downsampled {
		ratio := (point.Value - minVal) / (maxVal - minVal)
		index := int(ratio * 7)
		if index < 0 {
			index = 0
		}
		if index > 7 {
			index = 7
		}
		b.WriteRune([]rune(blocks)[index])
	}
	return b.String()
}

// downsampleSeries reduces the number of data points to maxBins by averaging adjacent points.
func downsampleSeries(points []model.TimeseriesPoint, maxBins int) []model.TimeseriesPoint {
	if len(points) <= maxBins || maxBins <= 0 {
		return points
	}

	result := make([]model.TimeseriesPoint, maxBins)

	for i := 0; i < maxBins; i++ {
		start := i * len(points) / maxBins
		end := (i + 1) * len(points) / maxBins

		var sum float64
		for _, p := range points[start:end] {
			sum += p.Value
		}
		result[i] = model.TimeseriesPoint{
			Minute: points[start].Minute,
			Value:  sum / float64(end-start),
		}
	}
	return result
}

func firstSuspiciousLocation(result model.AnalysisResult) string {
	if len(result.Diagnostics.LongestTransactions) > 0 {
		return formatSuspiciousLocation(result.Diagnostics.LongestTransactions[0])
	}
	if len(result.Diagnostics.LargestTransactions) > 0 {
		return formatSuspiciousLocation(result.Diagnostics.LargestTransactions[0])
	}
	if len(result.Diagnostics.WidestTransactions) > 0 {
		return formatSuspiciousLocation(result.Diagnostics.WidestTransactions[0])
	}
	if len(result.Diagnostics.DDLEvents) > 0 {
		ddl := result.Diagnostics.DDLEvents[0]
		return formatBinlogLocation(ddl.BinlogPath, ddl.PositionStart, ddl.PositionEnd)
	}
	return ""
}

func formatSuspiciousLocation(txn model.Transaction) string {
	if txn.BinlogPathStart == "" && txn.PositionStart == 0 && txn.PositionEnd == 0 {
		return i18n.T("time.notAvailable")
	}
	return formatBinlogLocationWithEnd(txn.BinlogPathStart, txn.PositionStart, txn.BinlogPathEnd, txn.PositionEnd)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
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
