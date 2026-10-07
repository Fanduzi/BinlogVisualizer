// Package report renders human-readable text reports from complete analysis results.
// input: analyzer-produced AnalysisResult values plus optional SQL context presentation controls.
// output: completeness-aware UTC-labelled incident briefs with a DDL occurrence timeline, open uncommitted DML, committed duration buckets, separate file/count-event bytes, a Top Threads session ranking, ranked complete transactions carrying server_id, thread_id, GTID, xid or XA xid, and user@host only when present, query text only when --sql-context allows it, labelled trusted replay, busiest minutes with the tables that produced those rows, and opt-in minute/pattern detail.
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

type tableRow struct {
	name   string
	total  string
	insert string
	update string
	delete string
	ddl    string
	txns   string
	events string
	share  string
}

// RenderText renders an AnalysisResult as human-readable text.
func RenderText(result model.AnalysisResult) (string, error) {
	return RenderTextWithOptions(result, DefaultOptions())
}

// RenderTextWithOptions renders an AnalysisResult with explicit presentation controls.
func RenderTextWithOptions(result model.AnalysisResult, opts Options) (string, error) {
	opts = normalizeOptions(opts)
	var buf strings.Builder

	renderDiagnosticSummary(&buf, result, opts)
	renderDDLTimeline(&buf, result.Diagnostics.DDLEvents, opts.TopN, opts.SQLContextMode)
	renderOpenDML(&buf, result.Diagnostics.OpenDMLGroups)
	renderTopTablesTable(&buf, result.Tables, opts.TopTables)
	renderNoPrimaryKey(&buf, result.Tables)
	renderTopThreads(&buf, result.Threads, result.ThreadsRankedBy, opts.TopThreads)
	renderTopTransactions(&buf, result, opts)
	renderTopFindings(&buf, result, opts)
	renderActivitySection(&buf, result)
	renderBusiestMinutes(&buf, result.Diagnostics.HotIntervals, opts.TopN)
	renderNextActions(&buf, result)

	if opts.ShowMinutes {
		renderMinuteDetails(&buf, result.Minutes, opts.TopN)
	}
	if opts.ShowPatterns {
		renderWriteShapePatterns(&buf, result.Patterns, result.PatternDrilldowns, opts.TopN, opts.SQLContextMode)
	}

	return buf.String(), nil
}

func renderDiagnosticSummary(buf *strings.Builder, result model.AnalysisResult, opts Options) {
	topN := opts.TopN
	summary := result.Summary
	buf.WriteString("=== " + i18n.T("report.text.summary") + " ===\n")
	buf.WriteString(fmt.Sprintf("  %s: %s - %s\n", i18n.T("report.label.timeRange"), formatTime(summary.StartTime), formatTime(summary.EndTime)))
	buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.label.timestamps"), i18n.T("report.value.binlogUTC")))
	buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.label.format"), i18n.T("report.text.rowImageSummary")))
	if label := dmlFilterLabel(result.Scope); label != "" {
		buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.label.dmlFilter"), label))
	}
	if line := primaryKeyView(result.Tables).summaryLine; line != "" {
		buf.WriteString("  " + line + "\n")
	}
	if rowValuesSuppressed(opts) {
		buf.WriteString("  " + i18n.T("report.text.rowValuesSuppressed") + "\n")
	} else if showRowValues(opts) {
		if note := columnNamesNote(result); note != "" {
			buf.WriteString("  " + note + "\n")
		}
	}
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalTransactions"), summary.TotalTransactions))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.partialTransactions"), summary.PartialTransactions))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.unknownTransactions"), summary.UnknownTransactions))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalRows"), summary.TotalRows))
	buf.WriteString(fmt.Sprintf("  %s: %d\n", i18n.T("report.label.totalEvents"), summary.TotalEvents))
	buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.html.analyze.ddlTimeline"), formatDDLTimelineSummary(result.Diagnostics.DDLEvents)))
	inputFileSize := i18n.T("time.notAvailable")
	if bytes, ok := selectedInputFileBytes(result.Diagnostics.FileCoverage); ok {
		inputFileSize = formatByteSize(bytes)
	}
	buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.label.inputFileSize"), inputFileSize))
	buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.label.countedEventBytes"), formatByteSize(countedEventBytes(result))))
	if bytes := largestTxnBytes(result); bytes > 0 {
		buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.label.largestTxnBytes"), formatByteSize(bytes)))
	}
	renderByteContributors(buf, result, topN)
	renderSelectedFileLines(buf, result.Diagnostics.FileCoverage)
	buf.WriteString("\n")
}

func renderActivitySection(buf *strings.Builder, result model.AnalysisResult) {
	buf.WriteString("=== " + i18n.T("report.text.activity") + " ===\n")
	tpsSpark := formatSparkline(result.Timeseries.TPSSeries)
	rowsSpark := formatSparkline(result.Timeseries.RowsSeries)
	resolution := formatSparklineResolution(len(result.Timeseries.TPSSeries))
	buf.WriteString(fmt.Sprintf("  %-8s %s  %s  %s\n", i18n.T("report.text.tpsShort")+":", tpsSpark, formatTPSPeak(result.Summary, result.Timeseries.TPSSeries), resolution))
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

	lines := collectTopFindingLines(result, opts.TopN)
	if len(lines) == 0 {
		buf.WriteString("  " + i18n.T("report.text.noFindings") + "\n\n")
		return
	}
	for _, line := range lines {
		buf.WriteString(line + "\n")
	}
	buf.WriteString("\n")
}

// collectTopFindingLines uses the same alerts/findings contract as JSON.
// Hot intervals, longest transactions, and DDL timelines are evidence, not extra findings.
func collectTopFindingLines(result model.AnalysisResult, topN int) []string {
	if topN <= 0 {
		return nil
	}
	lines := make([]string, 0, topN)
	for _, finding := range result.Diagnostics.Findings {
		lines = append(lines, fmt.Sprintf("  [%s] %s", finding.Severity, finding.Message))
		if len(lines) >= topN {
			return lines
		}
	}
	if len(lines) > 0 {
		return lines
	}
	for _, alert := range result.Alerts {
		lines = append(lines, fmt.Sprintf("  [%s] %s", alert.Severity, alert.Message))
		if len(lines) >= topN {
			break
		}
	}
	return lines
}

func renderNoPrimaryKey(buf *strings.Builder, tables []model.TableStats) {
	view := primaryKeyView(tables)
	if len(view.risks) == 0 {
		return
	}
	buf.WriteString("=== " + i18n.T("report.text.noPrimaryKey") + " ===\n")
	buf.WriteString("  " + i18n.T("report.text.noPrimaryKeyLead") + "\n")
	nameWidth := len("Table")
	for _, row := range view.risks {
		if len(row.name()) > nameWidth {
			nameWidth = len(row.name())
		}
	}
	buf.WriteString(fmt.Sprintf("  %-2s %-*s %6s %6s\n", "#", nameWidth, "Table", "UPDATE", "DELETE"))
	for i, row := range view.risks {
		buf.WriteString(fmt.Sprintf("  %-2d %-*s %6d %6d\n", i+1, nameWidth, row.name(), row.update, row.delete))
	}
	if len(view.insertOnly) > 0 {
		buf.WriteString("  " + i18n.Tf("report.text.noPrimaryKeyInsertOnly", map[string]any{
			"Tables": formatInsertOnlyTables(view.insertOnly),
		}) + "\n")
	}
	if view.note != "" {
		buf.WriteString("  " + view.note + "\n")
	}
	buf.WriteString("\n")
}

func renderTopTablesTable(buf *strings.Builder, tables []model.TableStats, topN int) {
	buf.WriteString("=== " + i18n.T("report.text.topTables") + " ===\n")
	if len(tables) == 0 {
		buf.WriteString("  " + i18n.T("report.text.noTableActivity") + "\n\n")
		return
	}

	displayedTables, omittedTables := limitTablesForDisplay(tables, topN)
	totalRows := 0
	for _, table := range tables {
		totalRows += table.TotalRows
	}

	rows := make([]tableRow, len(displayedTables))
	nameWidth := len("Table")
	for i, table := range displayedTables {
		name := table.Schema + "." + table.Table
		share := 0.0
		if totalRows > 0 {
			share = float64(table.TotalRows) * 100 / float64(totalRows)
		}
		rows[i] = tableRow{
			name:   name,
			total:  fmt.Sprintf("%d", table.TotalRows),
			insert: fmtOpCellText(table.InsertRows, table.TotalRows),
			update: fmtOpCellText(table.UpdateRows, table.TotalRows),
			delete: fmtOpCellText(table.DeleteRows, table.TotalRows),
			ddl:    fmtOpCellText(table.DDLCount, table.EventCount),
			txns:   fmt.Sprintf("%d", table.TxnCount),
			events: fmt.Sprintf("%d", table.EventCount),
			share:  fmt.Sprintf("%.1f%%", share),
		}
		if len(name) > nameWidth {
			nameWidth = len(name)
		}
	}

	totalWidth := maxInt(len("Affected Rows"), maxWidth(rows, func(r tableRow) string { return r.total }))
	insertWidth := maxInt(len("INSERT"), maxWidth(rows, func(r tableRow) string { return r.insert }))
	updateWidth := maxInt(len("UPDATE"), maxWidth(rows, func(r tableRow) string { return r.update }))
	deleteWidth := maxInt(len("DELETE"), maxWidth(rows, func(r tableRow) string { return r.delete }))
	ddlWidth := maxInt(len("DDL Events"), maxWidth(rows, func(r tableRow) string { return r.ddl }))
	txnsWidth := maxInt(len("Transactions"), maxWidth(rows, func(r tableRow) string { return r.txns }))
	eventsWidth := maxInt(len("Binlog Events"), maxWidth(rows, func(r tableRow) string { return r.events }))
	shareWidth := maxInt(len("Row Share"), maxWidth(rows, func(r tableRow) string { return r.share }))

	buf.WriteString(fmt.Sprintf("  %-2s %-*s %*s %*s %*s %*s %*s %*s %*s %*s\n",
		"#", nameWidth, "Table",
		totalWidth, "Affected Rows",
		insertWidth, "INSERT", updateWidth, "UPDATE", deleteWidth, "DELETE",
		ddlWidth, "DDL Events", txnsWidth, "Transactions",
		eventsWidth, "Binlog Events", shareWidth, "Row Share"))
	for i, row := range rows {
		buf.WriteString(fmt.Sprintf("  %-2d %-*s %*s %*s %*s %*s %*s %*s %*s %*s\n",
			i+1, nameWidth, row.name,
			totalWidth, row.total,
			insertWidth, row.insert, updateWidth, row.update, deleteWidth, row.delete,
			ddlWidth, row.ddl, txnsWidth, row.txns,
			eventsWidth, row.events, shareWidth, row.share))
	}
	if omittedTables > 0 {
		buf.WriteString("  " + omittedTablesLabel(omittedTables) + "\n")
	}
	buf.WriteString("  " + i18n.T("report.text.topTablesFootnote") + "\n")
	buf.WriteString("\n")
}

func maxWidth(rows []tableRow, extract func(tableRow) string) int {
	max := 0
	for _, row := range rows {
		if w := len(extract(row)); w > max {
			max = w
		}
	}
	return max
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func renderTopTransactions(buf *strings.Builder, result model.AnalysisResult, opts Options) {
	buf.WriteString("=== " + i18n.T("report.text.topTransactions") + " ===\n")
	topN := opts.TopN
	mode := opts.SQLContextMode

	largestLimit := minInt(3, topN)
	longestLimit := largestLimit
	otherLimit := minInt(1, topN)
	lines := make([]string, 0, largestLimit+longestLimit+otherLimit)
	printedQuery := false
	for _, txn := range limitTransactions(result.Diagnostics.LargestTransactions, largestLimit) {
		line := fmt.Sprintf("  %s: %s rows=%d tables=%d file=%s",
			i18n.T("report.text.largestTransaction"), txn.TxnKey, txn.TotalRows, len(txn.Tables), formatTxnEvidenceLocation(txn))
		if table := dominantTableName(txn.Tables); table != "" {
			line += " table=" + table
		}
		if txn.BinlogBytes > 0 {
			line += " bytes=" + formatByteSize(txn.BinlogBytes)
		}
		line = appendTxnIdentity(line, txn)
		lines = append(lines, line)
		if appendTextQueryLine(&lines, txn, mode) {
			printedQuery = true
		}
		appendReplayLine(&lines, txn, result.Diagnostics.ServerVersion)
		appendRowImageLines(&lines, txn, opts)
	}
	if line := formatCommittedDurationLine(result.Diagnostics.DurationBuckets); line != "" {
		lines = append(lines, "  "+line)
	}
	for _, txn := range limitTransactions(result.Diagnostics.LongestTransactions, longestLimit) {
		line := fmt.Sprintf("  %s: %s dur=%s rows=%d file=%s",
			i18n.T("report.text.longestTransaction"), txn.TxnKey, formatDuration(txn.Duration), txn.TotalRows, formatSuspiciousLocation(txn))
		lines = append(lines, appendTxnIdentity(line, txn))
		if appendTextQueryLine(&lines, txn, mode) {
			printedQuery = true
		}
		appendReplayLine(&lines, txn, result.Diagnostics.ServerVersion)
		appendRowImageLines(&lines, txn, opts)
	}
	for _, txn := range limitTransactions(result.Diagnostics.WidestTransactions, otherLimit) {
		line := fmt.Sprintf("  %s: %s tables=%d rows=%d file=%s",
			i18n.T("report.text.widestTransaction"), txn.TxnKey, len(txn.Tables), txn.TotalRows, formatSuspiciousLocation(txn))
		lines = append(lines, appendTxnIdentity(line, txn))
		if appendTextQueryLine(&lines, txn, mode) {
			printedQuery = true
		}
		appendReplayLine(&lines, txn, result.Diagnostics.ServerVersion)
		appendRowImageLines(&lines, txn, opts)
	}

	if !printedQuery {
		for _, txn := range result.Transactions {
			if appendTextQueryLine(&lines, txn, mode) {
				break
			}
		}
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
	if shouldSuggestOpenHTML(result) {
		buf.WriteString("  " + i18n.T("report.text.openHTML") + "\n")
	}
	if location := firstSuspiciousLocation(result); location != "" {
		buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.text.firstSuspiciousPosition"), location))
	}
	buf.WriteString("\n")
}

func shouldSuggestOpenHTML(result model.AnalysisResult) bool {
	// Zero row images with ignored Query-DML is not a successful ROW analysis.
	if result.Diagnostics.IgnoredQueryDMLEvents > 0 && result.Summary.TotalRows == 0 {
		return false
	}
	return true
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
		line := i18n.Tf("report.format.minuteActivity", map[string]any{
			"Minute":    formatTimeWithLayout(minute.Minute, "2006-01-02 15:04"),
			"TotalRows": minute.TotalRows,
			"TxnCount":  minute.TxnCount,
		})
		if tables := formatDrivingTables(minute.TableRows, topN); tables != "" {
			line += "  " + tables
		}
		buf.WriteString("  " + line + "\n")
	}
	buf.WriteString("\n")
}

func renderWriteShapePatterns(buf *strings.Builder, patterns []model.PatternStats, drilldowns []model.PatternDrilldown, topN int, mode SQLContextMode) {
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
		if mode != SQLContextOff && strings.TrimSpace(pattern.SampleQuerySummary) != "" {
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
			formatTimeWithLayout(minute.Minute, "2006-01-02 15:04"), minute.TotalRows, minute.TxnCount))
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
		if txn.QueryContext != nil && strings.TrimSpace(txn.QueryContext.SQL) != "" {
			sql := model.DisplayStoredSQL(txn.QueryContext.SQL, txn.QueryContext.Truncated, txn.QueryContext.OriginalBytes)
			return strings.Join(strings.Fields(sql), " ")
		}
		return strings.Join(strings.Fields(txn.QuerySummary), " ")
	case SQLContextSummary:
		fallthrough
	default:
		return strings.Join(strings.Fields(txn.QuerySummary), " ")
	}
}

func ddlStatementForMode(event model.DDLEvent, mode SQLContextMode) string {
	statement := strings.TrimSpace(event.Statement)
	switch mode {
	case SQLContextOff:
		return ""
	case SQLContextFull:
		return model.DisplayStoredSQL(statement, event.StatementTruncated, event.StatementOriginalBytes)
	default:
		original := event.StatementOriginalBytes
		if original <= 0 {
			original = len(statement)
		}
		return model.FormatQuerySummary(statement, original)
	}
}

func appendReplayLine(lines *[]string, txn model.Transaction, serverVersion string) {
	if cmd := mysqlbinlogCmd(txn, serverVersion); cmd != "" {
		*lines = append(*lines, "    "+i18n.T("report.label.fullTransactionReplay")+": "+cmd)
		return
	}
	if note := stdinReplayNote(txn); note != "" {
		*lines = append(*lines, "    "+note)
	}
}

func appendTextQueryLine(lines *[]string, txn model.Transaction, mode SQLContextMode) bool {
	query := transactionTextQuery(txn, mode)
	if query == "" {
		return false
	}
	*lines = append(*lines, "    "+i18n.T("report.label.query")+": "+query)
	return true
}

func renderTopThreads(buf *strings.Builder, threads []model.ThreadStats, rankedBy string, limit int) {
	buf.WriteString("=== " + threadSectionTitle(rankedBy) + " ===\n")
	if len(threads) == 0 {
		buf.WriteString("  " + i18n.T("report.text.noThreads") + "\n\n")
		return
	}
	shown, omitted := limitThreads(threads, limit)
	cols := threadColumnsOf(shown)
	rows := make([]threadTextRow, len(shown))
	for i, thread := range shown {
		rows[i] = threadTextRow{
			rank:   fmt.Sprintf("%d", i+1),
			thread: formatOptionalID(thread.ThreadID),
			server: formatOptionalID(thread.ServerID),
			actor:  formatUserHost(thread.ActorUser, thread.ActorHost),
			schema: formatThreadSchemas(thread),
			rows:   fmt.Sprintf("%d", thread.TotalRows),
			events: fmt.Sprintf("%d", thread.EventCount),
			bytes:  formatByteSize(thread.BinlogBytes),
			txns:   fmt.Sprintf("%d", thread.TxnCount),
			share:  fmt.Sprintf("%.1f%%", thread.Share*100),
		}
	}
	writeThreadTable(buf, cols, rows)
	if omitted > 0 {
		buf.WriteString("  " + omittedThreadsLabel(omitted) + "\n")
	}
	buf.WriteString("\n")
}

func threadSectionTitle(rankedBy string) string {
	key := "report.text.topThreadsByRows"
	switch rankedBy {
	case model.ThreadRankEvents:
		key = "report.text.topThreadsByEvents"
	case model.ThreadRankBytes:
		key = "report.text.topThreadsByBytes"
	case model.ThreadRankTransactions:
		key = "report.text.topThreadsByTransactions"
	}
	return i18n.T(key)
}

type threadColumnSet struct {
	thread bool
	server bool
	actor  bool
	schema bool
	rows   bool
	events bool
	bytes  bool
	txns   bool
}

func threadColumnsOf(threads []model.ThreadStats) threadColumnSet {
	var cols threadColumnSet
	for _, thread := range threads {
		if thread.ThreadID != 0 {
			cols.thread = true
		}
		if thread.ServerID != 0 {
			cols.server = true
		}
		if formatUserHost(thread.ActorUser, thread.ActorHost) != "" {
			cols.actor = true
		}
		if formatThreadSchemas(thread) != "" {
			cols.schema = true
		}
		if thread.TotalRows > 0 {
			cols.rows = true
		}
		if thread.EventCount > 0 {
			cols.events = true
		}
		if thread.BinlogBytes > 0 {
			cols.bytes = true
		}
		if thread.TxnCount > 0 {
			cols.txns = true
		}
	}
	return cols
}

type threadTextRow struct {
	rank   string
	thread string
	server string
	actor  string
	schema string
	rows   string
	events string
	bytes  string
	txns   string
	share  string
}

func writeThreadTable(buf *strings.Builder, cols threadColumnSet, rows []threadTextRow) {
	type col struct {
		show  bool
		title string
		width int
		value func(threadTextRow) string
	}
	columns := []col{
		{true, "#", 1, func(r threadTextRow) string { return r.rank }},
		{cols.thread, "thread_id", len("thread_id"), func(r threadTextRow) string { return r.thread }},
		{cols.server, "server_id", len("server_id"), func(r threadTextRow) string { return r.server }},
		{cols.actor, "user@host", len("user@host"), func(r threadTextRow) string { return r.actor }},
		{cols.schema, "schema", len("schema"), func(r threadTextRow) string { return r.schema }},
		{cols.rows, "rows", len("rows"), func(r threadTextRow) string { return r.rows }},
		{cols.events, "events", len("events"), func(r threadTextRow) string { return r.events }},
		{cols.bytes, "bytes", len("bytes"), func(r threadTextRow) string { return r.bytes }},
		{cols.txns, "txns", len("txns"), func(r threadTextRow) string { return r.txns }},
		{true, "share", len("share"), func(r threadTextRow) string { return r.share }},
	}
	for i := range columns {
		if !columns[i].show {
			continue
		}
		for _, row := range rows {
			if w := len(columns[i].value(row)); w > columns[i].width {
				columns[i].width = w
			}
		}
	}
	var header strings.Builder
	header.WriteString(" ")
	for _, column := range columns {
		if !column.show {
			continue
		}
		fmt.Fprintf(&header, " %-*s", column.width, column.title)
	}
	buf.WriteString(strings.TrimRight(header.String(), " ") + "\n")
	for _, row := range rows {
		var line strings.Builder
		line.WriteString(" ")
		for _, column := range columns {
			if !column.show {
				continue
			}
			fmt.Fprintf(&line, " %-*s", column.width, column.value(row))
		}
		buf.WriteString(strings.TrimRight(line.String(), " ") + "\n")
	}
}

func formatOptionalID(id uint32) string {
	if id == 0 {
		return "-"
	}
	return fmt.Sprintf("%d", id)
}

func formatThreadSchemas(thread model.ThreadStats) string {
	if len(thread.Schemas) == 0 {
		return thread.Schema
	}
	if len(thread.Schemas) <= 3 {
		return strings.Join(thread.Schemas, ",")
	}
	return fmt.Sprintf("%s +%d", thread.Schemas[0], len(thread.Schemas)-1)
}

func formatTPSPeak(summary model.WorkloadSummary, points []model.TimeseriesPoint) string {
	if summary.Duration < time.Second && summary.TotalTransactions >= 1 {
		return i18n.T("report.text.tpsSubSecond")
	}
	return formatPeakSeries(points)
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
	return fmt.Sprintf("%.1f at %s", peak.Value, formatTimeWithLayout(peak.Minute, "2006-01-02 15:04"))
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
	for _, finding := range result.Diagnostics.Findings {
		if location := suspiciousTransactionLocation(result, finding.TxnKey); location != "" {
			return location
		}
	}
	for _, alert := range result.Alerts {
		if location := suspiciousTransactionLocation(result, alert.TxnKey); location != "" {
			return location
		}
	}
	return ""
}

func suspiciousTransactionLocation(result model.AnalysisResult, txnKey string) string {
	if txnKey == "" {
		return ""
	}
	for _, txns := range [][]model.Transaction{
		result.Transactions,
		result.Diagnostics.LargestTransactions,
		result.Diagnostics.LongestTransactions,
		result.Diagnostics.WidestTransactions,
	} {
		for _, txn := range txns {
			if txn.TxnKey == txnKey {
				return formatSuspiciousLocation(txn)
			}
		}
	}
	return ""
}

func largestTxnBytes(result model.AnalysisResult) int64 {
	for _, txn := range result.Diagnostics.LargestByteTransactions {
		if txn.BinlogBytes > 0 {
			return txn.BinlogBytes
		}
	}
	var maxBytes int64
	for _, txn := range result.Diagnostics.LargestTransactions {
		if txn.BinlogBytes > maxBytes {
			maxBytes = txn.BinlogBytes
		}
	}
	return maxBytes
}

func dominantTableName(tables map[string]int) string {
	best := ""
	bestRows := -1
	for name, rows := range tables {
		if rows > bestRows || (rows == bestRows && (best == "" || name < best)) {
			best = name
			bestRows = rows
		}
	}
	return best
}

func formatByteSize(n int64) string {
	const kb = 1024
	switch {
	case n < kb:
		return fmt.Sprintf("%dB", n)
	case n < kb*kb:
		return fmt.Sprintf("%.1fKB", float64(n)/float64(kb))
	default:
		return fmt.Sprintf("%.1fMB", float64(n)/float64(kb*kb))
	}
}

func appendTxnIdentity(line string, txn model.Transaction) string {
	id := formatTxnIdentity(txn)
	if id == "" {
		return line
	}
	return line + " " + id
}

// formatTxnIdentity lists producer and session fields the transaction actually carries.
// Zero and empty values stay omitted so a missing binlog field is not printed as data.
func formatTxnIdentity(txn model.Transaction) string {
	parts := make([]string, 0, 6)
	if txn.ServerID != 0 {
		parts = append(parts, fmt.Sprintf("server_id=%d", txn.ServerID))
	}
	if txn.ThreadID != 0 {
		parts = append(parts, fmt.Sprintf("thread_id=%d", txn.ThreadID))
	}
	if txn.GTID != "" {
		parts = append(parts, "gtid="+txn.GTID)
	}
	if txn.XID != "" {
		parts = append(parts, "xid="+txn.XID)
	}
	if txn.XAXID != "" {
		parts = append(parts, "xa_xid="+txn.XAXID)
	}
	if userHost := formatUserHost(txn.ActorUser, txn.ActorHost); userHost != "" {
		parts = append(parts, "user@host="+userHost)
	}
	return strings.Join(parts, " ")
}

func formatUserHost(user, host string) string {
	switch {
	case user != "" && host != "":
		return user + "@" + host
	case user != "":
		return user
	default:
		return host
	}
}

func formatSuspiciousLocation(txn model.Transaction) string {
	if txn.BinlogPathStart == "" && txn.PositionStart == 0 && txn.PositionEnd == 0 {
		return i18n.T("time.notAvailable")
	}
	return formatBinlogLocationWithEnd(txn.BinlogPathStart, txn.PositionStart, txn.BinlogPathEnd, txn.PositionEnd)
}

func renderDDLTimeline(buf *strings.Builder, events []model.DDLEvent, limit int, mode SQLContextMode) {
	if len(events) == 0 {
		return
	}
	buf.WriteString("=== " + i18n.T("report.html.analyze.ddlTimeline") + " ===\n")
	buf.WriteString("  " + i18n.T("report.text.ddlOccurrenceNote") + "\n")
	shown := events
	extra := 0
	if limit > 0 && len(events) > limit {
		shown = events[:limit]
		extra = len(events) - limit
	}
	for _, event := range shown {
		location := formatBinlogLocation(event.BinlogPath, event.PositionStart, event.PositionEnd)
		if location == "" {
			location = i18n.T("time.notAvailable")
		}
		buf.WriteString(fmt.Sprintf("  %s  %s  %s  %s\n",
			formatTime(event.Timestamp), event.Operation, ddlObjectName(event), location))
		if stmt := ddlStatementForMode(event, mode); stmt != "" {
			buf.WriteString("    " + stmt + "\n")
		}
	}
	if extra > 0 {
		buf.WriteString("  " + i18n.Tf("report.text.omittedDDL", map[string]any{"Count": extra}) + "\n")
	}
	buf.WriteString("\n")
}

func ddlObjectName(event model.DDLEvent) string {
	object := strings.Trim(strings.TrimSpace(event.Schema+"."+event.Table), ".")
	if object == "" {
		object = strings.TrimSpace(event.Object)
	}
	if object == "" {
		return i18n.T("time.notAvailable")
	}
	return object
}

func renderOpenDML(buf *strings.Builder, groups []model.OpenDMLGroup) {
	if len(groups) == 0 {
		return
	}
	buf.WriteString("=== " + i18n.T("report.text.openUncommittedDML") + " ===\n")
	buf.WriteString("  " + i18n.T("report.text.openDMLNote") + "\n")
	for _, group := range groups {
		tables := joinedSortedTables(group.Tables)
		if tables == "" {
			tables = "-"
		}
		location := formatBinlogLocationWithEnd(group.BinlogPathStart, group.PositionStart, group.BinlogPathEnd, group.PositionEnd)
		if location == "" {
			location = i18n.T("time.notAvailable")
		}
		buf.WriteString(fmt.Sprintf("  %s dur=%s rows=%d tables=%s file=%s\n",
			group.TxnKey, formatDuration(group.Duration), group.TotalRows, tables, location))
	}
	buf.WriteString("\n")
}

func formatCommittedDurationLine(buckets []model.DurationBucket) string {
	if len(buckets) == 0 {
		return ""
	}
	parts := make([]string, len(buckets))
	for i, bucket := range buckets {
		parts[i] = fmt.Sprintf("%s=%d", bucket.Label, bucket.TxnCount)
	}
	return i18n.T("report.text.committedDuration") + ": " + strings.Join(parts, " ")
}

func renderByteContributors(buf *strings.Builder, result model.AnalysisResult, topN int) {
	limit := minInt(3, topN)
	var txnParts []string
	for i, txn := range result.Diagnostics.LargestByteTransactions {
		if i >= limit || txn.BinlogBytes <= 0 {
			break
		}
		txnParts = append(txnParts, txn.TxnKey+" "+formatByteSize(txn.BinlogBytes))
	}
	if len(txnParts) > 0 {
		buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.text.topTxnBytes"), strings.Join(txnParts, ", ")))
	}
	var tableParts []string
	for _, table := range topTablesByBytes(result.Tables, limit) {
		if table.BinlogBytes <= 0 {
			break
		}
		tableParts = append(tableParts, fmt.Sprintf("%s.%s %s", table.Schema, table.Table, formatByteSize(table.BinlogBytes)))
	}
	if len(tableParts) > 0 {
		buf.WriteString(fmt.Sprintf("  %s: %s\n", i18n.T("report.text.topTableBytes"), strings.Join(tableParts, ", ")))
	}
}

func topTablesByBytes(tables []model.TableStats, limit int) []model.TableStats {
	ranked := make([]model.TableStats, 0, len(tables))
	for _, table := range tables {
		if table.BinlogBytes > 0 {
			ranked = append(ranked, table)
		}
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].BinlogBytes != ranked[j].BinlogBytes {
			return ranked[i].BinlogBytes > ranked[j].BinlogBytes
		}
		left := ranked[i].Schema + "." + ranked[i].Table
		right := ranked[j].Schema + "." + ranked[j].Table
		return left < right
	})
	if limit > 0 && len(ranked) > limit {
		ranked = ranked[:limit]
	}
	return ranked
}

func renderSelectedFileLines(buf *strings.Builder, coverage model.FileCoverage) {
	if len(coverage.Selected) < 2 {
		return
	}
	buf.WriteString("  " + i18n.T("report.text.selectedFiles") + ":\n")
	var earliest, latest time.Time
	var total int64
	timed := 0
	for _, item := range coverage.Selected {
		size := i18n.T("time.notAvailable")
		if item.Size > 0 {
			size = formatByteSize(item.Size)
			total += item.Size
		}
		name := item.BinlogPath
		if name == "" {
			name = i18n.T("time.notAvailable")
		}
		buf.WriteString(fmt.Sprintf("    %s  %s  %s\n", name, size, formatFileSpan(item)))
		if !item.FirstEventAt.IsZero() && !item.LastEventAt.IsZero() {
			timed++
			if earliest.IsZero() || item.FirstEventAt.Before(earliest) {
				earliest = item.FirstEventAt
			}
			if item.LastEventAt.After(latest) {
				latest = item.LastEventAt
			}
		}
	}
	if timed == len(coverage.Selected) && total > 0 && !latest.Before(earliest) {
		buf.WriteString("  " + i18n.Tf("report.text.fileGrowthHint", map[string]any{
			"Files":    len(coverage.Selected),
			"Bytes":    formatByteSize(total),
			"Duration": formatDuration(latest.Sub(earliest)),
		}) + "\n")
	}
}

func formatFileSpan(item model.FileCoverageItem) string {
	if item.FirstEventAt.IsZero() && item.LastEventAt.IsZero() {
		return i18n.T("time.notAvailable")
	}
	return formatTime(item.FirstEventAt) + " - " + formatTime(item.LastEventAt)
}

func joinedSortedTables(tables map[string]int) string {
	if len(tables) == 0 {
		return ""
	}
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

func formatDDLTimelineSummary(events []model.DDLEvent) string {
	if len(events) == 0 {
		return "0"
	}
	seen := make(map[string]struct{}, len(events))
	ops := make([]string, 0, len(events))
	for _, event := range events {
		op := strings.TrimSpace(event.Operation)
		if op == "" {
			continue
		}
		if _, ok := seen[op]; ok {
			continue
		}
		seen[op] = struct{}{}
		ops = append(ops, op)
	}
	if len(ops) == 0 {
		return fmt.Sprintf("%d", len(events))
	}
	return fmt.Sprintf("%d (%s)", len(events), strings.Join(ops, ", "))
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func formatTime(t time.Time) string {
	return formatTimeWithLayout(t, "2006-01-02 15:04:05")
}

func formatTimeWithLayout(t time.Time, layout string) string {
	if t.IsZero() {
		return i18n.T("time.notAvailable")
	}
	return t.UTC().Format(layout + " UTC")
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

// fmtOpCellText formats an operation count with inline percentage for text reports.
// Returns "0 (—)" when denominator is 0, "count (pct%)" otherwise.
func fmtOpCellText(count int, denominator int) string {
	if denominator == 0 {
		return fmt.Sprintf("%d (\u2014)", count)
	}
	pct := float64(count) * 100 / float64(denominator)
	return fmt.Sprintf("%d (%.1f%%)", count, pct)
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
