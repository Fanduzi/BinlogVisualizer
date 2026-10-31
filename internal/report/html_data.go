// Per-section builders for the HTML report template data; the htmlReportData
// contract itself stays in html.go.

package report

import (
	"encoding/json"
	"fmt"
	"html/template"
	"strings"
	"time"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/report/view"
)

const (
	maxPeakMinutes        = 2
	maxRepresentativeTxns = 2
	maxTopAlerts          = 4
)

// buildHTMLData assembles the template data for the HTML report. Each report
// section is a named builder; inputs shared across sections (the
// display-limited table list, the minute chart layout) are computed once here
// and passed to the builders that need them.
func buildHTMLData(result model.AnalysisResult, opts Options, echartsJS string, built view.View) htmlReportData {
	opts = normalizeOptions(opts)
	d := htmlReportData{
		GeneratedAt:    opts.formatTime(time.Now()),
		TimestampBasis: opts.timestampBasis(),
		EChartsJS:      template.JS(echartsJS), //nolint:gosec // embedded ECharts bundle from static/echarts.min.js, not user input
		TopN:           opts.TopN,
	}
	buildHTMLHeader(&d, result, opts, built.Key)

	tables, omittedTables := limitTablesForDisplay(result.Tables, opts.TopTables)
	buildHTMLTables(&d, result, opts, tables, omittedTables)
	buildHTMLThreads(&d, result, opts)
	buildHTMLAlerts(&d, result.Alerts)
	buildHTMLDDLEvents(&d, result, opts)
	buildHTMLHotRows(&d, result, opts)
	buildHTMLScopeNotes(&d, result, opts)

	d.TransactionEvidence = buildHTMLTransactionEvidence(built.Evidence, result.Diagnostics.ServerVersion, opts)
	d.HasTransactionEvidence = len(d.TransactionEvidence) > 0

	buildHTMLHotIntervals(&d, result, opts)
	buildHTMLFileCoverage(&d, result, opts)
	buildHTMLFileSegments(&d, result, opts)

	chartLayout := opts.chartMinuteLayout(result.Minutes)
	buildHTMLMinuteChart(&d, result, opts, chartLayout)
	buildHTMLTPSSeries(&d, result, opts, chartLayout)
	buildHTMLTablesBar(&d, tables)
	buildHTMLOpsPie(&d, result.Tables)
	buildHTMLDrilldowns(&d, result, opts)

	d.ApplyDelayUnavailable, d.ApplyDelaySource, d.ApplyDelayStats, d.ApplyDelayTxns = buildHTMLApplyDelay(result.Diagnostics.ApplyDelay, opts.TopN, opts)
	return d
}

func buildHTMLHeader(d *htmlReportData, result model.AnalysisResult, opts Options, txns []model.Transaction) {
	d.TotalTxns = result.Summary.TotalTransactions
	d.PartialTxns = result.Summary.PartialTransactions
	d.UnknownTxns = result.Summary.UnknownTransactions
	d.TotalRows = result.Summary.TotalRows
	d.TotalEvents = result.Summary.TotalEvents
	d.CountedEventBytes = formatFileSize(countedEventBytes(result))
	if bytes, ok := selectedInputFileBytes(result.Diagnostics.FileCoverage); ok {
		d.InputFileSize = formatFileSize(bytes)
		d.HasInputFileSize = true
	}
	for _, txn := range txns {
		d.Transactions = append(d.Transactions, htmlTransactionLookup{
			TxnKey:   txn.TxnKey,
			Rows:     txn.TotalRows,
			Duration: txn.Duration.String(),
			Identity: formatTxnIdentity(txn),
		})
	}
	d.HasTransactions = len(d.Transactions) > 0

	if !result.Summary.StartTime.IsZero() {
		d.StartTime = opts.formatTime(result.Summary.StartTime)
		d.EndTime = opts.formatTime(result.Summary.EndTime)
		d.Duration = result.Summary.Duration.String()
	}
}

func buildHTMLTables(d *htmlReportData, result model.AnalysisResult, opts Options, tables []model.TableStats, omittedTables int) {
	if omittedTables > 0 {
		d.OmittedTables = omittedTablesLabel(omittedTables)
	}
	tableActivitySeries := make(map[string]htmlTableActivitySeries, len(result.Tables))
	for _, t := range tables {
		key := t.Schema + "." + t.Table
		domID := sanitizeDOMID(key)
		points := t.Activity
		if len(points) == 0 && result.MinuteTables != nil {
			points = result.MinuteTables.Activity(t.Schema, t.Table)
		}
		if len(points) > 0 {
			tableActivitySeries[key] = buildHTMLTableActivitySeries(points, opts)
		}
		d.Tables = append(d.Tables, htmlTableRow{
			Key:                  key,
			DOMID:                domID,
			Schema:               t.Schema,
			Table:                t.Table,
			Total:                t.TotalRows,
			BinlogBytes:          t.BinlogBytes,
			BinlogBytesFormatted: formatFileSize(t.BinlogBytes),
			Inserts:              t.InsertRows,
			Updates:              t.UpdateRows,
			Deletes:              t.DeleteRows,
			Txns:                 t.TxnCount,
			DDLCount:             t.DDLCount,
			EventCount:           t.EventCount,
			InsertPct:            fmtOpCell(t.InsertRows, t.TotalRows),
			UpdatePct:            fmtOpCell(t.UpdateRows, t.TotalRows),
			DeletePct:            fmtOpCell(t.DeleteRows, t.TotalRows),
			DDLPct:               fmtOpCell(t.DDLCount, t.EventCount),
			HasActivity:          len(points) > 0,
		})
	}
	d.TableActivitySeries = mustJSON(tableActivitySeries)
}

func buildHTMLThreads(d *htmlReportData, result model.AnalysisResult, opts Options) {
	d.ThreadsTitle = threadSectionTitle(result.ThreadsRankedBy)
	shownThreads, omittedThreads := limitThreads(result.Threads, opts.TopThreads)
	if omittedThreads > 0 {
		d.OmittedThreads = omittedThreadsLabel(omittedThreads)
	}
	for i, thread := range shownThreads {
		row := htmlThreadRow{
			Rank:   i + 1,
			Actor:  formatUserHost(thread.ActorUser, thread.ActorHost),
			Schema: formatThreadSchemas(thread),
			Rows:   thread.TotalRows,
			Events: thread.EventCount,
			Bytes:  formatByteSize(thread.BinlogBytes),
			Txns:   thread.TxnCount,
			Share:  fmt.Sprintf("%.1f%%", thread.Share*100),
		}
		if thread.ThreadID != 0 {
			row.ThreadID = fmt.Sprintf("%d", thread.ThreadID)
			d.ThreadShowID = true
		}
		if thread.ServerID != 0 {
			row.ServerID = fmt.Sprintf("%d", thread.ServerID)
			d.ThreadShowServer = true
		}
		if row.Actor != "" {
			d.ThreadShowActor = true
		}
		if row.Schema != "" {
			d.ThreadShowSchema = true
		}
		if thread.TotalRows > 0 {
			d.ThreadShowRows = true
		}
		if thread.EventCount > 0 {
			d.ThreadShowEvents = true
		}
		if thread.BinlogBytes > 0 {
			d.ThreadShowBytes = true
		}
		if thread.TxnCount > 0 {
			d.ThreadShowTxns = true
		}
		d.Threads = append(d.Threads, row)
	}
	d.HasThreads = len(d.Threads) > 0
}

func buildHTMLAlerts(d *htmlReportData, alerts []model.Alert) {
	for _, a := range alerts {
		badge := "INFO"
		switch a.Severity {
		case "warning":
			badge = "WARN"
		case "critical":
			badge = "CRIT"
		}
		d.Alerts = append(d.Alerts, htmlAlert{
			Severity: a.Severity,
			Message:  a.Message,
			Badge:    badge,
		})
	}
	d.HasAlerts = len(d.Alerts) > 0
	// Top 4 alerts for executive summary display.
	topLimit := len(d.Alerts)
	if topLimit > maxTopAlerts {
		topLimit = maxTopAlerts
	}
	d.TopAlerts = d.Alerts[:topLimit]
}

func buildHTMLDDLEvents(d *htmlReportData, result model.AnalysisResult, opts Options) {
	for _, ddl := range result.Diagnostics.DDLEvents {
		object := strings.Trim(strings.TrimSpace(ddl.Schema+"."+ddl.Table), ".")
		if object == "" {
			object = ddl.Object
		}
		view := ddlTimelineViewFor(ddl, result.Diagnostics.ServerVersion)
		d.DDLEvents = append(d.DDLEvents, htmlDDLEvent{
			Timestamp:   opts.formatTime(ddl.Timestamp),
			Operation:   ddl.Operation,
			Object:      object,
			Statement:   ddlStatementForMode(ddl, opts.SQLContextMode),
			Location:    formatBinlogLocation(ddl.BinlogPath, ddl.PositionStart, ddl.PositionEnd),
			Identity:    view.Identity,
			TxnStart:    view.TxnStart,
			Explain:     view.Explain,
			Mysqlbinlog: view.Mysqlbinlog,
			StopGTID:    view.StopGTID,
		})
	}
	d.HasDDLEvents = len(d.DDLEvents) > 0
	d.DDLCount = ddlStatementCount(result.Diagnostics.DDLEvents)
	d.DDLNote = ddlTargetPhrase(result.Diagnostics.DDLEvents)
}

func buildHTMLHotRows(d *htmlReportData, result model.AnalysisResult, opts Options) {
	if !hotRowHasSection(result.HotRows) {
		return
	}
	d.HasHotRows = true
	d.HotRowsLead = i18n.T("report.text.hotRowsLead")
	listed, omitted := limitHotRows(result.HotRows.Rows, opts.TopRows)
	if hotRowKeysHidden(opts) && len(listed) > 0 {
		d.HotRowsHidden = i18n.T("report.text.hotRowsHidden")
	}
	d.HotRows = make([]htmlHotRow, len(listed))
	for i, row := range listed {
		key := hotRowKey(row, opts)
		if key == "" {
			key = i18n.T("report.text.hotRowsHiddenKey")
		}
		d.HotRows[i] = htmlHotRow{
			Rank:         i + 1,
			Table:        hotRowTable(row),
			Key:          key,
			Touches:      row.Touches,
			Transactions: row.Transactions,
			First:        opts.formatTime(row.FirstTime),
			Last:         opts.formatTime(row.LastTime),
			FirstTxn:     hotRowTxnPlace(row.FirstGTID, row.FirstFile, row.FirstPos),
			LastTxn:      hotRowTxnPlace(row.LastGTID, row.LastFile, row.LastPos),
			Approximate:  row.Approximate,
		}
	}
	if omitted > 0 {
		d.HotRowsOmitted = i18n.Tf("report.text.hotRowsOmitted", map[string]any{"Count": omitted})
	}
	for _, gap := range result.HotRows.Unavailable {
		d.HotRowUnavailable = append(d.HotRowUnavailable, hotRowUnavailableLine(gap))
	}
	if result.HotRows.Overflow {
		d.HotRowsNote = hotRowOverflowLine(result.HotRows.TrackLimit)
	}
}

func buildHTMLScopeNotes(d *htmlReportData, result model.AnalysisResult, opts Options) {
	d.DMLFilter = dmlFilterLabel(result.Scope)
	pk := primaryKeyView(result.Tables)
	if len(pk.risks) == 0 {
		d.PrimaryKeyNote = pk.summaryLine
	} else {
		d.PrimaryKeyNote = pk.note
		d.NoPKTables = make([]htmlNoPKRow, len(pk.risks))
		for i, row := range pk.risks {
			d.NoPKTables[i] = htmlNoPKRow{Rank: i + 1, Name: row.name(), Update: row.update, Delete: row.delete}
		}
		if len(pk.insertOnly) > 0 {
			d.NoPKInsertOnly = i18n.Tf("report.text.noPrimaryKeyInsertOnly", map[string]any{
				"Tables": formatInsertOnlyTables(pk.insertOnly),
			})
		}
	}
	if rowValuesSuppressed(opts) {
		d.RowValuesNote = i18n.T("report.text.rowValuesSuppressed")
	} else if showRowValues(opts) {
		d.ColumnNamesNote = columnNamesNote(result)
	}
}

func buildHTMLHotIntervals(d *htmlReportData, result model.AnalysisResult, opts Options) {
	for _, interval := range result.Diagnostics.HotIntervals {
		tables, omitted := limitMinuteTables(rankedMinuteTables(interval.TableRows), opts.TopN)
		card := htmlHotInterval{
			Timestamp:            opts.formatTime(interval.Minute),
			Rows:                 interval.TotalRows,
			Txns:                 interval.TxnCount,
			Events:               interval.EventCount,
			BinlogBytes:          int(interval.BinlogBytes),
			BinlogBytesFormatted: formatFileSize(interval.BinlogBytes),
			DDLCount:             interval.DDLCount,
		}
		if len(tables) > 0 {
			card.Tables = make([]htmlTxnTable, len(tables))
			for i, table := range tables {
				card.Tables[i] = htmlTxnTable{Name: table.name, Rows: table.rows}
			}
		}
		if omitted > 0 {
			card.OmittedTablesLabel = omittedTablesLabel(omitted)
		}
		d.HotIntervals = append(d.HotIntervals, card)
	}
	d.HasHotIntervals = len(d.HotIntervals) > 0
}

func buildHTMLFileCoverage(d *htmlReportData, result model.AnalysisResult, opts Options) {
	for _, item := range result.Diagnostics.FileCoverage.Selected {
		d.FileCoverage.Selected = append(d.FileCoverage.Selected, htmlFileCoverageItem{
			BinlogPath:   item.BinlogPath,
			Reason:       item.Reason,
			Size:         formatCoverageSize(item.Size),
			FirstEventAt: opts.formatTime(item.FirstEventAt),
			LastEventAt:  opts.formatTime(item.LastEventAt),
		})
	}
	for _, item := range result.Diagnostics.FileCoverage.Skipped {
		d.FileCoverage.Skipped = append(d.FileCoverage.Skipped, htmlFileCoverageItem{
			BinlogPath: item.BinlogPath,
			Reason:     item.Reason,
			Size:       formatCoverageSize(item.Size),
		})
	}
	d.HasFileCoverage = len(d.FileCoverage.Selected) > 0 || len(d.FileCoverage.Skipped) > 0
}

func buildHTMLFileSegments(d *htmlReportData, result model.AnalysisResult, opts Options) {
	for _, seg := range result.Diagnostics.FileSegments {
		d.FileSegments = append(d.FileSegments, htmlFileSegment{
			StartTime:   opts.formatTimeWithLayout(seg.StartTime, "2006-01-02 15:04"),
			EndTime:     opts.formatTimeWithLayout(seg.EndTime, "2006-01-02 15:04"),
			BinlogBytes: seg.BinlogBytes,
			Rows:        seg.Rows,
			Events:      seg.Events,
		})
	}
	d.HasFileSegments = len(d.FileSegments) > 0

	// Throughput chart series
	throughputLabels := make([]string, 0, len(d.FileSegments))
	throughputBytes := make([]int64, 0, len(d.FileSegments))
	throughputRows := make([]int, 0, len(d.FileSegments))
	for _, seg := range d.FileSegments {
		throughputLabels = append(throughputLabels, seg.StartTime)
		throughputBytes = append(throughputBytes, seg.BinlogBytes)
		throughputRows = append(throughputRows, seg.Rows)
	}
	d.ThroughputLabels = mustJSON(throughputLabels)
	d.ThroughputBytes = mustJSON(throughputBytes)
	d.ThroughputRows = mustJSON(throughputRows)
}

func buildHTMLMinuteChart(d *htmlReportData, result model.AnalysisResult, opts Options, chartLayout string) {
	labels := make([]string, 0, len(result.Minutes))
	rows := make([]int, 0, len(result.Minutes))
	txns := make([]int, 0, len(result.Minutes))
	for _, m := range result.Minutes {
		labels = append(labels, opts.formatTimeWithLayout(m.Minute, chartLayout))
		rows = append(rows, m.TotalRows)
		txns = append(txns, m.TxnCount)
	}
	d.MinuteLabels, d.MinuteRows, d.MinuteTxns = mustJSON(labels), mustJSON(rows), mustJSON(txns)
}

func buildHTMLTPSSeries(d *htmlReportData, result model.AnalysisResult, opts Options, chartLayout string) {
	tpsLabels := make([]string, 0, len(result.Timeseries.TPSSeries))
	tpsValues := make([]float64, 0, len(result.Timeseries.TPSSeries))
	for _, point := range result.Timeseries.TPSSeries {
		tpsLabels = append(tpsLabels, opts.formatTimeWithLayout(point.Minute, chartLayout))
		tpsValues = append(tpsValues, point.Value)
	}
	if len(tpsLabels) == 0 {
		for _, m := range result.Minutes {
			tpsLabels = append(tpsLabels, opts.formatTimeWithLayout(m.Minute, chartLayout))
			tpsValues = append(tpsValues, float64(m.TxnCount)/60.0)
		}
	}
	d.TPSLabels = mustJSON(tpsLabels)
	d.TPSValues = mustJSON(tpsValues)
}

// buildHTMLTablesBar charts the display-limited table list, matching the Top
// Tables section; the ops pie below totals every analyzed table instead.
func buildHTMLTablesBar(d *htmlReportData, tables []model.TableStats) {
	barNames := make([]string, 0, len(tables))
	barRows := make([]int, 0, len(tables))
	for _, t := range tables {
		barNames = append(barNames, t.Schema+"."+t.Table)
		barRows = append(barRows, t.TotalRows)
	}
	d.TableBarNames = mustJSON(barNames)
	d.TableBarRows = mustJSON(barRows)
}

func buildHTMLOpsPie(d *htmlReportData, tables []model.TableStats) {
	var totalInserts, totalUpdates, totalDeletes int
	for _, t := range tables {
		totalInserts += t.InsertRows
		totalUpdates += t.UpdateRows
		totalDeletes += t.DeleteRows
	}
	pie := []map[string]any{
		{"name": "INSERT", "value": totalInserts},
		{"name": "UPDATE", "value": totalUpdates},
		{"name": "DELETE", "value": totalDeletes},
	}
	d.OpsPie = mustJSON(pie)
}

func buildHTMLDrilldowns(d *htmlReportData, result model.AnalysisResult, opts Options) {
	for _, dd := range result.PatternDrilldowns {
		hd := htmlDrilldown{
			PatternKey:    dd.PatternKey,
			Label:         dd.Label,
			WhySelected:   dd.WhySelected,
			ShareOfRows:   dd.ShareOfRows,
			ShareOfTxns:   dd.ShareOfTxns,
			AvgRowsPerTxn: dd.AvgRowsPerTxn,
			SignalFlags: htmlSignalFlags{
				Dominance: dd.SignalFlags.Dominance,
				Anomaly:   dd.SignalFlags.Anomaly,
			},
		}
		for i, m := range dd.BusiestMinutes {
			if i >= maxPeakMinutes {
				break
			}
			hd.BusiestMinutes = append(hd.BusiestMinutes, htmlPeakMinute{
				Minute:    opts.formatTimeWithLayout(m.Minute, "2006-01-02 15:04"),
				TotalRows: m.TotalRows,
				TxnCount:  m.TxnCount,
			})
		}
		for i, txn := range dd.RepresentativeTransactions {
			if i >= maxRepresentativeTxns {
				break
			}
			hd.RepTxns = append(hd.RepTxns, htmlRepTxn{
				TxnKey:    txn.TxnKey,
				TotalRows: txn.TotalRows,
				Duration:  txn.Duration.String(),
			})
		}
		d.Drilldowns = append(d.Drilldowns, hd)
	}
	d.HasDrilldowns = len(d.Drilldowns) > 0
}

func mustJSON(v any) template.JS {
	b, err := json.Marshal(v)
	if err != nil {
		return template.JS("[]") //nolint:gosec // constant empty JSON array
	}
	return template.JS(b) //nolint:gosec // json.Marshal escapes <, > and &, so it cannot close the script tag
}

func buildHTMLTableActivitySeries(points []model.TableActivityPoint, opts Options) htmlTableActivitySeries {
	series := htmlTableActivitySeries{
		Labels:     make([]string, 0, len(points)),
		Rows:       make([]int, 0, len(points)),
		InsertRows: make([]int, 0, len(points)),
		UpdateRows: make([]int, 0, len(points)),
		DeleteRows: make([]int, 0, len(points)),
	}
	for _, point := range points {
		series.Labels = append(series.Labels, opts.formatTimeWithLayout(point.Minute, opts.chartActivityLayout(points)))
		series.Rows = append(series.Rows, point.Rows)
		series.InsertRows = append(series.InsertRows, point.InsertRows)
		series.UpdateRows = append(series.UpdateRows, point.UpdateRows)
		series.DeleteRows = append(series.DeleteRows, point.DeleteRows)
	}
	return series
}
