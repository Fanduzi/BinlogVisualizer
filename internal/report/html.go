// Package report renders self-contained HTML reports from complete analysis results.
// input: analyzer-produced AnalysisResult values plus optional SQL context presentation controls.
// output: self-contained HTML with completeness, selected-file/count-event bytes, transaction lookup, and labelled trusted full-transaction replay commands.
// pos: HTML renderer for the CLI output path after analyzer Finalize.
// note: if this file changes, update this header and module README.md.
package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"sort"
	"strings"
	"time"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

const (
	maxPeakMinutes        = 2
	maxRepresentativeTxns = 2
)

// RenderHTML renders an AnalysisResult as a self-contained HTML report.
func RenderHTML(result model.AnalysisResult) (string, error) {
	return RenderHTMLWithOptions(result, DefaultOptions())
}

// RenderHTMLWithOptions renders an AnalysisResult as HTML with explicit presentation controls.
func RenderHTMLWithOptions(result model.AnalysisResult, opts Options) (string, error) {
	opts = normalizeOptions(opts)

	echartJS, err := ReadEmbeddedECharts()
	if err != nil {
		return "", err
	}

	tmpl, err := NewHTMLTemplate("report", htmlReportTemplate, template.FuncMap{
		"safeJS":     func(s string) template.JS { return template.JS(s) }, //nolint:gosec
		"json":       jsonMarshal,
		"mulFloat":   func(f float64, m float64) float64 { return f * m },
		"fmtIntHTML": fmtIntHTML,
	})
	if err != nil {
		return "", err
	}

	data := buildHTMLData(result, opts, echartJS)

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("render html template: %w", err)
	}
	return buf.String(), nil
}

func jsonMarshal(v any) (template.JS, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return template.JS(b), nil //nolint:gosec
}

type htmlReportData struct {
	GeneratedAt         string
	SourceFiles         string
	StartTime           string
	EndTime             string
	Duration            string
	TotalTxns           int
	PartialTxns         int
	UnknownTxns         int
	TotalRows           int
	TotalEvents         int
	DDLCount            int
	InputFileSize       string
	HasInputFileSize    bool
	CountedEventBytes   string
	Transactions        []htmlTransactionLookup
	HasTransactions     bool
	Tables              []htmlTableRow
	OmittedTables       string
	TableActivitySeries template.JS
	DDLEvents           []htmlDDLEvent
	HasDDLEvents        bool
	LargestTransactions []htmlTxnDiagnostic
	HasLargestTxns      bool
	LongestTransactions []htmlTxnDiagnostic
	HasLongestTxns      bool
	WidestTransactions  []htmlTxnDiagnostic
	HasWidestTxns       bool
	HotIntervals        []htmlHotInterval
	HasHotIntervals     bool
	FileCoverage        htmlFileCoverageData
	HasFileCoverage     bool
	FileSegments        []htmlFileSegment
	HasFileSegments     bool
	ThroughputLabels    template.JS
	ThroughputBytes     template.JS
	ThroughputRows      template.JS
	TPSLabels           template.JS
	TPSValues           template.JS
	Alerts              []htmlAlert
	HasAlerts           bool
	TopAlerts           []htmlAlert
	Drilldowns          []htmlDrilldown
	HasDrilldowns       bool
	MinuteLabels        template.JS
	MinuteRows          template.JS
	MinuteTxns          template.JS
	TableBarNames       template.JS
	TableBarRows        template.JS
	OpsPie              template.JS
	EChartsJS           template.JS
	TopN                int
}

type htmlDrilldown struct {
	PatternKey     string
	Label          string
	WhySelected    string
	ShareOfRows    float64
	ShareOfTxns    float64
	AvgRowsPerTxn  float64
	SignalFlags    htmlSignalFlags
	BusiestMinutes []htmlPeakMinute
	RepTxns        []htmlRepTxn
}

type htmlSignalFlags struct {
	Dominance bool
	Anomaly   bool
}

type htmlPeakMinute struct {
	Minute    string
	TotalRows int
	TxnCount  int
}

type htmlRepTxn struct {
	TxnKey    string
	TotalRows int
	Duration  string
}

type htmlTableRow struct {
	Key                  string
	DOMID                string
	Schema               string
	Table                string
	Total                int
	BinlogBytes          int64
	BinlogBytesFormatted string
	Inserts              int
	Updates              int
	Deletes              int
	Txns                 int
	DDLCount             int
	EventCount           int
	InsertPct            string
	UpdatePct            string
	DeletePct            string
	DDLPct               string
	HasActivity          bool
}

type htmlAlert struct {
	Severity string
	Message  string
	Badge    string
}

type htmlDDLEvent struct {
	Timestamp string
	Operation string
	Object    string
	Statement string
	Location  string
}

type htmlTxnDiagnostic struct {
	TxnKey               string
	Rows                 int
	Events               int
	Duration             string
	BinlogBytes          int
	BinlogBytesFormatted string
	Tables               []htmlTxnTable
	Location             string
	QuerySummary         string
	MysqlbinlogCmd       string
}

type htmlTransactionLookup struct {
	TxnKey   string
	Rows     int
	Duration string
}

type htmlTxnTable struct {
	Name string
	Rows int
}

type htmlHotInterval struct {
	Timestamp            string
	Rows                 int
	Txns                 int
	Events               int
	BinlogBytes          int
	BinlogBytesFormatted string
	DDLCount             int
}

type htmlFileCoverageData struct {
	Selected []htmlFileCoverageItem
	Skipped  []htmlFileCoverageItem
}

type htmlFileCoverageItem struct {
	BinlogPath   string
	Reason       string
	Size         string
	FirstEventAt string
	LastEventAt  string
}

type htmlFileSegment struct {
	StartTime   string
	EndTime     string
	BinlogBytes int64
	Rows        int
	Events      int
}

type htmlTableActivitySeries struct {
	Labels     []string `json:"labels"`
	Rows       []int    `json:"rows"`
	InsertRows []int    `json:"insert_rows"`
	UpdateRows []int    `json:"update_rows"`
	DeleteRows []int    `json:"delete_rows"`
}

func buildHTMLData(result model.AnalysisResult, opts Options, echartsJS string) htmlReportData {
	opts = normalizeOptions(opts)
	d := htmlReportData{
		GeneratedAt:       time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
		TotalTxns:         result.Summary.TotalTransactions,
		PartialTxns:       result.Summary.PartialTransactions,
		UnknownTxns:       result.Summary.UnknownTransactions,
		TotalRows:         result.Summary.TotalRows,
		TotalEvents:       result.Summary.TotalEvents,
		CountedEventBytes: formatFileSize(countedEventBytes(result)),
		EChartsJS:         template.JS(echartsJS), //nolint:gosec
		TopN:              opts.TopN,
	}
	if bytes, ok := selectedInputFileBytes(result.Diagnostics.FileCoverage); ok {
		d.InputFileSize = formatFileSize(bytes)
		d.HasInputFileSize = true
	}
	for _, txn := range result.Transactions {
		d.Transactions = append(d.Transactions, htmlTransactionLookup{
			TxnKey:   txn.TxnKey,
			Rows:     txn.TotalRows,
			Duration: txn.Duration.String(),
		})
	}
	d.HasTransactions = len(d.Transactions) > 0

	if !result.Summary.StartTime.IsZero() {
		d.StartTime = result.Summary.StartTime.Format("2006-01-02 15:04:05")
		d.EndTime = result.Summary.EndTime.Format("2006-01-02 15:04:05")
		d.Duration = result.Summary.Duration.String()
	}

	tableActivitySeries := make(map[string]htmlTableActivitySeries, len(result.Tables))

	// Tables
	tables, omittedTables := limitTablesForDisplay(result.Tables, opts.TopTables)
	if omittedTables > 0 {
		d.OmittedTables = omittedTablesLabel(omittedTables)
	}
	for _, t := range tables {
		key := t.Schema + "." + t.Table
		domID := sanitizeDOMID(key)
		if len(t.Activity) > 0 {
			tableActivitySeries[key] = buildHTMLTableActivitySeries(t.Activity)
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
			HasActivity:          len(t.Activity) > 0,
		})
	}
	d.TableActivitySeries = mustJSON(tableActivitySeries)

	// Alerts
	for _, a := range result.Alerts {
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
	if topLimit > 4 {
		topLimit = 4
	}
	d.TopAlerts = d.Alerts[:topLimit]

	for _, ddl := range result.Diagnostics.DDLEvents {
		object := strings.Trim(strings.TrimSpace(ddl.Schema+"."+ddl.Table), ".")
		if object == "" {
			object = ddl.Object
		}
		d.DDLEvents = append(d.DDLEvents, htmlDDLEvent{
			Timestamp: ddl.Timestamp.Format("2006-01-02 15:04:05"),
			Operation: ddl.Operation,
			Object:    object,
			Statement: ddl.Statement,
			Location:  formatBinlogLocation(ddl.BinlogPath, ddl.PositionStart, ddl.PositionEnd),
		})
	}
	d.HasDDLEvents = len(d.DDLEvents) > 0
	d.DDLCount = len(d.DDLEvents)

	for _, txn := range limitTransactions(result.Diagnostics.LargestTransactions, 1) {
		d.LargestTransactions = append(d.LargestTransactions, buildHTMLTxnDiagnostic(txn, result.Diagnostics.ServerVersion))
	}
	d.HasLargestTxns = len(d.LargestTransactions) > 0

	for _, txn := range limitTransactions(result.Diagnostics.LongestTransactions, 1) {
		d.LongestTransactions = append(d.LongestTransactions, buildHTMLTxnDiagnostic(txn, result.Diagnostics.ServerVersion))
	}
	d.HasLongestTxns = len(d.LongestTransactions) > 0

	for _, interval := range result.Diagnostics.HotIntervals {
		d.HotIntervals = append(d.HotIntervals, htmlHotInterval{
			Timestamp:            interval.Minute.Format("2006-01-02 15:04:05"),
			Rows:                 interval.TotalRows,
			Txns:                 interval.TxnCount,
			Events:               interval.EventCount,
			BinlogBytes:          int(interval.BinlogBytes),
			BinlogBytesFormatted: formatFileSize(interval.BinlogBytes),
			DDLCount:             interval.DDLCount,
		})
	}
	d.HasHotIntervals = len(d.HotIntervals) > 0

	// Widest transactions
	for _, txn := range limitTransactions(result.Diagnostics.WidestTransactions, 1) {
		d.WidestTransactions = append(d.WidestTransactions, buildHTMLTxnDiagnostic(txn, result.Diagnostics.ServerVersion))
	}
	d.HasWidestTxns = len(d.WidestTransactions) > 0

	// File coverage
	for _, item := range result.Diagnostics.FileCoverage.Selected {
		d.FileCoverage.Selected = append(d.FileCoverage.Selected, htmlFileCoverageItem{
			BinlogPath:   item.BinlogPath,
			Reason:       item.Reason,
			Size:         formatCoverageSize(item.Size),
			FirstEventAt: item.FirstEventAt.Format("2006-01-02 15:04:05"),
			LastEventAt:  item.LastEventAt.Format("2006-01-02 15:04:05"),
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

	// File segments and throughput chart data
	for _, seg := range result.Diagnostics.FileSegments {
		d.FileSegments = append(d.FileSegments, htmlFileSegment{
			StartTime:   seg.StartTime.Format("2006-01-02 15:04"),
			EndTime:     seg.EndTime.Format("2006-01-02 15:04"),
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

	// Chart data — minute timeline
	labels := make([]string, 0, len(result.Minutes))
	rows := make([]int, 0, len(result.Minutes))
	txns := make([]int, 0, len(result.Minutes))
	for _, m := range result.Minutes {
		labels = append(labels, m.Minute.Format("15:04"))
		rows = append(rows, m.TotalRows)
		txns = append(txns, m.TxnCount)
	}
	d.MinuteLabels = mustJSON(labels)
	d.MinuteRows = mustJSON(rows)
	d.MinuteTxns = mustJSON(txns)

	tpsLabels := make([]string, 0, len(result.Timeseries.TPSSeries))
	tpsValues := make([]float64, 0, len(result.Timeseries.TPSSeries))
	for _, point := range result.Timeseries.TPSSeries {
		tpsLabels = append(tpsLabels, point.Minute.Format("15:04"))
		tpsValues = append(tpsValues, point.Value)
	}
	if len(tpsLabels) == 0 {
		for _, m := range result.Minutes {
			tpsLabels = append(tpsLabels, m.Minute.Format("15:04"))
			tpsValues = append(tpsValues, float64(m.TxnCount)/60.0)
		}
	}
	d.TPSLabels = mustJSON(tpsLabels)
	d.TPSValues = mustJSON(tpsValues)

	// Chart data — top tables bar
	top := tables
	barNames := make([]string, 0, len(top))
	barRows := make([]int, 0, len(top))
	var totalInserts, totalUpdates, totalDeletes int
	for _, t := range result.Tables {
		totalInserts += t.InsertRows
		totalUpdates += t.UpdateRows
		totalDeletes += t.DeleteRows
	}
	for _, t := range top {
		barNames = append(barNames, t.Schema+"."+t.Table)
		barRows = append(barRows, t.TotalRows)
	}
	d.TableBarNames = mustJSON(barNames)
	d.TableBarRows = mustJSON(barRows)

	// Ops pie
	pie := []map[string]any{
		{"name": "INSERT", "value": totalInserts},
		{"name": "UPDATE", "value": totalUpdates},
		{"name": "DELETE", "value": totalDeletes},
	}
	d.OpsPie = mustJSON(pie)

	// Pattern Drilldowns
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
				Minute:    m.Minute.Format("2006-01-02 15:04"),
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

	return d
}

func limitTransactions(txns []model.Transaction, limit int) []model.Transaction {
	if limit <= 0 || len(txns) <= limit {
		return txns
	}
	return txns[:limit]
}

func mustJSON(v any) template.JS {
	b, err := json.Marshal(v)
	if err != nil {
		return template.JS("[]") //nolint:gosec
	}
	return template.JS(b) //nolint:gosec
}

func buildHTMLTableActivitySeries(points []model.TableActivityPoint) htmlTableActivitySeries {
	series := htmlTableActivitySeries{
		Labels:     make([]string, 0, len(points)),
		Rows:       make([]int, 0, len(points)),
		InsertRows: make([]int, 0, len(points)),
		UpdateRows: make([]int, 0, len(points)),
		DeleteRows: make([]int, 0, len(points)),
	}
	for _, point := range points {
		series.Labels = append(series.Labels, point.Minute.Format("15:04"))
		series.Rows = append(series.Rows, point.Rows)
		series.InsertRows = append(series.InsertRows, point.InsertRows)
		series.UpdateRows = append(series.UpdateRows, point.UpdateRows)
		series.DeleteRows = append(series.DeleteRows, point.DeleteRows)
	}
	return series
}

func sanitizeDOMID(raw string) string {
	replacer := strings.NewReplacer(".", "-", "_", "-", "/", "-", " ", "-")
	return replacer.Replace(strings.ToLower(raw))
}

func formatFileSize(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.1f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func formatCoverageSize(bytes int64) string {
	if bytes <= 0 {
		return i18n.T("time.notAvailable")
	}
	return formatFileSize(bytes)
}

func buildHTMLTxnDiagnostic(txn model.Transaction, serverVersion string) htmlTxnDiagnostic {
	return htmlTxnDiagnostic{
		TxnKey:               txn.TxnKey,
		Rows:                 txn.TotalRows,
		Events:               txn.EventCount,
		Duration:             txn.Duration.String(),
		BinlogBytes:          int(txn.BinlogBytes),
		BinlogBytesFormatted: formatFileSize(txn.BinlogBytes),
		Tables:               sortedTxnTables(txn.Tables),
		Location:             formatBinlogSpan(txn),
		QuerySummary:         txn.QuerySummary,
		MysqlbinlogCmd:       mysqlbinlogCmd(txn, serverVersion),
	}
}

func sortedTxnTables(tables map[string]int) []htmlTxnTable {
	if len(tables) == 0 {
		return nil
	}
	items := make([]htmlTxnTable, 0, len(tables))
	for key, rows := range tables {
		items = append(items, htmlTxnTable{Name: key, Rows: rows})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Rows == items[j].Rows {
			return items[i].Name < items[j].Name
		}
		return items[i].Rows > items[j].Rows
	})
	return items
}

func formatBinlogSpan(txn model.Transaction) string {
	return formatBinlogLocationWithEnd(txn.BinlogPathStart, txn.PositionStart, txn.BinlogPathEnd, txn.PositionEnd)
}

func formatBinlogLocation(path string, start, end int64) string {
	return formatBinlogLocationWithEnd(path, start, path, end)
}

func formatBinlogLocationWithEnd(startPath string, start int64, endPath string, end int64) string {
	switch {
	case startPath != "" && endPath != "" && startPath == endPath && start != 0 && end != 0:
		return fmt.Sprintf("%s:%d-%d", startPath, start, end)
	case startPath != "" && endPath != "" && start != 0 && end != 0:
		return fmt.Sprintf("%s:%d-%s:%d", startPath, start, endPath, end)
	case startPath != "" && start != 0:
		return fmt.Sprintf("%s:%d", startPath, start)
	case startPath != "":
		return startPath
	default:
		return ""
	}
}

// RenderHTMLToStdout writes the HTML report to stdout.
func RenderHTMLToStdout(result model.AnalysisResult, opts Options) error {
	text, err := RenderHTMLWithOptions(result, opts)
	if err != nil {
		return err
	}
	_, err = fmt.Print(text)
	return err
}

// fmtInt formats an integer with thousands separators.
func fmtIntHTML(n int) string {
	s := fmt.Sprintf("%d", n)
	if n < 1000 {
		return s
	}
	var result strings.Builder
	for i, ch := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteByte(',')
		}
		result.WriteRune(ch)
	}
	return result.String()
}

// fmtOpCell formats an operation count with inline percentage.
// Returns "0 (\u2014)" when denominator is 0, "count (pct%)" otherwise.
func fmtOpCell(count int, denominator int) string {
	if denominator == 0 {
		return fmtIntHTML(count) + " (\u2014)"
	}
	pct := float64(count) * 100 / float64(denominator)
	return fmt.Sprintf("%s (%.1f%%)", fmtIntHTML(count), pct)
}
