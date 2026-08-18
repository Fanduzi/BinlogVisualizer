// Package report renders self-contained HTML reports from bounded analysis results.
// input: analyzer-produced AnalysisResult values plus optional SQL context presentation controls.
// output: single self-contained HTML incident page with demoted charts and shared findings.
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
	TotalRows           int
	TotalEvents         int
	DDLCount            int
	Tables              []htmlTableRow
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
	Verdict             htmlAlert
	PeakMinute          htmlHotInterval
	HasPeakMinute       bool
	HottestTable        htmlTableRow
	HasHottestTable     bool
	IncidentTxn         htmlTxnDiagnostic
	HasIncidentTxn      bool
	RollbackHint        string
	HasRollbackHint     bool
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
	Key          string
	DOMID        string
	Schema       string
	Table        string
	Total        int
	Inserts      int
	Updates      int
	Deletes      int
	Txns         int
	DDLCount     int
	EventCount   int
	InsertPct    string
	UpdatePct    string
	DeletePct    string
	DDLPct       string
	HasActivity  bool
	QuerySummary string
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
	TxnKey       string
	Rows         int
	Events       int
	Duration     string
	BinlogBytes  int
	Tables       []htmlTxnTable
	Location     string
	LocationNote string
	QuerySummary string
	Inserts      int
	Updates      int
	Deletes      int
	HasOps       bool
}

type htmlTxnTable struct {
	Name string
	Rows int
}

type htmlHotInterval struct {
	Timestamp      string
	Rows           int
	Txns           int
	Events         int
	BinlogBytes    int
	DDLCount       int
	Tables         []htmlHotTable
	BaselineRows   int
	BaselineFactor float64
	HasBaseline    bool
	BaselineLabel  string
}

type htmlHotTable struct {
	Key  string
	Rows int
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
		GeneratedAt: time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
		TotalTxns:   result.Summary.TotalTransactions,
		TotalRows:   result.Summary.TotalRows,
		TotalEvents: result.Summary.TotalEvents,
		EChartsJS:   template.JS(echartsJS), //nolint:gosec
		TopN:        opts.TopN,
	}

	if !result.Summary.StartTime.IsZero() {
		d.StartTime = result.Summary.StartTime.Format("2006-01-02 15:04:05")
		d.EndTime = result.Summary.EndTime.Format("2006-01-02 15:04:05")
		d.Duration = result.Summary.Duration.String()
	}

	tableActivitySeries := make(map[string]htmlTableActivitySeries, len(result.Tables))

	// Tables
	tables := result.Tables
	if len(tables) > opts.TopN {
		tables = tables[:opts.TopN]
	}
	for _, t := range tables {
		key := t.Schema + "." + t.Table
		domID := sanitizeDOMID(key)
		if len(t.Activity) > 0 {
			tableActivitySeries[key] = buildHTMLTableActivitySeries(t.Activity)
		}
		d.Tables = append(d.Tables, htmlTableRow{
			Key:         key,
			DOMID:       domID,
			Schema:      t.Schema,
			Table:       t.Table,
			Total:       t.TotalRows,
			Inserts:     t.InsertRows,
			Updates:     t.UpdateRows,
			Deletes:     t.DeleteRows,
			Txns:        t.TxnCount,
			DDLCount:    t.DDLCount,
			EventCount:  t.EventCount,
			InsertPct:   fmtOpCell(t.InsertRows, t.TotalRows),
			UpdatePct:   fmtOpCell(t.UpdateRows, t.TotalRows),
			DeletePct:   fmtOpCell(t.DeleteRows, t.TotalRows),
			DDLPct:      fmtOpCell(t.DDLCount, t.EventCount),
			HasActivity: len(t.Activity) > 0,
		})
	}
	d.TableActivitySeries = mustJSON(tableActivitySeries)

	// Findings: same list the text report prints. Never treat empty alerts as healthy.
	findings := collectDisplayFindings(result, opts.TopN)
	for _, finding := range findings {
		d.Alerts = append(d.Alerts, htmlAlert{
			Severity: finding.Severity,
			Message:  finding.Message,
			Badge:    findingBadge(finding.Severity),
		})
	}
	d.HasAlerts = len(d.Alerts) > 0
	if len(findings) > 0 {
		d.Verdict = d.Alerts[0]
	} else {
		d.Verdict = htmlAlert{
			Severity: "info",
			Message:  i18n.T("report.text.noFindings"),
			Badge:    "INFO",
		}
	}

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
		d.LargestTransactions = append(d.LargestTransactions, buildHTMLTxnDiagnostic(txn, opts.SQLContextMode))
	}
	d.HasLargestTxns = len(d.LargestTransactions) > 0
	if d.HasLargestTxns {
		d.IncidentTxn = d.LargestTransactions[0]
		d.HasIncidentTxn = true
	}

	for _, txn := range limitTransactions(result.Diagnostics.LongestTransactions, 1) {
		d.LongestTransactions = append(d.LongestTransactions, buildHTMLTxnDiagnostic(txn, opts.SQLContextMode))
	}
	d.HasLongestTxns = len(d.LongestTransactions) > 0

	for i, interval := range result.Diagnostics.HotIntervals {
		item := buildHTMLHotInterval(interval, result.Minutes, result.Alerts)
		d.HotIntervals = append(d.HotIntervals, item)
		if i == 0 {
			d.PeakMinute = item
			d.HasPeakMinute = true
		}
	}
	d.HasHotIntervals = len(d.HotIntervals) > 0

	// Widest transactions
	for _, txn := range limitTransactions(result.Diagnostics.WidestTransactions, 1) {
		d.WidestTransactions = append(d.WidestTransactions, buildHTMLTxnDiagnostic(txn, opts.SQLContextMode))
	}
	d.HasWidestTxns = len(d.WidestTransactions) > 0

	// File coverage
	for _, item := range result.Diagnostics.FileCoverage.Selected {
		d.FileCoverage.Selected = append(d.FileCoverage.Selected, htmlFileCoverageItem{
			BinlogPath:   item.BinlogPath,
			Reason:       item.Reason,
			Size:         formatFileSize(item.Size),
			FirstEventAt: item.FirstEventAt.Format("2006-01-02 15:04:05"),
			LastEventAt:  item.LastEventAt.Format("2006-01-02 15:04:05"),
		})
	}
	for _, item := range result.Diagnostics.FileCoverage.Skipped {
		d.FileCoverage.Skipped = append(d.FileCoverage.Skipped, htmlFileCoverageItem{
			BinlogPath: item.BinlogPath,
			Reason:     item.Reason,
			Size:       formatFileSize(item.Size),
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
	top := result.Tables
	if len(top) > opts.TopN {
		top = top[:opts.TopN]
	}
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

	if len(d.Tables) > 0 {
		d.HottestTable = d.Tables[0]
		d.HasHottestTable = true
	}

	if location := firstUsableRollbackLocation(result); location != "" {
		d.RollbackHint = location
		d.HasRollbackHint = true
	}

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

func buildHTMLTxnDiagnostic(txn model.Transaction, mode SQLContextMode) htmlTxnDiagnostic {
	inserts := txn.Operations["INSERT"]
	updates := txn.Operations["UPDATE"]
	deletes := txn.Operations["DELETE"]
	return htmlTxnDiagnostic{
		TxnKey:       txn.TxnKey,
		Rows:         txn.TotalRows,
		Events:       txn.EventCount,
		Duration:     txn.Duration.String(),
		BinlogBytes:  int(txn.BinlogBytes),
		Tables:       sortedTxnTables(txn.Tables),
		Location:     formatRecordedTxnLocation(txn),
		LocationNote: xidOnlyLocationNote(txn),
		QuerySummary: transactionTextQuery(txn, mode),
		Inserts:      inserts,
		Updates:      updates,
		Deletes:      deletes,
		HasOps:       inserts+updates+deletes > 0,
	}
}

func buildHTMLHotInterval(interval model.MinuteBucket, minutes []model.MinuteBucket, alerts []model.Alert) htmlHotInterval {
	item := htmlHotInterval{
		Timestamp:   interval.Minute.Format("2006-01-02 15:04:05"),
		Rows:        interval.TotalRows,
		Txns:        interval.TxnCount,
		Events:      interval.EventCount,
		BinlogBytes: int(interval.BinlogBytes),
		DDLCount:    interval.DDLCount,
		Tables:      sortedHotTables(interval.TableRows),
	}
	if baseline, factor, ok := spikeBaselineFromAlerts(alerts, interval.Minute); ok {
		item.BaselineRows = baseline
		item.BaselineFactor = factor
		item.HasBaseline = true
		item.BaselineLabel = "alert"
		return item
	}
	if baseline, ok := windowRowMedian(minutes, interval.Minute); ok {
		item.BaselineRows = baseline
		item.HasBaseline = true
		item.BaselineLabel = "window"
		if baseline > 0 {
			item.BaselineFactor = float64(interval.TotalRows) / float64(baseline)
		}
	}
	return item
}

func sortedHotTables(tableRows map[string]int) []htmlHotTable {
	if len(tableRows) == 0 {
		return nil
	}
	items := make([]htmlHotTable, 0, len(tableRows))
	for key, rows := range tableRows {
		items = append(items, htmlHotTable{Key: key, Rows: rows})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Rows == items[j].Rows {
			return items[i].Key < items[j].Key
		}
		return items[i].Rows > items[j].Rows
	})
	if len(items) > 8 {
		items = items[:8]
	}
	return items
}

func spikeBaselineFromAlerts(alerts []model.Alert, peak time.Time) (int, float64, bool) {
	peakMinute := peak.Truncate(time.Minute)
	for _, alert := range alerts {
		if alert.Type != "spike" || alert.Details == nil {
			continue
		}
		if _, hasTable := alert.Details["table"]; hasTable {
			continue
		}
		if !alert.Minute.IsZero() && alert.Minute.Truncate(time.Minute) != peakMinute {
			continue
		}
		baseline, okBaseline := intFromAny(alert.Details["baseline"])
		factor, okFactor := floatFromAny(alert.Details["factor"])
		if okBaseline && okFactor {
			return baseline, factor, true
		}
	}
	return 0, 0, false
}

func windowRowMedian(minutes []model.MinuteBucket, peak time.Time) (int, bool) {
	peakMinute := peak.Truncate(time.Minute)
	values := make([]int, 0, len(minutes))
	for _, minute := range minutes {
		if minute.Minute.Truncate(time.Minute) == peakMinute {
			continue
		}
		values = append(values, minute.TotalRows)
	}
	if len(values) == 0 {
		return 0, false
	}
	sort.Ints(values)
	n := len(values)
	if n%2 == 1 {
		return values[n/2], true
	}
	return (values[n/2-1] + values[n/2]) / 2, true
}

func intFromAny(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}

func floatFromAny(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
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
