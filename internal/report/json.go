// Package report renders JSON reports from bounded analysis results.
// input: analyzer-produced AnalysisResult values plus optional SQL context and snapshot presentation controls.
// output: stable JSON objects with XA-aware transactions, mode-controlled query fields, optional snapshot envelope data, and mysqlbinlog_cmd replay strings.
// pos: JSON serializer for the CLI output path after analyzer Finalize.
// note: if this file changes, update this header and module README.md.
package report

import (
	"encoding/json"
	"io"
	"os"
	"time"

	"binlogviz/internal/model"
)

const currentReportVersion = 2

// jsonAnalysisResult is the JSON-serializable representation of AnalysisResult.
// Field names use snake_case for script-friendly output.
type jsonAnalysisResult struct {
	ReportVersion     int                    `json:"report_version"`
	Summary           jsonSummary            `json:"summary"`
	Timeseries        jsonTimeseries         `json:"timeseries"`
	Diagnostics       jsonDiagnostics        `json:"diagnostics"`
	Tables            []jsonTableStats       `json:"tables"`
	Transactions      []jsonTransaction      `json:"transactions"`
	Patterns          []jsonPatternStats     `json:"patterns"`
	Minutes           []jsonMinuteBucket     `json:"minutes"`
	Alerts            []jsonAlert            `json:"alerts"`
	Warnings          int                    `json:"warnings"`
	PatternDrilldowns []jsonPatternDrilldown `json:"pattern_drilldowns"`
	Snapshot          *jsonSnapshot          `json:"snapshot,omitempty"`
}

type jsonSummary struct {
	TotalTransactions int    `json:"total_transactions"`
	TotalRows         int    `json:"total_rows"`
	TotalEvents       int    `json:"total_events"`
	StartTime         string `json:"start_time"`
	EndTime           string `json:"end_time"`
	Duration          string `json:"duration"`
}

type jsonTimeseries struct {
	TPSSeries            []jsonTimeseriesPoint    `json:"tps_series"`
	RowsSeries           []jsonTimeseriesPoint    `json:"rows_series"`
	EventsSeries         []jsonTimeseriesPoint    `json:"events_series"`
	InsertEventSeries    []jsonTimeseriesPoint    `json:"insert_event_series"`
	UpdateEventSeries    []jsonTimeseriesPoint    `json:"update_event_series"`
	DeleteEventSeries    []jsonTimeseriesPoint    `json:"delete_event_series"`
	DDLEventSeries       []jsonTimeseriesPoint    `json:"ddl_event_series"`
	BinlogBytesSeries    []jsonTimeseriesPoint    `json:"binlog_bytes_series"`
	TxnSizeSeriesSummary jsonTxnSizeSeriesSummary `json:"txn_size_series_summary"`
}

type jsonTimeseriesPoint struct {
	Minute string  `json:"minute"`
	Value  float64 `json:"value"`
}

type jsonTxnSizeSeriesSummary struct {
	Buckets []jsonTxnSizeBucket `json:"buckets"`
}

type jsonTxnSizeBucket struct {
	Label       string `json:"label"`
	TxnCount    int    `json:"txn_count"`
	Rows        int    `json:"rows"`
	BinlogBytes int64  `json:"binlog_bytes"`
}

type jsonDiagnostics struct {
	FileCoverage          jsonFileCoverage  `json:"file_coverage"`
	DDLEvents             []jsonDDLEvent    `json:"ddl_events"`
	LargestTransactions   []jsonTransaction `json:"largest_transactions"`
	LongestTransactions   []jsonTransaction `json:"longest_transactions"`
	WidestTransactions    []jsonTransaction `json:"widest_transactions"`
	FileSegments          []jsonFileSegment `json:"file_segments"`
	HotIntervals          []jsonHotInterval `json:"hot_intervals"`
	Findings              []jsonFinding     `json:"findings"`
	InputFormatGuess      string            `json:"input_format_guess"`
	IgnoredQueryDMLEvents int               `json:"ignored_query_dml_events"`
}

type jsonFileCoverage struct {
	Selected []jsonFileCoverageItem `json:"selected"`
	Skipped  []jsonFileCoverageItem `json:"skipped"`
}

type jsonFileCoverageItem struct {
	BinlogPath   string `json:"binlog_path"`
	Reason       string `json:"reason,omitempty"`
	Size         int64  `json:"size"`
	FirstEventAt string `json:"first_event_at,omitempty"`
	LastEventAt  string `json:"last_event_at,omitempty"`
}

type jsonDDLEvent struct {
	BinlogPath    string `json:"binlog_path,omitempty"`
	Timestamp     string `json:"timestamp"`
	Schema        string `json:"schema,omitempty"`
	Table         string `json:"table,omitempty"`
	Operation     string `json:"operation"`
	Object        string `json:"object,omitempty"`
	Statement     string `json:"statement,omitempty"`
	PositionStart int64  `json:"position_start,omitempty"`
	PositionEnd   int64  `json:"position_end,omitempty"`
	BinlogBytes   int64  `json:"binlog_bytes,omitempty"`
}

type jsonHotInterval struct {
	Minute      string         `json:"minute"`
	TotalRows   int            `json:"total_rows"`
	TxnCount    int            `json:"txn_count"`
	EventCount  int            `json:"event_count"`
	BinlogBytes int64          `json:"binlog_bytes"`
	DDLCount    int            `json:"ddl_count"`
	TableRows   map[string]int `json:"table_rows,omitempty"`
}

type jsonFinding struct {
	Kind         string   `json:"kind"`
	Severity     string   `json:"severity"`
	Message      string   `json:"message"`
	TxnKey       string   `json:"txn_key,omitempty"`
	Minute       string   `json:"minute,omitempty"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
}

type jsonFileSegment struct {
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	BinlogBytes int64  `json:"binlog_bytes"`
	Rows        int    `json:"rows"`
	Events      int    `json:"events"`
}

type jsonTableStats struct {
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	TotalRows    int    `json:"total_rows"`
	InsertRows   int    `json:"insert_rows"`
	UpdateRows   int    `json:"update_rows"`
	UpdateEvents int    `json:"update_events"`
	DeleteRows   int    `json:"delete_rows"`
	TxnCount     int    `json:"txn_count"`
}

type jsonTransaction struct {
	TxnKey             string         `json:"txn_key"`
	XAXID              string         `json:"xa_xid,omitempty"`
	StartTime          string         `json:"start_time"`
	EndTime            string         `json:"end_time"`
	Duration           string         `json:"duration"`
	TotalRows          int            `json:"total_rows"`
	EventCount         int            `json:"event_count"`
	BinlogBytes        int64          `json:"binlog_bytes"`
	BinlogFileStart    string         `json:"binlog_file_start,omitempty"`
	BinlogFileEnd      string         `json:"binlog_file_end,omitempty"`
	PosStart           int64          `json:"pos_start,omitempty"`
	PosEnd             int64          `json:"pos_end,omitempty"`
	Tables             map[string]int `json:"tables,omitempty"`
	Operations         map[string]int `json:"operations,omitempty"`
	QuerySummary       string         `json:"query_summary,omitempty"`
	QuerySQL           string         `json:"query_sql,omitempty"`
	QueryTruncated     *bool          `json:"query_truncated,omitempty"`
	QueryOriginalBytes *int           `json:"query_original_bytes,omitempty"`
	MysqlbinlogCmd     string         `json:"mysqlbinlog_cmd,omitempty"`
}

type jsonPatternStats struct {
	PatternKey          string         `json:"pattern_key"`
	Label               string         `json:"label"`
	TotalRows           int            `json:"total_rows"`
	TxnCount            int            `json:"txn_count"`
	EventCount          int            `json:"event_count"`
	ShareOfRows         float64        `json:"share_of_rows"`
	ShareOfTransactions float64        `json:"share_of_txns"`
	AvgRowsPerTxn       float64        `json:"avg_rows_per_txn"`
	Tables              map[string]int `json:"tables"`
	Operations          map[string]int `json:"operations"`
	SampleQuerySummary  string         `json:"sample_query_summary,omitempty"`
}

type jsonMinuteBucket struct {
	Minute    string         `json:"minute"`
	TotalRows int            `json:"total_rows"`
	TxnCount  int            `json:"txn_count"`
	TableRows map[string]int `json:"table_rows,omitempty"`
}

type jsonAlert struct {
	Type     string         `json:"type"`
	Severity string         `json:"severity"`
	Message  string         `json:"message"`
	TxnKey   string         `json:"txn_key,omitempty"`
	Minute   string         `json:"minute,omitempty"`
	Details  map[string]any `json:"details,omitempty"`
}

type jsonSnapshot struct {
	Name             string              `json:"name"`
	Label            string              `json:"label"`
	CreatedAt        string              `json:"created_at"`
	BinlogvizVersion string              `json:"binlogviz_version"`
	InputMode        string              `json:"input_mode"`
	Input            jsonSnapshotInput   `json:"input"`
	Window           jsonSnapshotWindow  `json:"window"`
	Filters          jsonSnapshotFilters `json:"filters"`
}

type jsonSnapshotInput struct {
	Files   []string `json:"files"`
	FromDir string   `json:"from_dir"`
	Prefix  string   `json:"prefix"`
}

type jsonSnapshotWindow struct {
	StartTime string `json:"start_time"`
	EndTime   string `json:"end_time"`
}

type jsonSnapshotFilters struct {
	IncludeSchemas []string `json:"include_schema"`
	ExcludeSchemas []string `json:"exclude_schema"`
	IncludeTables  []string `json:"include_table"`
	ExcludeTables  []string `json:"exclude_table"`
}

type jsonPatternDrilldown struct {
	PatternKey                 string                  `json:"pattern_key"`
	Label                      string                  `json:"label"`
	WhySelected                string                  `json:"why_selected"`
	ShareOfRows                float64                 `json:"share_of_rows"`
	ShareOfTxns                float64                 `json:"share_of_txns"`
	AvgRowsPerTxn              float64                 `json:"avg_rows_per_txn"`
	SignalFlags                jsonPatternSignalFlags  `json:"signal_flags"`
	BusiestMinutes             []jsonPeakMinute        `json:"busiest_minutes"`
	RepresentativeTransactions []jsonRepresentativeTxn `json:"representative_transactions"`
}

type jsonPatternSignalFlags struct {
	Dominance bool `json:"dominance"`
	Anomaly   bool `json:"anomaly"`
}

type jsonPeakMinute struct {
	Minute    string `json:"minute"`
	TotalRows int    `json:"total_rows"`
	TxnCount  int    `json:"txn_count"`
}

type jsonRepresentativeTxn struct {
	TxnKey       string `json:"txn_key"`
	TotalRows    int    `json:"total_rows"`
	Duration     string `json:"duration"`
	QuerySummary string `json:"query_summary,omitempty"`
}

// RenderJSON serializes an AnalysisResult to JSON with stable, script-friendly field names.
func RenderJSON(result model.AnalysisResult) (string, error) {
	return RenderJSONWithOptions(result, DefaultOptions())
}

// RenderJSONWithOptions serializes an AnalysisResult with explicit presentation controls.
func RenderJSONWithOptions(result model.AnalysisResult, opts Options) (string, error) {
	jr := convertToJSON(result, normalizeOptions(opts))

	data, err := json.MarshalIndent(jr, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// RenderJSONTo writes the JSON output to the specified writer.
func RenderJSONTo(result model.AnalysisResult, w io.Writer) error {
	return RenderJSONToWithOptions(result, w, DefaultOptions())
}

// RenderJSONToWithOptions writes the JSON output with explicit presentation controls.
func RenderJSONToWithOptions(result model.AnalysisResult, w io.Writer, opts Options) error {
	jr := convertToJSON(result, normalizeOptions(opts))

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(jr)
}

// RenderJSONToStdout writes the JSON output to stdout.
func RenderJSONToStdout(result model.AnalysisResult) error {
	return RenderJSONTo(result, os.Stdout)
}

// RenderJSONToStdoutWithOptions writes the JSON output with explicit presentation controls.
func RenderJSONToStdoutWithOptions(result model.AnalysisResult, opts Options) error {
	return RenderJSONToWithOptions(result, os.Stdout, opts)
}

func convertToJSON(result model.AnalysisResult, opts Options) jsonAnalysisResult {
	return jsonAnalysisResult{
		ReportVersion:     currentReportVersion,
		Summary:           convertSummary(result.Summary),
		Timeseries:        convertTimeseries(result.Timeseries),
		Diagnostics:       convertDiagnostics(result.Diagnostics, opts.SQLContextMode),
		Tables:            convertTables(result.Tables),
		Transactions:      convertTransactions(result.Transactions, opts.SQLContextMode, result.Diagnostics.ServerVersion),
		Patterns:          convertPatterns(result.Patterns),
		Minutes:           convertMinutes(result.Minutes),
		Alerts:            convertAlerts(result.Alerts),
		Warnings:          result.Warnings,
		PatternDrilldowns: convertDrilldowns(result.PatternDrilldowns),
		Snapshot:          convertSnapshot(result.Snapshot),
	}
}

func convertTimeseries(ts model.Timeseries) jsonTimeseries {
	return jsonTimeseries{
		TPSSeries:            convertTimeseriesPoints(ts.TPSSeries),
		RowsSeries:           convertTimeseriesPoints(ts.RowsSeries),
		EventsSeries:         convertTimeseriesPoints(ts.EventsSeries),
		InsertEventSeries:    convertTimeseriesPoints(ts.InsertEventSeries),
		UpdateEventSeries:    convertTimeseriesPoints(ts.UpdateEventSeries),
		DeleteEventSeries:    convertTimeseriesPoints(ts.DeleteEventSeries),
		DDLEventSeries:       convertTimeseriesPoints(ts.DDLEventSeries),
		BinlogBytesSeries:    convertTimeseriesPoints(ts.BinlogBytesSeries),
		TxnSizeSeriesSummary: convertTxnSizeSeriesSummary(ts.TxnSizeSeriesSummary),
	}
}

func convertTimeseriesPoints(points []model.TimeseriesPoint) []jsonTimeseriesPoint {
	if points == nil {
		return []jsonTimeseriesPoint{}
	}
	result := make([]jsonTimeseriesPoint, len(points))
	for i, point := range points {
		result[i] = jsonTimeseriesPoint{
			Minute: formatJSONTime(point.Minute),
			Value:  point.Value,
		}
	}
	return result
}

func convertTxnSizeSeriesSummary(summary model.TxnSizeSeriesSummary) jsonTxnSizeSeriesSummary {
	return jsonTxnSizeSeriesSummary{
		Buckets: convertTxnSizeBuckets(summary.Buckets),
	}
}

func convertTxnSizeBuckets(buckets []model.TxnSizeBucket) []jsonTxnSizeBucket {
	if buckets == nil {
		return []jsonTxnSizeBucket{}
	}
	result := make([]jsonTxnSizeBucket, len(buckets))
	for i, bucket := range buckets {
		result[i] = jsonTxnSizeBucket{
			Label:       bucket.Label,
			TxnCount:    bucket.TxnCount,
			Rows:        bucket.Rows,
			BinlogBytes: bucket.BinlogBytes,
		}
	}
	return result
}

func convertDiagnostics(diagnostics model.Diagnostics, mode SQLContextMode) jsonDiagnostics {
	return jsonDiagnostics{
		FileCoverage:          convertFileCoverage(diagnostics.FileCoverage),
		DDLEvents:             convertDDLEvents(diagnostics.DDLEvents),
		LargestTransactions:   convertTransactions(diagnostics.LargestTransactions, mode, diagnostics.ServerVersion),
		LongestTransactions:   convertTransactions(diagnostics.LongestTransactions, mode, diagnostics.ServerVersion),
		WidestTransactions:    convertTransactions(diagnostics.WidestTransactions, mode, diagnostics.ServerVersion),
		FileSegments:          convertFileSegments(diagnostics.FileSegments),
		HotIntervals:          convertHotIntervals(diagnostics.HotIntervals),
		Findings:              convertFindings(diagnostics.Findings),
		InputFormatGuess:      diagnostics.InputFormatGuess,
		IgnoredQueryDMLEvents: diagnostics.IgnoredQueryDMLEvents,
	}
}

func convertFileCoverage(coverage model.FileCoverage) jsonFileCoverage {
	return jsonFileCoverage{
		Selected: convertFileCoverageItems(coverage.Selected),
		Skipped:  convertFileCoverageItems(coverage.Skipped),
	}
}

func convertFileCoverageItems(items []model.FileCoverageItem) []jsonFileCoverageItem {
	if items == nil {
		return []jsonFileCoverageItem{}
	}
	result := make([]jsonFileCoverageItem, len(items))
	for i, item := range items {
		result[i] = jsonFileCoverageItem{
			BinlogPath:   item.BinlogPath,
			Reason:       item.Reason,
			Size:         item.Size,
			FirstEventAt: formatJSONTime(item.FirstEventAt),
			LastEventAt:  formatJSONTime(item.LastEventAt),
		}
	}
	return result
}

func convertDDLEvents(events []model.DDLEvent) []jsonDDLEvent {
	if events == nil {
		return []jsonDDLEvent{}
	}
	result := make([]jsonDDLEvent, len(events))
	for i, event := range events {
		result[i] = jsonDDLEvent{
			BinlogPath:    event.BinlogPath,
			Timestamp:     formatJSONTime(event.Timestamp),
			Schema:        event.Schema,
			Table:         event.Table,
			Operation:     event.Operation,
			Object:        event.Object,
			Statement:     event.Statement,
			PositionStart: event.PositionStart,
			PositionEnd:   event.PositionEnd,
			BinlogBytes:   event.BinlogBytes,
		}
	}
	return result
}

func convertHotIntervals(intervals []model.MinuteBucket) []jsonHotInterval {
	if intervals == nil {
		return []jsonHotInterval{}
	}
	result := make([]jsonHotInterval, len(intervals))
	for i, interval := range intervals {
		result[i] = jsonHotInterval{
			Minute:      formatJSONTime(interval.Minute),
			TotalRows:   interval.TotalRows,
			TxnCount:    interval.TxnCount,
			EventCount:  interval.EventCount,
			BinlogBytes: interval.BinlogBytes,
			DDLCount:    interval.DDLCount,
			TableRows:   copyStringIntMap(interval.TableRows),
		}
	}
	return result
}

func convertFindings(findings []model.Finding) []jsonFinding {
	if findings == nil {
		return []jsonFinding{}
	}
	result := make([]jsonFinding, len(findings))
	for i, finding := range findings {
		result[i] = jsonFinding{
			Kind:         finding.Kind,
			Severity:     finding.Severity,
			Message:      finding.Message,
			TxnKey:       finding.TxnKey,
			Minute:       formatJSONTime(finding.Minute),
			EvidenceRefs: copyStringSlice(finding.EvidenceRefs),
		}
	}
	return result
}

func convertFileSegments(segments []model.FileSegment) []jsonFileSegment {
	if segments == nil {
		return []jsonFileSegment{}
	}
	result := make([]jsonFileSegment, len(segments))
	for i, seg := range segments {
		result[i] = jsonFileSegment{
			StartTime:   formatJSONTime(seg.StartTime),
			EndTime:     formatJSONTime(seg.EndTime),
			BinlogBytes: seg.BinlogBytes,
			Rows:        seg.Rows,
			Events:      seg.Events,
		}
	}
	return result
}

func convertSummary(s model.WorkloadSummary) jsonSummary {
	return jsonSummary{
		TotalTransactions: s.TotalTransactions,
		TotalRows:         s.TotalRows,
		TotalEvents:       s.TotalEvents,
		StartTime:         formatJSONTime(s.StartTime),
		EndTime:           formatJSONTime(s.EndTime),
		Duration:          s.Duration.String(),
	}
}

func convertTables(tables []model.TableStats) []jsonTableStats {
	if tables == nil {
		return []jsonTableStats{}
	}
	result := make([]jsonTableStats, len(tables))
	for i, t := range tables {
		result[i] = jsonTableStats{
			Schema:       t.Schema,
			Table:        t.Table,
			TotalRows:    t.TotalRows,
			InsertRows:   t.InsertRows,
			UpdateRows:   t.UpdateRows,
			UpdateEvents: t.UpdateEvents,
			DeleteRows:   t.DeleteRows,
			TxnCount:     t.TxnCount,
		}
	}
	return result
}

func convertTransactions(txns []model.Transaction, mode SQLContextMode, serverVersion string) []jsonTransaction {
	if txns == nil {
		return []jsonTransaction{}
	}
	result := make([]jsonTransaction, len(txns))
	for i, t := range txns {
		jt := jsonTransaction{
			TxnKey:          t.TxnKey,
			XAXID:           t.XAXID,
			StartTime:       formatJSONTime(t.StartTime),
			EndTime:         formatJSONTime(t.EndTime),
			Duration:        t.Duration.String(),
			TotalRows:       t.TotalRows,
			EventCount:      t.EventCount,
			BinlogBytes:     t.BinlogBytes,
			BinlogFileStart: t.BinlogPathStart,
			BinlogFileEnd:   t.BinlogPathEnd,
			PosStart:        t.PositionStart,
			PosEnd:          t.PositionEnd,
			Tables:          copyStringIntMap(t.Tables),
			Operations:      copyStringIntMap(t.Operations),
		}
		switch mode {
		case SQLContextOff:
			// omit all query-related fields
		case SQLContextFull:
			jt.QuerySummary = t.QuerySummary
			if t.QueryContext != nil {
				jt.QuerySQL = t.QueryContext.SQL
				jt.QueryTruncated = boolPtr(t.QueryContext.Truncated)
				jt.QueryOriginalBytes = intPtr(t.QueryContext.OriginalBytes)
			}
		case SQLContextSummary:
			fallthrough
		default:
			jt.QuerySummary = t.QuerySummary
			if t.QueryContext != nil {
				jt.QueryTruncated = boolPtr(t.QueryContext.Truncated)
				jt.QueryOriginalBytes = intPtr(t.QueryContext.OriginalBytes)
			}
		}
		jt.MysqlbinlogCmd = mysqlbinlogCmd(t, serverVersion)
		result[i] = jt
	}
	return result
}

func convertPatterns(patterns []model.PatternStats) []jsonPatternStats {
	if patterns == nil {
		return []jsonPatternStats{}
	}
	result := make([]jsonPatternStats, len(patterns))
	for i, p := range patterns {
		result[i] = jsonPatternStats{
			PatternKey:          p.PatternKey,
			Label:               p.Label,
			TotalRows:           p.TotalRows,
			TxnCount:            p.TxnCount,
			EventCount:          p.EventCount,
			ShareOfRows:         p.ShareOfRows,
			ShareOfTransactions: p.ShareOfTransactions,
			AvgRowsPerTxn:       p.AvgRowsPerTxn,
			Tables:              copyStringIntMap(p.Tables),
			Operations:          copyStringIntMap(p.Operations),
			SampleQuerySummary:  p.SampleQuerySummary,
		}
	}
	return result
}

func convertMinutes(minutes []model.MinuteBucket) []jsonMinuteBucket {
	if minutes == nil {
		return []jsonMinuteBucket{}
	}
	result := make([]jsonMinuteBucket, len(minutes))
	for i, m := range minutes {
		result[i] = jsonMinuteBucket{
			Minute:    m.Minute.Format(time.RFC3339),
			TotalRows: m.TotalRows,
			TxnCount:  m.TxnCount,
			TableRows: copyStringIntMap(m.TableRows),
		}
	}
	return result
}

func convertAlerts(alerts []model.Alert) []jsonAlert {
	if alerts == nil {
		return []jsonAlert{}
	}
	result := make([]jsonAlert, len(alerts))
	for i, a := range alerts {
		result[i] = jsonAlert{
			Type:     a.Type,
			Severity: a.Severity,
			Message:  a.Message,
			TxnKey:   a.TxnKey,
			Minute:   formatJSONTime(a.Minute),
			Details:  copyStringAnyMap(a.Details),
		}
	}
	return result
}

func convertSnapshot(snapshot *model.Snapshot) *jsonSnapshot {
	if snapshot == nil {
		return nil
	}
	return &jsonSnapshot{
		Name:             snapshot.Name,
		Label:            snapshot.Label,
		CreatedAt:        formatJSONTime(snapshot.CreatedAt),
		BinlogvizVersion: snapshot.BinlogvizVersion,
		InputMode:        snapshot.InputMode,
		Input:            convertSnapshotInput(snapshot.Input),
		Window:           convertSnapshotWindow(snapshot.Window),
		Filters:          convertSnapshotFilters(snapshot.Filters),
	}
}

func convertSnapshotInput(input model.SnapshotInput) jsonSnapshotInput {
	return jsonSnapshotInput{
		Files:   copyStringSlice(input.Files),
		FromDir: input.FromDir,
		Prefix:  input.Prefix,
	}
}

func convertSnapshotWindow(window model.SnapshotWindow) jsonSnapshotWindow {
	return jsonSnapshotWindow{
		StartTime: formatJSONTime(window.StartTime),
		EndTime:   formatJSONTime(window.EndTime),
	}
}

func convertSnapshotFilters(filters model.SnapshotFilters) jsonSnapshotFilters {
	return jsonSnapshotFilters{
		IncludeSchemas: copyStringSlice(filters.IncludeSchemas),
		ExcludeSchemas: copyStringSlice(filters.ExcludeSchemas),
		IncludeTables:  copyStringSlice(filters.IncludeTables),
		ExcludeTables:  copyStringSlice(filters.ExcludeTables),
	}
}

func convertDrilldowns(drilldowns []model.PatternDrilldown) []jsonPatternDrilldown {
	if drilldowns == nil {
		return []jsonPatternDrilldown{}
	}
	result := make([]jsonPatternDrilldown, len(drilldowns))
	for i, d := range drilldowns {
		result[i] = jsonPatternDrilldown{
			PatternKey:    d.PatternKey,
			Label:         d.Label,
			WhySelected:   d.WhySelected,
			ShareOfRows:   d.ShareOfRows,
			ShareOfTxns:   d.ShareOfTxns,
			AvgRowsPerTxn: d.AvgRowsPerTxn,
			SignalFlags: jsonPatternSignalFlags{
				Dominance: d.SignalFlags.Dominance,
				Anomaly:   d.SignalFlags.Anomaly,
			},
			BusiestMinutes:             convertPeakMinutes(d.BusiestMinutes),
			RepresentativeTransactions: convertRepresentativeTxns(d.RepresentativeTransactions),
		}
		// Enforce hard caps at render boundary as a safety net
		if len(result[i].BusiestMinutes) > 2 {
			result[i].BusiestMinutes = result[i].BusiestMinutes[:2]
		}
		if len(result[i].RepresentativeTransactions) > 2 {
			result[i].RepresentativeTransactions = result[i].RepresentativeTransactions[:2]
		}
	}
	return result
}

func convertPeakMinutes(minutes []model.PatternPeakMinute) []jsonPeakMinute {
	if minutes == nil {
		return []jsonPeakMinute{}
	}
	result := make([]jsonPeakMinute, len(minutes))
	for i, m := range minutes {
		result[i] = jsonPeakMinute{
			Minute:    formatJSONTime(m.Minute),
			TotalRows: m.TotalRows,
			TxnCount:  m.TxnCount,
		}
	}
	return result
}

func convertRepresentativeTxns(txns []model.PatternRepresentativeTxn) []jsonRepresentativeTxn {
	if txns == nil {
		return []jsonRepresentativeTxn{}
	}
	result := make([]jsonRepresentativeTxn, len(txns))
	for i, t := range txns {
		result[i] = jsonRepresentativeTxn{
			TxnKey:       t.TxnKey,
			TotalRows:    t.TotalRows,
			Duration:     t.Duration.String(),
			QuerySummary: t.QuerySummary,
		}
	}
	return result
}

func copyStringSlice(values []string) []string {
	if values == nil {
		return []string{}
	}
	result := make([]string, len(values))
	copy(result, values)
	return result
}

func copyStringIntMap(m map[string]int) map[string]int {
	if m == nil {
		return nil
	}
	result := make(map[string]int, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func copyStringAnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func formatJSONTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func boolPtr(v bool) *bool {
	return &v
}

func intPtr(v int) *int {
	return &v
}
