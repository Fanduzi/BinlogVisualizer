// Package report renders JSON reports from bounded analysis results.
// input: analyzer-produced AnalysisResult values with explicit workload identity, canonical scope, provenance/selector evidence, SQL context, and snapshot presentation controls.
// output: report-v3 JSON with workload identity/scope, RFC3339 UTC timestamps, selection evidence, completeness, safe replay, XA/provenance, SQL modes, full table data, list counts, counted bytes, DDL timeline events, optional open uncommitted DML groups, optional committed duration buckets, optional byte-ranked transactions, optional Ignored QUERY counts, optional open-explicit-group counts, unmapped events, and snapshots.
// pos: JSON serializer for the CLI output path after analyzer Finalize.
// note: if this file changes, update this header and module README.md.
package report

import (
	"encoding/json"
	"io"
	"os"
	"time"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

const currentReportVersion = 3

// jsonAnalysisResult is the JSON-serializable representation of AnalysisResult.
// Field names use snake_case for script-friendly output.
type jsonAnalysisResult struct {
	ReportVersion       int                    `json:"report_version"`
	WorkloadID          string                 `json:"workload_id,omitempty"`
	Scope               *jsonSnapshotFilters   `json:"scope,omitempty"`
	Provenance          *jsonProvenance        `json:"provenance,omitempty"`
	Selection           *jsonSelection         `json:"selection,omitempty"`
	SQLContext          jsonSQLContext         `json:"sql_context"`
	Summary             jsonSummary            `json:"summary"`
	Timeseries          jsonTimeseries         `json:"timeseries"`
	Diagnostics         jsonDiagnostics        `json:"diagnostics"`
	Tables              []jsonTableStats       `json:"tables"`
	Threads             []jsonThreadStats      `json:"threads"`
	ThreadsListed       int                    `json:"threads_listed"`
	ThreadsOmitted      int                    `json:"threads_omitted"`
	ThreadsRankedBy     string                 `json:"threads_ranked_by,omitempty"`
	Transactions        []jsonTransaction      `json:"transactions"`
	TransactionsListed  int                    `json:"transactions_listed"`
	TransactionsOmitted int                    `json:"transactions_omitted"`
	Patterns            []jsonPatternStats     `json:"patterns"`
	Minutes             []jsonMinuteBucket     `json:"minutes"`
	Alerts              []jsonAlert            `json:"alerts"`
	Warnings            int                    `json:"warnings"`
	PatternDrilldowns   []jsonPatternDrilldown `json:"pattern_drilldowns"`
	Snapshot            *jsonSnapshot          `json:"snapshot,omitempty"`
	ColumnNamesNote     string                 `json:"column_names_note,omitempty"`
	RowValuesNote       string                 `json:"row_values_note,omitempty"`
}

type jsonSelection struct {
	RequestedStartPosition *int64   `json:"requested_start_position,omitempty"`
	RequestedStopPosition  *int64   `json:"requested_stop_position,omitempty"`
	EffectiveStartPosition *int64   `json:"effective_start_position,omitempty"`
	EffectiveStopPosition  *int64   `json:"effective_stop_position,omitempty"`
	IncludeGTIDs           []string `json:"include_gtids,omitempty"`
	ExcludeGTIDs           []string `json:"exclude_gtids,omitempty"`
	ResolvedGTIDFlavor     string   `json:"resolved_gtid_flavor,omitempty"`
	MatchedGTIDs           []string `json:"matched_gtids,omitempty"`
}

type jsonSQLContext struct {
	Mode      SQLContextMode `json:"mode"`
	Available bool           `json:"available"`
}

type jsonProvenance struct {
	ServerIDs      []uint32 `json:"server_ids,omitempty"`
	ServerVersions []string `json:"server_versions,omitempty"`
	ServerFlavors  []string `json:"server_flavors,omitempty"`
	MixedProducers bool     `json:"mixed_producers"`
}

type jsonSummary struct {
	TotalTransactions   int    `json:"total_transactions"`
	PartialTransactions int    `json:"partial_transactions"`
	UnknownTransactions int    `json:"unknown_transactions"`
	TotalRows           int    `json:"total_rows"`
	TotalEvents         int    `json:"total_events"`
	StartTime           string `json:"start_time"`
	EndTime             string `json:"end_time"`
	Duration            string `json:"duration"`
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
	FileCoverage            jsonFileCoverage     `json:"file_coverage"`
	CountedEventBytes       int64                `json:"counted_event_bytes"`
	DDLEvents               []jsonDDLEvent       `json:"ddl_events"`
	LargestTransactions     []jsonTransaction    `json:"largest_transactions"`
	LongestTransactions     []jsonTransaction    `json:"longest_transactions"`
	WidestTransactions      []jsonTransaction    `json:"widest_transactions"`
	LargestByteTransactions []jsonTransaction    `json:"largest_byte_transactions,omitempty"`
	OpenDMLGroups           []jsonOpenDMLGroup   `json:"open_dml_groups,omitempty"`
	DurationBuckets         []jsonDurationBucket `json:"duration_buckets,omitempty"`
	FileSegments            []jsonFileSegment    `json:"file_segments"`
	HotIntervals            []jsonHotInterval    `json:"hot_intervals"`
	Findings                []jsonFinding        `json:"findings"`
	InputFormatGuess        string               `json:"input_format_guess"`
	IgnoredQueryDMLEvents   int                  `json:"ignored_query_dml_events"`
	IgnoredQueryEvents      int                  `json:"ignored_query_events,omitempty"`
	OpenExplicitGroups      int                  `json:"open_explicit_groups,omitempty"`
	UnmappedEvents          int                  `json:"unmapped_events,omitempty"`
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

type jsonOpenDMLGroup struct {
	TxnKey          string         `json:"txn_key"`
	GTID            string         `json:"gtid,omitempty"`
	StartTime       string         `json:"start_time"`
	EndTime         string         `json:"end_time"`
	Duration        string         `json:"duration"`
	TotalRows       int            `json:"total_rows"`
	Tables          map[string]int `json:"tables,omitempty"`
	BinlogFileStart string         `json:"binlog_file_start,omitempty"`
	BinlogFileEnd   string         `json:"binlog_file_end,omitempty"`
	PosStart        int64          `json:"pos_start,omitempty"`
	PosEnd          int64          `json:"pos_end,omitempty"`
	Note            string         `json:"note"`
}

type jsonDurationBucket struct {
	Label    string `json:"label"`
	TxnCount int    `json:"txn_count"`
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

type jsonThreadStats struct {
	ThreadID     uint32     `json:"thread_id,omitempty"`
	ServerID     uint32     `json:"server_id,omitempty"`
	Actor        *jsonActor `json:"actor,omitempty"`
	Schema       string     `json:"schema,omitempty"`
	Schemas      []string   `json:"schemas,omitempty"`
	Rows         int        `json:"rows"`
	Events       int        `json:"events"`
	Transactions int        `json:"transactions"`
	BinlogBytes  int64      `json:"binlog_bytes,omitempty"`
	Share        float64    `json:"share"`
	ShareOfRows  float64    `json:"share_of_rows"`
}

type jsonTransaction struct {
	TxnKey             string         `json:"txn_key"`
	XAXID              string         `json:"xa_xid,omitempty"`
	ServerID           uint32         `json:"server_id,omitempty"`
	ServerVersion      string         `json:"server_version,omitempty"`
	ServerFlavor       string         `json:"server_flavor,omitempty"`
	GTID               string         `json:"gtid,omitempty"`
	ThreadID           uint32         `json:"thread_id,omitempty"`
	XID                string         `json:"xid,omitempty"`
	Actor              *jsonActor     `json:"actor,omitempty"`
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
	Completeness       string         `json:"completeness"`
	ReplayAvailable    bool           `json:"replay_available"`
	ReplayScope        string         `json:"replay_scope,omitempty"`
	ReplayNote         string         `json:"replay_note,omitempty"`
	Tables             map[string]int `json:"tables,omitempty"`
	Operations         map[string]int `json:"operations,omitempty"`
	QuerySummary       string         `json:"query_summary,omitempty"`
	QuerySQL           string         `json:"query_sql,omitempty"`
	QueryTruncated     *bool          `json:"query_truncated,omitempty"`
	QueryOriginalBytes *int           `json:"query_original_bytes,omitempty"`
	MysqlbinlogCmd     string         `json:"mysqlbinlog_cmd,omitempty"`
	Rows               []jsonRowImage `json:"rows,omitempty"`
	RowsOmitted        int            `json:"rows_omitted,omitempty"`
}

type jsonRowImage struct {
	Schema  string   `json:"schema,omitempty"`
	Table   string   `json:"table,omitempty"`
	Op      string   `json:"op"`
	Columns []string `json:"columns"`
	Names   string   `json:"names"`
	Before  []any    `json:"before,omitempty"`
	After   []any    `json:"after,omitempty"`
	Changed []string `json:"changed,omitempty"`
}

type jsonActor struct {
	User string `json:"user,omitempty"`
	Host string `json:"host,omitempty"`
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
	DML            []string `json:"dml,omitempty"`
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
	transactionsListed := len(result.Transactions)
	transactionsOmitted := result.Summary.TotalTransactions - transactionsListed
	if transactionsOmitted < 0 {
		transactionsOmitted = 0
	}
	threads, threadsOmitted := limitThreads(result.Threads, opts.TopThreads)
	converted := jsonAnalysisResult{
		ReportVersion:       currentReportVersion,
		WorkloadID:          result.WorkloadID,
		Scope:               convertScope(result.Scope),
		Provenance:          convertProvenance(result.Provenance),
		Selection:           convertSelection(result.Selection),
		SQLContext:          jsonSQLContext{Mode: opts.SQLContextMode, Available: result.SQLContextAvailable},
		Summary:             convertSummary(result.Summary),
		Timeseries:          convertTimeseries(result.Timeseries),
		Diagnostics:         convertDiagnostics(result.Diagnostics, opts),
		Tables:              convertTables(result.Tables),
		Threads:             convertThreads(threads),
		ThreadsListed:       len(threads),
		ThreadsOmitted:      threadsOmitted,
		ThreadsRankedBy:     result.ThreadsRankedBy,
		Transactions:        convertTransactions(result.Transactions, opts, result.Diagnostics.ServerVersion),
		TransactionsListed:  transactionsListed,
		TransactionsOmitted: transactionsOmitted,
		Patterns:            convertPatterns(result.Patterns, opts.SQLContextMode),
		Minutes:             convertMinutes(result.Minutes),
		Alerts:              convertAlerts(result.Alerts),
		Warnings:            result.Warnings,
		PatternDrilldowns:   convertDrilldowns(result.PatternDrilldowns, opts.SQLContextMode),
		Snapshot:            convertSnapshot(result.Snapshot),
	}
	if rowValuesSuppressed(opts) {
		converted.RowValuesNote = i18n.T("report.text.rowValuesSuppressed")
	} else if showRowValues(opts) {
		converted.ColumnNamesNote = columnNamesNote(result)
	}
	return converted
}

func convertSelection(selection *model.AnalysisSelection) *jsonSelection {
	if selection == nil {
		return nil
	}
	return &jsonSelection{
		RequestedStartPosition: cloneJSONInt64(selection.RequestedStartPosition),
		RequestedStopPosition:  cloneJSONInt64(selection.RequestedStopPosition),
		EffectiveStartPosition: cloneJSONInt64(selection.EffectiveStartPosition),
		EffectiveStopPosition:  cloneJSONInt64(selection.EffectiveStopPosition),
		IncludeGTIDs:           copyStringSlice(selection.IncludeGTIDs),
		ExcludeGTIDs:           copyStringSlice(selection.ExcludeGTIDs),
		ResolvedGTIDFlavor:     selection.ResolvedGTIDFlavor,
		MatchedGTIDs:           copyStringSlice(selection.MatchedGTIDs),
	}
}

func cloneJSONInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func convertScope(scope *model.SnapshotFilters) *jsonSnapshotFilters {
	if scope == nil {
		return nil
	}
	converted := convertSnapshotFilters(*scope)
	return &converted
}

func convertProvenance(provenance model.ReportProvenance) *jsonProvenance {
	if len(provenance.ServerIDs) == 0 && len(provenance.ServerVersions) == 0 && len(provenance.ServerFlavors) == 0 {
		return nil
	}
	return &jsonProvenance{
		ServerIDs:      append([]uint32(nil), provenance.ServerIDs...),
		ServerVersions: copyStringSlice(provenance.ServerVersions),
		ServerFlavors:  copyStringSlice(provenance.ServerFlavors),
		MixedProducers: provenance.MixedProducers,
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

func convertDiagnostics(diagnostics model.Diagnostics, opts Options) jsonDiagnostics {
	mode := opts.SQLContextMode
	return jsonDiagnostics{
		FileCoverage:            convertFileCoverage(diagnostics.FileCoverage),
		CountedEventBytes:       diagnostics.CountedEventBytes,
		DDLEvents:               convertDDLEvents(diagnostics.DDLEvents, mode),
		LargestTransactions:     convertTransactions(diagnostics.LargestTransactions, opts, diagnostics.ServerVersion),
		LongestTransactions:     convertTransactions(diagnostics.LongestTransactions, opts, diagnostics.ServerVersion),
		WidestTransactions:      convertTransactions(diagnostics.WidestTransactions, opts, diagnostics.ServerVersion),
		LargestByteTransactions: convertOptionalTransactions(diagnostics.LargestByteTransactions, opts, diagnostics.ServerVersion),
		OpenDMLGroups:           convertOpenDMLGroups(diagnostics.OpenDMLGroups),
		DurationBuckets:         convertDurationBuckets(diagnostics.DurationBuckets),
		FileSegments:            convertFileSegments(diagnostics.FileSegments),
		HotIntervals:            convertHotIntervals(diagnostics.HotIntervals),
		Findings:                convertFindings(diagnostics.Findings),
		InputFormatGuess:        diagnostics.InputFormatGuess,
		IgnoredQueryDMLEvents:   diagnostics.IgnoredQueryDMLEvents,
		IgnoredQueryEvents:      diagnostics.IgnoredQueryEvents,
		OpenExplicitGroups:      diagnostics.OpenExplicitGroups,
		UnmappedEvents:          diagnostics.UnmappedEvents,
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

func convertDDLEvents(events []model.DDLEvent, mode SQLContextMode) []jsonDDLEvent {
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
			Statement:     ddlStatementForMode(event, mode),
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
		TotalTransactions:   s.TotalTransactions,
		PartialTransactions: s.PartialTransactions,
		UnknownTransactions: s.UnknownTransactions,
		TotalRows:           s.TotalRows,
		TotalEvents:         s.TotalEvents,
		StartTime:           formatJSONTime(s.StartTime),
		EndTime:             formatJSONTime(s.EndTime),
		Duration:            s.Duration.String(),
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

func convertOptionalTransactions(txns []model.Transaction, opts Options, serverVersion string) []jsonTransaction {
	if len(txns) == 0 {
		return nil
	}
	return convertTransactions(txns, opts, serverVersion)
}

func convertOpenDMLGroups(groups []model.OpenDMLGroup) []jsonOpenDMLGroup {
	if len(groups) == 0 {
		return nil
	}
	out := make([]jsonOpenDMLGroup, len(groups))
	for i, group := range groups {
		out[i] = jsonOpenDMLGroup{
			TxnKey:          group.TxnKey,
			GTID:            group.GTID,
			StartTime:       formatJSONTime(group.StartTime),
			EndTime:         formatJSONTime(group.EndTime),
			Duration:        group.Duration.String(),
			TotalRows:       group.TotalRows,
			Tables:          copyStringIntMap(group.Tables),
			BinlogFileStart: group.BinlogPathStart,
			BinlogFileEnd:   group.BinlogPathEnd,
			PosStart:        group.PositionStart,
			PosEnd:          group.PositionEnd,
			Note:            model.OpenDMLNote,
		}
	}
	return out
}

func convertDurationBuckets(buckets []model.DurationBucket) []jsonDurationBucket {
	if len(buckets) == 0 {
		return nil
	}
	out := make([]jsonDurationBucket, len(buckets))
	for i, bucket := range buckets {
		out[i] = jsonDurationBucket{Label: bucket.Label, TxnCount: bucket.TxnCount}
	}
	return out
}

func convertTransactions(txns []model.Transaction, opts Options, serverVersion string) []jsonTransaction {
	mode := opts.SQLContextMode
	if txns == nil {
		return []jsonTransaction{}
	}
	result := make([]jsonTransaction, len(txns))
	for i, t := range txns {
		jt := jsonTransaction{
			TxnKey:          t.TxnKey,
			XAXID:           t.XAXID,
			ServerID:        t.ServerID,
			ServerVersion:   t.ServerVersion,
			ServerFlavor:    t.ServerFlavor,
			GTID:            t.GTID,
			ThreadID:        t.ThreadID,
			XID:             t.XID,
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
			Completeness:    string(t.EffectiveCompleteness()),
			ReplayAvailable: txnReplayAvailable(t),
			Tables:          copyStringIntMap(t.Tables),
			Operations:      copyStringIntMap(t.Operations),
		}
		if t.ActorUser != "" || t.ActorHost != "" {
			jt.Actor = &jsonActor{User: t.ActorUser, Host: t.ActorHost}
		}
		if jt.ReplayAvailable {
			jt.ReplayScope = "full_transaction"
		}
		jt.ReplayNote = stdinReplayNote(t)
		switch mode {
		case SQLContextOff:
			// omit all query-related fields
		case SQLContextFull:
			jt.QuerySummary = t.QuerySummary
			if t.QueryContext != nil {
				jt.QuerySQL = model.DisplayStoredSQL(t.QueryContext.SQL, t.QueryContext.Truncated, t.QueryContext.OriginalBytes)
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
		if showRowValues(opts) {
			jt.Rows = convertRowImages(t.RowImages)
			jt.RowsOmitted = t.RowImagesOmitted
		}
		result[i] = jt
	}
	return result
}

func convertThreads(threads []model.ThreadStats) []jsonThreadStats {
	if len(threads) == 0 {
		return []jsonThreadStats{}
	}
	out := make([]jsonThreadStats, len(threads))
	for i, thread := range threads {
		item := jsonThreadStats{
			ThreadID:     thread.ThreadID,
			ServerID:     thread.ServerID,
			Schema:       thread.Schema,
			Rows:         thread.TotalRows,
			Events:       thread.EventCount,
			Transactions: thread.TxnCount,
			BinlogBytes:  thread.BinlogBytes,
			Share:        thread.Share,
			ShareOfRows:  thread.ShareOfRows,
		}
		if thread.ActorUser != "" || thread.ActorHost != "" {
			item.Actor = &jsonActor{User: thread.ActorUser, Host: thread.ActorHost}
		}
		if len(thread.Schemas) > 1 {
			item.Schemas = append([]string(nil), thread.Schemas...)
		}
		out[i] = item
	}
	return out
}

func convertPatterns(patterns []model.PatternStats, mode SQLContextMode) []jsonPatternStats {
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
		}
		if mode != SQLContextOff {
			result[i].SampleQuerySummary = p.SampleQuerySummary
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
			Minute:    formatJSONTime(m.Minute),
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
		DML:            copyStringSlice(filters.IncludeDML),
	}
}

func convertRowImages(images []model.RowImage) []jsonRowImage {
	if len(images) == 0 {
		return nil
	}
	out := make([]jsonRowImage, len(images))
	for i, image := range images {
		out[i] = jsonRowImage{
			Schema:  image.Schema,
			Table:   image.Table,
			Op:      image.Op,
			Columns: append([]string(nil), image.Columns...),
			Names:   image.Names,
			Before:  jsonCells(image.Before),
			After:   jsonCells(image.After),
			Changed: append([]string(nil), image.Changed...),
		}
	}
	return out
}

func jsonCells(cells []model.RowCell) []any {
	if cells == nil {
		return nil
	}
	out := make([]any, len(cells))
	for i, cell := range cells {
		if cell.Null {
			out[i] = nil
			continue
		}
		out[i] = cell.Text
	}
	return out
}

func convertDrilldowns(drilldowns []model.PatternDrilldown, mode SQLContextMode) []jsonPatternDrilldown {
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
			RepresentativeTransactions: convertRepresentativeTxns(d.RepresentativeTransactions, mode),
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

func convertRepresentativeTxns(txns []model.PatternRepresentativeTxn, mode SQLContextMode) []jsonRepresentativeTxn {
	if txns == nil {
		return []jsonRepresentativeTxn{}
	}
	result := make([]jsonRepresentativeTxn, len(txns))
	for i, t := range txns {
		result[i] = jsonRepresentativeTxn{TxnKey: t.TxnKey, TotalRows: t.TotalRows, Duration: t.Duration.String()}
		if mode != SQLContextOff {
			result[i].QuerySummary = t.QuerySummary
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
		result[k] = normalizeJSONTimeValue(v)
	}
	return result
}

func normalizeJSONTimeValue(value any) any {
	switch value := value.(type) {
	case time.Time:
		return formatJSONTime(value)
	case map[string]any:
		return copyStringAnyMap(value)
	case []any:
		result := make([]any, len(value))
		for i, item := range value {
			result[i] = normalizeJSONTimeValue(item)
		}
		return result
	default:
		return value
	}
}

func formatJSONTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func boolPtr(v bool) *bool {
	return &v
}

func intPtr(v int) *int {
	return &v
}
