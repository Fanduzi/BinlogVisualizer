// Package report renders self-contained HTML reports from complete analysis results.
// input: analyzer-produced AnalysisResult values plus optional SQL context presentation controls.
// output: self-contained HTML with UTC-labelled timestamps (a minute chart crossing midnight prefixes the date to every label), completeness, a Top Threads session ranking, deduplicated transaction evidence, bounded transaction lookup, per-transaction server_id, thread_id, GTID, xid or XA xid, and user@host only when present, query text only when --sql-context allows it, selected-file/count-event bytes, hot intervals naming the tables that produced each minute's rows, a DDL card that counts statements and notes the target count when those differ, replica apply delay after activity, and labelled trusted full-transaction replay commands.
// pos: HTML renderer for the CLI output path after analyzer Finalize. Transaction lookup and evidence come from internal/report/view.
// note: if this file changes, update this header and module README.md.
package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"sort"
	"strings"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/report/view"
)

// RenderHTML renders an AnalysisResult as a self-contained HTML report.
func RenderHTML(result model.AnalysisResult) (string, error) {
	return RenderHTMLWithOptions(result, DefaultOptions())
}

// RenderHTMLWithOptions renders an AnalysisResult as HTML with explicit presentation controls.
func RenderHTMLWithOptions(result model.AnalysisResult, opts Options) (string, error) {
	opts = normalizeOptions(opts)
	return renderHTML(result, opts, newView(result, opts))
}

func renderHTML(result model.AnalysisResult, opts Options, built view.View) (string, error) {
	echartJS, err := ReadEmbeddedECharts()
	if err != nil {
		return "", err
	}

	tmpl, err := NewHTMLTemplate("report", htmlReportTemplate, template.FuncMap{
		"safeJS":     func(s string) template.JS { return template.JS(s) }, //nolint:gosec // template helper; no report template calls it with input today
		"json":       jsonMarshal,
		"mulFloat":   func(f float64, m float64) float64 { return f * m },
		"fmtIntHTML": fmtIntHTML,
	})
	if err != nil {
		return "", err
	}

	data := buildHTMLData(result, opts, echartJS, built)

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
	return template.JS(b), nil //nolint:gosec // json.Marshal escapes <, > and &, so it cannot close the script tag
}

type htmlReportData struct {
	GeneratedAt            string
	TimestampBasis         string
	SourceFiles            string
	StartTime              string
	EndTime                string
	Duration               string
	TotalTxns              int
	PartialTxns            int
	UnknownTxns            int
	TotalRows              int
	TotalEvents            int
	DDLCount               int
	DDLNote                string
	InputFileSize          string
	HasInputFileSize       bool
	CountedEventBytes      string
	Transactions           []htmlTransactionLookup
	HasTransactions        bool
	TransactionEvidence    []htmlTxnDiagnostic
	HasTransactionEvidence bool
	Tables                 []htmlTableRow
	OmittedTables          string
	ThreadsTitle           string
	Threads                []htmlThreadRow
	HasThreads             bool
	OmittedThreads         string
	ThreadShowID           bool
	ThreadShowServer       bool
	ThreadShowActor        bool
	ThreadShowSchema       bool
	ThreadShowRows         bool
	ThreadShowEvents       bool
	ThreadShowBytes        bool
	ThreadShowTxns         bool
	HasHotRows             bool
	HotRowsLead            string
	HotRows                []htmlHotRow
	HotRowsHidden          string
	HotRowsOmitted         string
	HotRowUnavailable      []string
	HotRowsNote            string
	TableActivitySeries    template.JS
	DDLEvents              []htmlDDLEvent
	HasDDLEvents           bool
	HotIntervals           []htmlHotInterval
	HasHotIntervals        bool
	FileCoverage           htmlFileCoverageData
	HasFileCoverage        bool
	FileSegments           []htmlFileSegment
	HasFileSegments        bool
	ThroughputLabels       template.JS
	ThroughputBytes        template.JS
	ThroughputRows         template.JS
	TPSLabels              template.JS
	TPSValues              template.JS
	Alerts                 []htmlAlert
	HasAlerts              bool
	TopAlerts              []htmlAlert
	Drilldowns             []htmlDrilldown
	HasDrilldowns          bool
	MinuteLabels           template.JS
	MinuteRows             template.JS
	MinuteTxns             template.JS
	TableBarNames          template.JS
	TableBarRows           template.JS
	OpsPie                 template.JS
	EChartsJS              template.JS
	TopN                   int
	DMLFilter              string
	ColumnNamesNote        string
	RowValuesNote          string
	PrimaryKeyNote         string
	NoPKTables             []htmlNoPKRow
	NoPKInsertOnly         string
	ApplyDelayUnavailable  bool
	ApplyDelaySource       bool
	ApplyDelayStats        string
	ApplyDelayTxns         []htmlApplyDelayTxn
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

type htmlHotRow struct {
	Rank         int
	Table        string
	Key          string
	Touches      int
	Transactions int
	First        string
	Last         string
	FirstTxn     string
	LastTxn      string
	Approximate  bool
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

type htmlNoPKRow struct {
	Rank   int
	Name   string
	Update int
	Delete int
}

type htmlAlert struct {
	Severity string
	Message  string
	Badge    string
}

type htmlDDLEvent struct {
	Timestamp   string
	Operation   string
	Object      string
	Statement   string
	Location    string
	Identity    string
	TxnStart    string
	Explain     string
	Mysqlbinlog string
	StopGTID    string
}

type htmlTxnDiagnostic struct {
	TxnKey               string
	Reasons              []string
	Rows                 int
	Events               int
	Duration             string
	BinlogBytes          int
	BinlogBytesFormatted string
	Tables               []htmlTxnTable
	Location             string
	QuerySummary         string
	MysqlbinlogCmd       string
	ReplayNote           string
	Identity             string
	RowText              string
}

type htmlThreadRow struct {
	Rank     int
	ThreadID string
	ServerID string
	Actor    string
	Schema   string
	Rows     int
	Events   int
	Bytes    string
	Txns     int
	Share    string
}

type htmlTransactionLookup struct {
	TxnKey   string
	Rows     int
	Duration string
	Identity string
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
	Tables               []htmlTxnTable
	OmittedTablesLabel   string
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

func buildHTMLTransactionEvidence(evidence []view.Evidence, serverVersion string, opts Options) []htmlTxnDiagnostic {
	mode := opts.SQLContextMode
	out := make([]htmlTxnDiagnostic, 0, len(evidence))
	for _, item := range evidence {
		diagnostic := buildHTMLTxnDiagnostic(item.Txn, serverVersion, mode)
		diagnostic.RowText = strings.TrimPrefix(formatRowImageBlock(item.Txn, opts), "    ")
		diagnostic.Reasons = make([]string, len(item.Reasons))
		for i, reason := range item.Reasons {
			diagnostic.Reasons[i] = evidenceReasonLabel(reason)
		}
		out = append(out, diagnostic)
	}
	return out
}

func evidenceReasonLabel(reason view.Reason) string {
	switch reason {
	case view.ReasonLongest:
		return i18n.T("report.html.analyze.longestTransactionsByDuration")
	case view.ReasonWidest:
		return i18n.T("report.html.analyze.widestTransactionsByTouchedTables")
	default:
		return i18n.T("report.html.analyze.largestTransactionsByRows")
	}
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

func buildHTMLTxnDiagnostic(txn model.Transaction, serverVersion string, mode SQLContextMode) htmlTxnDiagnostic {
	return htmlTxnDiagnostic{
		TxnKey:               txn.TxnKey,
		Rows:                 txn.TotalRows,
		Events:               txn.EventCount,
		Duration:             txn.Duration.String(),
		BinlogBytes:          int(txn.BinlogBytes),
		BinlogBytesFormatted: formatFileSize(txn.BinlogBytes),
		Tables:               sortedTxnTables(txn.Tables),
		Location:             formatBinlogSpan(txn),
		QuerySummary:         transactionTextQuery(txn, mode),
		MysqlbinlogCmd:       mysqlbinlogCmd(txn, serverVersion),
		ReplayNote:           stdinReplayNote(txn),
		Identity:             formatTxnIdentity(txn),
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
