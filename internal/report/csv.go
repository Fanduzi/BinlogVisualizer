// Package report writes one report dataset as a flat delimited table.
// input: a finished AnalysisResult, presentation options, and a csv/tsv format.
// output: one header row plus one record per dataset element on the writer,
// RFC 4180 quoting via encoding/csv (TSV = same writer with Comma '\t').
// pos: the machine-readable export surface under the analyze command (ADR 0026).
// note: if this file changes, update this header and module README.md.
package report

import (
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"binlogviz/internal/model"
)

// writeDelimited renders the dataset selected by opts.CSVDataset as CSV or TSV.
// An empty dataset still writes the header row so consumers keep a schema.
func writeDelimited(w io.Writer, result model.AnalysisResult, opts Options, format Format) error {
	cw := csv.NewWriter(w)
	if format == FormatTSV {
		cw.Comma = '\t'
	}
	header, rows := delimitedRows(result, opts)
	if err := cw.Write(header); err != nil {
		return err
	}
	for _, row := range rows {
		if err := cw.Write(row); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func delimitedRows(result model.AnalysisResult, opts Options) ([]string, [][]string) {
	switch opts.CSVDataset {
	case CSVDatasetTransactions:
		return delimitedTransactions(result, opts)
	case CSVDatasetPatterns:
		return delimitedPatterns(result, opts)
	case CSVDatasetFingerprints:
		return delimitedFingerprints(result, opts)
	case CSVDatasetTables:
		fallthrough
	default:
		return delimitedTables(result)
	}
}

var csvTablesHeader = []string{
	"schema", "table", "total_rows", "insert_rows", "update_rows",
	"update_events", "delete_rows", "txn_count", "event_count",
	"binlog_bytes", "ddl_count", "key_status",
}

func delimitedTables(result model.AnalysisResult) ([]string, [][]string) {
	rows := make([][]string, 0, len(result.Tables))
	for _, t := range result.Tables {
		rows = append(rows, []string{
			t.Schema,
			t.Table,
			strconv.Itoa(t.TotalRows),
			strconv.Itoa(t.InsertRows),
			strconv.Itoa(t.UpdateRows),
			strconv.Itoa(t.UpdateEvents),
			strconv.Itoa(t.DeleteRows),
			strconv.Itoa(t.TxnCount),
			strconv.Itoa(t.EventCount),
			strconv.FormatInt(t.BinlogBytes, 10),
			strconv.Itoa(t.DDLCount),
			tableKeyStatus(t),
		})
	}
	return csvTablesHeader, rows
}

var csvTransactionsHeader = []string{
	"txn_key", "gtid", "xa_xid", "thread_id", "xid",
	"start_time", "end_time", "duration_ms", "total_rows", "event_count",
	"binlog_bytes", "completeness", "binlog_file_start", "binlog_file_end",
	"pos_start", "pos_end", "tables", "operations", "query_summary",
}

func delimitedTransactions(result model.AnalysisResult, opts Options) ([]string, [][]string) {
	rows := make([][]string, 0, len(result.Transactions))
	for _, t := range result.Transactions {
		querySummary := ""
		if opts.SQLContextMode != SQLContextOff {
			querySummary = t.QuerySummary
		}
		rows = append(rows, []string{
			t.TxnKey,
			t.GTID,
			t.XAXID,
			strconv.FormatUint(uint64(t.ThreadID), 10),
			t.XID,
			opts.formatJSONTime(t.StartTime),
			opts.formatJSONTime(t.EndTime),
			strconv.FormatInt(t.Duration.Milliseconds(), 10),
			strconv.Itoa(t.TotalRows),
			strconv.Itoa(t.EventCount),
			strconv.FormatInt(t.BinlogBytes, 10),
			string(t.EffectiveCompleteness()),
			t.BinlogPathStart,
			t.BinlogPathEnd,
			strconv.FormatInt(t.PositionStart, 10),
			strconv.FormatInt(t.PositionEnd, 10),
			delimitedCountMap(t.Tables),
			delimitedCountMap(t.Operations),
			querySummary,
		})
	}
	return csvTransactionsHeader, rows
}

var csvPatternsHeader = []string{
	"pattern_key", "label", "total_rows", "txn_count", "event_count",
	"share_of_rows", "share_of_txns", "avg_rows_per_txn",
	"tables", "operations", "sample_query_summary",
}

func delimitedPatterns(result model.AnalysisResult, opts Options) ([]string, [][]string) {
	rows := make([][]string, 0, len(result.Patterns))
	for _, p := range result.Patterns {
		sample := ""
		if opts.SQLContextMode != SQLContextOff {
			sample = p.SampleQuerySummary
		}
		rows = append(rows, []string{
			p.PatternKey,
			p.Label,
			strconv.Itoa(p.TotalRows),
			strconv.Itoa(p.TxnCount),
			strconv.Itoa(p.EventCount),
			strconv.FormatFloat(p.ShareOfRows, 'g', -1, 64),
			strconv.FormatFloat(p.ShareOfTransactions, 'g', -1, 64),
			strconv.FormatFloat(p.AvgRowsPerTxn, 'g', -1, 64),
			delimitedCountMap(p.Tables),
			delimitedCountMap(p.Operations),
			sample,
		})
	}
	return csvPatternsHeader, rows
}

var csvFingerprintsHeader = []string{
	"fingerprint", "statements", "rows", "share_of_rows", "tables", "sample",
}

func delimitedFingerprints(result model.AnalysisResult, opts Options) ([]string, [][]string) {
	fps := limitFingerprints(result.StatementFingerprints, opts.TopFingerprints)
	rows := make([][]string, 0, len(fps))
	for _, f := range fps {
		sample := ""
		if opts.SQLContextMode != SQLContextOff {
			sample = f.Sample
		}
		rows = append(rows, []string{
			f.Fingerprint,
			strconv.Itoa(f.Statements),
			strconv.Itoa(f.Rows),
			strconv.FormatFloat(f.ShareOfRows, 'g', -1, 64),
			delimitedCountMap(f.Tables),
			sample,
		})
	}
	return csvFingerprintsHeader, rows
}

// limitFingerprints applies the --top-fingerprints bound; zero lists all
// tracked groups (the 8192-group map cap still applies upstream).
func limitFingerprints(fps []model.StatementFingerprint, limit int) []model.StatementFingerprint {
	if limit <= 0 || len(fps) <= limit {
		return fps
	}
	return fps[:limit]
}

// delimitedCountMap serializes a count map into one deterministic cell:
// name=count pairs joined by ';', names sorted.
func delimitedCountMap(counts map[string]int) string {
	if len(counts) == 0 {
		return ""
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for i, name := range names {
		if i > 0 {
			b.WriteByte(';')
		}
		fmt.Fprintf(&b, "%s=%d", name, counts[name])
	}
	return b.String()
}
