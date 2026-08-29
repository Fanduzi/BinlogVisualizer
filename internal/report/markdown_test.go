// Package report verifies Markdown rendering structure, placeholders, and writer behavior.
// input: synthetic AnalysisResult fixtures plus explicit SQL context presentation modes.
// output: regression coverage for Markdown incident evidence, section content, placeholder keys, and writer wrappers.
// pos: Markdown renderer regression suite guarding user-facing GitHub-flavored report output.
// note: if this file changes, update this header and module README.md.
package report

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

func forceEnglishReportLocale(t *testing.T) {
	t.Helper()
	i18n.ResetForTesting()
	if err := i18n.Init("en"); err != nil {
		t.Fatalf("init i18n: %v", err)
	}
	t.Cleanup(func() {
		i18n.ResetForTesting()
		_ = i18n.Init("en")
	})
}

func sampleMarkdownResult() model.AnalysisResult {
	return model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 12,
			TotalRows:         1200,
			TotalEvents:       48,
			StartTime:         time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC),
			EndTime:           time.Date(2026, 3, 9, 10, 15, 0, 0, time.UTC),
			Duration:          15 * time.Minute,
		},
		Tables: []model.TableStats{
			{
				Schema:     "shop|core",
				Table:      "orders|archive",
				TotalRows:  1200,
				InsertRows: 700,
				UpdateRows: 400,
				DeleteRows: 100,
				TxnCount:   4,
			},
		},
		Transactions: []model.Transaction{
			{
				TxnKey:       "txn-1",
				Completeness: model.TransactionComplete,
				TotalRows:    120,
				Duration:     5 * time.Second,
				Tables:       map[string]int{"payments": 1, "orders": 1},
				Operations:   map[string]int{"UPDATE": 1, "INSERT": 1},
				QuerySummary: "UPDATE orders SET status = ? WHERE id = ?",
				QueryContext: model.NewQueryContext("UPDATE orders SET status = 'paid' WHERE id = 42"),
			},
		},
		Minutes: []model.MinuteBucket{
			{
				Minute:    time.Date(2026, 3, 9, 10, 5, 0, 0, time.UTC),
				TotalRows: 500,
				TxnCount:  3,
			},
		},
		Alerts: []model.Alert{
			{Severity: "critical", Message: "table drift on shop|core.orders|archive"},
		},
	}
}

type failingWriter struct{}

func (failingWriter) Write(_ []byte) (int, error) {
	return 0, errors.New("write failed")
}

func captureStdoutMarkdown(t *testing.T, fn func() error) (string, error) {
	t.Helper()

	originalStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("create pipe: %v", err)
	}
	os.Stdout = w
	defer func() {
		os.Stdout = originalStdout
	}()

	runErr := fn()
	if err := w.Close(); err != nil {
		t.Fatalf("close writer pipe: %v", err)
	}

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatalf("read stdout pipe: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close reader pipe: %v", err)
	}
	return buf.String(), runErr
}

func TestRenderMarkdownIncludesStructuredSectionsAndEscaping(t *testing.T) {
	forceEnglishReportLocale(t)

	out, err := RenderMarkdown(sampleMarkdownResult())
	if err != nil {
		t.Fatalf("RenderMarkdown returned error: %v", err)
	}

	expectedSnippets := []string{
		"# BinlogViz Report",
		"## Workload Summary",
		"| Total Transactions | 12 |",
		"| shop\\|core | orders\\|archive | 1,200 | 700 | 400 | 100 | 4 |",
		"| 1 | txn-1 | complete | no | 120 | 0 | 5.0s | N/A | orders, payments | INSERT, UPDATE |",
		"> `UPDATE orders SET status = ? WHERE id = ?`",
		"| 2026-03-09 10:05:00 | 500 | 3 |",
		"| critical | N/A | table drift on shop\\|core.orders\\|archive | N/A | N/A | N/A |",
	}
	for _, snippet := range expectedSnippets {
		if !strings.Contains(out, snippet) {
			t.Fatalf("expected snippet %q in markdown output:\n%s", snippet, out)
		}
	}
}

func TestRenderMarkdownIncludesReplayableTransactionEvidence(t *testing.T) {
	forceEnglishReportLocale(t)

	result := model.AnalysisResult{
		Transactions: []model.Transaction{withFullReplaySpan(model.Transaction{
			TxnKey:          "txn-real",
			TotalRows:       400,
			Duration:        5 * time.Second,
			BinlogBytes:     77914563,
			BinlogPathStart: "/data/mysql/mysql-bin.000008",
			BinlogPathEnd:   "/data/mysql/mysql-bin.000008",
			PositionStart:   385,
			PositionEnd:     77914948,
			Tables:          map[string]int{"orders": 400},
			Operations:      map[string]int{"UPDATE": 400},
		})},
		Diagnostics: model.Diagnostics{ServerVersion: "11.4.2-MariaDB-log"},
	}

	out, err := RenderMarkdown(result)
	if err != nil {
		t.Fatalf("RenderMarkdown returned error: %v", err)
	}

	for _, snippet := range []string{
		"| # | Txn Key | Completeness | Full Replay | Rows | Bytes | Duration | File:Position | Tables | Operations |",
		"| 1 | txn-real | complete | yes | 400 | 77,914,563 | 5.0s | /data/mysql/mysql-bin.000008:385-77914948 | orders | UPDATE |",
		"`mysqlbinlog_cmd`",
		"mariadb-binlog --base64-output=DECODE-ROWS -v --start-position=385 --stop-position=77914948 /data/mysql/mysql-bin.000008",
	} {
		if !strings.Contains(out, snippet) {
			t.Fatalf("expected snippet %q in markdown output:\n%s", snippet, out)
		}
	}
}

func TestRenderMarkdownKeepsTransactionRowsTogether(t *testing.T) {
	forceEnglishReportLocale(t)

	result := model.AnalysisResult{
		Transactions: []model.Transaction{
			withFullReplaySpan(model.Transaction{
				TxnKey:          "txn-one",
				TotalRows:       10,
				BinlogBytes:     1000,
				BinlogPathStart: "/data/mysql/mysql-bin.000008",
				BinlogPathEnd:   "/data/mysql/mysql-bin.000008",
				PositionStart:   100,
				PositionEnd:     200,
			}),
			withFullReplaySpan(model.Transaction{
				TxnKey:          "txn-two",
				TotalRows:       20,
				BinlogBytes:     2000,
				BinlogPathStart: "/data/mysql/mysql-bin.000008",
				BinlogPathEnd:   "/data/mysql/mysql-bin.000008",
				PositionStart:   300,
				PositionEnd:     400,
			}),
		},
	}

	out, err := RenderMarkdown(result)
	if err != nil {
		t.Fatalf("RenderMarkdown returned error: %v", err)
	}
	secondRow := strings.Index(out, "| 2 | txn-two |")
	firstReplayCommand := strings.Index(out, "```text")
	if secondRow < 0 || firstReplayCommand < 0 || secondRow > firstReplayCommand {
		t.Fatalf("all transaction rows must precede replay command blocks:\n%s", out)
	}
	if strings.Count(out, "`mysqlbinlog_cmd`") != 2 {
		t.Fatalf("expected one replay command per usable transaction:\n%s", out)
	}
}

func TestRenderMarkdownPreservesLargeTransactionByteCounts(t *testing.T) {
	forceEnglishReportLocale(t)

	out, err := RenderMarkdown(model.AnalysisResult{
		Transactions: []model.Transaction{{TxnKey: "txn-large", Completeness: model.TransactionComplete, BinlogBytes: 3_000_000_000}},
	})
	if err != nil {
		t.Fatalf("RenderMarkdown returned error: %v", err)
	}
	if !strings.Contains(out, "| 1 | txn-large | complete | no | 0 | 3,000,000,000 | 0s | N/A |  |  |") {
		t.Fatalf("expected the full int64 byte count in Markdown:\n%s", out)
	}
}

func TestRenderMarkdownIncludesIncidentDiagnosticsAndDegradesMissingSpans(t *testing.T) {
	forceEnglishReportLocale(t)

	result := model.AnalysisResult{
		Transactions: []model.Transaction{{
			TxnKey:       "txn-missing",
			Completeness: model.TransactionComplete,
			TotalRows:    2,
		}},
		Diagnostics: model.Diagnostics{
			InputFormatGuess:      "MIXED",
			IgnoredQueryDMLEvents: 2,
			DDLEvents: []model.DDLEvent{{
				Timestamp:     time.Date(2026, 3, 9, 10, 5, 0, 0, time.UTC),
				Operation:     "ALTER TABLE",
				Schema:        "shop|core",
				Table:         "orders",
				Statement:     "ALTER TABLE shop|core.orders ADD COLUMN note TEXT",
				BinlogPath:    "mysql-bin.000123",
				PositionStart: 100,
				PositionEnd:   200,
			}},
			Findings: []model.Finding{{
				Kind:         "large_transaction",
				Severity:     "warning",
				Message:      `large\|transaction detected`,
				TxnKey:       "txn-missing",
				Minute:       time.Date(2026, 3, 9, 10, 6, 0, 0, time.UTC),
				EvidenceRefs: []string{"transactions:txn-missing", "rows|2"},
			}},
		},
		Alerts: []model.Alert{{
			Type:     "input_format",
			Severity: "warning",
			Message:  "MIXED: counted 2 ROW images, ignored 2 Query-DML events",
		}},
	}

	out, err := RenderMarkdown(result)
	if err != nil {
		t.Fatalf("RenderMarkdown returned error: %v", err)
	}

	for _, snippet := range []string{
		"| Format | MIXED |",
		"| Ignored Query-DML Events | 2 |",
		"## DDL Timeline",
		"| 2026-03-09 10:05:00 | ALTER TABLE | shop\\|core.orders | ALTER TABLE shop\\|core.orders ADD COLUMN note TEXT | mysql-bin.000123:100-200 |",
		"## Findings",
		`| warning | large_transaction | large\\\|transaction detected | txn-missing | 2026-03-09 10:06:00 | transactions:txn-missing, rows\|2 |`,
		"| warning | input_format | MIXED: counted 2 ROW images, ignored 2 Query-DML events | N/A | N/A | N/A |",
		"| 1 | txn-missing | complete | no | 2 | 0 | 0s | N/A |  |  |",
	} {
		if !strings.Contains(out, snippet) {
			t.Fatalf("expected snippet %q in markdown output:\n%s", snippet, out)
		}
	}
	if strings.Contains(out, "mysqlbinlog_cmd") || strings.Contains(out, "--start-position") {
		t.Fatalf("missing span must not emit a replay command:\n%s", out)
	}
}

func TestRenderMarkdownTopTablesReportsOmittedTables(t *testing.T) {
	result := sampleMarkdownResult()
	result.Tables = []model.TableStats{
		{Schema: "shop", Table: "orders", TotalRows: 4},
		{Schema: "shop", Table: "payments", TotalRows: 2},
	}
	result.Transactions = nil
	result.Alerts = nil

	out, err := RenderMarkdownWithOptions(result, Options{TopN: 1})
	if err != nil {
		t.Fatalf("RenderMarkdownWithOptions returned error: %v", err)
	}
	if !strings.Contains(out, "| shop | orders |") || strings.Contains(out, "| shop | payments |") {
		t.Fatalf("expected only the top table to be displayed\n%s", out)
	}
	if !strings.Contains(out, "… and 1 more tables") {
		t.Fatalf("expected omitted-table count\n%s", out)
	}
}

func TestRenderMarkdownEmptySectionsUseStablePlaceholders(t *testing.T) {
	forceEnglishReportLocale(t)

	out, err := RenderMarkdown(model.AnalysisResult{})
	if err != nil {
		t.Fatalf("RenderMarkdown returned error: %v", err)
	}

	expectedSnippets := []string{
		"## Workload Summary",
		"_(no table activity)_",
		"_(no transactions)_",
		"_(no minute activity)_",
	}
	for _, snippet := range expectedSnippets {
		if !strings.Contains(out, snippet) {
			t.Fatalf("expected snippet %q in markdown output:\n%s", snippet, out)
		}
	}
	if strings.Contains(out, "## Alerts") {
		t.Fatalf("expected empty alerts to omit alerts section, got:\n%s", out)
	}
}

func TestRenderMarkdownRespectsSQLContextMode(t *testing.T) {
	forceEnglishReportLocale(t)

	result := model.AnalysisResult{
		Transactions: []model.Transaction{sampleTransactionWithQueryContext()},
	}

	summaryOut, err := RenderMarkdownWithOptions(result, Options{SQLContextMode: SQLContextSummary})
	if err != nil {
		t.Fatalf("summary mode error: %v", err)
	}
	if !strings.Contains(summaryOut, "> `UPDATE orders SET status = ? WHERE id = ?`") {
		t.Fatalf("summary mode should render query summary, got:\n%s", summaryOut)
	}

	offOut, err := RenderMarkdownWithOptions(result, Options{SQLContextMode: SQLContextOff})
	if err != nil {
		t.Fatalf("off mode error: %v", err)
	}
	if strings.Contains(offOut, "UPDATE orders SET status = ? WHERE id = ?") {
		t.Fatalf("off mode should omit query summary, got:\n%s", offOut)
	}
}

func TestRenderMarkdownWriterHelpers(t *testing.T) {
	forceEnglishReportLocale(t)

	result := sampleMarkdownResult()

	var buf bytes.Buffer
	if err := RenderMarkdownToWithOptions(result, &buf, Options{SQLContextMode: SQLContextSummary}); err != nil {
		t.Fatalf("RenderMarkdownToWithOptions returned error: %v", err)
	}
	if !strings.Contains(buf.String(), "# BinlogViz Report") {
		t.Fatalf("expected markdown to be written to buffer, got:\n%s", buf.String())
	}

	if err := RenderMarkdownTo(result, failingWriter{}); err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Fatalf("expected write error from RenderMarkdownTo, got: %v", err)
	}

	stdoutOut, err := captureStdoutMarkdown(t, func() error {
		return RenderMarkdownToStdoutWithOptions(result, Options{SQLContextMode: SQLContextSummary})
	})
	if err != nil {
		t.Fatalf("RenderMarkdownToStdoutWithOptions returned error: %v", err)
	}
	if !strings.Contains(stdoutOut, "# BinlogViz Report") {
		t.Fatalf("expected markdown on stdout, got:\n%s", stdoutOut)
	}
}
