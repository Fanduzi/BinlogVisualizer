// Package report verifies Markdown rendering structure, placeholders, and writer behavior.
// input: synthetic AnalysisResult fixtures plus explicit SQL context presentation modes.
// output: regression coverage for Markdown section content, placeholder keys, and writer wrappers.
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
		"| 1 | 120 | 5.0s | orders, payments | INSERT, UPDATE |",
		"> `UPDATE orders SET status = ? WHERE id = ?`",
		"| 2026-03-09 10:05:00 | 500 | 3 |",
		"> **[CRITICAL]** table drift on shop\\|core.orders\\|archive",
	}
	for _, snippet := range expectedSnippets {
		if !strings.Contains(out, snippet) {
			t.Fatalf("expected snippet %q in markdown output:\n%s", snippet, out)
		}
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
