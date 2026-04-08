// Package binlogviz verifies cross-section result integrity for real analyze output.
// input: real fixture-backed analyze JSON rendered by the command path.
// output: semantic invariant checks spanning summary, tables, transactions, minutes, and warnings.
// pos: command-layer regression suite guarding result correctness beyond simple shape validation.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/report"
)

type integritySummary struct {
	TotalTransactions int    `json:"total_transactions"`
	TotalRows         int    `json:"total_rows"`
	StartTime         string `json:"start_time"`
	EndTime           string `json:"end_time"`
}

type integrityTable struct {
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	TotalRows int    `json:"total_rows"`
	TxnCount  int    `json:"txn_count"`
}

type integrityMinute struct {
	TxnCount int `json:"txn_count"`
}

type integrityReport struct {
	Summary      integritySummary  `json:"summary"`
	Tables       []integrityTable  `json:"tables"`
	Transactions []struct{}        `json:"transactions"`
	Minutes      []integrityMinute `json:"minutes"`
	Warnings     int               `json:"warnings"`
}

func TestAnalyzeJSONResultIntegrityRealFixture(t *testing.T) {
	fixture := mustFixturePath(t, "minimal.binlog")
	out, err := captureStdoutRun(t, func() error {
		return runAnalysisWithReportOptions([]string{fixture}, analyzer.DefaultOptions(), report.DefaultOptions(), "json")
	})
	if err != nil {
		t.Fatalf("runAnalysisWithReportOptions returned error: %v", err)
	}

	var got integrityReport
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}

	requireNonEmptyForwardWindow(t, got.Summary.StartTime, got.Summary.EndTime)
	requireSummaryTxnCountCoversRenderedTransactions(t, got.Summary.TotalTransactions, got.Transactions)
	requireTableRowsCoverSummaryRows(t, got.Tables, got.Summary.TotalRows)
	requirePositiveTxnCountsForNonEmptyTables(t, got.Tables)
	requireAtLeastOneMinuteTxnCount(t, got.Minutes)
	requireWarningsRoundTripCompatible(t, got.Warnings)
}

func requireNonEmptyForwardWindow(t *testing.T, start, end string) {
	t.Helper()

	if start == "" || end == "" {
		t.Fatalf("expected non-empty summary window, got start=%q end=%q", start, end)
	}

	startTime, err := time.Parse(time.RFC3339, start)
	if err != nil {
		t.Fatalf("parse start time %q: %v", start, err)
	}
	endTime, err := time.Parse(time.RFC3339, end)
	if err != nil {
		t.Fatalf("parse end time %q: %v", end, err)
	}
	if endTime.Before(startTime) {
		t.Fatalf("expected end_time >= start_time, got start=%q end=%q", start, end)
	}
}

func requireSummaryTxnCountCoversRenderedTransactions(t *testing.T, totalTransactions int, txns []struct{}) {
	t.Helper()

	if totalTransactions < len(txns) {
		t.Fatalf("summary.total_transactions=%d smaller than rendered transactions=%d", totalTransactions, len(txns))
	}
}

func requireTableRowsCoverSummaryRows(t *testing.T, tables []integrityTable, totalRows int) {
	t.Helper()

	sum := 0
	for _, table := range tables {
		sum += table.TotalRows
	}
	if sum < totalRows {
		t.Fatalf("rendered table rows=%d smaller than summary.total_rows=%d", sum, totalRows)
	}
}

func requirePositiveTxnCountsForNonEmptyTables(t *testing.T, tables []integrityTable) {
	t.Helper()

	for _, table := range tables {
		if table.TotalRows > 0 && table.TxnCount <= 0 {
			t.Fatalf("expected positive txn_count for non-empty table %s.%s, got rows=%d txn_count=%d", table.Schema, table.Table, table.TotalRows, table.TxnCount)
		}
	}
}

func requireAtLeastOneMinuteTxnCount(t *testing.T, minutes []integrityMinute) {
	t.Helper()

	for _, minute := range minutes {
		if minute.TxnCount > 0 {
			return
		}
	}
	t.Fatal("expected at least one minute bucket with txn_count > 0")
}

func requireWarningsRoundTripCompatible(t *testing.T, warnings int) {
	t.Helper()

	if warnings < 0 {
		t.Fatalf("warnings must be non-negative, got %d", warnings)
	}
}
