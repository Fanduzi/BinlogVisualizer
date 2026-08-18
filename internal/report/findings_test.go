// Package report verifies text and HTML share the same analyze findings list.
// input: synthetic AnalysisResult values with empty alerts plus a hot-interval spike.
// output: assertions that HTML verdict/findings match the text Top Findings lines.
// pos: regression coverage for the empty-alerts-vs-synthesized-spike mismatch.
// note: if this file changes, keep internal/report/README.md synchronized.
package report

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestCollectDisplayFindingsMatchesTextTopFindings(t *testing.T) {
	result := productTextFixture()
	if len(result.Alerts) != 0 {
		t.Fatal("fixture must keep alerts empty so the synthesized spike path is exercised")
	}

	textOut, err := RenderText(result)
	if err != nil {
		t.Fatalf("RenderText() error: %v", err)
	}
	findings := collectDisplayFindings(result, DefaultTopN)
	if len(findings) == 0 {
		t.Fatal("expected synthesized findings from hot interval / txn / DDL")
	}
	if findings[0].Severity != "critical" || !strings.Contains(findings[0].Message, "Write spike") {
		t.Fatalf("expected first finding to be a critical write spike, got %+v", findings[0])
	}
	for _, finding := range findings {
		line := fmt.Sprintf("[%s] %s", finding.Severity, finding.Message)
		if !strings.Contains(textOut, line) {
			t.Fatalf("text report missing finding %q\n%s", line, textOut)
		}
	}
}

func TestRenderHTMLFindingsParityWithText(t *testing.T) {
	result := productTextFixture()

	textOut, err := RenderText(result)
	if err != nil {
		t.Fatalf("RenderText() error: %v", err)
	}
	htmlOut, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}

	if strings.Contains(htmlOut, "workload looks healthy") {
		t.Fatalf("HTML called an empty alerts list healthy while text has findings\n%s", htmlOut)
	}

	findings := collectDisplayFindings(result, DefaultTopN)
	if len(findings) == 0 {
		t.Fatal("expected findings")
	}
	verdictIdx := strings.Index(htmlOut, `id="incident-verdict"`)
	if verdictIdx < 0 {
		t.Fatal("expected incident verdict")
	}
	if !strings.Contains(htmlOut[verdictIdx:], findings[0].Message) {
		t.Fatalf("HTML verdict missing first text finding %q", findings[0].Message)
	}
	for _, finding := range findings {
		if !strings.Contains(htmlOut, finding.Message) {
			t.Fatalf("HTML missing text finding %q", finding.Message)
		}
		line := fmt.Sprintf("[%s] %s", finding.Severity, finding.Message)
		if !strings.Contains(textOut, line) {
			t.Fatalf("text missing finding %q\n%s", line, textOut)
		}
	}
}

func TestRenderHTMLIncidentFirstShowsPeakTableTxnBeforeTheme(t *testing.T) {
	out, err := RenderHTML(productHTMLFixture())
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}

	verdictIdx := strings.Index(out, `id="incident-verdict"`)
	peakIdx := strings.Index(out, `id="peak-minute"`)
	tableIdx := strings.Index(out, `id="hottest-table"`)
	txnIdx := strings.Index(out, `id="largest-txn"`)
	themeIdx := strings.Index(out, `class="theme-switcher"`)
	tpsIdx := strings.Index(out, `id="chart-tps"`)
	if verdictIdx < 0 || peakIdx < 0 || tableIdx < 0 || txnIdx < 0 || themeIdx < 0 || tpsIdx < 0 {
		t.Fatalf("expected incident markers in HTML")
	}
	if !(verdictIdx < peakIdx && peakIdx < tableIdx && tableIdx < txnIdx && txnIdx < themeIdx && txnIdx < tpsIdx) {
		t.Fatalf("expected verdict/peak/table/txn before theme switcher and TPS chart, got verdict=%d peak=%d table=%d txn=%d theme=%d tps=%d",
			verdictIdx, peakIdx, tableIdx, txnIdx, themeIdx, tpsIdx)
	}
	for _, token := range []string{
		"shop.orders · 8,000",
		"mysql-bin.000044:12345-23456",
		"UPDATE shop.orders SET status",
		"vs window median",
	} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected incident token %q", token)
		}
	}
}

func TestRenderHTMLEmptyFindingsDoesNotClaimHealthy(t *testing.T) {
	out, err := RenderHTML(model.AnalysisResult{})
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}
	if strings.Contains(out, "workload looks healthy") || strings.Contains(out, "负载看起来健康") {
		t.Fatalf("empty HTML report still claims the workload is healthy\n%s", out)
	}
	if !strings.Contains(out, "No high-signal findings detected.") {
		t.Fatalf("expected shared no-findings copy, got HTML without it")
	}
}

func TestRenderHTMLUsesAlertSpikeBaselineWhenPresent(t *testing.T) {
	result := productTextFixture()
	peak := result.Diagnostics.HotIntervals[0].Minute
	result.Alerts = []model.Alert{{
		Type:     "spike",
		Severity: "warning",
		Minute:   peak,
		Message:  "Write spike detected",
		Details: map[string]any{
			"rows":     9000,
			"baseline": 1200,
			"factor":   7.5,
		},
	}}

	out, err := RenderHTML(result)
	if err != nil {
		t.Fatalf("RenderHTML() error: %v", err)
	}
	if !strings.Contains(out, "vs spike baseline") || !strings.Contains(out, "1,200") {
		t.Fatalf("expected analyzer spike baseline to be surfaced\n%s", out)
	}
}

func TestRenderHTMLScreenshotLikeTinyEmptyAlertsIsNotHealthy(t *testing.T) {
	// Mirrors the v0.21.0 / live MariaDB first screens: tiny window, empty alerts.
	start := time.Date(2026, 3, 15, 14, 10, 26, 0, time.UTC)
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 4,
			TotalRows:         5,
			TotalEvents:       16,
			StartTime:         start,
			EndTime:           start,
		},
		Tables: []model.TableStats{{
			Schema: "shop", Table: "orders", TotalRows: 5, InsertRows: 3, UpdateRows: 2, TxnCount: 4,
		}},
		Diagnostics: model.Diagnostics{
			HotIntervals: []model.MinuteBucket{{
				Minute: start, TotalRows: 5, TxnCount: 4,
				TableRows: map[string]int{"shop.orders": 5},
			}},
			LargestTransactions: []model.Transaction{{
				TxnKey:          "txn-1",
				TotalRows:       2,
				BinlogPathStart: "mysql-bin.000123",
				PositionStart:   240,
				PositionEnd:     480,
			}},
		},
	}

	textOut, err := RenderText(result)
	if err != nil {
		t.Fatal(err)
	}
	htmlOut, err := RenderHTML(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(textOut, "[critical] Write spike at 2026-03-15 14:10: rows=5, txns=4") {
		t.Fatalf("expected text critical spike\n%s", textOut)
	}
	if strings.Contains(htmlOut, "workload looks healthy") {
		t.Fatal("screenshot-like tiny report still shows the old healthy first screen")
	}
	if !strings.Contains(htmlOut, "Write spike at 2026-03-15 14:10: rows=5, txns=4") {
		t.Fatalf("HTML verdict missing the text spike\n%s", htmlOut)
	}
	for _, token := range []string{`id="incident-verdict"`, `id="peak-minute"`, `id="hottest-table"`, `id="largest-txn"`, "shop.orders"} {
		if !strings.Contains(htmlOut, token) {
			t.Fatalf("expected first-screen token %q", token)
		}
	}
	if strings.Index(htmlOut, `id="incident-verdict"`) > strings.Index(htmlOut, `class="theme-switcher"`) {
		t.Fatal("theme switcher still precedes the incident verdict")
	}
}

func TestCollectDisplayFindingsRespectsTopN(t *testing.T) {
	result := productTextFixture()
	got := collectDisplayFindings(result, 1)
	if len(got) != 1 {
		t.Fatalf("expected 1 finding, got %d", len(got))
	}
	if got[0].Severity != "critical" {
		t.Fatalf("expected critical spike first, got %+v", got[0])
	}
}
