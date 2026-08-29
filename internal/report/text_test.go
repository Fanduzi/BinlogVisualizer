// Package report verifies incident-brief text rendering and opt-in detail sections.
// input: synthetic AnalysisResult fixtures with summary, table, minute, pattern, and diagnostic evidence.
// output: regression coverage for default diagnostic sections, byte coverage, table limits, detail flags, and finding/alert-backed suspicious positions.
// pos: text renderer regression suite guarding user-facing CLI report formatting.
// note: if this file changes, update this header and module README.md.
package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func sampleTransactionWithQueryContext() model.Transaction {
	return model.Transaction{
		TxnKey:       "txn-1",
		TotalRows:    100,
		EventCount:   2,
		Duration:     5 * time.Second,
		QuerySummary: "UPDATE orders SET status = ? WHERE id = ?",
		QueryContext: model.NewQueryContext("UPDATE orders SET status = 'paid' WHERE id = 42"),
	}
}

func TestRenderTextTopFindingsComeFromAlertsNotHotIntervals(t *testing.T) {
	result := productTextFixture()
	result.Alerts = nil
	result.Diagnostics.Findings = nil
	result.Diagnostics.HotIntervals = []model.MinuteBucket{{
		Minute:    time.Date(2026, 3, 15, 14, 10, 0, 0, time.UTC),
		TotalRows: 5,
		TxnCount:  4,
	}}
	result.Diagnostics.LongestTransactions = []model.Transaction{{
		TxnKey:    "txn-tiny",
		TotalRows: 2,
		Duration:  0,
		Tables:    map[string]int{"testdb.users": 2},
	}}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out, "[critical] Write spike") {
		t.Fatalf("text findings synthesized a critical spike from a 5-row hot interval:\n%s", out)
	}
	if strings.Contains(out, "[warning] Longest transaction") {
		t.Fatalf("text findings synthesized a warning from a 0s/2-row transaction:\n%s", out)
	}
	if !strings.Contains(out, "No high-signal findings detected.") {
		t.Fatalf("expected empty alerts/findings to render no findings:\n%s", out)
	}
}

func TestRenderTextCleanReportOmitsFirstSuspiciousPosition(t *testing.T) {
	forceEnglishReportLocale(t)
	result := productTextFixture()
	result.Alerts = nil
	result.Diagnostics.Findings = nil

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out, "First suspicious position") {
		t.Fatalf("clean report must not label transaction evidence as suspicious:\n%s", out)
	}
	if !strings.Contains(out, "Largest transaction: txn-largest") {
		t.Fatalf("clean report must retain normal largest transaction evidence:\n%s", out)
	}
}

func TestRenderTextUsesTransactionBackedFindingLocation(t *testing.T) {
	forceEnglishReportLocale(t)
	result := productTextFixture()
	result.Alerts = nil
	result.Diagnostics.Findings = []model.Finding{{
		Kind:   "large_transaction",
		TxnKey: "txn-largest",
	}}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "First suspicious position: mysql-bin.000044:210") {
		t.Fatalf("expected suspicious position from the finding's transaction:\n%s", out)
	}
}

func TestRenderTextUsesTransactionBackedAlertLocation(t *testing.T) {
	forceEnglishReportLocale(t)
	result := productTextFixture()
	result.Diagnostics.Findings = nil
	result.Alerts = []model.Alert{{
		Type:   "large_transaction",
		TxnKey: "txn-largest",
	}}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "First suspicious position: mysql-bin.000044:210") {
		t.Fatalf("expected suspicious position from the alert's transaction:\n%s", out)
	}
}

func TestRenderTextTopFindingsUseSameAlertsAsJSON(t *testing.T) {
	result := productTextFixture()
	result.Diagnostics.Findings = []model.Finding{{
		Kind:     "spike",
		Severity: "warning",
		Message:  "Write spike detected",
	}}
	result.Alerts = []model.Alert{{
		Type:     "spike",
		Severity: "warning",
		Message:  "Write spike detected",
	}}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "[warning] Write spike detected") {
		t.Fatalf("expected text findings to use the JSON alert/finding message:\n%s", out)
	}
	if strings.Contains(out, "[critical] Write spike") {
		t.Fatalf("text findings should not invent a critical severity:\n%s", out)
	}
}

func TestRenderTextIncidentBriefPutsTablesAndTxnsBeforeFindings(t *testing.T) {
	result := productTextFixture()
	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tables := strings.Index(out, "=== Top Tables ===")
	txns := strings.Index(out, "=== Top Transactions ===")
	findings := strings.Index(out, "=== Top Findings ===")
	if tables < 0 || txns < 0 || findings < 0 {
		t.Fatalf("missing incident-brief sections\n%s", out)
	}
	if !(tables < txns && txns < findings) {
		t.Fatalf("expected tables then txns then findings, got tables=%d txns=%d findings=%d\n%s", tables, txns, findings, out)
	}
	if !strings.Contains(out, "ROW images / logical rows") {
		t.Fatalf("expected ROW/logical-row capability line in summary\n%s", out)
	}
}

func TestRenderTextDefaultIsConciseDiagnosticSummary(t *testing.T) {
	result := productTextFixture()

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{"Summary", "Top Findings", "Activity", "Top Tables", "Top Transactions", "Next Actions"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected default text report to contain %q\n%s", token, out)
		}
	}
	for _, hidden := range []string{"Minute Activity", "Top Patterns", "2026-04-17 09:01: 9000 rows", "Write Shape Patterns"} {
		if strings.Contains(out, hidden) {
			t.Fatalf("default text report should hide %q\n%s", hidden, out)
		}
	}
}

func TestRenderTextDistinguishesInputFileSizeFromCountedEventBytes(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			FileCoverage: model.FileCoverage{
				Selected: []model.FileCoverageItem{
					{BinlogPath: "mysql-bin.000001", Size: 1000},
					{BinlogPath: "mysql-bin.000002", Size: 2000},
				},
			},
			CountedEventBytes: 250,
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{
		"Input File Size: 2.9KB",
		"Counted Event Bytes: 250B",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected text output to contain %q, got:\n%s", want, out)
		}
	}
}

func TestRenderTextShowsUnavailableWhenSelectedFileSizeIsMissing(t *testing.T) {
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			FileCoverage: model.FileCoverage{
				Selected: []model.FileCoverageItem{{BinlogPath: "legacy.binlog"}},
			},
			CountedEventBytes: 250,
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "Input File Size: N/A") {
		t.Fatalf("expected missing file size to be unavailable, got:\n%s", out)
	}
	if strings.Contains(out, "Input File Size: 0B") {
		t.Fatalf("missing file size must not render as zero, got:\n%s", out)
	}
}

func TestRenderTextTopTablesUsesAlignedTableAndTopLimit(t *testing.T) {
	result := productTextFixture()
	result.Tables = []model.TableStats{
		{Schema: "epct", Table: "enter_dealer_contract_base_quarter", TotalRows: 2744948, TxnCount: 1486, EventCount: 128304},
		{Schema: "shop", Table: "users", TotalRows: 1000, TxnCount: 20, EventCount: 80},
	}
	out, err := RenderTextWithOptions(result, Options{TopN: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "#  Table") || !strings.Contains(out, "Affected Rows") || !strings.Contains(out, "Row Share") {
		t.Fatalf("expected explicit top tables header\n%s", out)
	}
	if !strings.Contains(out, "DML shares are of affected rows; DDL share is of binlog events.") {
		t.Fatalf("expected top tables metric explanation\n%s", out)
	}
	if !strings.Contains(out, "epct.enter_dealer_contract_base_quarter") {
		t.Fatalf("expected top table\n%s", out)
	}
	if strings.Contains(out, "shop.users") {
		t.Fatalf("expected top limit to hide second table\n%s", out)
	}

	var header, row string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "  #  Table") {
			header = line
		}
		if strings.Contains(line, "epct.enter_dealer_contract_base_quarter") {
			row = line
		}
	}
	if header == "" || row == "" {
		t.Fatalf("expected top tables header and row\n%s", out)
	}
	if got, want := strings.Index(row, "2744948")+len("2744948"), strings.Index(header, "Affected Rows")+len("Affected Rows"); got != want {
		t.Fatalf("affected rows column is misaligned: row ends at %d, header ends at %d\n%s", got, want, out)
	}
}

func TestRenderTextTopTablesOperationBreakdown(t *testing.T) {
	result := productTextFixture()
	result.Tables = []model.TableStats{
		{
			Schema: "shop", Table: "orders",
			TotalRows: 1000, InsertRows: 600, UpdateRows: 300, DeleteRows: 100,
			TxnCount: 50, EventCount: 200, DDLCount: 2,
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, want := range []string{
		"INSERT", "UPDATE", "DELETE", "DDL Events",
		"600 (60.0%)", "300 (30.0%)", "100 (10.0%)", "2 (1.0%)",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected text to contain %q\n%s", want, out)
		}
	}
}

func TestRenderTextTopTablesInsertOnly(t *testing.T) {
	result := productTextFixture()
	result.Tables = []model.TableStats{
		{
			Schema: "shop", Table: "orders",
			TotalRows: 5000, InsertRows: 5000, UpdateRows: 0, DeleteRows: 0,
			TxnCount: 100, EventCount: 100, DDLCount: 0,
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "5000 (100.0%)") {
		t.Fatalf("expected INSERT 100%%\n%s", out)
	}
	if !strings.Contains(out, "0 (0.0%)") {
		t.Fatalf("expected zero operations to show 0%%\n%s", out)
	}
}

func TestRenderTextTopTablesUpdateOnly(t *testing.T) {
	result := productTextFixture()
	result.Tables = []model.TableStats{
		{
			Schema: "shop", Table: "orders",
			TotalRows: 3000, InsertRows: 0, UpdateRows: 3000, DeleteRows: 0,
			TxnCount: 50, EventCount: 150, DDLCount: 0,
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "3000 (100.0%)") {
		t.Fatalf("expected UPDATE 100%%\n%s", out)
	}
	if !strings.Contains(out, "0 (0.0%)") {
		t.Fatalf("expected zero INSERT/DELETE to show 0%%\n%s", out)
	}
}

func TestRenderTextTopTablesDeleteOnly(t *testing.T) {
	result := productTextFixture()
	result.Tables = []model.TableStats{
		{
			Schema: "shop", Table: "orders",
			TotalRows: 2000, InsertRows: 0, UpdateRows: 0, DeleteRows: 2000,
			TxnCount: 30, EventCount: 100, DDLCount: 0,
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "2000 (100.0%)") {
		t.Fatalf("expected DELETE 100%%\n%s", out)
	}
	if !strings.Contains(out, "0 (0.0%)") {
		t.Fatalf("expected zero INSERT/UPDATE to show 0%%\n%s", out)
	}
}

func TestRenderTextTopTablesDDLZeroDenominator(t *testing.T) {
	result := productTextFixture()
	result.Tables = []model.TableStats{
		{
			Schema: "shop", Table: "orders",
			TotalRows: 100, InsertRows: 100, UpdateRows: 0, DeleteRows: 0,
			TxnCount: 1, EventCount: 0, DDLCount: 0,
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "0 (\u2014)") {
		t.Fatalf("expected em dash for zero event denominator\n%s", out)
	}
}

func TestRenderTextTopTablesDDLZeroAffectedRows(t *testing.T) {
	result := productTextFixture()
	result.Tables = []model.TableStats{
		{
			Schema: "shop", Table: "orders",
			TotalRows: 0, InsertRows: 0, UpdateRows: 0, DeleteRows: 0,
			TxnCount: 1, EventCount: 50, DDLCount: 5,
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "0 (\u2014)") {
		t.Fatalf("expected em dash for zero affected rows denominator\n%s", out)
	}
	if !strings.Contains(out, "5 (10.0%)") {
		t.Fatalf("expected DDL percentage when EventCount > 0\n%s", out)
	}
}

func TestRenderTextTopTablesDDLOnly(t *testing.T) {
	result := productTextFixture()
	result.Tables = []model.TableStats{
		{
			Schema: "shop", Table: "orders",
			TotalRows: 0, InsertRows: 0, UpdateRows: 0, DeleteRows: 0,
			TxnCount: 1, EventCount: 10, DDLCount: 10,
		},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "10 (100.0%)") {
		t.Fatalf("expected DDL 100%%\n%s", out)
	}
}

func TestRenderTextTopTablesFootnote(t *testing.T) {
	result := productTextFixture()
	result.Tables = []model.TableStats{
		{Schema: "shop", Table: "orders", TotalRows: 100, InsertRows: 100, TxnCount: 1, EventCount: 10},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(out, "DML shares are of affected rows") {
		t.Fatalf("expected footnote about DML denominator\n%s", out)
	}
	if !strings.Contains(out, "DDL share is of binlog events") {
		t.Fatalf("expected footnote about DDL denominator\n%s", out)
	}
}

func TestRenderTextTopTablesReportsOmittedTables(t *testing.T) {
	result := productTextFixture()
	result.Summary.TotalRows = 6
	result.Tables = []model.TableStats{
		{Schema: "shop", Table: "orders", TotalRows: 4, InsertRows: 4, EventCount: 1},
		{Schema: "shop", Table: "payments", TotalRows: 2, InsertRows: 2, EventCount: 1},
	}

	out, err := RenderTextWithOptions(result, Options{TopN: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "shop.orders") || strings.Contains(out, "shop.payments") {
		t.Fatalf("expected only the top table to be displayed\n%s", out)
	}
	if !strings.Contains(out, "66.7%") {
		t.Fatalf("expected row share to use all table rows as the denominator\n%s", out)
	}
	if !strings.Contains(out, "… and 1 more tables") {
		t.Fatalf("expected omitted-table count\n%s", out)
	}
}

func TestRenderTextTopTablesSupportsExplicitUnlimitedLimit(t *testing.T) {
	result := productTextFixture()
	result.Tables = []model.TableStats{
		{Schema: "shop", Table: "orders", TotalRows: 4},
		{Schema: "shop", Table: "payments", TotalRows: 2},
	}

	out, err := RenderTextWithOptions(result, Options{TopN: 1, TopTablesSet: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "shop.orders") || !strings.Contains(out, "shop.payments") {
		t.Fatalf("expected explicit unlimited table output\n%s", out)
	}
	if strings.Contains(out, "more tables") {
		t.Fatalf("did not expect omitted-table count for unlimited output\n%s", out)
	}
}

func TestRenderTextTopTransactionsUsesTopLimit(t *testing.T) {
	result := productTextFixture()
	result.Diagnostics.LargestTransactions = []model.Transaction{
		{TxnKey: "txn-largest-a", TotalRows: 5000, Duration: 5 * time.Second, Tables: map[string]int{"shop.orders": 5000}},
		{TxnKey: "txn-largest-b", TotalRows: 4000, Duration: 4 * time.Second, Tables: map[string]int{"shop.users": 4000}},
	}
	result.Diagnostics.LongestTransactions = []model.Transaction{
		{TxnKey: "txn-long-a", TotalRows: 20, Duration: 45 * time.Second, Tables: map[string]int{"shop.accounts": 20}},
		{TxnKey: "txn-long-b", TotalRows: 18, Duration: 30 * time.Second, Tables: map[string]int{"shop.settlements": 18}},
	}
	result.Diagnostics.WidestTransactions = []model.Transaction{
		{TxnKey: "txn-wide-a", TotalRows: 120, Duration: 8 * time.Second, Tables: map[string]int{"shop.orders": 1, "shop.users": 1, "shop.payments": 1, "shop.audit_logs": 1}},
		{TxnKey: "txn-wide-b", TotalRows: 100, Duration: 7 * time.Second, Tables: map[string]int{"shop.orders": 1, "shop.users": 1, "shop.shipments": 1}},
	}

	out, err := RenderTextWithOptions(result, Options{TopN: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{"txn-largest-a", "txn-long-a", "txn-wide-a"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected top transaction token %q\n%s", token, out)
		}
	}
	for _, token := range []string{"txn-largest-b", "txn-long-b", "txn-wide-b"} {
		if strings.Contains(out, token) {
			t.Fatalf("expected top limit to hide token %q\n%s", token, out)
		}
	}
}

func TestRenderTextShowsUpToThreeLargestTransactions(t *testing.T) {
	result := productTextFixture()
	result.Diagnostics.LargestTransactions = []model.Transaction{
		{TxnKey: "txn-largest-a", TotalRows: 5000, BinlogBytes: 4096, Tables: map[string]int{"shop.orders": 5000}},
		{TxnKey: "txn-largest-b", TotalRows: 4000, Tables: map[string]int{"shop.users": 4000}},
		{TxnKey: "txn-largest-c", TotalRows: 3000, Tables: map[string]int{"shop.payments": 3000}},
		{TxnKey: "txn-largest-d", TotalRows: 2000, Tables: map[string]int{"shop.audit": 2000}},
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{"txn-largest-a", "txn-largest-b", "txn-largest-c", "table=shop.orders", "Largest txn: 4.0KB"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected token %q\n%s", token, out)
		}
	}
	if strings.Contains(out, "txn-largest-d") {
		t.Fatalf("expected fourth largest txn to stay hidden\n%s", out)
	}
}

func TestRenderTextActivitySectionIncludesMiniSeries(t *testing.T) {
	result := productTextFixture()

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, token := range []string{"TPS", "Rows/min", "▁"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected activity token %q\n%s", token, out)
		}
	}
}

func TestRenderTextSubsecondTPSIsNAWhileRowsPerMinuteStayNumeric(t *testing.T) {
	start := time.Date(2026, 4, 17, 9, 0, 0, 0, time.UTC)
	result := productTextFixture()
	result.Summary.Duration = 0
	result.Summary.TotalTransactions = 4
	result.Timeseries.TPSSeries = []model.TimeseriesPoint{{Minute: start, Value: 4.0 / 60.0}}
	result.Timeseries.RowsSeries = []model.TimeseriesPoint{{Minute: start, Value: 4}}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tpsLine := activityLineContaining(out, "TPS:")
	if !strings.Contains(tpsLine, "N/A (sub-second)") {
		t.Fatalf("sub-second span with transactions must print N/A TPS, got %q", tpsLine)
	}
	if strings.Contains(tpsLine, "0.0") || strings.Contains(tpsLine, "0.1") {
		t.Fatalf("sub-second TPS line still shows TxnCount/60 %q", tpsLine)
	}

	rowsLine := activityLineContaining(out, "Rows/min:")
	if !strings.Contains(rowsLine, "4.0 at") {
		t.Fatalf("Rows/min must still show the minute row count, got %q", rowsLine)
	}
	if strings.Contains(rowsLine, "N/A (sub-second)") {
		t.Fatalf("Rows/min must not inherit the TPS N/A label, got %q", rowsLine)
	}
}

func TestRenderTextDurationAtLeastOneSecondKeepsNumericTPSPeak(t *testing.T) {
	result := productTextFixture()

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tpsLine := activityLineContaining(out, "TPS:")
	if strings.Contains(tpsLine, "N/A (sub-second)") {
		t.Fatalf("duration ≥ 1s must keep numeric TPS peak, got %q", tpsLine)
	}
	if !strings.Contains(tpsLine, "22.5 at") {
		t.Fatalf("expected fixture peak 22.5, got %q", tpsLine)
	}
}

func activityLineContaining(out, token string) string {
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, token) {
			return line
		}
	}
	return ""
}

func TestRenderTextDetailsCanShowMinuteAndPatternSections(t *testing.T) {
	result := productTextFixture()
	result.Patterns = []model.PatternStats{{
		PatternKey:    "shop.orders|UPDATE|medium",
		Label:         "shop.orders / UPDATE / medium batch",
		TotalRows:     9000,
		TxnCount:      100,
		AvgRowsPerTxn: 90,
	}}

	out, err := RenderTextWithOptions(result, Options{Details: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, token := range []string{"Minute Details", "Write Shape Patterns", "shop.orders / UPDATE / medium batch"} {
		if !strings.Contains(out, token) {
			t.Fatalf("expected detail output to contain %q\n%s", token, out)
		}
	}
}

func TestRenderTextDefaultDoesNotRenderPatternDrilldowns(t *testing.T) {
	result := productTextFixture()
	result.Patterns = []model.PatternStats{{
		PatternKey:    "shop.orders|UPDATE|medium",
		Label:         "shop.orders / UPDATE / medium batch",
		TotalRows:     9000,
		TxnCount:      100,
		AvgRowsPerTxn: 90,
	}}
	result.PatternDrilldowns = []model.PatternDrilldown{{
		PatternKey:  "shop.orders|UPDATE|medium",
		Label:       "shop.orders / UPDATE / medium batch",
		WhySelected: "high row share",
	}}

	out, err := RenderText(result)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(out, "high row share") || strings.Contains(out, "drilldown:") {
		t.Fatalf("default text path rendered pattern drilldown\n%s", out)
	}
}

func TestRenderTextAndHTMLShareTopNDefault(t *testing.T) {
	if DefaultOptions().TopN != DefaultTopN {
		t.Fatalf("default options top N drifted from product default")
	}
}

func productTextFixture() model.AnalysisResult {
	start := time.Date(2026, 4, 17, 9, 0, 0, 0, time.UTC)
	return model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 120,
			TotalRows:         10000,
			TotalEvents:       500,
			StartTime:         start,
			EndTime:           start.Add(10 * time.Minute),
			Duration:          10 * time.Minute,
		},
		Timeseries: model.Timeseries{
			TPSSeries:  []model.TimeseriesPoint{{Minute: start.Add(time.Minute), Value: 22.5}},
			RowsSeries: []model.TimeseriesPoint{{Minute: start.Add(time.Minute), Value: 9000}},
		},
		Tables: []model.TableStats{
			{Schema: "shop", Table: "orders", TotalRows: 9000, TxnCount: 100, EventCount: 300},
			{Schema: "shop", Table: "users", TotalRows: 1000, TxnCount: 20, EventCount: 80},
		},
		Minutes: []model.MinuteBucket{
			{Minute: start, TotalRows: 1000, TxnCount: 10},
			{Minute: start.Add(time.Minute), TotalRows: 9000, TxnCount: 100},
		},
		Diagnostics: model.Diagnostics{
			HotIntervals: []model.MinuteBucket{{Minute: start.Add(time.Minute), TotalRows: 9000, TxnCount: 100}},
			DDLEvents: []model.DDLEvent{{
				Timestamp: start.Add(2 * time.Minute),
				Operation: "ALTER",
				Schema:    "shop",
				Table:     "orders",
				Statement: "ALTER TABLE shop.orders ADD COLUMN marker INT",
			}},
			LongestTransactions: []model.Transaction{{
				TxnKey:          "txn-long",
				TotalRows:       500,
				Duration:        45 * time.Second,
				BinlogPathStart: "mysql-bin.000044",
				PositionStart:   100,
				PositionEnd:     200,
			}},
			LargestTransactions: []model.Transaction{{
				TxnKey:          "txn-largest",
				TotalRows:       5000,
				Duration:        6 * time.Second,
				BinlogPathStart: "mysql-bin.000044",
				PositionStart:   210,
				PositionEnd:     420,
				Tables:          map[string]int{"shop.orders": 5000},
			}},
			WidestTransactions: []model.Transaction{{
				TxnKey:          "txn-wide",
				TotalRows:       600,
				Duration:        8 * time.Second,
				Tables:          map[string]int{"shop.orders": 200, "shop.users": 200, "shop.payments": 200},
				BinlogPathStart: "mysql-bin.000045",
				PositionStart:   500,
				PositionEnd:     700,
			}},
		},
	}
}

func TestDownsampleSeriesReturnsOriginalWhenUnderMax(t *testing.T) {
	points := []model.TimeseriesPoint{
		{Minute: time.Now(), Value: 1},
		{Minute: time.Now().Add(time.Minute), Value: 2},
		{Minute: time.Now().Add(2 * time.Minute), Value: 3},
	}
	result := downsampleSeries(points, 10)
	if len(result) != 3 {
		t.Fatalf("expected 3 points, got %d", len(result))
	}
}

func TestDownsampleSeriesAveragesBuckets(t *testing.T) {
	points := make([]model.TimeseriesPoint, 100)
	base := time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC)
	for i := range points {
		points[i] = model.TimeseriesPoint{
			Minute: base.Add(time.Duration(i) * time.Minute),
			Value:  float64(i),
		}
	}
	result := downsampleSeries(points, 5)
	if len(result) != 5 {
		t.Fatalf("expected 5 points, got %d", len(result))
	}
	// First bucket averages indices 0-19: avg = 9.5
	if result[0].Value < 9.0 || result[0].Value > 10.0 {
		t.Fatalf("expected first bucket avg ~9.5, got %.1f", result[0].Value)
	}
}

func TestDownsampleSeriesPreservesFirstBucketMinute(t *testing.T) {
	points := make([]model.TimeseriesPoint, 100)
	base := time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC)
	for i := range points {
		points[i] = model.TimeseriesPoint{
			Minute: base.Add(time.Duration(i) * time.Minute),
			Value:  float64(i),
		}
	}
	result := downsampleSeries(points, 5)
	if !result[0].Minute.Equal(base) {
		t.Fatalf("expected first bucket minute %s, got %s", base, result[0].Minute)
	}
}
