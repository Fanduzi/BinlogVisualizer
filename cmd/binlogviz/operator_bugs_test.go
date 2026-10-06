// Package binlogviz covers operator-facing analyze regressions: STATEMENT I/O, MIXED alerts, sub-second TPS, and fixture product contracts.
// input: mock STATEMENT/MIXED parser events and real binlog fixtures through runAnalysis seams.
// output: exit-code and stdout/stderr/JSON contracts: STATEMENT writes no report; MIXED writes a report plus input_format alert; object-filter no-data exits 2; sub-second TPS is N/A.
// pos: command-layer regression suite for analyze operator I/O bugs.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
)

func TestAnalyzeMinimalBinlogSubsecondTPSIsNotZero(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")

	textOut, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysis([]string{fixture}, analyzer.DefaultOptions(), "text")
	})
	if err != nil {
		t.Fatalf("text analyze: %v", err)
	}

	tpsLine := activityLineContaining(textOut, "TPS:")
	if tpsLine == "" {
		t.Fatalf("expected a TPS activity line, got:\n%s", textOut)
	}
	if !strings.Contains(tpsLine, "N/A (sub-second)") {
		t.Fatalf("same-second fixture must not look like a TPS parse failure; want N/A (sub-second), got %q", tpsLine)
	}
	if strings.Contains(tpsLine, "0.0") || strings.Contains(tpsLine, "0.1") {
		t.Fatalf("sub-second TPS line still shows TxnCount/60 peak %q", tpsLine)
	}

	rowsLine := activityLineContaining(textOut, "Rows/min:")
	if rowsLine == "" {
		t.Fatalf("expected a Rows/min activity line, got:\n%s", textOut)
	}
	if strings.Contains(rowsLine, "N/A") {
		t.Fatalf("Rows/min must stay a minute row count, got %q", rowsLine)
	}

	jsonOut, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysis([]string{fixture}, analyzer.DefaultOptions(), "json")
	})
	if err != nil {
		t.Fatalf("json analyze: %v", err)
	}
	var decoded struct {
		Timeseries struct {
			TPSSeries []struct {
				Value json.RawMessage `json:"value"`
			} `json:"tps_series"`
		} `json:"timeseries"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", err, jsonOut)
	}
	if len(decoded.Timeseries.TPSSeries) == 0 {
		t.Fatal("expected numeric tps_series points")
	}
	for i, point := range decoded.Timeseries.TPSSeries {
		raw := strings.TrimSpace(string(point.Value))
		if raw == "" || raw[0] == '"' {
			t.Fatalf("tps_series[%d].value must remain a JSON number, got %s", i, raw)
		}
	}
}

func TestAnalyzeSpanOfAtLeastOneSecondPrintsNumericTPSPeak(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	parser := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: now, EventType: "WRITE_ROWS", Schema: "shop", Table: "orders", RowCount: 10},
			{Timestamp: now.Add(time.Second), EventType: "XID"},
			{Timestamp: now.Add(2 * time.Second), EventType: "WRITE_ROWS", Schema: "shop", Table: "orders", RowCount: 5},
			{Timestamp: now.Add(3 * time.Second), EventType: "XID"},
		},
	}

	stdout, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParser([]string{"dummy.binlog"}, analyzer.DefaultOptions(), "text", parser)
	})
	if err != nil {
		t.Fatalf("text analyze: %v", err)
	}
	tpsLine := activityLineContaining(stdout, "TPS:")
	if tpsLine == "" {
		t.Fatalf("expected a TPS activity line, got:\n%s", stdout)
	}
	if strings.Contains(tpsLine, "N/A (sub-second)") {
		t.Fatalf("duration ≥ 1s must keep a numeric TPS peak, got %q", tpsLine)
	}
	if !strings.Contains(tpsLine, " at ") {
		t.Fatalf("expected numeric TPS peak with timestamp, got %q", tpsLine)
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

func TestAnalyzeFixtureTextFindingsMatchJSONNoCriticalSpike(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")

	textOut, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysis([]string{fixture}, analyzer.DefaultOptions(), "text")
	})
	if err != nil {
		t.Fatalf("text analyze: %v", err)
	}
	if strings.Contains(textOut, "[critical] Write spike") {
		t.Fatalf("5-row fixture text report invented a critical spike:\n%s", textOut)
	}
	if strings.Contains(textOut, "[warning] Longest transaction") {
		t.Fatalf("0s fixture transaction should not be a warning finding:\n%s", textOut)
	}
	if !strings.Contains(textOut, "No high-signal findings detected.") {
		t.Fatalf("expected text findings to stay empty when JSON has no alerts:\n%s", textOut)
	}

	jsonOut, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysis([]string{fixture}, analyzer.DefaultOptions(), "json")
	})
	if err != nil {
		t.Fatalf("json analyze: %v", err)
	}

	var decoded struct {
		Alerts      []any `json:"alerts"`
		Warnings    int   `json:"warnings"`
		Diagnostics struct {
			Findings []any `json:"findings"`
		} `json:"diagnostics"`
	}
	if err := json.Unmarshal([]byte(jsonOut), &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", err, jsonOut)
	}
	if len(decoded.Alerts) != 0 || decoded.Warnings != 0 || len(decoded.Diagnostics.Findings) != 0 {
		t.Fatalf("JSON should have empty alerts/findings, got alerts=%d warnings=%d findings=%d",
			len(decoded.Alerts), decoded.Warnings, len(decoded.Diagnostics.Findings))
	}
}

func TestAnalyzeFixtureCountsLogicalUpdateRows(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")

	out, err := captureStdoutRun(t, func() error {
		return runAnalysisWithReportOptions([]string{fixture}, analyzer.DefaultOptions(), report.DefaultOptions(), "json")
	})
	if err != nil {
		t.Fatalf("json analyze: %v", err)
	}

	var decoded struct {
		Summary struct {
			TotalRows         int `json:"total_rows"`
			TotalTransactions int `json:"total_transactions"`
		} `json:"summary"`
		Tables []struct {
			UpdateRows   int `json:"update_rows"`
			UpdateEvents int `json:"update_events"`
			InsertRows   int `json:"insert_rows"`
			DeleteRows   int `json:"delete_rows"`
			TotalRows    int `json:"total_rows"`
		} `json:"tables"`
		Transactions []struct {
			Operations map[string]int `json:"operations"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", err, out)
	}
	if decoded.Summary.TotalRows != 4 {
		t.Fatalf("total_rows=%d, want 4 logical rows", decoded.Summary.TotalRows)
	}
	if decoded.Summary.TotalTransactions != 4 {
		t.Fatalf("total_transactions=%d, want 4", decoded.Summary.TotalTransactions)
	}
	if len(decoded.Tables) != 1 {
		t.Fatalf("tables=%d, want 1", len(decoded.Tables))
	}
	table := decoded.Tables[0]
	if table.InsertRows != 2 || table.UpdateRows != 1 || table.DeleteRows != 1 || table.TotalRows != 4 {
		t.Fatalf("table I/U/D/total = %d/%d/%d/%d, want 2/1/1/4", table.InsertRows, table.UpdateRows, table.DeleteRows, table.TotalRows)
	}
	if table.UpdateEvents != 1 {
		t.Fatalf("update_events=%d, want 1", table.UpdateEvents)
	}

	var updateOps int
	for _, txn := range decoded.Transactions {
		updateOps += txn.Operations["UPDATE"]
	}
	if updateOps != 1 {
		t.Fatalf("transaction operations.UPDATE sum=%d, want 1", updateOps)
	}
}

func TestAnalyzeStatementBinlogWarnsAndExitsNonZero(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	parser := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: now, EventType: "QUERY", Query: "INSERT INTO dogfood.users VALUES (1)"},
			{Timestamp: now.Add(time.Second), EventType: "QUERY", Query: "UPDATE dogfood.users SET name='x'"},
			{Timestamp: now.Add(2 * time.Second), EventType: "QUERY", Query: "DELETE FROM dogfood.users WHERE id=1"},
			{Timestamp: now.Add(3 * time.Second), EventType: "QUERY", Query: "INSERT INTO dogfood.users VALUES (2)"},
			{Timestamp: now.Add(4 * time.Second), EventType: "QUERY", Query: "UPDATE dogfood.users SET name='y'"},
		},
	}

	runLikeMain := func(format string) (string, string, error) {
		return captureStdoutStderrRun(t, func() error {
			err := runAnalysisWithParser([]string{"dummy.binlog"}, analyzer.DefaultOptions(), format, parser)
			if err != nil {
				fmt.Fprintln(os.Stderr, "Error:", err)
			}
			return err
		})
	}

	stdout, stderr, err := runLikeMain("text")
	if err == nil {
		t.Fatal("expected non-zero exit for STATEMENT binlog with zero row images")
	}
	if stdout != "" {
		t.Fatalf("STATEMENT text analyze must leave stdout empty, got %q", stdout)
	}
	if !strings.Contains(err.Error(), binlog.StatementOrMixedWarning) {
		t.Fatalf("error=%v, want %q", err, binlog.StatementOrMixedWarning)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected Error: once, got %q", stderr)
	}
	if strings.Count(stderr, binlog.StatementOrMixedWarning) != 1 {
		t.Fatalf("expected STATEMENT sentence once, got %q", stderr)
	}
	assertNoUsageDump(t, stderr)

	jsonOut, jsonErrStderr, jsonErr := runLikeMain("json")
	if jsonErr == nil {
		t.Fatal("expected non-zero exit for STATEMENT JSON analyze")
	}
	if jsonOut != "" {
		t.Fatalf("STATEMENT JSON analyze must not write a document, got %q", jsonOut)
	}
	if strings.Count(jsonErrStderr, "Error:") != 1 {
		t.Fatalf("expected JSON Error: once, got %q", jsonErrStderr)
	}
	if strings.Count(jsonErrStderr, binlog.StatementOrMixedWarning) != 1 {
		t.Fatalf("expected JSON STATEMENT sentence once, got %q", jsonErrStderr)
	}
	assertNoUsageDump(t, jsonErrStderr)
}

func TestAnalyzeMixedBinlogWarnsAndRecordsIgnoredQueryDML(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	parser := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: now, EventType: "QUERY", Query: "INSERT INTO dogfood.users VALUES (1)"},
			{Timestamp: now.Add(time.Second), EventType: "QUERY", Query: "UPDATE dogfood.users SET name=UUID()"},
			{Timestamp: now.Add(2 * time.Second), EventType: "UPDATE_ROWS", Schema: "dogfood", Table: "users", RowCount: 1},
			{Timestamp: now.Add(3 * time.Second), EventType: "XID"},
		},
	}

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParser([]string{"dummy.binlog"}, analyzer.DefaultOptions(), "json", parser)
	})
	if err != nil {
		t.Fatalf("MIXED with row images should exit 0, got %v", err)
	}
	if !strings.Contains(stderr, binlog.StatementOrMixedWarning) {
		t.Fatalf("stderr missing format warning:\n%s", stderr)
	}

	var decoded struct {
		Summary struct {
			TotalRows         int `json:"total_rows"`
			TotalTransactions int `json:"total_transactions"`
		} `json:"summary"`
		Diagnostics struct {
			InputFormatGuess      string `json:"input_format_guess"`
			IgnoredQueryDMLEvents int    `json:"ignored_query_dml_events"`
		} `json:"diagnostics"`
		Alerts []struct {
			Type     string `json:"type"`
			Severity string `json:"severity"`
			Message  string `json:"message"`
		} `json:"alerts"`
		Warnings int `json:"warnings"`
		Tables   []struct {
			Schema string `json:"schema"`
			Table  string `json:"table"`
		} `json:"tables"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", err, stdout)
	}
	if decoded.Diagnostics.InputFormatGuess != binlog.InputFormatMixed {
		t.Fatalf("input_format_guess=%q, want MIXED", decoded.Diagnostics.InputFormatGuess)
	}
	if decoded.Diagnostics.IgnoredQueryDMLEvents != 2 {
		t.Fatalf("ignored_query_dml_events=%d, want 2", decoded.Diagnostics.IgnoredQueryDMLEvents)
	}
	if decoded.Summary.TotalRows != 1 || decoded.Summary.TotalTransactions != 1 {
		t.Fatalf("MIXED should count only the ROW subset, got rows=%d txns=%d", decoded.Summary.TotalRows, decoded.Summary.TotalTransactions)
	}
	if len(decoded.Tables) != 1 || decoded.Tables[0].Schema+"."+decoded.Tables[0].Table != "dogfood.users" {
		t.Fatalf("expected dogfood.users from the ROW image, got %#v", decoded.Tables)
	}
	var formatAlert *struct {
		Type     string `json:"type"`
		Severity string `json:"severity"`
		Message  string `json:"message"`
	}
	for i := range decoded.Alerts {
		if decoded.Alerts[i].Type == "input_format" {
			formatAlert = &decoded.Alerts[i]
			break
		}
	}
	if formatAlert == nil {
		t.Fatalf("MIXED JSON must carry type=input_format so automation can see ignored Query-DML; alerts=%#v", decoded.Alerts)
	}
	if formatAlert.Severity != "warning" {
		t.Fatalf("input_format severity=%q, want warning", formatAlert.Severity)
	}
	wantMsg := "MIXED: counted 1 ROW images, ignored 2 Query-DML events"
	if formatAlert.Message != wantMsg {
		t.Fatalf("input_format message=%q, want %q", formatAlert.Message, wantMsg)
	}
	if decoded.Warnings != 0 {
		t.Fatalf("warnings is truncated-query count, want 0 when nothing was truncated, got %d", decoded.Warnings)
	}
}

func TestAnalyzeRowFixtureJSONHasNoInputFormatAlert(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := mustFixturePath(t, "minimal.binlog")

	out, err := captureStdoutRun(t, func() error {
		return runAnalysisWithReportOptions([]string{fixture}, analyzer.DefaultOptions(), report.DefaultOptions(), "json")
	})
	if err != nil {
		t.Fatalf("ROW fixture json analyze: %v", err)
	}

	var decoded struct {
		Alerts []struct {
			Type string `json:"type"`
		} `json:"alerts"`
		Warnings int `json:"warnings"`
	}
	if err := json.Unmarshal([]byte(out), &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", err, out)
	}
	for _, alert := range decoded.Alerts {
		if alert.Type == "input_format" {
			t.Fatalf("ROW with no Query-DML must not emit input_format alert, got %#v", decoded.Alerts)
		}
	}
	if decoded.Warnings != 0 {
		t.Fatalf("ROW fixture warnings=%d, want 0", decoded.Warnings)
	}
}

func TestAnalyzeObjectFilterNoRowsExitsTwoWithoutReport(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	now := time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)
	parser := &mockParser{
		events: []binlog.RawEvent{
			{Timestamp: now, EventType: "QUERY", Query: "BEGIN"},
			{Timestamp: now.Add(time.Second), EventType: "WRITE_ROWS", Schema: "dogfood", Table: "orders", RowCount: 5},
			{Timestamp: now.Add(2 * time.Second), EventType: "XID"},
		},
	}
	opts := analyzer.DefaultOptions()
	opts.ExcludeSchemas = []string{"dogfood"}

	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		runErr := runAnalysisWithParser([]string{"dummy.binlog"}, opts, "json", parser)
		if runErr != nil {
			fmt.Fprintln(os.Stderr, "Error:", runErr)
		}
		return runErr
	})
	assertAnalyzeNoDataExit(t, stdout, stderr, err, "filter matched no events")
}

func TestAnalyzeIncludeTableKeepsNonTableObjects(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		{Timestamp: now, EventType: "QUERY", Query: "CREATE VIEW obj.v1 AS SELECT 1"},
		{Timestamp: now.Add(time.Second), EventType: "QUERY", Query: "CREATE EVENT obj.ev1 ON SCHEDULE EVERY 1 HOUR DO SELECT 1"},
		{Timestamp: now.Add(2 * time.Second), EventType: "QUERY", Query: "CREATE FUNCTION obj.f1() RETURNS INT RETURN 1"},
		{Timestamp: now.Add(3 * time.Second), EventType: "QUERY", Query: "CREATE PROCEDURE obj.p1() SELECT 1"},
		{Timestamp: now.Add(4 * time.Second), EventType: "QUERY", Query: "CREATE TRIGGER obj.trg_ai BEFORE INSERT ON obj.t FOR EACH ROW SET NEW.v = 1"},
		{Timestamp: now.Add(5 * time.Second), EventType: "QUERY", Query: "DROP TRIGGER obj.trg_ai"},
		{Timestamp: now.Add(6 * time.Second), EventType: "QUERY", Query: "BEGIN"},
		{Timestamp: now.Add(7 * time.Second), EventType: "WRITE_ROWS", Schema: "obj", Table: "t", RowCount: 2},
		{Timestamp: now.Add(8 * time.Second), EventType: "XID"},
	}
	for _, name := range []string{"obj.v1", "obj.ev1", "obj.f1", "obj.p1", "obj.trg_ai"} {
		opts := analyzer.DefaultOptions()
		opts.IncludeTables = []string{name}
		stdout, stderr, err := captureStdoutStderrRun(t, func() error {
			return runAnalysisWithParser([]string{"dummy.binlog"}, opts, "text", &mockParser{events: events})
		})
		if err != nil {
			t.Fatalf("include %s: %v\nstderr=%s", name, err, stderr)
		}
		if !strings.Contains(stdout, name) {
			t.Fatalf("include %s report missing the object:\n%s", name, stdout)
		}
	}
	opts := analyzer.DefaultOptions()
	opts.IncludeTables = []string{"obj.missing"}
	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		runErr := runAnalysisWithParser([]string{"dummy.binlog"}, opts, "text", &mockParser{events: events})
		if runErr != nil {
			fmt.Fprintln(os.Stderr, "Error:", runErr)
		}
		return runErr
	})
	assertAnalyzeNoDataExit(t, stdout, stderr, err, "filter matched no events")

	opts.IncludeTables = []string{"obj.t"}
	stdout, stderr, err = captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParser([]string{"dummy.binlog"}, opts, "text", &mockParser{events: events})
	})
	if err != nil {
		t.Fatalf("include base table: %v\nstderr=%s", err, stderr)
	}
	if strings.Contains(stdout, "TRIGGER") {
		t.Fatalf("base table filter kept trigger DDL:\n%s", stdout)
	}
}

func TestAnalyzeAuthDDLRedactedInEveryFormat(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	const password = "MariaVia8"
	const hash = "*FAA7A7FD08B88CBFE862FC3A7D612FB937B13476"
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		{Timestamp: now, EventType: "GTID", GTID: "0-11-1", ServerFlavor: "mariadb", PositionStart: 100, PositionEnd: 140},
		{Timestamp: now.Add(time.Second), EventType: "QUERY", ServerFlavor: "mariadb", Query: "CREATE USER 'm5'@'%' IDENTIFIED VIA mysql_native_password USING PASSWORD('" + password + "')", PositionStart: 140, PositionEnd: 200},
		{Timestamp: now.Add(2 * time.Second), EventType: "GTID", GTID: "0-11-2", ServerFlavor: "mariadb", PositionStart: 200, PositionEnd: 240},
		{Timestamp: now.Add(3 * time.Second), EventType: "QUERY", ServerFlavor: "mariadb", Query: "ALTER USER 'm5'@'%' IDENTIFIED VIA mysql_native_password USING '" + hash + "'", PositionStart: 240, PositionEnd: 300},
		{Timestamp: now.Add(4 * time.Second), EventType: "GTID", GTID: "0-11-3", ServerFlavor: "mariadb", PositionStart: 300, PositionEnd: 340},
		{Timestamp: now.Add(5 * time.Second), EventType: "QUERY", ServerFlavor: "mariadb", Query: "SET PASSWORD FOR 'm2'@'%' = PASSWORD('a1')", PositionStart: 340, PositionEnd: 400},
		{Timestamp: now.Add(6 * time.Second), EventType: "GTID", GTID: "0-11-4", ServerFlavor: "mariadb", PositionStart: 400, PositionEnd: 440},
		{Timestamp: now.Add(7 * time.Second), EventType: "QUERY", ServerFlavor: "mariadb", Query: "CREATE USER 'u'@'%' IDENTIFIED VIA mysql_native_password USING PASSWORD('chainA') OR ed25519 USING PASSWORD('chainB')", PositionStart: 440, PositionEnd: 520},
	}
	secrets := []string{password, hash, "chainA", "chainB", "PASSWORD('a1')", "'a1'"}
	for _, mode := range []report.SQLContextMode{report.SQLContextOff, report.SQLContextSummary, report.SQLContextFull} {
		for _, format := range []string{"text", "json", "markdown", "html"} {
			stdout, stderr, err := captureStdoutStderrRun(t, func() error {
				return runAnalysisWithParserAndTempDirAndReportOptions([]string{"dummy.binlog"}, analyzer.DefaultOptions(), report.Options{SQLContextMode: mode}, format, &mockParser{events: events}, "", nil)
			})
			if err != nil {
				t.Fatalf("%s %s: %v\nstderr=%s", mode, format, err, stderr)
			}
			for _, secret := range secrets {
				if strings.Contains(stdout, secret) {
					t.Fatalf("%s %s leaked %q", mode, format, secret)
				}
			}
			if mode == report.SQLContextOff {
				if strings.Contains(stdout, "<secret>") || strings.Contains(stdout, "&lt;secret&gt;") {
					t.Fatalf("%s %s printed the DDL statement:\n%s", mode, format, stdout)
				}
				continue
			}
			if !strings.Contains(stdout, "secret") {
				t.Fatalf("%s %s missing redaction:\n%s", mode, format, stdout)
			}
			if !strings.Contains(stdout, "SET PASSWORD") {
				t.Fatalf("%s %s dropped SET PASSWORD:\n%s", mode, format, stdout)
			}
		}
	}
}

func TestAnalyzeFullDDLStatementIsCapped(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	tail := "TAILMARKER_ZZZ"
	sql := "CREATE TABLE lng.wide (" + strings.Repeat("c INT, ", 800) + tail + " INT)"
	full := strings.Join(strings.Fields(sql), " ")
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	parser := &mockParser{events: []binlog.RawEvent{{
		Timestamp: now, EventType: "QUERY", Query: sql, Schema: "lng",
	}}}
	for _, format := range []string{"text", "json", "markdown", "html"} {
		stdout, stderr, err := captureStdoutStderrRun(t, func() error {
			return runAnalysisWithParserAndTempDirAndReportOptions([]string{"dummy.binlog"}, analyzer.DefaultOptions(), report.Options{SQLContextMode: report.SQLContextFull}, format, parser, "", nil)
		})
		if err != nil {
			t.Fatalf("full %s: %v\nstderr=%s", format, err, stderr)
		}
		marker := " … [truncated: 4096 of " + strconv.Itoa(len(full)) + " bytes]"
		if !strings.Contains(stdout, marker) {
			t.Fatalf("full %s missing %q", format, marker)
		}
		if strings.Contains(stdout, tail) {
			t.Fatalf("full %s printed the uncapped tail", format)
		}
	}
	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParserAndTempDirAndReportOptions([]string{"dummy.binlog"}, analyzer.DefaultOptions(), report.Options{SQLContextMode: report.SQLContextSummary}, "text", parser, "", nil)
	})
	if err != nil {
		t.Fatalf("summary: %v\nstderr=%s", err, stderr)
	}
	summaryMarker := " … [truncated: " + strconv.Itoa(model.MaxQuerySummaryChars) + " of " + strconv.Itoa(len(full)) + " bytes]"
	if !strings.Contains(stdout, summaryMarker) {
		t.Fatalf("summary missing %q\n%s", summaryMarker, stdout)
	}
}
