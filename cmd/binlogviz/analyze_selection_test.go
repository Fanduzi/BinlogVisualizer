// Package binlogviz tests analyze selection contracts through the CLI and streaming pipeline.
// input: analyze commands, synthetic parser events, position/GTID selectors, and report renderers.
// output: regressions for exact position boundaries, transaction-group GTID filtering, no-data, and report evidence.
// pos: public analyze selection acceptance coverage for issue #52.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/clierr"
	"binlogviz/internal/compare"
	"binlogviz/internal/i18n"
)

func TestAnalyzePositionSelectorsRequireOneExplicitFile(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "multiple files", args: []string{"one.bin", "two.bin", "--start-position", "4"}},
		{name: "discovery", args: []string{"--from-dir", ".", "--prefix", "mysql-bin", "--stop-position", "100"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newAnalyzeCommand()
			cmd.SetArgs(tt.args)
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), "exactly one explicit binlog file") {
				t.Fatalf("error = %v, want explicit single-file position error", err)
			}
		})
	}
}

func TestAnalyzePositionSelectorsRejectDirectoryArgument(t *testing.T) {
	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{t.TempDir(), "--start-position", "4"})
	err := cmd.Execute()
	// A directory is refused as an input before the position selectors are
	// checked (#268): exit 4, not a corrupt binlog.
	if err == nil || !strings.Contains(err.Error(), "is a directory") || ExitCode(err) != 4 {
		t.Fatalf("error = %v (exit %d), want a directory input error", err, ExitCode(err))
	}
}

func TestAnalyzeRejectsMixedGTIDSelectorFlavorBeforeReadingFiles(t *testing.T) {
	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{
		"missing.bin",
		"--include-gtids", "24bc7850-2c16-11e6-a073-0242ac110002:1",
		"--exclude-gtids", "0-7-1857",
	})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "mixed GTID selector flavors") {
		t.Fatalf("error = %v", err)
	}
}

func TestAnalyzeRejectsReversedPositionRangeBeforeReadingFiles(t *testing.T) {
	cmd := newAnalyzeCommand()
	cmd.SetArgs([]string{"missing.bin", "--start-position", "200", "--stop-position", "100"})
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "--stop-position must be greater") {
		t.Fatalf("error = %v", err)
	}
}

func TestAnalyzePositionWindowKeepsPartialTransactionContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mysql-bin.000001")
	if err := os.WriteFile(path, make([]byte, 200), 0o600); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	parser := &mockParser{events: []binlog.RawEvent{
		{Timestamp: base, EventType: "FORMAT_DESCRIPTION", BinlogPath: path, PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "8.0.36"},
		{Timestamp: base.Add(time.Second), EventType: "GTID", BinlogPath: path, PositionStart: 100, PositionEnd: 120, Position: 120, GTID: "24bc7850-2c16-11e6-a073-0242ac110002:7", ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "QUERY", Query: "BEGIN", BinlogPath: path, PositionStart: 120, PositionEnd: 140, Position: 140, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "TABLE_MAP", Schema: "shop", Table: "orders", BinlogPath: path, PositionStart: 140, PositionEnd: 160, Position: 160, ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "WRITE_ROWS", Schema: "shop", Table: "orders", RowCount: 2, BinlogPath: path, PositionStart: 160, PositionEnd: 180, Position: 180, ServerFlavor: "mysql"},
		{Timestamp: base.Add(5 * time.Second), EventType: "XID", XID: "9", BinlogPath: path, PositionStart: 180, PositionEnd: 200, Position: 200, ServerFlavor: "mysql"},
	}}
	start, stop := int64(160), int64(200)
	opts := analyzer.DefaultOptions()
	opts.StartPosition = &start
	opts.StopPosition = &stop

	stdout, err := captureStdoutRun(t, func() error {
		return runAnalysisWithParser([]string{path}, opts, "json", parser)
	})
	if err != nil {
		t.Fatalf("analyze failed: %v", err)
	}
	var got struct {
		Selection struct {
			RequestedStartPosition *int64 `json:"requested_start_position"`
			RequestedStopPosition  *int64 `json:"requested_stop_position"`
			EffectiveStartPosition *int64 `json:"effective_start_position"`
			EffectiveStopPosition  *int64 `json:"effective_stop_position"`
		} `json:"selection"`
		Transactions []struct {
			Completeness    string `json:"completeness"`
			ReplayAvailable bool   `json:"replay_available"`
			TotalRows       int    `json:"total_rows"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("decode report: %v\n%s", err, stdout)
	}
	if len(got.Transactions) != 1 || got.Transactions[0].Completeness != "partial_start" || !got.Transactions[0].ReplayAvailable || got.Transactions[0].TotalRows != 2 {
		t.Fatalf("transaction = %+v, want retained partial_start with proven full replay", got.Transactions)
	}
	if got.Selection.RequestedStartPosition == nil || *got.Selection.RequestedStartPosition != start ||
		got.Selection.RequestedStopPosition == nil || *got.Selection.RequestedStopPosition != stop ||
		got.Selection.EffectiveStartPosition == nil || *got.Selection.EffectiveStartPosition != start ||
		got.Selection.EffectiveStopPosition == nil || *got.Selection.EffectiveStopPosition != stop {
		t.Fatalf("selection evidence = %+v", got.Selection)
	}
	// Position selectors alone are not GTID selectors: the selection has no
	// matched GTID evidence, not matched_transactions 0 (#381 QA F4).
	var raw struct {
		Selection map[string]json.RawMessage `json:"selection"`
	}
	if err := json.Unmarshal([]byte(stdout), &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"matched_transactions", "matched_gtid_set", "include_gtids", "exclude_gtids"} {
		if value, ok := raw.Selection[key]; ok {
			t.Fatalf("position-only selection has %s=%s", key, value)
		}
	}
}

func TestAnalyzeTimeOnlyReportOmitsSelection(t *testing.T) {
	start := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	opts := analyzer.DefaultOptions()
	opts.Start = &start
	parser := &mockParser{events: []binlog.RawEvent{
		{Timestamp: start, EventType: "QUERY", Query: "CREATE TABLE shop.orders (id INT)", PositionStart: 100, PositionEnd: 140, Position: 140},
	}}
	stdout, err := captureStdoutRun(t, func() error {
		return runAnalysisWithParser([]string{"synthetic.bin"}, opts, "json", parser)
	})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("decode report: %v\n%s", err, stdout)
	}
	if _, ok := got["selection"]; ok {
		t.Fatalf("time-only report unexpectedly contains selection: %s", stdout)
	}
}

func TestAnalyzePositionBoundaryValidationAndNoMatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mysql-bin.000001")
	if err := os.WriteFile(path, make([]byte, 200), 0o600); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 30, 10, 30, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		{Timestamp: base, EventType: "FORMAT_DESCRIPTION", BinlogPath: path, PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "8.0.36"},
		{Timestamp: base.Add(time.Second), EventType: "QUERY", Query: "BEGIN", BinlogPath: path, PositionStart: 100, PositionEnd: 120, Position: 120},
		{Timestamp: base.Add(2 * time.Second), EventType: "WRITE_ROWS", Schema: "shop", Table: "orders", RowCount: 1, BinlogPath: path, PositionStart: 120, PositionEnd: 180, Position: 180},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", BinlogPath: path, PositionStart: 180, PositionEnd: 200, Position: 200},
	}
	tests := []struct {
		name     string
		start    *int64
		stop     *int64
		wantText string
		wantExit int
	}{
		{name: "mid event", start: int64Pointer(150), wantText: "not an exact event boundary", wantExit: 3},
		{name: "out of range", stop: int64Pointer(201), wantText: "out of range", wantExit: 3},
		{name: "EOF no match", start: int64Pointer(200), wantExit: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := analyzer.DefaultOptions()
			opts.StartPosition, opts.StopPosition = tt.start, tt.stop
			stdout, err := captureStdoutRun(t, func() error {
				return runAnalysisWithParser([]string{path}, opts, "json", &mockParser{events: events})
			})
			if ExitCode(err) != tt.wantExit || tt.wantText != "" && !strings.Contains(err.Error(), tt.wantText) || stdout != "" {
				t.Fatalf("exit=%d err=%v stdout=%q", ExitCode(err), err, stdout)
			}
		})
	}
}

func int64Pointer(value int64) *int64 { return &value }

func TestAnalyzeGTIDNoMatchExitsTwoWithoutReport(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	selector, err := analyzer.ParseGTIDSelector([]string{sid + ":1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 30, 13, 0, 0, 0, time.UTC)
	parser := &mockParser{events: []binlog.RawEvent{
		{Timestamp: base, EventType: "FORMAT_DESCRIPTION", PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "8.0.36"},
		{Timestamp: base.Add(time.Second), EventType: "GTID", PositionStart: 100, PositionEnd: 120, Position: 120, GTID: sid + ":2", ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "QUERY", Query: "BEGIN", PositionStart: 120, PositionEnd: 140, Position: 140, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "WRITE_ROWS", Schema: "shop", Table: "orders", RowCount: 1, PositionStart: 140, PositionEnd: 160, Position: 160, ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "XID", PositionStart: 160, PositionEnd: 180, Position: 180, ServerFlavor: "mysql"},
	}}
	opts := analyzer.DefaultOptions()
	opts.GTIDSelector = selector
	stdout, err := captureStdoutRun(t, func() error {
		return runAnalysisWithParser([]string{"synthetic.bin"}, opts, "json", parser)
	})
	if ExitCode(err) != 2 || stdout != "" {
		t.Fatalf("exit=%d err=%v stdout=%q", ExitCode(err), err, stdout)
	}
}

func TestAnalyzeGTIDSelectionReportRoundTrips(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	selector, err := analyzer.ParseGTIDSelector([]string{sid + ":1-3"}, []string{sid + ":2"})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 30, 13, 30, 0, 0, time.UTC)
	parser := &mockParser{events: []binlog.RawEvent{
		{Timestamp: base, EventType: "FORMAT_DESCRIPTION", PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "GTID", PositionStart: 100, PositionEnd: 120, Position: 120, GTID: sid + ":1", ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "QUERY", Query: "BEGIN", PositionStart: 120, PositionEnd: 140, Position: 140, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "ROWS_QUERY", QuerySQL: "INSERT INTO shop.orders VALUES (1)", PositionStart: 140, PositionEnd: 160, Position: 160, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "WRITE_ROWS", Schema: "shop", Table: "orders", RowCount: 1, PositionStart: 160, PositionEnd: 180, Position: 180, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(5 * time.Second), EventType: "XID", XID: "9", PositionStart: 180, PositionEnd: 200, Position: 200, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
	}}
	opts := analyzer.DefaultOptions()
	opts.GTIDSelector = selector
	stdout, err := captureStdoutRun(t, func() error {
		return runAnalysisWithParser([]string{"synthetic.bin"}, opts, "json", parser)
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := compare.DecodeReportJSON([]byte(stdout))
	if err != nil {
		t.Fatalf("decode report: %v\n%s", err, stdout)
	}
	if decoded.Selection == nil || decoded.Selection.ResolvedGTIDFlavor != "mysql" || len(decoded.Selection.IncludeGTIDs) != 1 || decoded.Selection.IncludeGTIDs[0] != sid+":1-3" || len(decoded.Selection.ExcludeGTIDs) != 1 || decoded.Selection.ExcludeGTIDs[0] != sid+":2" || len(decoded.Selection.MatchedGTIDSet) != 1 || decoded.Selection.MatchedGTIDSet[0] != sid+":1" || decoded.Selection.MatchedTransactions == nil || *decoded.Selection.MatchedTransactions != 1 {
		t.Fatalf("selection = %+v", decoded.Selection)
	}
	if len(decoded.Transactions) != 1 || decoded.Transactions[0].GTID != sid+":1" || decoded.Transactions[0].Completeness != "complete" || decoded.Provenance == nil || len(decoded.Provenance.ServerIDs) != 1 || decoded.Transactions[0].QuerySummary == "" {
		t.Fatalf("prior report fields lost: provenance=%+v transactions=%+v", decoded.Provenance, decoded.Transactions)
	}
}

// TestAnalyzeMatchedGTIDIntervalCeilingExitsTen pins the #379 ceiling: when
// the matched GTIDs leave more separate intervals than
// analyzer.MatchedGTIDIntervalLimit, analyze stops with exit 10 and
// reason matched_gtid_intervals instead of growing without bound.
func TestAnalyzeMatchedGTIDIntervalCeilingExitsTen(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	old := analyzer.MatchedGTIDIntervalLimit
	analyzer.MatchedGTIDIntervalLimit = 2
	t.Cleanup(func() { analyzer.MatchedGTIDIntervalLimit = old })
	selector, err := analyzer.ParseGTIDSelector(nil, []string{sid + ":2:4"})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	events := []binlog.RawEvent{{Timestamp: base, EventType: "FORMAT_DESCRIPTION", PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "8.0.36", ServerFlavor: "mysql"}}
	pos := int64(100)
	add := func(ev binlog.RawEvent) {
		ev.Timestamp, ev.PositionStart, ev.PositionEnd, ev.Position, ev.ServerFlavor = base, pos, pos+20, uint32(pos+20), "mysql"
		events = append(events, ev)
		pos += 20
	}
	for gno := 1; gno <= 5; gno++ {
		add(binlog.RawEvent{EventType: "GTID", GTID: fmt.Sprintf("%s:%d", sid, gno)})
		add(binlog.RawEvent{EventType: "QUERY", Query: "BEGIN"})
		add(binlog.RawEvent{EventType: "WRITE_ROWS", Schema: "shop", Table: "orders", RowCount: 1})
		add(binlog.RawEvent{EventType: "XID"})
	}
	for _, flashback := range []bool{false, true} {
		opts := analyzer.DefaultOptions()
		opts.GTIDSelector = selector
		opts.Flashback = flashback
		stdout, err := captureStdoutRun(t, func() error {
			return runAnalysisWithParser([]string{"synthetic.bin"}, opts, "json", &mockParser{events: events})
		})
		kind, details := clierr.Of(err)
		if ExitCode(err) != 10 || kind != clierr.KindResource || stdout != "" ||
			details["reason"] != "matched_gtid_intervals" || details["limit"] != 2 || details["intervals"] != 3 {
			t.Fatalf("flashback=%v: exit=%d kind=%v details=%v err=%v stdout=%q", flashback, ExitCode(err), kind, details, err, stdout)
		}
		if !strings.Contains(err.Error(), "--include-gtids/--exclude-gtids") {
			t.Fatalf("error does not say how to narrow the run: %v", err)
		}
	}
	// The message is localized (#381 QA F3).
	i18n.ResetForTesting()
	t.Cleanup(i18n.ResetForTesting)
	if err := i18n.Init("zh-CN"); err != nil {
		t.Fatal(err)
	}
	opts := analyzer.DefaultOptions()
	opts.GTIDSelector = selector
	_, err = captureStdoutRun(t, func() error {
		return runAnalysisWithParser([]string{"synthetic.bin"}, opts, "json", &mockParser{events: events})
	})
	if ExitCode(err) != 10 || !strings.Contains(err.Error(), "至少需要 3 个独立的 GTID 区间，最多只能跟踪 2 个") {
		t.Fatalf("zh-CN: exit=%d err=%v", ExitCode(err), err)
	}
}

// TestAnalyzeMatchedGTIDDroppedFormMessage pins the MariaDB fail-safe at the
// CLI (#384 QA F1/F2): once the per-server form is dropped, a matched
// transaction that repeats a sequence number stops analyze with the
// "dropped" message, in en and zh-CN, and its count includes that
// transaction: 0-8-6 joins 0-8-5 and 0-8-7, so 10, not 11.
func TestAnalyzeMatchedGTIDDroppedFormMessage(t *testing.T) {
	old := analyzer.MatchedGTIDIntervalLimit
	analyzer.MatchedGTIDIntervalLimit = 4
	t.Cleanup(func() { analyzer.MatchedGTIDIntervalLimit = old })
	selector, err := analyzer.ParseGTIDSelector(nil, []string{"0-8-3"})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	events := []binlog.RawEvent{{Timestamp: base, EventType: "FORMAT_DESCRIPTION", PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "10.11.6-MariaDB-log", ServerFlavor: "mariadb"}}
	pos := int64(100)
	add := func(ev binlog.RawEvent) {
		ev.Timestamp, ev.PositionStart, ev.PositionEnd, ev.Position, ev.ServerFlavor = base, pos, pos+20, uint32(pos+20), "mariadb"
		events = append(events, ev)
		pos += 20
	}
	txn := func(gtid string) {
		add(binlog.RawEvent{EventType: "GTID", GTID: gtid})
		add(binlog.RawEvent{EventType: "QUERY", Query: "BEGIN"})
		add(binlog.RawEvent{EventType: "WRITE_ROWS", Schema: "shop", Table: "orders", RowCount: 1})
		add(binlog.RawEvent{EventType: "XID"})
	}
	for seq := 1; seq <= 12; seq++ {
		txn(fmt.Sprintf("0-%d-%d", 7+seq%2, seq))
	}
	txn("0-8-6")
	run := func() error {
		opts := analyzer.DefaultOptions()
		opts.GTIDSelector = selector
		stdout, err := captureStdoutRun(t, func() error {
			return runAnalysisWithParser([]string{"synthetic.bin"}, opts, "json", &mockParser{events: events})
		})
		if stdout != "" {
			t.Fatalf("stdout %q", stdout)
		}
		return err
	}
	forceEnglishRuntimeOutput(t)
	err = run()
	kind, details := clierr.Of(err)
	if ExitCode(err) != 10 || kind != clierr.KindResource || details["reason"] != "matched_gtid_intervals" || details["limit"] != 4 || details["intervals"] != 10 {
		t.Fatalf("exit=%d kind=%v details=%v err=%v", ExitCode(err), kind, details, err)
	}
	if want := "need at least 10 separate GTID intervals kept per server, because a sequence number repeats; that form was dropped when all domains together passed 4"; !strings.Contains(err.Error(), want) {
		t.Fatalf("en message: %v", err)
	}
	i18n.ResetForTesting()
	t.Cleanup(i18n.ResetForTesting)
	if err := i18n.Init("zh-CN"); err != nil {
		t.Fatal(err)
	}
	if err := run(); ExitCode(err) != 10 || !strings.Contains(err.Error(), "至少需要 10 个按 server 记录的独立 GTID 区间") {
		t.Fatalf("zh-CN: exit=%d err=%v", ExitCode(err), err)
	}
}
