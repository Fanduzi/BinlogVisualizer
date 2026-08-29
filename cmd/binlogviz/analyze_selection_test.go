// Package binlogviz tests analyze selection contracts through the CLI and streaming pipeline.
// input: analyze commands, synthetic parser events, position/GTID selectors, and report renderers.
// output: regressions for exact position boundaries, transaction-group GTID filtering, no-data, and report evidence.
// pos: public analyze selection acceptance coverage for issue #52.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
	"binlogviz/internal/compare"
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
	if err == nil || !strings.Contains(err.Error(), "exactly one explicit binlog file") || !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("error = %v, want explicit regular-file position error", err)
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
		{Timestamp: base, EventType: "FormatDescriptionEvent", BinlogPath: path, PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "8.0.36"},
		{Timestamp: base.Add(time.Second), EventType: "GTIDEvent", BinlogPath: path, PositionStart: 100, PositionEnd: 120, Position: 120, GTID: "24bc7850-2c16-11e6-a073-0242ac110002:7", ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "QueryEvent", Query: "BEGIN", BinlogPath: path, PositionStart: 120, PositionEnd: 140, Position: 140, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "TableMapEvent", Schema: "shop", Table: "orders", BinlogPath: path, PositionStart: 140, PositionEnd: 160, Position: 160, ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "WriteRowsEventV2", Schema: "shop", Table: "orders", RowCount: 2, BinlogPath: path, PositionStart: 160, PositionEnd: 180, Position: 180, ServerFlavor: "mysql"},
		{Timestamp: base.Add(5 * time.Second), EventType: "XIDEvent", XID: "9", BinlogPath: path, PositionStart: 180, PositionEnd: 200, Position: 200, ServerFlavor: "mysql"},
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
}

func TestAnalyzeTimeOnlyReportOmitsSelection(t *testing.T) {
	start := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	opts := analyzer.DefaultOptions()
	opts.Start = &start
	parser := &mockParser{events: []binlog.RawEvent{
		{Timestamp: start, EventType: "QueryEvent", Query: "CREATE TABLE shop.orders (id INT)", PositionStart: 100, PositionEnd: 140, Position: 140},
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
		{Timestamp: base, EventType: "FormatDescriptionEvent", BinlogPath: path, PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "8.0.36"},
		{Timestamp: base.Add(time.Second), EventType: "QueryEvent", Query: "BEGIN", BinlogPath: path, PositionStart: 100, PositionEnd: 120, Position: 120},
		{Timestamp: base.Add(2 * time.Second), EventType: "WriteRowsEventV2", Schema: "shop", Table: "orders", RowCount: 1, BinlogPath: path, PositionStart: 120, PositionEnd: 180, Position: 180},
		{Timestamp: base.Add(3 * time.Second), EventType: "XIDEvent", BinlogPath: path, PositionStart: 180, PositionEnd: 200, Position: 200},
	}
	tests := []struct {
		name     string
		start    *int64
		stop     *int64
		wantText string
		wantExit int
	}{
		{name: "mid event", start: int64Pointer(150), wantText: "not an exact event boundary", wantExit: 1},
		{name: "out of range", stop: int64Pointer(201), wantText: "out of range", wantExit: 1},
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
		{Timestamp: base, EventType: "FormatDescriptionEvent", PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "8.0.36"},
		{Timestamp: base.Add(time.Second), EventType: "GTIDEvent", PositionStart: 100, PositionEnd: 120, Position: 120, GTID: sid + ":2", ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "QueryEvent", Query: "BEGIN", PositionStart: 120, PositionEnd: 140, Position: 140, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "WriteRowsEventV2", Schema: "shop", Table: "orders", RowCount: 1, PositionStart: 140, PositionEnd: 160, Position: 160, ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "XIDEvent", PositionStart: 160, PositionEnd: 180, Position: 180, ServerFlavor: "mysql"},
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
		{Timestamp: base, EventType: "FormatDescriptionEvent", PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "GTIDEvent", PositionStart: 100, PositionEnd: 120, Position: 120, GTID: sid + ":1", ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "QueryEvent", Query: "BEGIN", PositionStart: 120, PositionEnd: 140, Position: 140, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "RowsQueryEvent", QuerySQL: "INSERT INTO shop.orders VALUES (1)", PositionStart: 140, PositionEnd: 160, Position: 160, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "WriteRowsEventV2", Schema: "shop", Table: "orders", RowCount: 1, PositionStart: 160, PositionEnd: 180, Position: 180, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(5 * time.Second), EventType: "XIDEvent", XID: "9", PositionStart: 180, PositionEnd: 200, Position: 200, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
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
	if decoded.Selection == nil || decoded.Selection.ResolvedGTIDFlavor != "mysql" || len(decoded.Selection.IncludeGTIDs) != 1 || decoded.Selection.IncludeGTIDs[0] != sid+":1-3" || len(decoded.Selection.ExcludeGTIDs) != 1 || decoded.Selection.ExcludeGTIDs[0] != sid+":2" || len(decoded.Selection.MatchedGTIDs) != 1 || decoded.Selection.MatchedGTIDs[0] != sid+":1" {
		t.Fatalf("selection = %+v", decoded.Selection)
	}
	if len(decoded.Transactions) != 1 || decoded.Transactions[0].GTID != sid+":1" || decoded.Transactions[0].Completeness != "complete" || decoded.Provenance == nil || len(decoded.Provenance.ServerIDs) != 1 || decoded.Transactions[0].QuerySummary == "" {
		t.Fatalf("prior report fields lost: provenance=%+v transactions=%+v", decoded.Provenance, decoded.Transactions)
	}
}
