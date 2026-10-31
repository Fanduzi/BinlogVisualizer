// Package binlogviz tests the --thread-id selector at the CLI boundary (ADR 0027).
// input: analyze/flashback/redo flag strings and synthetic parser events with QUERY-carried thread ids.
// output: flag parse errors, selection.thread_ids report evidence, no-match exit 2, and relay-log prefix discovery.
// pos: command-level coverage for the thread selector; analyzer semantics live in internal/analyzer.
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

	"github.com/spf13/cobra"
)

func TestThreadIDFlagRejectsMalformedValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "non numeric", args: []string{"--thread-id", "abc"}},
		{name: "zero", args: []string{"--thread-id", "0"}},
		{name: "overflow", args: []string{"--thread-id", "4294967296"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, cmd := range []*cobra.Command{newAnalyzeCommand(), newFlashbackCommand(), newRedoCommand()} {
				cmd.SetArgs(append([]string{"missing.bin"}, tc.args...))
				err := cmd.Execute()
				if err == nil || !strings.Contains(err.Error(), "invalid --thread-id") {
					t.Fatalf("%s: err = %v, want invalid --thread-id", cmd.Name(), err)
				}
			}
		})
	}
}

func TestAnalyzeThreadIDSelectionReportRoundTrips(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	base := time.Date(2026, 11, 2, 14, 0, 0, 0, time.UTC)
	parser := &mockParser{events: []binlog.RawEvent{
		{Timestamp: base, EventType: "FORMAT_DESCRIPTION", PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "GTID", PositionStart: 100, PositionEnd: 120, Position: 120, GTID: sid + ":1", ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "QUERY", Query: "BEGIN", ThreadID: 41, PositionStart: 120, PositionEnd: 140, Position: 140, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "WRITE_ROWS", Schema: "shop", Table: "orders", RowCount: 1, PositionStart: 140, PositionEnd: 160, Position: 160, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "XID", XID: "9", PositionStart: 160, PositionEnd: 180, Position: 180, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(5 * time.Second), EventType: "GTID", PositionStart: 180, PositionEnd: 200, Position: 200, GTID: sid + ":2", ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(6 * time.Second), EventType: "QUERY", Query: "BEGIN", ThreadID: 42, PositionStart: 200, PositionEnd: 220, Position: 220, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(7 * time.Second), EventType: "WRITE_ROWS", Schema: "shop", Table: "orders", RowCount: 1, PositionStart: 220, PositionEnd: 240, Position: 240, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
		{Timestamp: base.Add(8 * time.Second), EventType: "XID", XID: "10", PositionStart: 240, PositionEnd: 260, Position: 260, ServerID: 7, ServerVersion: "8.0.36", ServerFlavor: "mysql"},
	}}
	opts := analyzer.DefaultOptions()
	opts.ThreadIDs = []uint32{41}
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
	if decoded.Selection == nil || len(decoded.Selection.ThreadIDs) != 1 || decoded.Selection.ThreadIDs[0] != 41 {
		t.Fatalf("selection = %+v", decoded.Selection)
	}
	if len(decoded.Transactions) != 1 || decoded.Transactions[0].ThreadID != 41 {
		t.Fatalf("transactions = %+v", decoded.Transactions)
	}
	// A thread selector is not a GTID selector: no matched GTID evidence.
	var raw struct {
		Selection map[string]json.RawMessage `json:"selection"`
	}
	if err := json.Unmarshal([]byte(stdout), &raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"matched_transactions", "matched_gtid_set", "include_gtids", "exclude_gtids"} {
		if value, ok := raw.Selection[key]; ok {
			t.Fatalf("thread-only selection has %s=%s", key, value)
		}
	}
}

func TestAnalyzeThreadIDNoMatchExitsTwoWithoutReport(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	base := time.Date(2026, 11, 2, 14, 30, 0, 0, time.UTC)
	parser := &mockParser{events: []binlog.RawEvent{
		{Timestamp: base, EventType: "FORMAT_DESCRIPTION", PositionStart: 4, PositionEnd: 100, Position: 100, ServerVersion: "8.0.36"},
		{Timestamp: base.Add(time.Second), EventType: "GTID", PositionStart: 100, PositionEnd: 120, Position: 120, GTID: sid + ":2", ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "QUERY", Query: "BEGIN", ThreadID: 42, PositionStart: 120, PositionEnd: 140, Position: 140, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "WRITE_ROWS", Schema: "shop", Table: "orders", RowCount: 1, PositionStart: 140, PositionEnd: 160, Position: 160, ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "XID", PositionStart: 160, PositionEnd: 180, Position: 180, ServerFlavor: "mysql"},
	}}
	opts := analyzer.DefaultOptions()
	opts.ThreadIDs = []uint32{41}
	stdout, err := captureStdoutRun(t, func() error {
		return runAnalysisWithParser([]string{"synthetic.bin"}, opts, "json", parser)
	})
	if ExitCode(err) != 2 || stdout != "" {
		t.Fatalf("exit=%d err=%v stdout=%q", ExitCode(err), err, stdout)
	}
}

// ADR 0028: --prefix matches relay-bin.NNNNNN the same way it matches
// mysql-bin.NNNNNN — a relay log is a valid input while the server keeps it.
// Subdirectories and non-matching names stay excluded.
func TestDiscoverBinlogPathsMatchesRelayLogPrefix(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"relay-bin.000002", "relay-bin.000001", "mysql-bin.000001", "relay-bin.index"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "relay-bin.000007"), 0o755); err != nil {
		t.Fatal(err)
	}
	paths, err := discoverBinlogPaths(dir, "relay-bin")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || filepath.Base(paths[0]) != "relay-bin.000001" || filepath.Base(paths[1]) != "relay-bin.000002" {
		t.Fatalf("paths = %v, want relay-bin.000001, relay-bin.000002 in numeric order", paths)
	}
}
