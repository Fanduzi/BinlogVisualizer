package binlogviz

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/binlog"
)

func TestAnalyzeTextDDLOnlyShowsTimeline(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 10, 2, 1, 0, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		mysqlCommandGTID(ts, 1, 100, 180),
		mysqlCommandQuery(ts.Add(time.Second), "ALTER TABLE app.orders ADD COLUMN marker INT", 180, 260),
	}
	stdout, stderr, err := runAnalyzeTextWithParser(t, events, analyzer.DefaultOptions())
	if err != nil {
		t.Fatalf("DDL-only must stay exit 0, got %v\nstderr=%s\nstdout=%s", err, stderr, stdout)
	}
	for _, want := range []string{
		"DDL Timeline",
		"DDL occurrence timeline (not MDL or lock-wait duration)",
		"ALTER TABLE",
		"app.orders",
		"mysql-bin.000001:180-260",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("missing %q\n%s", want, stdout)
		}
	}
}

func TestAnalyzeOpenBeginWithRowsNamesEvidence(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 10, 2, 3, 0, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		mysqlCommandGTID(ts, 9, 100, 160),
		mysqlCommandQuery(ts.Add(time.Second), "BEGIN", 160, 200),
		mysqlCommandRows(ts.Add(2*time.Second), 3, 200, 280),
		mysqlCommandGTID(ts.Add(50*time.Second), 10, 280, 340),
	}
	stdout, stderr, err := runAnalyzeTextWithParser(t, events, analyzer.DefaultOptions())
	if err == nil || ExitCode(err) != 1 || stdout != "" {
		t.Fatalf("exit %d err=%v stdout=%q", ExitCode(err), err, stdout)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("stderr=%s", stderr)
	}
	for _, want := range []string{
		"open BEGIN without close",
		"not lock-contention proof",
		"rows=3",
		"app.orders",
		"50s",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error missing %q: %v", want, err)
		}
	}
}

func TestAnalyzeEOFOpenDMLJSON(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 10, 2, 2, 0, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		mysqlCommandGTID(ts, 7, 100, 140),
		mysqlCommandQuery(ts.Add(time.Second), "BEGIN", 140, 180),
		mysqlCommandRows(ts.Add(45*time.Second), 4, 180, 420),
	}
	opts := analyzer.DefaultOptions()
	opts.LargeTxnDuration = 30 * time.Second
	stdout, stderr, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParser([]string{"dummy.binlog"}, opts, "json", &mockParser{events: events})
	})
	if err != nil {
		t.Fatalf("EOF open DML must stay exit 0, got %v\nstderr=%s", err, stderr)
	}
	var decoded struct {
		Diagnostics struct {
			OpenExplicitGroups int `json:"open_explicit_groups"`
			OpenDMLGroups      []struct {
				TxnKey    string         `json:"txn_key"`
				TotalRows int            `json:"total_rows"`
				Duration  string         `json:"duration"`
				Tables    map[string]int `json:"tables"`
				Note      string         `json:"note"`
				PosStart  int64          `json:"pos_start"`
				PosEnd    int64          `json:"pos_end"`
			} `json:"open_dml_groups"`
		} `json:"diagnostics"`
	}
	if jsonErr := json.Unmarshal([]byte(stdout), &decoded); jsonErr != nil {
		t.Fatalf("json: %v\n%s", jsonErr, stdout)
	}
	if decoded.Diagnostics.OpenExplicitGroups != 1 || len(decoded.Diagnostics.OpenDMLGroups) != 1 {
		t.Fatalf("diagnostics = %+v", decoded.Diagnostics)
	}
	group := decoded.Diagnostics.OpenDMLGroups[0]
	if group.TotalRows != 4 || group.Duration != "45s" || group.Tables["app.orders"] != 4 || group.PosStart == 0 || group.PosEnd == 0 {
		t.Fatalf("group = %+v", group)
	}
	if !strings.Contains(group.Note, "not lock-contention proof") {
		t.Fatalf("note = %q", group.Note)
	}
}

func TestAnalyzeTextShowsCommittedDuration(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 10, 2, 4, 0, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		mysqlCommandGTID(ts, 11, 100, 140),
		mysqlCommandQuery(ts.Add(time.Second), "BEGIN", 140, 180),
		mysqlCommandRows(ts.Add(2*time.Second), 8, 180, 260),
		mysqlCommandXID(ts.Add(3*time.Second), 260, 280),
		mysqlCommandGTID(ts.Add(time.Minute), 12, 280, 320),
		mysqlCommandQuery(ts.Add(time.Minute+time.Second), "BEGIN", 320, 360),
		mysqlCommandRows(ts.Add(time.Minute+44*time.Second), 1, 360, 9000),
		mysqlCommandXID(ts.Add(time.Minute+45*time.Second), 9000, 9080),
	}
	opts := analyzer.DefaultOptions()
	opts.LargeTxnRows = 1000
	opts.LargeTxnDuration = 30 * time.Second
	stdout, stderr, err := runAnalyzeTextWithParser(t, events, opts)
	if err != nil {
		t.Fatalf("analyze: %v\nstderr=%s\nstdout=%s", err, stderr, stdout)
	}
	for _, want := range []string{
		"Committed duration:",
		">=30s=1",
		"Longest transaction:",
		"dur=45.0s",
		"exceeds duration threshold (45s)",
		"Top txn bytes:",
	} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("missing %q\n%s", want, stdout)
		}
	}
}

func runAnalyzeTextWithParser(t *testing.T, events []binlog.RawEvent, opts analyzer.Options) (string, string, error) {
	t.Helper()
	return captureStdoutStderrRun(t, func() error {
		err := runAnalysisWithParser([]string{"dummy.binlog"}, opts, "text", &mockParser{events: events})
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error:", err)
		}
		return err
	})
}
