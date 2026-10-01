// Package binlogviz verifies GTID groups that DBAs close with plain ROLLBACK or XA END.
// input: injected QUERY events through runAnalysisWithParser, ROW images, and a following business GTID.
// output: exit 0 and the following business transaction for plain ROLLBACK and for XA END with no later XA close; zero-row ROLLBACK stays off the report; exit 1 open BEGIN, SAVEPOINT-not-a-close, and Ignored-only next-GTID errors; JSON open_explicit_groups when BEGIN reaches EOF.
// pos: operator I/O seam for the plain-ROLLBACK and XA-END release rules.
// note: if this file changes, update this header and module README.md.
package binlogviz

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/binlog"
	"binlogviz/internal/i18n"
)

func TestAnalyzePlainRollbackThenBusinessExitsZero(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	for _, query := range []string{"ROLLBACK", "ROLLBACK WORK", "ROLLBACK;"} {
		t.Run(query, func(t *testing.T) {
			stdout, stderr, err := runAnalyzeLikeMainWithParser(t, rollbackThenBusinessEvents(query, false))
			if err != nil {
				t.Fatalf("analyze exit %d: %v\nstderr=%s", ExitCode(err), err, stderr)
			}
			if strings.Contains(stderr, "conflicting GTID") || strings.Contains(stdout, "conflicting GTID") {
				t.Fatalf("plain rollback must not abort, stderr=%s", stderr)
			}
			assertRolledBackGroupOmitted(t, stdout, mysqlIssue74SID+":40", 1)
		})
	}
}

func TestAnalyzeRollbackWithRowsThenBusinessExitsZero(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, rollbackThenBusinessEvents("ROLLBACK", true))
	if err != nil {
		t.Fatalf("analyze exit %d: %v\nstderr=%s", ExitCode(err), err, stderr)
	}
	var decoded struct {
		Summary struct {
			TotalTransactions int `json:"total_transactions"`
			TotalRows         int `json:"total_rows"`
		} `json:"summary"`
		Transactions []struct {
			GTID      string `json:"gtid"`
			TotalRows int    `json:"total_rows"`
		} `json:"transactions"`
	}
	if jsonErr := json.Unmarshal([]byte(stdout), &decoded); jsonErr != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", jsonErr, stdout)
	}
	if decoded.Summary.TotalTransactions != 2 || decoded.Summary.TotalRows != 5 {
		t.Fatalf("want rolled-back rows plus the next GTID, got txns=%d rows=%d stderr=%s", decoded.Summary.TotalTransactions, decoded.Summary.TotalRows, stderr)
	}
	byGTID := map[string]int{}
	for _, txn := range decoded.Transactions {
		byGTID[txn.GTID] = txn.TotalRows
	}
	if byGTID[mysqlIssue74SID+":39"] != 4 || byGTID[mysqlIssue74SID+":40"] != 1 {
		t.Fatalf("transactions = %+v", decoded.Transactions)
	}
}

func TestAnalyzeRollbackToSavepointThenNextGTIDStillConflicts(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, rollbackThenBusinessEvents("ROLLBACK TO SAVEPOINT s1", false))
	if err == nil || ExitCode(err) != 1 {
		t.Fatalf("exit %d err=%v\nstderr=%s\nstdout=%s", ExitCode(err), err, stderr, stdout)
	}
	if !strings.Contains(err.Error(), "open BEGIN without close") || !strings.Contains(err.Error(), "ROLLBACK TO SAVEPOINT is not a group close") {
		t.Fatalf("error must name the open BEGIN and the SAVEPOINT rollback, got %v", err)
	}
	if strings.Contains(err.Error(), "conflicting GTID") {
		t.Fatalf("must not say conflicting GTID, got %v", err)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected Error: once, got %q", stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want empty", stdout)
	}
}

func TestAnalyzeBeginThenNextGTIDStillConflicts(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 9, 16, 9, 0, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		mysqlCommandGTID(ts, 39, 100, 180),
		mysqlCommandQuery(ts.Add(time.Second), "BEGIN", 180, 220),
		mysqlCommandGTID(ts.Add(2*time.Second), 40, 220, 300),
		mysqlCommandQuery(ts.Add(3*time.Second), "BEGIN", 300, 340),
		mysqlCommandRows(ts.Add(4*time.Second), 1, 340, 420),
		mysqlCommandXID(ts.Add(5*time.Second), 420, 440),
	}
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, events)
	if err == nil || ExitCode(err) != 1 || !strings.Contains(err.Error(), "open BEGIN without close") {
		t.Fatalf("exit %d err=%v stdout=%s", ExitCode(err), err, stdout)
	}
	if strings.Contains(err.Error(), "conflicting GTID") || strings.Contains(err.Error(), "SAVEPOINT") || strings.Contains(err.Error(), "Ignored QUERY") {
		t.Fatalf("bare BEGIN error must not look like a GTID conflict, SAVEPOINT, or Ignored QUERY, got %v", err)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected Error: once, got %q", stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q, want empty", stdout)
	}
}

func TestAnalyzeIgnoredOnlyGroupThenNextGTIDNamesIgnoredQuery(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 9, 16, 9, 30, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		mysqlCommandGTID(ts, 39, 100, 180),
		mysqlCommandQuery(ts.Add(time.Second), "SET timestamp=1710000000", 180, 220),
		mysqlCommandGTID(ts.Add(2*time.Second), 40, 220, 300),
		mysqlCommandQuery(ts.Add(3*time.Second), "BEGIN", 300, 340),
		mysqlCommandRows(ts.Add(4*time.Second), 1, 340, 420),
		mysqlCommandXID(ts.Add(5*time.Second), 420, 440),
	}
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, events)
	if err == nil || ExitCode(err) != 1 {
		t.Fatalf("exit %d err=%v stdout=%s", ExitCode(err), err, stdout)
	}
	if !strings.Contains(err.Error(), "Ignored QUERY does not close the transaction group") || !strings.Contains(err.Error(), "not a missing COMMIT") {
		t.Fatalf("error must say Ignored QUERY is not a close and not a missing COMMIT, got %v", err)
	}
	if strings.Contains(err.Error(), "conflicting GTID") || strings.Contains(err.Error(), "open BEGIN") {
		t.Fatalf("Ignored-only error must not look like a GTID conflict or an open BEGIN, got %v", err)
	}
	if strings.Count(stderr, "Error:") != 1 || stdout != "" {
		t.Fatalf("stdout=%q stderr=%s", stdout, stderr)
	}
}

func TestAnalyzeSavepointThenCommitStillCloses(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 9, 16, 9, 40, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		mysqlCommandGTID(ts, 39, 100, 180),
		mysqlCommandQuery(ts.Add(time.Second), "BEGIN", 180, 220),
		mysqlCommandQuery(ts.Add(2*time.Second), "ROLLBACK TO SAVEPOINT s1", 220, 260),
		mysqlCommandXID(ts.Add(3*time.Second), 260, 280),
		mysqlCommandGTID(ts.Add(4*time.Second), 40, 280, 360),
		mysqlCommandQuery(ts.Add(5*time.Second), "BEGIN", 360, 400),
		mysqlCommandRows(ts.Add(6*time.Second), 1, 400, 480),
		mysqlCommandXID(ts.Add(7*time.Second), 480, 500),
	}
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, events)
	if err != nil {
		t.Fatalf("COMMIT after SAVEPOINT rollback must close the group, exit %d: %v\nstderr=%s", ExitCode(err), err, stderr)
	}
	assertRolledBackGroupOmitted(t, stdout, mysqlIssue74SID+":40", 1)
}

func TestAnalyzeEOFOpenBeginCountsExplicitGroup(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 9, 16, 9, 50, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		mysqlCommandGTID(ts, 39, 100, 180),
		mysqlCommandQuery(ts.Add(time.Second), "BEGIN", 180, 220),
		mysqlCommandRows(ts.Add(2*time.Second), 2, 220, 300),
	}
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, events)
	if err != nil {
		t.Fatalf("end of input must not fail the open BEGIN, exit %d: %v\nstderr=%s", ExitCode(err), err, stderr)
	}
	var decoded struct {
		Diagnostics struct {
			OpenExplicitGroups int `json:"open_explicit_groups"`
		} `json:"diagnostics"`
	}
	if jsonErr := json.Unmarshal([]byte(stdout), &decoded); jsonErr != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", jsonErr, stdout)
	}
	if decoded.Diagnostics.OpenExplicitGroups != 1 {
		t.Fatalf("open_explicit_groups=%d, want 1\n%s", decoded.Diagnostics.OpenExplicitGroups, stdout)
	}
}

func TestAnalyzeOpenBeginErrorIsLocalized(t *testing.T) {
	i18n.ResetForTesting()
	if err := i18n.Init("zh-CN"); err != nil {
		t.Fatalf("init zh-CN: %v", err)
	}
	t.Cleanup(i18n.ResetForTesting)

	ts := time.Date(2026, 9, 16, 9, 0, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		mysqlCommandGTID(ts, 39, 100, 180),
		mysqlCommandQuery(ts.Add(time.Second), "BEGIN", 180, 220),
		mysqlCommandGTID(ts.Add(2*time.Second), 40, 220, 300),
	}
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, events)
	if err == nil || ExitCode(err) != 1 || stdout != "" {
		t.Fatalf("exit %d err=%v stdout=%q", ExitCode(err), err, stdout)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected Error: once, got %q", stderr)
	}
	if !strings.Contains(err.Error(), "显式 BEGIN 未关闭") || strings.Contains(err.Error(), "open BEGIN without close") {
		t.Fatalf("zh-CN error must name the unclosed BEGIN, got %v", err)
	}
	if strings.Contains(err.Error(), "conflicting GTID") {
		t.Fatalf("must not say conflicting GTID, got %v", err)
	}
}

func TestAnalyzeXAEndThenNextGTIDExitsZero(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, xaEndThenBusinessEvents())
	if err != nil {
		t.Fatalf("analyze exit %d: %v\nstderr=%s", ExitCode(err), err, stderr)
	}
	if strings.Contains(stderr, "conflicting GTID") {
		t.Fatalf("XA END then next GTID must not abort, stderr=%s", stderr)
	}
	var decoded struct {
		Summary struct {
			TotalTransactions int `json:"total_transactions"`
			TotalRows         int `json:"total_rows"`
		} `json:"summary"`
		Transactions []struct {
			GTID         string `json:"gtid"`
			TotalRows    int    `json:"total_rows"`
			XAXID        string `json:"xa_xid"`
			Completeness string `json:"completeness"`
			PosEnd       int64  `json:"pos_end"`
		} `json:"transactions"`
	}
	if jsonErr := json.Unmarshal([]byte(stdout), &decoded); jsonErr != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", jsonErr, stdout)
	}
	if decoded.Summary.TotalTransactions != 2 || decoded.Summary.TotalRows != 3 {
		t.Fatalf("want XA rows plus the next GTID, got txns=%d rows=%d body=%s", decoded.Summary.TotalTransactions, decoded.Summary.TotalRows, stdout)
	}
	byGTID := map[string]struct {
		rows int
		xid  string
		end  int64
		comp string
	}{}
	for _, txn := range decoded.Transactions {
		byGTID[txn.GTID] = struct {
			rows int
			xid  string
			end  int64
			comp string
		}{txn.TotalRows, txn.XAXID, txn.PosEnd, txn.Completeness}
	}
	xa := byGTID[mysqlIssue74SID+":39"]
	if xa.rows != 2 || xa.xid != "'batch-end'" || xa.end != 420 || xa.comp != "complete" {
		t.Fatalf("XA group = %+v, want 2 rows, xid 'batch-end', position_end 420, complete", xa)
	}
	if byGTID[mysqlIssue74SID+":40"].rows != 1 {
		t.Fatalf("following GTID = %+v", decoded.Transactions)
	}
}

func assertRolledBackGroupOmitted(t *testing.T, stdout, businessGTID string, rows int) {
	t.Helper()
	var decoded struct {
		Summary struct {
			TotalTransactions int `json:"total_transactions"`
			TotalRows         int `json:"total_rows"`
		} `json:"summary"`
		Transactions []struct {
			GTID string `json:"gtid"`
		} `json:"transactions"`
	}
	if err := json.Unmarshal([]byte(stdout), &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", err, stdout)
	}
	if decoded.Summary.TotalTransactions != 1 || decoded.Summary.TotalRows != rows {
		t.Fatalf("want one business transaction with %d rows, got txns=%d rows=%d", rows, decoded.Summary.TotalTransactions, decoded.Summary.TotalRows)
	}
	if len(decoded.Transactions) != 1 || decoded.Transactions[0].GTID != businessGTID {
		t.Fatalf("transactions = %+v, want only %s", decoded.Transactions, businessGTID)
	}
}

func rollbackThenBusinessEvents(rollback string, withRows bool) []binlog.RawEvent {
	ts := time.Date(2026, 9, 16, 9, 0, 0, 0, time.UTC)
	events := []binlog.RawEvent{
		mysqlCommandGTID(ts, 39, 100, 180),
		mysqlCommandQuery(ts.Add(time.Second), "BEGIN", 180, 220),
	}
	pos := int64(220)
	if withRows {
		events = append(events, mysqlCommandRows(ts.Add(2*time.Second), 4, pos, pos+80))
		pos += 80
	}
	events = append(events,
		mysqlCommandQuery(ts.Add(3*time.Second), rollback, pos, pos+40),
		mysqlCommandGTID(ts.Add(4*time.Second), 40, pos+40, pos+120),
		mysqlCommandQuery(ts.Add(5*time.Second), "BEGIN", pos+120, pos+160),
		mysqlCommandRows(ts.Add(6*time.Second), 1, pos+160, pos+260),
		mysqlCommandXID(ts.Add(7*time.Second), pos+260, pos+280),
	)
	return events
}

func xaEndThenBusinessEvents() []binlog.RawEvent {
	ts := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	return []binlog.RawEvent{
		mysqlCommandGTID(ts, 39, 100, 140),
		mysqlCommandQuery(ts.Add(time.Second), "XA START 'batch-end'", 140, 180),
		mysqlCommandRows(ts.Add(2*time.Second), 2, 180, 400),
		mysqlCommandQuery(ts.Add(3*time.Second), "XA END 'batch-end'", 400, 420),
		mysqlCommandGTID(ts.Add(4*time.Second), 40, 420, 460),
		mysqlCommandQuery(ts.Add(5*time.Second), "BEGIN", 460, 500),
		mysqlCommandRows(ts.Add(6*time.Second), 1, 500, 580),
		mysqlCommandXID(ts.Add(7*time.Second), 580, 600),
	}
}
