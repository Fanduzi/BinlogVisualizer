// Package binlogviz verifies Unclassified QUERY and Ignored QUERY command I/O.
// input: injected mock parser events through runAnalysisWithParser.
// output: exit 1 Unclassified QUERY (empty stdout, one Error: line, statement prefix, English and zh-CN), including anonymous empty-identity groups on the next GTID and at EOF, exit 2 ADMIN-only no-data, Ignored QUERY JSON count distinct from ignored_query_dml_events, and ADMIN-then-business exit 0.
// pos: #74/#78 operator I/O seam for ADR-0001/0003 QUERY classes.
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
	"binlogviz/internal/i18n"
)

const mysqlIssue74SID = "67451bcd-b010-11f1-a85a-822b383dbcd0"

func TestAnalyzeUnclassifiedQueryExitsOneWithPrefix(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, unclassifiedCheckTableEvents())
	if err == nil {
		t.Fatal("Unclassified QUERY must fail analyze")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit=%d, want 1; err=%v", got, err)
	}
	if stdout != "" {
		t.Fatalf("Unclassified QUERY stdout must be empty, got %q", stdout)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected Error: once, got %q", stderr)
	}
	assertNoUsageDump(t, stderr)
	if strings.Contains(err.Error(), "conflicting GTID") {
		t.Fatalf("must not say conflicting GTID, got %v", err)
	}
	if !strings.Contains(err.Error(), "Unclassified QUERY") || !strings.Contains(err.Error(), "CHECK TABLE app.orders") {
		t.Fatalf("English error must include Unclassified QUERY and the prefix, got %v", err)
	}
}

func TestAnalyzeUnclassifiedQueryErrorIsLocalized(t *testing.T) {
	i18n.ResetForTesting()
	if err := i18n.Init("zh-CN"); err != nil {
		t.Fatalf("init zh-CN: %v", err)
	}
	t.Cleanup(i18n.ResetForTesting)

	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, unclassifiedCheckTableEvents())
	if err == nil {
		t.Fatal("Unclassified QUERY must fail analyze")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit=%d, want 1; err=%v", got, err)
	}
	if stdout != "" {
		t.Fatalf("zh-CN Unclassified QUERY stdout must be empty, got %q", stdout)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected Error: once, got %q", stderr)
	}
	assertNoUsageDump(t, stderr)
	if strings.Contains(err.Error(), "conflicting GTID") {
		t.Fatalf("must not say conflicting GTID, got %v", err)
	}
	if !strings.Contains(err.Error(), "未分类") || !strings.Contains(err.Error(), "CHECK TABLE app.orders") {
		t.Fatalf("zh-CN error must include 未分类 and the prefix, got %v", err)
	}
}

func TestAnalyzeAnonymousUnclassifiedQueryExitsOneWithPrefix(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	events := unclassifiedCheckTableEvents()
	events[0] = mysqlAnonymousCommandGTID(events[0].Timestamp, events[0].PositionStart, events[0].PositionEnd)
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, events)
	if err == nil {
		t.Fatal("anonymous Unclassified QUERY must fail analyze")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit=%d, want 1; err=%v", got, err)
	}
	if stdout != "" {
		t.Fatalf("anonymous Unclassified QUERY stdout must be empty, got %q", stdout)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected Error: once, got %q", stderr)
	}
	assertNoUsageDump(t, stderr)
	if strings.Contains(err.Error(), "conflicting GTID") {
		t.Fatalf("must not say conflicting GTID, got %v", err)
	}
	if !strings.Contains(err.Error(), "Unclassified QUERY") || !strings.Contains(err.Error(), "CHECK TABLE app.orders") {
		t.Fatalf("English error must include Unclassified QUERY and the prefix, got %v", err)
	}
}

func TestAnalyzeFinalizeAnonymousUnclassifiedOnlyExitsOne(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 9, 16, 14, 30, 0, 0, time.UTC)
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, []binlog.RawEvent{
		mysqlAnonymousCommandGTID(ts, 100, 180),
		mysqlCommandQuery(ts.Add(time.Second), "SET ROLE ALL", 180, 260),
	})
	if err == nil {
		t.Fatal("finalize anonymous Unclassified QUERY must fail")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit=%d, want 1; err=%v", got, err)
	}
	if stdout != "" {
		t.Fatalf("finalize anonymous Unclassified QUERY stdout must be empty, got %q", stdout)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected Error: once, got %q", stderr)
	}
	assertNoUsageDump(t, stderr)
	if strings.Contains(err.Error(), "conflicting GTID") || strings.Contains(err.Error(), "no analyzable events") {
		t.Fatalf("anonymous finalize must be Unclassified QUERY, got %v", err)
	}
	if !strings.Contains(err.Error(), "Unclassified QUERY") || !strings.Contains(err.Error(), "SET ROLE ALL") {
		t.Fatalf("anonymous finalize error must include Unclassified QUERY and the prefix, got %v", err)
	}
}

func TestAnalyzeFinalizeUnclassifiedOnlyExitsOne(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 9, 16, 9, 30, 0, 0, time.UTC)
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, []binlog.RawEvent{
		mysqlCommandGTID(ts, 39, 100, 180),
		mysqlCommandQuery(ts.Add(time.Second), "SET ROLE ALL", 180, 260),
	})
	if err == nil {
		t.Fatal("finalize Unclassified QUERY must fail")
	}
	if got := ExitCode(err); got != 1 {
		t.Fatalf("exit=%d, want 1; err=%v", got, err)
	}
	if stdout != "" {
		t.Fatalf("finalize Unclassified QUERY stdout must be empty, got %q", stdout)
	}
	if strings.Count(stderr, "Error:") != 1 {
		t.Fatalf("expected Error: once, got %q", stderr)
	}
	assertNoUsageDump(t, stderr)
	if strings.Contains(err.Error(), "conflicting GTID") || strings.Contains(err.Error(), "no analyzable events") {
		t.Fatalf("finalize must be Unclassified QUERY, got %v", err)
	}
	if !strings.Contains(err.Error(), "Unclassified QUERY") || !strings.Contains(err.Error(), "SET ROLE ALL") {
		t.Fatalf("finalize error must include Unclassified QUERY and the prefix, got %v", err)
	}
}

func TestAnalyzeAdminOnlyFileExitsNoData(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	stdout, stderr, err := runAnalyzeLikeMainWithParser(t, []binlog.RawEvent{
		mysqlCommandGTID(ts, 39, 100, 180),
		mysqlCommandQuery(ts.Add(time.Second), "FLUSH PRIVILEGES", 180, 240),
	})
	assertAnalyzeNoDataExit(t, stdout, stderr, err, "no analyzable events")
}

func TestAnalyzeAdminThenBusinessExitsZero(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 9, 16, 11, 0, 0, 0, time.UTC)
	stdout, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParser([]string{"dummy.binlog"}, analyzer.DefaultOptions(), "json", &mockParser{
			events: []binlog.RawEvent{
				mysqlCommandGTID(ts, 39, 100, 180),
				mysqlCommandQuery(ts.Add(time.Second), "ANALYZE TABLE app.orders", 180, 240),
				mysqlCommandGTID(ts.Add(2*time.Second), 40, 240, 320),
				mysqlCommandQuery(ts.Add(3*time.Second), "BEGIN", 320, 360),
				{
					Timestamp:     ts.Add(4 * time.Second),
					EventType:     "WRITE_ROWS",
					Schema:        "app",
					Table:         "orders",
					RowCount:      3,
					ServerFlavor:  "mysql",
					BinlogPath:    "mysql-bin.000001",
					PositionStart: 360,
					PositionEnd:   480,
					BinlogBytes:   120,
				},
				{
					Timestamp:     ts.Add(5 * time.Second),
					EventType:     "XID",
					ServerFlavor:  "mysql",
					BinlogPath:    "mysql-bin.000001",
					PositionStart: 480,
					PositionEnd:   500,
					BinlogBytes:   20,
				},
			},
		})
	})
	if err != nil {
		t.Fatalf("ADMIN then business must exit 0, got %v", err)
	}
	var decoded struct {
		Summary struct {
			TotalTransactions int `json:"total_transactions"`
			TotalRows         int `json:"total_rows"`
		} `json:"summary"`
		Transactions []struct {
			GTID string `json:"gtid"`
		} `json:"transactions"`
		Diagnostics struct {
			DDLEvents []any `json:"ddl_events"`
		} `json:"diagnostics"`
	}
	if jsonErr := json.Unmarshal([]byte(stdout), &decoded); jsonErr != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", jsonErr, stdout)
	}
	if decoded.Summary.TotalTransactions != 1 || decoded.Summary.TotalRows != 3 {
		t.Fatalf("want one business transaction with 3 rows, got txns=%d rows=%d", decoded.Summary.TotalTransactions, decoded.Summary.TotalRows)
	}
	if len(decoded.Transactions) != 1 || decoded.Transactions[0].GTID != mysqlIssue74SID+":40" {
		t.Fatalf("business GTID = %+v, want :40", decoded.Transactions)
	}
	if len(decoded.Diagnostics.DDLEvents) != 0 {
		t.Fatalf("ADMIN must stay off the DDL timeline, got %+v", decoded.Diagnostics.DDLEvents)
	}
}

func TestAnalyzeJSONCountsIgnoredQuerySeparatelyFromQueryDML(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	ts := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	stdout, _, err := captureStdoutStderrRun(t, func() error {
		return runAnalysisWithParser([]string{"dummy.binlog"}, analyzer.DefaultOptions(), "json", &mockParser{
			events: []binlog.RawEvent{
				mysqlCommandGTID(ts, 39, 100, 180),
				mysqlCommandQuery(ts.Add(time.Second), "SET timestamp=1710000000", 180, 220),
				mysqlCommandQuery(ts.Add(2*time.Second), "SET NAMES utf8mb4", 220, 260),
				mysqlCommandQuery(ts.Add(3*time.Second), "BEGIN", 260, 300),
				{
					Timestamp:     ts.Add(4 * time.Second),
					EventType:     "WRITE_ROWS",
					Schema:        "app",
					Table:         "orders",
					RowCount:      1,
					ServerFlavor:  "mysql",
					BinlogPath:    "mysql-bin.000001",
					PositionStart: 300,
					PositionEnd:   400,
					BinlogBytes:   100,
				},
				{
					Timestamp:     ts.Add(5 * time.Second),
					EventType:     "XID",
					ServerFlavor:  "mysql",
					BinlogPath:    "mysql-bin.000001",
					PositionStart: 400,
					PositionEnd:   420,
					BinlogBytes:   20,
				},
			},
		})
	})
	if err != nil {
		t.Fatalf("Ignored QUERY before BEGIN must succeed, got %v", err)
	}
	var decoded struct {
		Summary struct {
			TotalTransactions int `json:"total_transactions"`
			TotalRows         int `json:"total_rows"`
		} `json:"summary"`
		Diagnostics struct {
			IgnoredQueryDMLEvents int `json:"ignored_query_dml_events"`
			IgnoredQueryEvents    int `json:"ignored_query_events"`
			UnmappedEvents        int `json:"unmapped_events"`
		} `json:"diagnostics"`
	}
	if jsonErr := json.Unmarshal([]byte(stdout), &decoded); jsonErr != nil {
		t.Fatalf("json.Unmarshal: %v\n%s", jsonErr, stdout)
	}
	if decoded.Summary.TotalTransactions != 1 || decoded.Summary.TotalRows != 1 {
		t.Fatalf("want one attached business transaction, got txns=%d rows=%d", decoded.Summary.TotalTransactions, decoded.Summary.TotalRows)
	}
	if decoded.Diagnostics.IgnoredQueryEvents != 2 {
		t.Fatalf("ignored_query_events=%d, want 2", decoded.Diagnostics.IgnoredQueryEvents)
	}
	if decoded.Diagnostics.IgnoredQueryDMLEvents != 0 {
		t.Fatalf("ignored_query_dml_events=%d, want 0", decoded.Diagnostics.IgnoredQueryDMLEvents)
	}
	if decoded.Diagnostics.UnmappedEvents != 0 {
		t.Fatalf("unmapped_events=%d, want 0", decoded.Diagnostics.UnmappedEvents)
	}
}

func runAnalyzeLikeMainWithParser(t *testing.T, events []binlog.RawEvent) (string, string, error) {
	t.Helper()
	return captureStdoutStderrRun(t, func() error {
		err := runAnalysisWithParser([]string{"dummy.binlog"}, analyzer.DefaultOptions(), "json", &mockParser{events: events})
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error:", err)
		}
		return err
	})
}

func unclassifiedCheckTableEvents() []binlog.RawEvent {
	ts := time.Date(2026, 9, 16, 9, 0, 0, 0, time.UTC)
	return []binlog.RawEvent{
		mysqlCommandGTID(ts, 39, 100, 180),
		mysqlCommandQuery(ts.Add(time.Second), "CHECK TABLE app.orders", 180, 260),
		mysqlCommandGTID(ts.Add(2*time.Second), 40, 260, 340),
		mysqlCommandQuery(ts.Add(3*time.Second), "BEGIN", 340, 380),
		{
			Timestamp:     ts.Add(4 * time.Second),
			EventType:     "WRITE_ROWS",
			Schema:        "app",
			Table:         "orders",
			RowCount:      1,
			ServerFlavor:  "mysql",
			BinlogPath:    "mysql-bin.000001",
			PositionStart: 380,
			PositionEnd:   480,
			BinlogBytes:   100,
		},
		{
			Timestamp:     ts.Add(5 * time.Second),
			EventType:     "XID",
			ServerFlavor:  "mysql",
			BinlogPath:    "mysql-bin.000001",
			PositionStart: 480,
			PositionEnd:   500,
			BinlogBytes:   20,
		},
	}
}

func mysqlAnonymousCommandGTID(ts time.Time, start, end int64) binlog.RawEvent {
	return binlog.RawEvent{
		Timestamp:     ts,
		EventType:     "GTID",
		ServerFlavor:  "mysql",
		BinlogPath:    "mysql-bin.000001",
		PositionStart: start,
		PositionEnd:   end,
		BinlogBytes:   end - start,
	}
}

func mysqlCommandGTID(ts time.Time, seq int, start, end int64) binlog.RawEvent {
	return binlog.RawEvent{
		Timestamp:     ts,
		EventType:     "GTID",
		GTID:          mysqlIssue74SID + ":" + strconv.Itoa(seq),
		ServerFlavor:  "mysql",
		BinlogPath:    "mysql-bin.000001",
		PositionStart: start,
		PositionEnd:   end,
		BinlogBytes:   end - start,
	}
}

func mysqlCommandQuery(ts time.Time, query string, start, end int64) binlog.RawEvent {
	return binlog.RawEvent{
		Timestamp:     ts,
		EventType:     "QUERY",
		Query:         query,
		ServerFlavor:  "mysql",
		Schema:        "app",
		BinlogPath:    "mysql-bin.000001",
		PositionStart: start,
		PositionEnd:   end,
		BinlogBytes:   end - start,
	}
}
