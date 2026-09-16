// Package analyzer verifies ADMIN, Ignored QUERY, and Unclassified QUERY at the normalize-plus-Analyzer seam.
// input: synthetic parser-shaped RawEvents (canonical kinds, GTID only on GTID events) run through binlog.NormalizeRawEventInto then Analyzer.Consume.
// output: assertions that ANALYZE TABLE / OPTIMIZE TABLE / FLUSH PRIVILEGES / SET DEFAULT ROLE / exact FLUSH TABLES close GTID-started non-explicit groups without DDL or zero-row report transactions; Unclassified QUERY fails with a prefix; Ignored QUERY stays open and next GTID conflicts; explicit BEGIN/XA_START still conflicts.
// pos: #74 QUERY-class regression at the normalize-plus-Analyzer seam; binary decoding of on-disk binlog is not exercised.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/binlog"
	"binlogviz/internal/model"
)

const mysqlIssue72SID = "67451bcd-b010-11f1-a85a-822b383dbcd0"

func TestAnalyzerClosesMySQLIndependentAdminQueriesBeforeNextGTID(t *testing.T) {
	ts := time.Date(2026, 9, 12, 10, 0, 0, 0, time.UTC)
	queries := []string{
		"ANALYZE TABLE `app`.`orders`",
		"OPTIMIZE TABLE `app`.`orders`",
		"FLUSH PRIVILEGES",
		"FLUSH PRIVILEGES;",
		"FLUSH TABLES",
		"SET DEFAULT ROLE admin TO 'app'@'%'",
		"analyze table app.orders",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			result, err := normalizeAndAnalyze(t, mysqlAdminThenBusiness(ts, query))
			if err != nil {
				t.Fatalf("analyze after %q: %v", query, err)
			}
			assertSingleBusinessTransaction(t, result, mysqlIssue72SID+":40", 3)
			if len(result.Diagnostics.DDLEvents) != 0 {
				t.Fatalf("admin query %q leaked onto the DDL timeline: %+v", query, result.Diagnostics.DDLEvents)
			}
			for _, table := range result.Tables {
				if table.DDLCount != 0 {
					t.Fatalf("admin query %q incremented table DDLCount: %+v", query, table)
				}
			}
		})
	}
}

func TestAnalyzerSeparatesConsecutiveAdminGTIDGroups(t *testing.T) {
	ts := time.Date(2026, 9, 12, 11, 0, 0, 0, time.UTC)
	raws := []binlog.RawEvent{
		mysqlGTID(ts, 39, 100, 180),
		mysqlQuery(ts.Add(time.Second), "ANALYZE TABLE app.orders", 180, 240),
		mysqlGTID(ts.Add(2*time.Second), 40, 240, 320),
		mysqlQuery(ts.Add(3*time.Second), "FLUSH PRIVILEGES", 320, 380),
		mysqlGTID(ts.Add(4*time.Second), 41, 380, 460),
		mysqlQuery(ts.Add(5*time.Second), "SET DEFAULT ROLE NONE TO 'app'@'%'", 460, 540),
		mysqlGTID(ts.Add(6*time.Second), 42, 540, 620),
		mysqlQuery(ts.Add(7*time.Second), "BEGIN", 620, 660),
		mysqlRows(ts.Add(8*time.Second), 2, 660, 760),
		mysqlXID(ts.Add(9*time.Second), 760, 780),
	}

	result, err := normalizeAndAnalyze(t, raws)
	if err != nil {
		t.Fatalf("analyze consecutive admin groups: %v", err)
	}
	assertSingleBusinessTransaction(t, result, mysqlIssue72SID+":42", 2)
	if result.Transactions[0].PositionStart != 540 {
		t.Fatalf("business group started at pos %d, want GTID :42 start 540 so admin groups did not merge", result.Transactions[0].PositionStart)
	}
	if len(result.Diagnostics.DDLEvents) != 0 {
		t.Fatalf("consecutive admin groups leaked DDL: %+v", result.Diagnostics.DDLEvents)
	}
}

func TestAnalyzerClosesMariaDBFlushPrivilegesBeforeNextGTID(t *testing.T) {
	ts := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	raws := []binlog.RawEvent{
		mariaGTID(ts, "0-7-5", 100, 160),
		mariaQuery(ts.Add(time.Second), "FLUSH PRIVILEGES", 160, 220),
		mariaGTID(ts.Add(2*time.Second), "0-7-6", 220, 280),
		mariaQuery(ts.Add(3*time.Second), "BEGIN", 280, 320),
		{
			Timestamp:     ts.Add(4 * time.Second),
			EventType:     "WRITE_ROWS",
			Schema:        "incident",
			Table:         "events",
			RowCount:      1,
			ServerFlavor:  "mariadb",
			BinlogPath:    "mysql-bin.000001",
			PositionStart: 320,
			PositionEnd:   400,
			BinlogBytes:   80,
		},
		{
			Timestamp:     ts.Add(5 * time.Second),
			EventType:     "XID",
			ServerFlavor:  "mariadb",
			BinlogPath:    "mysql-bin.000001",
			PositionStart: 400,
			PositionEnd:   420,
			BinlogBytes:   20,
		},
	}

	result, err := normalizeAndAnalyze(t, raws)
	if err != nil {
		t.Fatalf("analyze MariaDB FLUSH PRIVILEGES: %v", err)
	}
	assertSingleBusinessTransaction(t, result, "0-7-6", 1)
}

func TestAnalyzerUnclassifiedQueryFailsInsteadOfConflictingGTID(t *testing.T) {
	ts := time.Date(2026, 9, 12, 13, 0, 0, 0, time.UTC)
	unclassified := []string{
		"SET ROLE ALL",
		"CHECK TABLE app.orders",
		"FLUSH TABLES WITH READ LOCK",
	}
	for _, query := range unclassified {
		t.Run(query, func(t *testing.T) {
			_, err := normalizeAndAnalyze(t, mysqlAdminThenBusiness(ts, query))
			if err == nil {
				t.Fatalf("unclassified QUERY %q must fail analyze", query)
			}
			if strings.Contains(err.Error(), "conflicting GTID") {
				t.Fatalf("unclassified QUERY %q must not be reported as conflicting GTID, got %v", query, err)
			}
			if !strings.Contains(err.Error(), "Unclassified QUERY") {
				t.Fatalf("unclassified QUERY %q error must name Unclassified QUERY, got %v", query, err)
			}
			if !strings.Contains(err.Error(), query) {
				t.Fatalf("unclassified QUERY error must contain the statement prefix %q, got %v", query, err)
			}
		})
	}
}

func TestAnalyzerIgnoredQueryDoesNotCloseAndNextGTIDConflicts(t *testing.T) {
	ts := time.Date(2026, 9, 12, 13, 15, 0, 0, time.UTC)
	ignored := []string{
		"SET timestamp=1710000000",
		"SET NAMES utf8mb4",
		"SET @foo=1",
	}
	for _, query := range ignored {
		t.Run(query, func(t *testing.T) {
			_, err := normalizeAndAnalyze(t, mysqlAdminThenBusiness(ts, query))
			if err == nil || !strings.Contains(err.Error(), "conflicting GTID") {
				t.Fatalf("Ignored QUERY %q must leave the transaction group open, got %v", query, err)
			}
			if strings.Contains(err.Error(), "Unclassified QUERY") {
				t.Fatalf("Ignored QUERY %q must not become Unclassified QUERY, got %v", query, err)
			}
		})
	}
}

func TestAnalyzerIgnoredQueryThenBeginStillAttachesToGTID(t *testing.T) {
	ts := time.Date(2026, 9, 12, 13, 20, 0, 0, time.UTC)
	raws := []binlog.RawEvent{
		mysqlGTID(ts, 39, 100, 180),
		mysqlQuery(ts.Add(time.Second), "SET timestamp=1710000000", 180, 220),
		mysqlQuery(ts.Add(2*time.Second), "SET NAMES utf8mb4", 220, 260),
		mysqlQuery(ts.Add(3*time.Second), "BEGIN", 260, 300),
		mysqlRows(ts.Add(4*time.Second), 2, 300, 420),
		mysqlXID(ts.Add(5*time.Second), 420, 440),
	}
	result, err := normalizeAndAnalyze(t, raws)
	if err != nil {
		t.Fatalf("Ignored QUERY before BEGIN must stay attached, got %v", err)
	}
	assertSingleBusinessTransaction(t, result, mysqlIssue72SID+":39", 2)
}

func TestAnalyzerAdminOrUnclassifiedAfterExplicitBeginStillConflicts(t *testing.T) {
	ts := time.Date(2026, 9, 12, 15, 30, 0, 0, time.UTC)
	cases := []struct {
		name  string
		start string
		query string
	}{
		{name: "BEGIN_ADMIN", start: "BEGIN", query: "ANALYZE TABLE app.orders"},
		{name: "BEGIN_UNCLASSIFIED", start: "BEGIN", query: "CHECK TABLE app.orders"},
		{name: "XA_START_UNCLASSIFIED", start: "XA START 'batch-74'", query: "SET ROLE ALL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raws := []binlog.RawEvent{
				mysqlGTID(ts, 39, 100, 180),
				mysqlQuery(ts.Add(time.Second), tc.start, 180, 220),
				mysqlQuery(ts.Add(2*time.Second), tc.query, 220, 280),
				mysqlGTID(ts.Add(3*time.Second), 40, 280, 360),
			}
			_, err := normalizeAndAnalyze(t, raws)
			if err == nil || !strings.Contains(err.Error(), "conflicting GTID") {
				t.Fatalf("%s then next GTID must stay conflicting GTID, got %v", tc.name, err)
			}
		})
	}
}

func TestAnalyzerFinalizeUnclassifiedOnlyGroupFails(t *testing.T) {
	ts := time.Date(2026, 9, 12, 13, 30, 0, 0, time.UTC)
	raws := []binlog.RawEvent{
		mysqlGTID(ts, 39, 100, 180),
		mysqlQuery(ts.Add(time.Second), "CHECK TABLE app.orders", 180, 260),
	}
	_, err := normalizeAndAnalyze(t, raws)
	if err == nil {
		t.Fatal("finalize of an unclassified-only GTID-started transaction group must fail")
	}
	if strings.Contains(err.Error(), "conflicting GTID") {
		t.Fatalf("finalize must be Unclassified QUERY, not conflicting GTID, got %v", err)
	}
	if !strings.Contains(err.Error(), "Unclassified QUERY") || !strings.Contains(err.Error(), "CHECK TABLE app.orders") {
		t.Fatalf("finalize error must include Unclassified QUERY and the statement prefix, got %v", err)
	}
}

func TestAnalyzerExplicitBeginThenNextGTIDStillConflicts(t *testing.T) {
	ts := time.Date(2026, 9, 12, 14, 0, 0, 0, time.UTC)
	raws := []binlog.RawEvent{
		mysqlGTID(ts, 39, 100, 180),
		mysqlQuery(ts.Add(time.Second), "BEGIN", 180, 220),
		mysqlGTID(ts.Add(2*time.Second), 40, 220, 300),
	}
	_, err := normalizeAndAnalyze(t, raws)
	if err == nil || !strings.Contains(err.Error(), "conflicting GTID") {
		t.Fatalf("expected conflicting GTID inside an explicit transaction, got %v", err)
	}
}

func TestAnalyzerAdminDoesNotCloseExplicitBeginOrXAStartBeforeNextGTID(t *testing.T) {
	ts := time.Date(2026, 9, 12, 15, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		query string
	}{
		{name: "BEGIN", query: "BEGIN"},
		{name: "XA_START", query: "XA START 'batch-72'"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raws := []binlog.RawEvent{
				mysqlGTID(ts, 39, 100, 180),
				mysqlQuery(ts.Add(time.Second), tc.query, 180, 220),
				mysqlQuery(ts.Add(2*time.Second), "ANALYZE TABLE app.orders", 220, 280),
				mysqlGTID(ts.Add(3*time.Second), 40, 280, 360),
			}
			_, err := normalizeAndAnalyze(t, raws)
			if err == nil || !strings.Contains(err.Error(), "conflicting GTID") {
				t.Fatalf("%s then ADMIN must leave the explicit group open, got %v", tc.name, err)
			}
		})
	}
}

func TestIndependentAdminQueryClosesGroupAtQueryEndPosition(t *testing.T) {
	ts := time.Date(2026, 9, 12, 16, 0, 0, 0, time.UTC)
	raws := mysqlAdminThenBusiness(ts, "ANALYZE TABLE app.orders")
	events := normalizeRawEvents(t, raws)

	var adminEnd int64
	for _, ev := range events {
		if ev.EventType == "ADMIN" {
			adminEnd = ev.PositionEnd
		}
	}
	if adminEnd != 260 {
		t.Fatalf("ADMIN PositionEnd = %d, want QUERY end 260", adminEnd)
	}

	builder := NewTransactionBuilder()
	for _, ev := range events {
		if err := builder.Consume(ev); err != nil {
			t.Fatalf("transaction builder consume %s: %v", ev.EventType, err)
		}
	}
	groups := builder.Completed()
	if len(groups) < 2 {
		t.Fatalf("closed groups = %+v, want admin group plus business group", groups)
	}
	adminGroup := groups[0]
	if adminGroup.GTID != mysqlIssue72SID+":39" {
		t.Fatalf("admin group GTID = %q, want :39", adminGroup.GTID)
	}
	if adminGroup.PositionStart != 100 || adminGroup.PositionEnd != adminEnd {
		t.Fatalf("admin group span [%d,%d), want [100,%d) ending on the management QUERY", adminGroup.PositionStart, adminGroup.PositionEnd, adminEnd)
	}
	if adminGroup.TotalRows != 0 {
		t.Fatalf("admin group should have zero rows, got %+v", adminGroup)
	}

	result, err := consumeAnalyzer(events)
	if err != nil {
		t.Fatalf("analyze after admin close: %v", err)
	}
	assertSingleBusinessTransaction(t, result, mysqlIssue72SID+":40", 3)
	if result.Transactions[0].PositionStart != 260 || result.Transactions[0].PositionEnd != 520 {
		t.Fatalf("business span [%d,%d), want [260,520) after the closed admin QUERY", result.Transactions[0].PositionStart, result.Transactions[0].PositionEnd)
	}
}

func normalizeAndAnalyze(t *testing.T, raws []binlog.RawEvent) (*model.AnalysisResult, error) {
	t.Helper()
	return consumeAnalyzer(normalizeRawEvents(t, raws))
}

func normalizeRawEvents(t *testing.T, raws []binlog.RawEvent) []model.NormalizedEvent {
	t.Helper()
	events := make([]model.NormalizedEvent, 0, len(raws))
	var dst model.NormalizedEvent
	for i, raw := range raws {
		ok, err := binlog.NormalizeRawEventInto(raw, &dst)
		if err != nil {
			t.Fatalf("NormalizeRawEventInto raw[%d] %s: %v", i, raw.EventType, err)
		}
		if !ok {
			continue
		}
		events = append(events, dst)
	}
	return events
}

func consumeAnalyzer(events []model.NormalizedEvent) (*model.AnalysisResult, error) {
	a := New(Options{})
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			return nil, err
		}
	}
	return a.Finalize()
}

func assertSingleBusinessTransaction(t *testing.T, result *model.AnalysisResult, gtid string, rows int) {
	t.Helper()
	if result == nil {
		t.Fatal("nil analysis result")
	}
	if len(result.Transactions) != 1 || result.Summary.TotalTransactions != 1 {
		t.Fatalf("report transactions = %+v (summary=%+v), want exactly one business transaction", result.Transactions, result.Summary)
	}
	txn := result.Transactions[0]
	if txn.GTID != gtid {
		t.Fatalf("business GTID = %q, want %q", txn.GTID, gtid)
	}
	if txn.TotalRows != rows || result.Summary.TotalRows != rows {
		t.Fatalf("business rows = %d summary=%d, want %d", txn.TotalRows, result.Summary.TotalRows, rows)
	}
}

func mysqlAdminThenBusiness(ts time.Time, query string) []binlog.RawEvent {
	return []binlog.RawEvent{
		mysqlGTID(ts, 39, 100, 180),
		mysqlQuery(ts.Add(time.Second), query, 180, 260),
		mysqlGTID(ts.Add(2*time.Second), 40, 260, 340),
		mysqlQuery(ts.Add(3*time.Second), "BEGIN", 340, 380),
		mysqlRows(ts.Add(4*time.Second), 3, 380, 500),
		mysqlXID(ts.Add(5*time.Second), 500, 520),
	}
}

func mysqlGTID(ts time.Time, seq int, start, end int64) binlog.RawEvent {
	return binlog.RawEvent{
		Timestamp:     ts,
		EventType:     "GTID",
		GTID:          mysqlIssue72SID + ":" + strconv.Itoa(seq),
		ServerFlavor:  "mysql",
		BinlogPath:    "mysql-bin.000001",
		PositionStart: start,
		PositionEnd:   end,
		BinlogBytes:   end - start,
	}
}

func mysqlQuery(ts time.Time, query string, start, end int64) binlog.RawEvent {
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

func mysqlRows(ts time.Time, rows int, start, end int64) binlog.RawEvent {
	return binlog.RawEvent{
		Timestamp:     ts,
		EventType:     "WRITE_ROWS",
		Schema:        "app",
		Table:         "orders",
		RowCount:      rows,
		ServerFlavor:  "mysql",
		BinlogPath:    "mysql-bin.000001",
		PositionStart: start,
		PositionEnd:   end,
		BinlogBytes:   end - start,
	}
}

func mysqlXID(ts time.Time, start, end int64) binlog.RawEvent {
	return binlog.RawEvent{
		Timestamp:     ts,
		EventType:     "XID",
		ServerFlavor:  "mysql",
		BinlogPath:    "mysql-bin.000001",
		PositionStart: start,
		PositionEnd:   end,
		BinlogBytes:   end - start,
	}
}

func mariaGTID(ts time.Time, gtid string, start, end int64) binlog.RawEvent {
	return binlog.RawEvent{
		Timestamp:     ts,
		EventType:     "GTID",
		GTID:          gtid,
		ServerFlavor:  "mariadb",
		BinlogPath:    "mysql-bin.000001",
		PositionStart: start,
		PositionEnd:   end,
		BinlogBytes:   end - start,
	}
}

func mariaQuery(ts time.Time, query string, start, end int64) binlog.RawEvent {
	return binlog.RawEvent{
		Timestamp:     ts,
		EventType:     "QUERY",
		Query:         query,
		ServerFlavor:  "mariadb",
		BinlogPath:    "mysql-bin.000001",
		PositionStart: start,
		PositionEnd:   end,
		BinlogBytes:   end - start,
	}
}
