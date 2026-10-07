package analyzer

import (
	"errors"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestAnalyzeDDLOnlyKeepsTimeline(t *testing.T) {
	ts := time.Date(2026, 10, 2, 1, 0, 0, 0, time.UTC)
	result, err := New(DefaultOptions()).Analyze([]model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", ServerFlavor: "mysql", GTID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1", BinlogPath: "mysql-bin.000001", PositionStart: 4, PositionEnd: 100},
		{Timestamp: ts.Add(time.Second), EventType: "DDL", ServerFlavor: "mysql", Schema: "shop", Table: "orders", QuerySQL: "ALTER TABLE shop.orders ADD COLUMN marker INT", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 180},
	})
	if err != nil {
		t.Fatalf("DDL-only analyze: %v", err)
	}
	if result.Summary.TotalTransactions != 0 || result.Summary.TotalRows != 0 {
		t.Fatalf("DDL-only must stay off the transaction list, got txns=%d rows=%d", result.Summary.TotalTransactions, result.Summary.TotalRows)
	}
	if result.Summary.TotalEvents == 0 || len(result.Diagnostics.DDLEvents) != 1 {
		t.Fatalf("expected counted DDL evidence, events=%d ddl=%+v", result.Summary.TotalEvents, result.Diagnostics.DDLEvents)
	}
	ddl := result.Diagnostics.DDLEvents[0]
	if ddl.Operation != "ALTER TABLE" || ddl.Schema != "shop" || ddl.Table != "orders" || ddl.PositionStart != 100 {
		t.Fatalf("DDL timeline = %+v", ddl)
	}
	if ddl.GTID != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1" || ddl.TxnStartPos != 4 || ddl.TxnStartPath != "mysql-bin.000001" {
		t.Fatalf("DDL group identity = gtid %q start %s:%d, want the GTID event at 4", ddl.GTID, ddl.TxnStartPath, ddl.TxnStartPos)
	}
	if ddl.PositionStart == ddl.TxnStartPos {
		t.Fatalf("txn start must be the GTID event, not the query at %d", ddl.PositionStart)
	}
}

func TestAnalyzeDDLTimelineAnonymousGTIDOmitsIdentity(t *testing.T) {
	ts := time.Date(2026, 10, 2, 1, 0, 0, 0, time.UTC)
	result, err := New(DefaultOptions()).Analyze([]model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", ServerFlavor: "mysql", BinlogPath: "mysql-bin.000001", PositionStart: 4, PositionEnd: 100},
		{Timestamp: ts.Add(time.Second), EventType: "DDL", ServerID: 1, ThreadID: 9, Schema: "shop", Table: "orders", QuerySQL: "DROP TABLE shop.orders", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 180},
	})
	if err != nil {
		t.Fatalf("anonymous DDL: %v", err)
	}
	if len(result.Diagnostics.DDLEvents) != 1 {
		t.Fatalf("DDL events = %+v", result.Diagnostics.DDLEvents)
	}
	ddl := result.Diagnostics.DDLEvents[0]
	if ddl.GTID != "" || ddl.TxnStartPos != 4 || ddl.ServerID != 1 || ddl.ThreadID != 9 {
		t.Fatalf("anonymous DDL = %+v, want empty GTID, start 4, and the query event's server/thread", ddl)
	}
}

func TestAnalyzeDDLTimelineWithoutGTIDUsesQueryStart(t *testing.T) {
	ts := time.Date(2026, 10, 2, 1, 0, 0, 0, time.UTC)
	result, err := New(DefaultOptions()).Analyze([]model.NormalizedEvent{
		{Timestamp: ts, EventType: "DDL", QuerySQL: "DROP TABLE shop.orders", Schema: "shop", Table: "orders", BinlogPath: "mysql-bin.000001", PositionStart: 219, PositionEnd: 300, ServerID: 1},
	})
	if err != nil {
		t.Fatalf("gtid-off DDL: %v", err)
	}
	ddl := result.Diagnostics.DDLEvents[0]
	if ddl.GTID != "" || ddl.TxnStartPos != 219 || ddl.PositionStart != 219 {
		t.Fatalf("gtid-off DDL = %+v", ddl)
	}
}

func TestAnalyzeDDLTimelineKeepsGTIDWhenSelectorBuffersTheGroup(t *testing.T) {
	ts := time.Date(2026, 10, 2, 3, 0, 0, 0, time.UTC)
	const sid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	selector, err := ParseGTIDSelector([]string{sid + ":2"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.GTIDSelector = selector
	result, err := New(opts).Analyze([]model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", ServerFlavor: "mysql", GTID: sid + ":1", BinlogPath: "mysql-bin.000001", PositionStart: 4, PositionEnd: 50},
		{Timestamp: ts.Add(time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 50, PositionEnd: 80},
		{Timestamp: ts.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, BinlogPath: "mysql-bin.000001", PositionStart: 80, PositionEnd: 120},
		{Timestamp: ts.Add(3 * time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 120, PositionEnd: 150},
		{Timestamp: ts.Add(4 * time.Second), EventType: "GTID", ServerFlavor: "mysql", GTID: sid + ":2", BinlogPath: "mysql-bin.000001", PositionStart: 150, PositionEnd: 200},
		{Timestamp: ts.Add(5 * time.Second), EventType: "DDL", ServerFlavor: "mysql", QuerySQL: "TRUNCATE TABLE shop.orders", BinlogPath: "mysql-bin.000001", PositionStart: 200, PositionEnd: 280},
		{Timestamp: ts.Add(6 * time.Second), EventType: "GTID", ServerFlavor: "mysql", GTID: sid + ":3", BinlogPath: "mysql-bin.000001", PositionStart: 280, PositionEnd: 320},
		{Timestamp: ts.Add(7 * time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 320, PositionEnd: 360},
		{Timestamp: ts.Add(8 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, BinlogPath: "mysql-bin.000001", PositionStart: 360, PositionEnd: 400},
		{Timestamp: ts.Add(9 * time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 400, PositionEnd: 430},
	})
	if err != nil {
		t.Fatalf("selected DDL: %v", err)
	}
	if len(result.Diagnostics.DDLEvents) != 1 {
		t.Fatalf("DDL events = %+v", result.Diagnostics.DDLEvents)
	}
	ddl := result.Diagnostics.DDLEvents[0]
	if ddl.Operation != "TRUNCATE TABLE" || ddl.GTID != sid+":2" || ddl.TxnStartPos != 150 || ddl.PositionStart != 200 {
		t.Fatalf("selected DDL = %+v", ddl)
	}
	if result.Summary.TotalRows != 0 {
		t.Fatalf("included GTID is DDL-only, rows = %d", result.Summary.TotalRows)
	}
}

func TestAnalyzeEOFOpenDMLGroupIsFirstClass(t *testing.T) {
	ts := time.Date(2026, 10, 2, 2, 0, 0, 0, time.UTC)
	opts := DefaultOptions()
	opts.LargeTxnDuration = 30 * time.Second
	result, err := New(opts).Analyze([]model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", ServerFlavor: "mysql", GTID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:7", BinlogPath: "mysql-bin.000008", PositionStart: 100, PositionEnd: 140},
		{Timestamp: ts.Add(time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000008", PositionStart: 140, PositionEnd: 180},
		{Timestamp: ts.Add(45 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 4, BinlogPath: "mysql-bin.000008", PositionStart: 180, PositionEnd: 420},
	})
	if err != nil {
		t.Fatalf("EOF open BEGIN: %v", err)
	}
	if result.Diagnostics.OpenExplicitGroups != 1 || len(result.Diagnostics.OpenDMLGroups) != 1 {
		t.Fatalf("open groups=%d dml=%d", result.Diagnostics.OpenExplicitGroups, len(result.Diagnostics.OpenDMLGroups))
	}
	group := result.Diagnostics.OpenDMLGroups[0]
	if group.TotalRows != 4 || group.Tables["shop.orders"] != 4 || group.Duration != 45*time.Second {
		t.Fatalf("open DML = %+v", group)
	}
	if group.BinlogPathStart != "mysql-bin.000008" || group.PositionStart != 100 || group.PositionEnd != 420 {
		t.Fatalf("open DML span = %s %d-%d", group.BinlogPathStart, group.PositionStart, group.PositionEnd)
	}
	if len(result.Diagnostics.LongestTransactions) != 0 {
		t.Fatalf("open DML must stay out of committed duration ranking, got %+v", result.Diagnostics.LongestTransactions)
	}
	if len(result.Diagnostics.Findings) == 0 || result.Diagnostics.Findings[0].Kind != "open_dml_group" || result.Diagnostics.Findings[0].Severity != "warning" {
		t.Fatalf("findings = %+v", result.Diagnostics.Findings)
	}
	if !strings.Contains(result.Diagnostics.Findings[0].Message, "shop.orders") || !strings.Contains(result.Diagnostics.Findings[0].Message, "uncommitted") {
		t.Fatalf("finding message = %q", result.Diagnostics.Findings[0].Message)
	}
}

func TestAnalyzeEOFBeginWithoutRowsIsNotOpenDML(t *testing.T) {
	ts := time.Date(2026, 10, 2, 2, 30, 0, 0, time.UTC)
	result, err := New(DefaultOptions()).Analyze([]model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", ServerFlavor: "mysql", GTID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:8", BinlogPath: "mysql-bin.000008", PositionStart: 100, PositionEnd: 140},
		{Timestamp: ts.Add(time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000008", PositionStart: 140, PositionEnd: 180},
	})
	if err != nil {
		t.Fatalf("EOF BEGIN: %v", err)
	}
	if result.Diagnostics.OpenExplicitGroups != 1 || len(result.Diagnostics.OpenDMLGroups) != 0 {
		t.Fatalf("groups=%d dml=%d", result.Diagnostics.OpenExplicitGroups, len(result.Diagnostics.OpenDMLGroups))
	}
}

func TestAnalyzeNextGTIDOpenDMLErrorNamesEvidence(t *testing.T) {
	ts := time.Date(2026, 10, 2, 3, 0, 0, 0, time.UTC)
	_, err := New(DefaultOptions()).Analyze([]model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", ServerFlavor: "mysql", GTID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:9", BinlogPath: "mysql-bin.000009", PositionStart: 100, PositionEnd: 160},
		{Timestamp: ts.Add(time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000009", PositionStart: 160, PositionEnd: 200},
		{Timestamp: ts.Add(2 * time.Second), EventType: "ROWS", Schema: "app", Table: "orders", Operation: "INSERT", RowCount: 3, BinlogPath: "mysql-bin.000009", PositionStart: 200, PositionEnd: 280},
		{Timestamp: ts.Add(50 * time.Second), EventType: "GTID", ServerFlavor: "mysql", GTID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:10", BinlogPath: "mysql-bin.000009", PositionStart: 280, PositionEnd: 340},
	})
	var openBegin *OpenBeginError
	if err == nil || !errors.As(err, &openBegin) {
		t.Fatalf("expected OpenBeginError, got %v", err)
	}
	if openBegin.Rows != 3 || openBegin.Tables != "app.orders" || openBegin.Duration != 50*time.Second {
		t.Fatalf("open begin evidence = %+v", openBegin)
	}
	if !strings.Contains(err.Error(), "open BEGIN without close") || !strings.Contains(err.Error(), "not lock-contention proof") || !strings.Contains(err.Error(), "mysql-bin.000009:100-280") {
		t.Fatalf("error = %v", err)
	}
}

func TestAnalyzeCommittedDurationAndByteRanking(t *testing.T) {
	ts := time.Date(2026, 10, 2, 4, 0, 0, 0, time.UTC)
	opts := DefaultOptions()
	opts.LargeTxnRows = 1000
	opts.LargeTxnDuration = 30 * time.Second
	events := append(committedTxn(ts, "11", 100, 200, 2*time.Second, 50), committedTxn(ts.Add(time.Minute), "12", 200, 5000, 45*time.Second, 1)...)
	result, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if len(result.Diagnostics.LongestTransactions) < 2 || result.Diagnostics.LongestTransactions[0].Duration != 45*time.Second {
		t.Fatalf("longest = %+v", result.Diagnostics.LongestTransactions)
	}
	if len(result.Diagnostics.LargestByteTransactions) == 0 || result.Diagnostics.LargestByteTransactions[0].BinlogBytes != 4800 {
		t.Fatalf("byte leaders = %+v", result.Diagnostics.LargestByteTransactions)
	}
	if !hasDurationBucket(result.Diagnostics.DurationBuckets, ">=30s", 1) || !hasDurationBucket(result.Diagnostics.DurationBuckets, "1s-10s", 1) {
		t.Fatalf("buckets = %+v", result.Diagnostics.DurationBuckets)
	}
	var sawDuration bool
	for _, finding := range result.Diagnostics.Findings {
		if strings.Contains(finding.Message, "exceeds duration threshold (45s)") {
			sawDuration = true
		}
	}
	if !sawDuration {
		t.Fatalf("findings = %+v", result.Diagnostics.Findings)
	}
}

func committedTxn(ts time.Time, seq string, start, end int64, span time.Duration, rows int) []model.NormalizedEvent {
	path := "mysql-bin.000010"
	gtid := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:" + seq
	return []model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", ServerFlavor: "mysql", GTID: gtid, BinlogPath: path, PositionStart: start, PositionEnd: start + 40},
		{Timestamp: ts.Add(time.Second), EventType: "BEGIN", BinlogPath: path, PositionStart: start + 40, PositionEnd: start + 80},
		{Timestamp: ts.Add(span - time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: rows, BinlogPath: path, PositionStart: start + 80, PositionEnd: end - 20},
		{Timestamp: ts.Add(span), EventType: "XID", BinlogPath: path, PositionStart: end - 20, PositionEnd: end},
	}
}

func hasDurationBucket(buckets []model.DurationBucket, label string, count int) bool {
	for _, bucket := range buckets {
		if bucket.Label == label && bucket.TxnCount == count {
			return true
		}
	}
	return false
}
