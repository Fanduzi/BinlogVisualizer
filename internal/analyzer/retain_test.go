// Package analyzer verifies which transaction groups become report transactions.
// input: Analyzer.Analyze sequences that mix ROW images, XA identity, GTID-started DDL, XA END, and XA ROLLBACK.
// output: assertions that the report keeps recovery XA evidence, omits DDL-only groups, and closes on ROLLBACK not END.
// pos: test surface for ADR-0002 at the Analyzer interface, not the grouping state machine.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestAnalyzerKeepsZeroRowXACommitAsTransaction(t *testing.T) {
	ts := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC)
	xid := "X'6276742d3537',X'',1"
	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", GTID: "0-7-1859", XAXID: xid, BinlogPath: "mariadb-bin.000001", PositionStart: 3802, PositionEnd: 3900},
		{Timestamp: ts.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "xa_a", Operation: "INSERT", RowCount: 2, BinlogPath: "mariadb-bin.000001", PositionStart: 3900, PositionEnd: 4100},
		{Timestamp: ts.Add(2 * time.Second), EventType: "XA_PREPARE", XAXID: xid, BinlogPath: "mariadb-bin.000001", PositionStart: 4100, PositionEnd: 4444},
		{Timestamp: ts.Add(3 * time.Second), EventType: "GTID", GTID: "0-7-1860", BinlogPath: "mariadb-bin.000001", PositionStart: 4444, PositionEnd: 4500},
		{Timestamp: ts.Add(3 * time.Second), EventType: "XA_COMMIT", XAXID: xid, BinlogPath: "mariadb-bin.000001", PositionStart: 4500, PositionEnd: 4560},
	}

	result, err := New(Options{}).Analyze(events)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if len(result.Transactions) != 2 {
		t.Fatalf("report transactions = %+v, want prepared XA plus zero-row XA COMMIT", result.Transactions)
	}
	commit := result.Transactions[1]
	if commit.GTID != "0-7-1860" || commit.XAXID != xid || commit.TotalRows != 0 {
		t.Fatalf("zero-row XA COMMIT dropped from the report: %+v", commit)
	}
}

func TestAnalyzerOmitsDDLOnlyGroupsFromTransactions(t *testing.T) {
	ts := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", ServerFlavor: "mariadb", GTID: "0-7-10"},
		{Timestamp: ts.Add(time.Second), EventType: "DDL", ServerFlavor: "mariadb", GTID: "0-7-10", Schema: "incident", QuerySQL: "CREATE DATABASE incident"},
		{Timestamp: ts.Add(2 * time.Second), EventType: "GTID", ServerFlavor: "mariadb", GTID: "0-7-11"},
		{Timestamp: ts.Add(3 * time.Second), EventType: "DDL", ServerFlavor: "mariadb", GTID: "0-7-11", Schema: "incident", Table: "events", QuerySQL: "CREATE TABLE incident.events (id bigint)"},
	}

	result, err := New(Options{}).Analyze(events)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if len(result.Transactions) != 0 || result.Summary.TotalTransactions != 0 {
		t.Fatalf("DDL-only groups leaked into report transactions: %+v", result.Transactions)
	}
	if len(result.Diagnostics.DDLEvents) != 2 {
		t.Fatalf("DDL timeline = %+v, want both CREATE statements", result.Diagnostics.DDLEvents)
	}
}

func TestAnalyzerXARollbackClosesGroupBeforeNextGTID(t *testing.T) {
	ts := time.Date(2026, 9, 6, 10, 0, 0, 0, time.UTC)
	xid := "X'726f6c6c6261636b',X'',1"
	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", GTID: "0-7-10", XAXID: xid, BinlogPath: "mariadb-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: ts.Add(time.Second), EventType: "XA_START", XAXID: xid, BinlogPath: "mariadb-bin.000001", PositionStart: 140, PositionEnd: 180},
		{Timestamp: ts.Add(2 * time.Second), EventType: "XA_ROLLBACK", XAXID: xid, BinlogPath: "mariadb-bin.000001", PositionStart: 180, PositionEnd: 220},
		{Timestamp: ts.Add(3 * time.Second), EventType: "GTID", GTID: "0-7-11", BinlogPath: "mariadb-bin.000001", PositionStart: 220, PositionEnd: 260},
		{Timestamp: ts.Add(4 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, BinlogPath: "mariadb-bin.000001", PositionStart: 260, PositionEnd: 300},
		{Timestamp: ts.Add(5 * time.Second), EventType: "XID", BinlogPath: "mariadb-bin.000001", PositionStart: 300, PositionEnd: 320},
	}

	result, err := New(Options{}).Analyze(events)
	if err != nil {
		t.Fatalf("analyze after XA ROLLBACK: %v", err)
	}
	if len(result.Transactions) != 2 {
		t.Fatalf("report transactions = %+v, want XA ROLLBACK evidence plus the next GTID", result.Transactions)
	}
	byGTID := map[string]model.Transaction{}
	for _, txn := range result.Transactions {
		byGTID[txn.GTID] = txn
	}
	rollback := byGTID["0-7-10"]
	if rollback.XAXID != xid || rollback.TotalRows != 0 {
		t.Fatalf("XA ROLLBACK missing from the report: %+v", result.Transactions)
	}
	next := byGTID["0-7-11"]
	if next.TotalRows != 1 {
		t.Fatalf("following GTID not a separate transaction: %+v", result.Transactions)
	}
}

func TestAnalyzerXAEndDoesNotCloseGroup(t *testing.T) {
	ts := time.Date(2026, 9, 6, 11, 0, 0, 0, time.UTC)
	xid := "X'656e642d6b656570',X'',1"
	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", GTID: "0-7-20", XAXID: xid, BinlogPath: "mariadb-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: ts.Add(time.Second), EventType: "XA_START", XAXID: xid, BinlogPath: "mariadb-bin.000001", PositionStart: 140, PositionEnd: 180},
		{Timestamp: ts.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "xa_a", Operation: "INSERT", RowCount: 2, BinlogPath: "mariadb-bin.000001", PositionStart: 180, PositionEnd: 400},
		{Timestamp: ts.Add(3 * time.Second), EventType: "XA_END", XAXID: xid, BinlogPath: "mariadb-bin.000001", PositionStart: 400, PositionEnd: 420},
		{Timestamp: ts.Add(4 * time.Second), EventType: "XA_PREPARE", XAXID: xid, BinlogPath: "mariadb-bin.000001", PositionStart: 420, PositionEnd: 480},
	}

	result, err := New(Options{}).Analyze(events)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}
	if len(result.Transactions) != 1 {
		t.Fatalf("XA END closed the group early: %+v", result.Transactions)
	}
	txn := result.Transactions[0]
	if txn.TotalRows != 2 || txn.PositionEnd != 480 {
		t.Fatalf("group should close on XA PREPARE, not XA END: %+v", txn)
	}
}

func TestAnalyzerConflictingGTIDFailsAnalyze(t *testing.T) {
	ts := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: ts, EventType: "GTID", GTID: "0-7-1848"},
		{Timestamp: ts.Add(time.Second), EventType: "BEGIN"},
		{Timestamp: ts.Add(2 * time.Second), EventType: "GTID", GTID: "0-7-1849"},
	}

	_, err := New(Options{}).Analyze(events)
	if err == nil || !strings.Contains(err.Error(), "conflicting GTID") {
		t.Fatalf("expected conflicting GTID to fail analyze, got %v", err)
	}
}
