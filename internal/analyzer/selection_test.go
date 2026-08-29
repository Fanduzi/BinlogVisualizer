// Package analyzer tests selector intersection at complete transaction-group boundaries.
// input: normalized MySQL/MariaDB, anonymous, rotated, XA, and LOAD_DATA transaction sequences.
// output: selected aggregate/report evidence without transaction fragments, including anonymous-group and unkeyed-event rejection, across storage modes.
// pos: public Analyzer selection integration coverage for issue #52.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestAnalyzerGTIDSelectorFiltersCompleteGroups(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	selector, err := ParseGTIDSelector([]string{sid + ":1-3"}, []string{sid + ":2"})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 30, 11, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: sid + ":1", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "GTID", GTID: sid + ":2", ServerFlavor: "mysql"},
		{Timestamp: base.Add(5 * time.Second), EventType: "BEGIN", ServerFlavor: "mysql"},
		{Timestamp: base.Add(6 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2, ServerFlavor: "mysql"},
		{Timestamp: base.Add(7 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
		{Timestamp: base.Add(8 * time.Second), EventType: "BEGIN", ServerFlavor: "mysql"},
		{Timestamp: base.Add(9 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 4, ServerFlavor: "mysql"},
		{Timestamp: base.Add(10 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
	}
	opts := DefaultOptions()
	opts.GTIDSelector = selector
	result, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.TotalTransactions != 1 || result.Summary.TotalRows != 1 || len(result.Transactions) != 1 || result.Transactions[0].GTID != sid+":1" {
		t.Fatalf("selected result = %+v, transactions=%+v", result.Summary, result.Transactions)
	}
	if result.Selection == nil || result.Selection.ResolvedGTIDFlavor != "mysql" || len(result.Selection.MatchedGTIDs) != 1 || result.Selection.MatchedGTIDs[0] != sid+":1" {
		t.Fatalf("selection evidence = %+v", result.Selection)
	}
}

func TestAnalyzerGTIDSelectorExcludesAnonymousCompleteGroup(t *testing.T) {
	selector, err := ParseGTIDSelector(nil, []string{"24bc7850-2c16-11e6-a073-0242ac110002:1"})
	if err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.GTIDSelector = selector
	base := time.Date(2026, 8, 30, 11, 15, 0, 0, time.UTC)
	result, err := New(opts).Analyze([]model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.TotalRows != 0 || result.Summary.TotalTransactions != 0 || len(result.Transactions) != 0 {
		t.Fatalf("anonymous group was retained: summary=%+v transactions=%+v", result.Summary, result.Transactions)
	}
}

func TestAnalyzerGTIDExcludeOnlyDoesNotRetainAnonymousDDL(t *testing.T) {
	selector, err := ParseGTIDSelector(nil, []string{"24bc7850-2c16-11e6-a073-0242ac110002:1"})
	if err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.GTIDSelector = selector
	result, err := New(opts).Analyze([]model.NormalizedEvent{
		{
			Timestamp:    time.Date(2026, 8, 30, 11, 20, 0, 0, time.UTC),
			EventType:    "DDL",
			Schema:       "shop",
			Table:        "orders",
			QuerySQL:     "ALTER TABLE shop.orders ADD COLUMN status TINYINT",
			ServerFlavor: "mysql",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.TotalEvents != 0 || result.Summary.TotalRows != 0 || result.Summary.TotalTransactions != 0 || len(result.Transactions) != 0 || len(result.Diagnostics.DDLEvents) != 0 {
		t.Fatalf("anonymous DDL was retained: summary=%+v transactions=%+v ddl=%+v", result.Summary, result.Transactions, result.Diagnostics.DDLEvents)
	}
}

func TestAnalyzerGTIDExcludeOnlySkipsAnonymousContextAndKeepsLaterKeyedGroup(t *testing.T) {
	selector, err := ParseGTIDSelector(nil, []string{"24bc7850-2c16-11e6-a073-0242ac110002:1"})
	if err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.GTIDSelector = selector
	base := time.Date(2026, 8, 30, 11, 21, 0, 0, time.UTC)
	result, err := New(opts).Analyze([]model.NormalizedEvent{
		{Timestamp: base, EventType: "TABLE_MAP", Schema: "shop", Table: "orders", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "GTID", GTID: "24bc7850-2c16-11e6-a073-0242ac110002:2", ServerFlavor: "mysql"},
		{Timestamp: base.Add(5 * time.Second), EventType: "BEGIN", ServerFlavor: "mysql"},
		{Timestamp: base.Add(6 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5, ServerFlavor: "mysql"},
		{Timestamp: base.Add(7 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.TotalEvents != 4 || result.Summary.TotalRows != 5 || result.Summary.TotalTransactions != 1 || len(result.Transactions) != 1 || result.Transactions[0].GTID != "24bc7850-2c16-11e6-a073-0242ac110002:2" || len(result.Minutes) != 1 {
		t.Fatalf("anonymous context or group was retained incorrectly: summary=%+v transactions=%+v minutes=%+v", result.Summary, result.Transactions, result.Minutes)
	}
}

func TestAnalyzerGTIDSelectionKeepsRotatedXALOADDataGroupWhole(t *testing.T) {
	selector, err := ParseGTIDSelector([]string{"0-7-1857"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 30, 11, 30, 0, 0, time.UTC)
	const xid = "'batch-52','b1',7"
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: "0-7-1857", ServerFlavor: "mariadb", BinlogPath: "mariadb-bin.000001", PositionStart: 100, PositionEnd: 120},
		{Timestamp: base.Add(time.Second), EventType: "XA_START", XAXID: xid, ServerFlavor: "mariadb", BinlogPath: "mariadb-bin.000001", PositionStart: 120, PositionEnd: 140},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS_QUERY", Operation: "LOAD_DATA", QuerySQL: "LOAD DATA INFILE '/tmp/orders.csv' INTO TABLE shop.orders", ServerFlavor: "mariadb", BinlogPath: "mariadb-bin.000001", PositionStart: 140, PositionEnd: 180},
		{Timestamp: base.Add(3 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, ServerFlavor: "mariadb", BinlogPath: "mariadb-bin.000001", PositionStart: 180, PositionEnd: 220},
		{Timestamp: base.Add(4 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2, ServerFlavor: "mariadb", BinlogPath: "mariadb-bin.000002", PositionStart: 4, PositionEnd: 44},
		{Timestamp: base.Add(5 * time.Second), EventType: "XA_PREPARE", XAXID: xid, ServerFlavor: "mariadb", BinlogPath: "mariadb-bin.000002", PositionStart: 44, PositionEnd: 80},
		{Timestamp: base.Add(6 * time.Second), EventType: "XA_COMMIT", XAXID: xid, ServerFlavor: "mariadb", BinlogPath: "mariadb-bin.000002", PositionStart: 80, PositionEnd: 110},
	}
	opts := DefaultOptions()
	opts.GTIDSelector = selector
	result, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Transactions) != 1 {
		t.Fatalf("transactions = %+v", result.Transactions)
	}
	txn := result.Transactions[0]
	if txn.Completeness != model.TransactionComplete || txn.XAXID != xid || txn.TotalRows != 3 || txn.Operations["LOAD_DATA"] != 3 || txn.BinlogPathStart != "mariadb-bin.000001" || txn.BinlogPathEnd != "mariadb-bin.000002" || txn.FullReplayAvailable() {
		t.Fatalf("rotated XA/LOAD_DATA transaction = %+v", txn)
	}
}

func TestAnalyzerGTIDSelectorRejectsConflictingAndUnresolvedFlavor(t *testing.T) {
	selector, err := ParseGTIDSelector([]string{"0-7-1857"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		events []model.NormalizedEvent
		want   string
	}{
		{name: "conflicting", events: []model.NormalizedEvent{{EventType: "BEGIN", ServerFlavor: "mysql"}}, want: "conflicts with input flavor"},
		{name: "unresolved", events: []model.NormalizedEvent{{EventType: "BEGIN"}, {EventType: "XID"}}, want: "cannot resolve GTID flavor"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := DefaultOptions()
			opts.GTIDSelector = selector
			_, err := New(opts).Analyze(tt.events)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestAnalyzerGTIDSelectorRejectsUnprovenTransactionBoundary(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	selector, err := ParseGTIDSelector([]string{sid + ":1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.GTIDSelector = selector
	_, err = New(opts).Analyze([]model.NormalizedEvent{
		{EventType: "GTID", GTID: sid + ":1", ServerFlavor: "mysql"},
		{EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, ServerFlavor: "mysql"},
	})
	if err == nil || !strings.Contains(err.Error(), "unresolved boundaries") {
		t.Fatalf("error = %v", err)
	}
}

func TestAnalyzerGTIDSelectionInMemoryAndDuckDBParity(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	selector, err := ParseGTIDSelector([]string{sid + ":7"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 8, 30, 11, 45, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: sid + ":7", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS_QUERY", QuerySQL: "INSERT INTO shop.orders VALUES (7)", QueryOriginalBytes: 39, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
	}
	opts := DefaultOptions()
	opts.DetailStoreMode = DetailStoreDuckDB
	opts.GTIDSelector = selector
	want, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewDuckDBStore(t.TempDir()+"/selection.duckdb", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, err := NewWithStore(opts, store).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Summary, want.Summary) || !reflect.DeepEqual(got.Transactions, want.Transactions) || !reflect.DeepEqual(got.Selection, want.Selection) {
		t.Fatalf("DuckDB parity mismatch\nwant=%+v %+v %+v\ngot=%+v %+v %+v", want.Summary, want.Transactions, want.Selection, got.Summary, got.Transactions, got.Selection)
	}
}

func TestAnalyzerIntersectsTimeAndPositionWithoutCompletingCutTransaction(t *testing.T) {
	base := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	startTime, endTime := base.Add(2*time.Second), base.Add(2*time.Second)
	startPosition, stopPosition := int64(100), int64(160)
	opts := DefaultOptions()
	opts.Start, opts.End = &startTime, &endTime
	opts.StartPosition, opts.StopPosition = &startPosition, &stopPosition
	result, err := New(opts).Analyze([]model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: "24bc7850-2c16-11e6-a073-0242ac110002:9", PositionStart: 40, PositionEnd: 60},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", PositionStart: 60, PositionEnd: 100},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2, PositionStart: 100, PositionEnd: 120},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3, PositionStart: 120, PositionEnd: 140},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", PositionStart: 140, PositionEnd: 160},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Transactions) != 1 || result.Transactions[0].Completeness != model.TransactionPartialBoth || result.Transactions[0].TotalRows != 3 || result.Transactions[0].FullReplayAvailable() {
		t.Fatalf("transaction = %+v", result.Transactions)
	}
	if result.Selection == nil || result.Selection.EffectiveStartPosition == nil || *result.Selection.EffectiveStartPosition != 120 || result.Selection.EffectiveStopPosition == nil || *result.Selection.EffectiveStopPosition != 140 {
		t.Fatalf("selection = %+v", result.Selection)
	}
}

func TestAnalyzerPositionStopClassifiesPartialEnd(t *testing.T) {
	base := time.Date(2026, 8, 30, 12, 30, 0, 0, time.UTC)
	startPosition, stopPosition := int64(40), int64(120)
	opts := DefaultOptions()
	opts.StartPosition, opts.StopPosition = &startPosition, &stopPosition
	result, err := New(opts).Analyze([]model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: "24bc7850-2c16-11e6-a073-0242ac110002:10", PositionStart: 40, PositionEnd: 60},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", PositionStart: 60, PositionEnd: 80},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3, PositionStart: 80, PositionEnd: 100},
		{Timestamp: base.Add(3 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 5, PositionStart: 100, PositionEnd: 120},
		{Timestamp: base.Add(4 * time.Second), EventType: "XID", PositionStart: 120, PositionEnd: 140},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Transactions) != 1 || result.Transactions[0].Completeness != model.TransactionPartialEnd || result.Transactions[0].TotalRows != 8 || result.Transactions[0].FullReplayAvailable() {
		t.Fatalf("transaction = %+v", result.Transactions)
	}
}

func TestAnalyzerPositionCutReplayRequiresProvenFullGroup(t *testing.T) {
	base := time.Date(2026, 8, 30, 12, 45, 0, 0, time.UTC)
	startPosition := int64(100)
	opts := DefaultOptions()
	opts.StartPosition = &startPosition

	proven, err := New(opts).Analyze([]model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: "24bc7850-2c16-11e6-a073-0242ac110002:11", BinlogPath: "mysql-bin.000001", PositionStart: 40, PositionEnd: 60, BinlogBytes: 20},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 60, PositionEnd: 100, BinlogBytes: 40},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3, BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 180, BinlogBytes: 80},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 180, PositionEnd: 220, BinlogBytes: 40},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(proven.Transactions) != 1 || proven.Transactions[0].Completeness != model.TransactionPartialStart || proven.Transactions[0].FullReplaySpan == nil || !proven.Transactions[0].FullReplayAvailable() {
		t.Fatalf("proven selector cut = %+v", proven.Transactions)
	}

	unproven, err := New(opts).Analyze([]model.NormalizedEvent{
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3, BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 180, BinlogBytes: 80},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 180, PositionEnd: 220, BinlogBytes: 40},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(unproven.Transactions) != 1 || unproven.Transactions[0].Completeness != model.TransactionUnknown || unproven.Transactions[0].FullReplaySpan != nil || unproven.Transactions[0].FullReplayAvailable() {
		t.Fatalf("unproven selector cut = %+v", unproven.Transactions)
	}
}

func TestAnalyzerGTIDAndPositionSelectorsIntersectOnProvenGroup(t *testing.T) {
	const gtid = "24bc7850-2c16-11e6-a073-0242ac110002:12"
	selector, err := ParseGTIDSelector([]string{gtid}, nil)
	if err != nil {
		t.Fatal(err)
	}
	startPosition := int64(100)
	opts := DefaultOptions()
	opts.StartPosition = &startPosition
	opts.GTIDSelector = selector
	base := time.Date(2026, 8, 30, 12, 50, 0, 0, time.UTC)
	result, err := New(opts).Analyze([]model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid, ServerFlavor: "mysql", BinlogPath: "mysql-bin.000001", PositionStart: 40, PositionEnd: 60, BinlogBytes: 20},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", ServerFlavor: "mysql", BinlogPath: "mysql-bin.000001", PositionStart: 60, PositionEnd: 100, BinlogBytes: 40},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3, ServerFlavor: "mysql", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 180, BinlogBytes: 80},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", ServerFlavor: "mysql", BinlogPath: "mysql-bin.000001", PositionStart: 180, PositionEnd: 220, BinlogBytes: 40},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Transactions) != 1 || result.Transactions[0].Completeness != model.TransactionPartialStart || result.Transactions[0].FullReplaySpan == nil || !result.Transactions[0].FullReplayAvailable() || result.Selection == nil || len(result.Selection.MatchedGTIDs) != 1 || result.Selection.MatchedGTIDs[0] != gtid {
		t.Fatalf("intersected selection = %+v transactions=%+v", result.Selection, result.Transactions)
	}
}
