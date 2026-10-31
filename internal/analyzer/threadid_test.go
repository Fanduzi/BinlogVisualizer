// Package analyzer tests the --thread-id transaction-group selector (ADR 0027).
// input: normalized MySQL transaction sequences whose QUERY events carry binlog thread ids.
// output: selected groups aggregated whole (the pre-attribution GTID included); unmatched and unattributed groups dropped; planner SQL only for kept groups.
// pos: public Analyzer coverage for --thread-id alongside the GTID selector tests in selection_test.go.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
)

func TestParseThreadIDs(t *testing.T) {
	for _, tc := range []struct {
		name   string
		values []string
		want   []uint32
		err    string
	}{
		{name: "empty", values: nil, want: nil},
		{name: "single", values: []string{"7"}, want: []uint32{7}},
		{name: "comma separated", values: []string{"11,3,7"}, want: []uint32{3, 7, 11}},
		{name: "repeated flags and whitespace", values: []string{" 11 ", "3"}, want: []uint32{3, 11}},
		{name: "deduplicated and sorted", values: []string{"11", "3,11"}, want: []uint32{3, 11}},
		{name: "max uint32", values: []string{"4294967295"}, want: []uint32{4294967295}},
		{name: "zero is the unattributed marker", values: []string{"0"}, err: "invalid --thread-id"},
		{name: "non numeric", values: []string{"abc"}, err: "invalid --thread-id"},
		{name: "negative", values: []string{"-1"}, err: "invalid --thread-id"},
		{name: "overflow", values: []string{"4294967296"}, err: "invalid --thread-id"},
		{name: "blank tokens only", values: []string{" , "}, err: "invalid --thread-id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseThreadIDs(tc.values)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("err = %v, want %q", err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseThreadIDs: %v", err)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// threadTxn is a complete GTID-opened transaction group; tid rides on the
// BEGIN query exactly where the parser places SlaveProxyID.
func threadTxn(base time.Time, gtid string, tid uint32, rows int) []model.NormalizedEvent {
	return []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid, ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", ThreadID: tid, ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: rows, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
	}
}

func TestAnalyzerThreadIDSelectorFiltersCompleteGroups(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	base := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	events := append(threadTxn(base, sid+":1", 7, 1), threadTxn(base.Add(4*time.Second), sid+":2", 9, 2)...)
	opts := DefaultOptions()
	opts.ThreadIDs = []uint32{7}
	result, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.TotalTransactions != 1 || result.Summary.TotalRows != 1 || len(result.Transactions) != 1 || result.Transactions[0].ThreadID != 7 {
		t.Fatalf("selected result = %+v, transactions=%+v", result.Summary, result.Transactions)
	}
	// The whole group counts, including the GTID event that arrived before
	// the thread id was known — selection keeps a group whole or not at all.
	if result.Summary.TotalEvents != 4 {
		t.Fatalf("TotalEvents = %d, want 4 (GTID + BEGIN + ROWS + XID of the kept group)", result.Summary.TotalEvents)
	}
	if result.Selection == nil || len(result.Selection.ThreadIDs) != 1 || result.Selection.ThreadIDs[0] != 7 {
		t.Fatalf("selection evidence = %+v", result.Selection)
	}
}

func TestAnalyzerThreadIDSelectorDropsUnattributedGroups(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	base := time.Date(2026, 11, 2, 10, 30, 0, 0, time.UTC)
	// Group 1 never carries a thread id (a binlog without SlaveProxyID, or
	// server-internal writes); --thread-id must not match it.
	events := append(threadTxn(base, sid+":1", 0, 3), threadTxn(base.Add(4*time.Second), sid+":2", 9, 2)...)
	opts := DefaultOptions()
	opts.ThreadIDs = []uint32{9}
	result, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.TotalTransactions != 1 || result.Summary.TotalRows != 2 || len(result.Transactions) != 1 || result.Transactions[0].ThreadID != 9 {
		t.Fatalf("unattributed group leaked or attributed group dropped: summary=%+v transactions=%+v", result.Summary, result.Transactions)
	}
}

func TestAnalyzerThreadIDAndGTIDSelectorsIntersect(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	base := time.Date(2026, 11, 2, 11, 0, 0, 0, time.UTC)
	events := append(threadTxn(base, sid+":1", 7, 1), threadTxn(base.Add(4*time.Second), sid+":2", 9, 2)...)
	selector, err := ParseGTIDSelector([]string{sid + ":1"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Same GTID and same thread are both required: gtid1 belongs to thread 7,
	// so asking for thread 9 keeps nothing even though the GTID matches.
	opts := DefaultOptions()
	opts.GTIDSelector = selector
	opts.ThreadIDs = []uint32{9}
	out, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if out.Summary.TotalTransactions != 0 || out.Summary.TotalRows != 0 {
		t.Fatalf("GTID-matched but thread-unmatched group was kept: %+v", out.Summary)
	}

	opts.ThreadIDs = []uint32{7}
	out, err = New(opts).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if out.Summary.TotalTransactions != 1 || out.Summary.TotalRows != 1 || len(out.Transactions) != 1 || out.Transactions[0].GTID != sid+":1" || out.Transactions[0].ThreadID != 7 {
		t.Fatalf("intersected result = %+v transactions=%+v", out.Summary, out.Transactions)
	}
}

func TestAnalyzerThreadIDSelectorKeepsAttributedDDLGroup(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	base := time.Date(2026, 11, 2, 11, 30, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: sid + ":5", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "DDL", ThreadID: 7, Schema: "shop", Table: "orders", QuerySQL: "ALTER TABLE shop.orders ADD COLUMN status TINYINT", ServerFlavor: "mysql"},
	}
	events = append(events, threadTxn(base.Add(2*time.Second), sid+":6", 9, 2)...)

	opts := DefaultOptions()
	opts.ThreadIDs = []uint32{7}
	result, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Diagnostics.DDLEvents) != 1 || result.Summary.TotalRows != 0 {
		t.Fatalf("DDL group not kept or rows leaked: ddl=%+v summary=%+v", result.Diagnostics.DDLEvents, result.Summary)
	}

	opts.ThreadIDs = []uint32{9}
	result, err = New(opts).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Diagnostics.DDLEvents) != 0 || result.Summary.TotalRows != 2 {
		t.Fatalf("DDL group kept or rows dropped: ddl=%+v summary=%+v", result.Diagnostics.DDLEvents, result.Summary)
	}
}

func TestAnalyzerThreadIDSelectorStagesFlashbackOnlyForSelectedGroups(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	base := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	row := func(value string) []model.FlashRow {
		return []model.FlashRow{{
			Schema: "shop", Table: "orders", Op: "INSERT",
			Columns: []string{"id"}, Cols: []model.FlashCol{{Base: "int"}},
			After: []string{value}, PK: []int{0},
		}}
	}
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: sid + ":1", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", ThreadID: 7, ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, FlashRows: row("1"), ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "GTID", GTID: sid + ":2", ServerFlavor: "mysql"},
		{Timestamp: base.Add(5 * time.Second), EventType: "BEGIN", ThreadID: 9, ServerFlavor: "mysql"},
		{Timestamp: base.Add(6 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, FlashRows: row("9"), ServerFlavor: "mysql"},
		{Timestamp: base.Add(7 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
	}
	opts := DefaultOptions()
	opts.Flashback = true
	opts.ThreadIDs = []uint32{7}
	a := New(opts)
	if _, err := a.Analyze(events); err != nil {
		t.Fatal(err)
	}
	sql, err := a.Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "DELETE FROM `shop`.`orders`") || !strings.Contains(sql, "`id` <=> 1") {
		t.Fatalf("missing undo for the selected group:\n%s", sql)
	}
	if strings.Contains(sql, "`<=> 9") {
		t.Fatalf("unselected thread's row staged into the script:\n%s", sql)
	}
}

func TestAnalyzerThreadIDSelectorStagesRedoOnlyForSelectedGroups(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	base := time.Date(2026, 11, 2, 12, 30, 0, 0, time.UTC)
	row := func(value int64) []model.RedoRow {
		return []model.RedoRow{{
			Schema: "shop", Table: "orders", Op: "INSERT",
			Columns: []string{"id"}, Names: model.RowNamesFull,
			Cols:  []model.FlashCol{{Typ: mysql.MYSQL_TYPE_LONG, HasSign: true, Base: "int"}},
			After: []any{value}, PK: []int{0},
		}}
	}
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: sid + ":1", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", ThreadID: 7, ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, RedoRows: row(1), ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "GTID", GTID: sid + ":2", ServerFlavor: "mysql"},
		{Timestamp: base.Add(5 * time.Second), EventType: "BEGIN", ThreadID: 9, ServerFlavor: "mysql"},
		{Timestamp: base.Add(6 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, RedoRows: row(9), ServerFlavor: "mysql"},
		{Timestamp: base.Add(7 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
	}
	opts := DefaultOptions()
	opts.Redo = true
	opts.ThreadIDs = []uint32{7}
	a := New(opts)
	if _, err := a.Analyze(events); err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	if _, err := a.RedoPlanner().WriteTo(&sb); err != nil {
		t.Fatal(err)
	}
	sql := sb.String()
	if !strings.Contains(sql, "INSERT INTO `shop`.`orders`") {
		t.Fatalf("missing redo for the selected group:\n%s", sql)
	}
	if strings.Contains(sql, "VALUES (9)") || strings.Contains(sql, "SELECT 9") {
		t.Fatalf("unselected thread's row staged into the script:\n%s", sql)
	}
}

// --where composes inside a selected group: the thread filter picks the
// group, the row predicate still decides which of its rows are staged.
func TestAnalyzerThreadIDComposesWithRowWhere(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	base := time.Date(2026, 11, 2, 13, 0, 0, 0, time.UTC)
	row := func(value string) []model.FlashRow {
		return []model.FlashRow{{
			Schema: "shop", Table: "orders", Op: "INSERT",
			Columns: []string{"id"}, Cols: []model.FlashCol{{Base: "int"}},
			After: []string{value}, PK: []int{0},
		}}
	}
	where, err := mysqlexpr.CompileWhere("id = 2")
	if err != nil {
		t.Fatal(err)
	}
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: sid + ":1", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", ThreadID: 7, ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, FlashRows: row("1"), ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
		{Timestamp: base.Add(4 * time.Second), EventType: "GTID", GTID: sid + ":2", ServerFlavor: "mysql"},
		{Timestamp: base.Add(5 * time.Second), EventType: "BEGIN", ThreadID: 9, ServerFlavor: "mysql"},
		{Timestamp: base.Add(6 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, FlashRows: row("2"), ServerFlavor: "mysql"},
		{Timestamp: base.Add(7 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
	}
	opts := DefaultOptions()
	opts.Flashback = true
	opts.ThreadIDs = []uint32{7}
	opts.RowWhere = where
	a := New(opts)
	if _, err := a.Analyze(events); err != nil {
		t.Fatal(err)
	}
	sql, err := a.Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	// Thread 7's row is id=1, which --where drops; thread 9's id=2 row would
	// satisfy --where but its group is not selected. Nothing stages.
	if strings.Contains(sql, "DELETE FROM") || strings.Contains(sql, "orders") {
		t.Fatalf("where-unmatched or thread-unmatched row staged:\n%s", sql)
	}
}

func TestAnalyzerThreadIDSelectorFailsOnLateAttributionDisagreement(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	base := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	// ROWS precedes every thread id, so the group is decided undecidable;
	// the BEGIN that follows still merges thread id 7 into the same group.
	// No valid producer emits rows before attribution — the drain must fail
	// rather than silently mis-select.
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: sid + ":1", ServerFlavor: "mysql"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 1, ServerFlavor: "mysql"},
		{Timestamp: base.Add(2 * time.Second), EventType: "BEGIN", ThreadID: 7, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", ServerFlavor: "mysql"},
	}
	opts := DefaultOptions()
	opts.ThreadIDs = []uint32{7}
	if _, err := New(opts).Analyze(events); err == nil || !strings.Contains(err.Error(), "cannot apply --thread-id") {
		t.Fatalf("err = %v, want thread-id disagreement failure", err)
	}
}
