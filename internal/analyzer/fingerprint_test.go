// Package analyzer verifies statement fingerprinting end to end.
// input: literal Rows_query text through FingerprintSQL, and synthetic
// NormalizedEvent streams through Analyzer for grouping, ranking, filtering,
// and absent-text behavior (ADR 0026).
// output: deterministic normalized strings; bounded ranked groups scoped to
// the same retained rows the report counts.
// pos: unit tests for fingerprint.go plus the builder/aggregator wiring in
// transactions.go and report_aggregator.go.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestFingerprintSQLNormalizesStatements(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"string literal", "UPDATE users SET name = 'alice' WHERE id = 7", "update users set name=? where id=?"},
		{"double quoted literal", `UPDATE users SET name = "alice" WHERE id = 7`, "update users set name=? where id=?"},
		{"escaped quote stays one literal", `UPDATE t SET n = 'it\'s ok'`, "update t set n=?"},
		{"doubled quote stays one literal", "UPDATE t SET n = 'it''s'", "update t set n=?"},
		{"float and negative numbers", "UPDATE t SET a = 1.5, b = -2, c = +3", "update t set a=?, b=-?, c=+?"},
		{"scientific", "UPDATE t SET a = 1e-5, b = 2E+7", "update t set a=?, b=?"},
		{"hex and binary literals", "UPDATE t SET a = 0x1F, b = 0b101", "update t set a=?, b=?"},
		{"prefixed literals", "UPDATE t SET a = X'1f', b = B'01', c = N'n'", "update t set a=?, b=?, c=?"},
		{"parameter placeholders", "UPDATE t SET a = ?, b = %s, c = :val", "update t set a=?, b=?, c=?"},
		{"whitespace folds", "UPDATE   t\nSET   a = 1\tWHERE b = 2", "update t set a=? where b=?"},
		{"spacing style invariant", "UPDATE t SET a=1,b=2 WHERE id=3", "update t set a=?, b=? where id=?"},
		{"trailing semicolon dropped", "DELETE FROM t WHERE id = 1;", "delete from t where id=?"},
		{"in list collapses", "DELETE FROM t WHERE id IN (1, 2, 3)", "delete from t where id in (?)"},
		{"in list no space collapses", "DELETE FROM t WHERE id IN(1,2)", "delete from t where id in (?)"},
		{"in list with null collapses", "DELETE FROM t WHERE id IN (NULL, 1)", "delete from t where id in (?)"},
		{"values groups collapse", "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')", "insert into t values (?)"},
		{"signed literals collapse", "INSERT INTO t VALUES (1, -2), (3, +4)", "insert into t values (?)"},
		{"nested in group collapses", "DELETE FROM t WHERE (a, b) IN ((1, 2), (3, 4))", "delete from t where (a, b) in (?)"},
		{"column list survives", "INSERT INTO t (a, b, c) VALUES (1, 2, 3)", "insert into t(a, b, c) values (?)"},
		{"identifiers keep case", "UPDATE MyTable SET MyCol = 1", "update MyTable set MyCol=?"},
		{"keywords fold", "UPDATE T SET A = 1 WHERE B IS NOT NULL", "update T set A=? where B is not null"},
		{"backticks stripped", "UPDATE `my schema`.`My Table` SET `x` = 1", "update my schema.My Table set x=?"},
		{"function folds", "SELECT COUNT(*) FROM t", "select count(*) from t"},
		{"function case folds", "SELECT Now() FROM t", "select now() from t"},
		{"star stays spaced", "SELECT * FROM t WHERE a*b = 3", "select * from t where a * b=?"},
		{"dotted name stays tight", "UPDATE shop.orders SET a = 1", "update shop.orders set a=?"},
		{"line comment stripped", "UPDATE t SET a = 1 -- bump counter\nWHERE id = 2", "update t set a=? where id=?"},
		{"hash comment stripped", "UPDATE t SET a = 1 # bump\nWHERE id = 2", "update t set a=? where id=?"},
		{"block comment stripped", "UPDATE /* hint */ t SET a = 1", "update t set a=?"},
		{"versioned comment stripped", "UPDATE t SET a = 1 /*!80000 INVISIBLE */", "update t set a=?"},
		{"minus minus not comment", "UPDATE t SET a = a--1", "update t set a=a--?"},
		{"subquery stays expanded", "DELETE FROM t WHERE id IN (SELECT id FROM u)", "delete from t where id in (select id from u)"},
		{"empty", "", ""},
		{"whitespace only", "   \n\t  ", ""},
		{"only comment", "-- nothing\n", ""},
		{"digit led identifier", "UPDATE 2fa_codes SET v = 1", "update 2fa_codes set v=?"},
		{"between stays two literals", "DELETE FROM t WHERE n BETWEEN 1 AND 10", "delete from t where n between ? and ?"},
		{"is null stays", "UPDATE t SET a = 1 WHERE b IS NULL", "update t set a=? where b is null"},
		{"cast function", "UPDATE t SET a = CAST(1 AS UNSIGNED)", "update t set a=cast(? as UNSIGNED)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FingerprintSQL(tc.in); got != tc.want {
				t.Fatalf("FingerprintSQL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// fpTxn builds a begin/rows/xid transaction carrying one Rows_query statement.
func fpTxn(base time.Time, key, sql, schema, table, op string, rows int) []model.NormalizedEvent {
	return []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: key},
		{Timestamp: base.Add(time.Second), EventType: "ROWS_QUERY", TxnKey: key, QuerySQL: sql, QueryOriginalBytes: len(sql)},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", TxnKey: key, Schema: schema, Table: table, Operation: op, RowCount: rows},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", TxnKey: key},
	}
}

func TestAnalyzerGroupsIdenticalStatementsAcrossTransactions(t *testing.T) {
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	var events []model.NormalizedEvent
	// Same shape with different literals groups once; statements count
	// occurrences, rows accumulate across transactions.
	events = append(events, fpTxn(base, "t1", "UPDATE orders SET status='done' WHERE id = 1", "shop", "orders", "UPDATE", 3)...)
	events = append(events, fpTxn(base.Add(time.Minute), "t2", "update orders set status='done' where id = 2", "shop", "orders", "UPDATE", 2)...)
	events = append(events, fpTxn(base.Add(2*time.Minute), "t3", "DELETE FROM orders WHERE id = 9", "shop", "orders", "DELETE", 1)...)

	result, err := New(Options{}).Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	fps := result.StatementFingerprints
	if len(fps) != 2 {
		t.Fatalf("expected 2 fingerprint groups, got %d: %+v", len(fps), fps)
	}
	top := fps[0]
	if top.Fingerprint != "update orders set status=? where id=?" {
		t.Fatalf("top fingerprint = %q", top.Fingerprint)
	}
	if top.Statements != 2 || top.Rows != 5 {
		t.Fatalf("top group statements/rows = %d/%d, want 2/5", top.Statements, top.Rows)
	}
	if top.Tables["shop.orders"] != 5 {
		t.Fatalf("top group table rows = %v", top.Tables)
	}
	if top.Sample == "" {
		t.Fatal("expected first-seen sample text")
	}
	// Ranking is rows-first: the UPDATE group outranks DELETE 5-to-1.
	if fps[1].Fingerprint != "delete from orders where id=?" || fps[1].Rows != 1 {
		t.Fatalf("second fingerprint = %+v", fps[1])
	}
	if fps[0].ShareOfRows != 5.0/6.0 || fps[1].ShareOfRows != 1.0/6.0 {
		t.Fatalf("shares = %v / %v", fps[0].ShareOfRows, fps[1].ShareOfRows)
	}
}

func TestAnalyzerCountsIdenticalTextsAsSeparateOccurrences(t *testing.T) {
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	sql := "UPDATE orders SET qty = qty + 1 WHERE id = 5"
	// One transaction, two identical statements under two ROWS_QUERY events:
	// statements must be 2, not 1 — the dedup key is occurrence, not text.
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS_QUERY", TxnKey: "t1", QuerySQL: sql},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 1},
		{Timestamp: base.Add(3 * time.Second), EventType: "ROWS_QUERY", TxnKey: "t1", QuerySQL: sql},
		{Timestamp: base.Add(4 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: 1},
		{Timestamp: base.Add(5 * time.Second), EventType: "XID", TxnKey: "t1"},
	}
	result, err := New(Options{}).Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.StatementFingerprints) != 1 {
		t.Fatalf("expected 1 group, got %+v", result.StatementFingerprints)
	}
	fp := result.StatementFingerprints[0]
	if fp.Statements != 2 || fp.Rows != 2 {
		t.Fatalf("statements/rows = %d/%d, want 2/2", fp.Statements, fp.Rows)
	}
}

func TestAnalyzerFingerprintMergesMultiTableStatement(t *testing.T) {
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	sql := "INSERT INTO audit_log SELECT * FROM events"
	// One statement whose row events span two tables merges both into the
	// group's table attribution.
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS_QUERY", TxnKey: "t1", QuerySQL: sql},
		{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "app", Table: "audit_log", Operation: "INSERT", RowCount: 4},
		{Timestamp: base.Add(3 * time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "app", Table: "audit_idx", Operation: "INSERT", RowCount: 2},
		{Timestamp: base.Add(4 * time.Second), EventType: "XID", TxnKey: "t1"},
	}
	result, err := New(Options{}).Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.StatementFingerprints) != 1 {
		t.Fatalf("expected 1 group, got %+v", result.StatementFingerprints)
	}
	fp := result.StatementFingerprints[0]
	if fp.Statements != 1 || fp.Rows != 6 {
		t.Fatalf("statements/rows = %d/%d, want 1/6", fp.Statements, fp.Rows)
	}
	if fp.Tables["app.audit_log"] != 4 || fp.Tables["app.audit_idx"] != 2 {
		t.Fatalf("tables = %v", fp.Tables)
	}
}

func TestAnalyzerFingerprintExcludesFilteredStatements(t *testing.T) {
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	// Under --include-table shop.orders, the statement writing shop.logs is
	// filtered out with its row event; the fingerprint ranking must match the
	// rows the report actually counted.
	var events []model.NormalizedEvent
	events = append(events, fpTxn(base, "t1", "UPDATE orders SET a = 1", "shop", "orders", "UPDATE", 5)...)
	events = append(events, fpTxn(base.Add(time.Minute), "t2", "DELETE FROM logs WHERE id = 1", "shop", "logs", "DELETE", 9)...)

	opts := DefaultOptions()
	opts.IncludeTables = []string{"shop.orders"}
	result, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.StatementFingerprints) != 1 {
		t.Fatalf("expected only the retained statement, got %+v", result.StatementFingerprints)
	}
	fp := result.StatementFingerprints[0]
	if fp.Fingerprint != "update orders set a=?" || fp.Rows != 5 {
		t.Fatalf("fingerprint = %+v", fp)
	}
}

func TestAnalyzerFingerprintExcludesUnmatchedGTIDTransactions(t *testing.T) {
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	mkGTID := func(n int) string {
		return "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:" + string(rune('0'+n))
	}
	var events []model.NormalizedEvent
	mkGTIDTxn := func(key, gtid, sql string, rows int) {
		events = append(events,
			model.NormalizedEvent{Timestamp: base, EventType: "GTID", TxnKey: key, GTID: gtid},
			model.NormalizedEvent{Timestamp: base.Add(time.Second), EventType: "ROWS_QUERY", TxnKey: key, QuerySQL: sql},
			model.NormalizedEvent{Timestamp: base.Add(2 * time.Second), EventType: "ROWS", TxnKey: key, Schema: "shop", Table: "orders", Operation: "UPDATE", RowCount: rows},
			model.NormalizedEvent{Timestamp: base.Add(3 * time.Second), EventType: "XID", TxnKey: key},
		)
	}
	mkGTIDTxn("t1", mkGTID(1), "UPDATE orders SET a = 1", 3)
	mkGTIDTxn("t2", mkGTID(2), "DELETE FROM orders WHERE id = 2", 7)

	selector, err := ParseGTIDSelector(nil, []string{mkGTID(2)})
	if err != nil {
		t.Fatal(err)
	}
	opts := DefaultOptions()
	opts.GTIDSelector = selector
	result, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.StatementFingerprints) != 1 {
		t.Fatalf("expected only the matched GTID's statement, got %+v", result.StatementFingerprints)
	}
	if result.StatementFingerprints[0].Fingerprint != "update orders set a=?" {
		t.Fatalf("fingerprint = %q", result.StatementFingerprints[0].Fingerprint)
	}
}

func TestAnalyzerFingerprintAbsentRowsQueryIsEmpty(t *testing.T) {
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN", TxnKey: "t1"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", TxnKey: "t1", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 3},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID", TxnKey: "t1"},
	}
	result, err := New(Options{}).Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.StatementFingerprints) != 0 {
		t.Fatalf("expected no fingerprints without ROWS_QUERY text, got %+v", result.StatementFingerprints)
	}
	if result.SQLContextAvailable {
		t.Fatal("sql context must stay unavailable so renderers can explain the empty list")
	}
}

func TestAnalyzerFingerprintCoversAllTransactionsNotOnlyListed(t *testing.T) {
	base := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	var events []model.NormalizedEvent
	// 15 transactions; --top-transactions 5 lists only 5. The fingerprint
	// stream sees every retained statement regardless.
	for i := 0; i < 15; i++ {
		events = append(events, fpTxn(base.Add(time.Duration(i)*time.Minute),
			"t"+strings.Repeat("x", i+1), "UPDATE orders SET a = 1", "shop", "orders", "UPDATE", 1)...)
	}
	opts := DefaultOptions()
	opts.TopTransactions = 5
	result, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.StatementFingerprints) != 1 {
		t.Fatalf("expected 1 group, got %+v", result.StatementFingerprints)
	}
	if fp := result.StatementFingerprints[0]; fp.Statements != 15 || fp.Rows != 15 {
		t.Fatalf("statements/rows = %d/%d, want 15/15", fp.Statements, fp.Rows)
	}
}

func TestReportAggregatorFingerprintOverflowCapsGroups(t *testing.T) {
	agg := NewReportAggregator(Options{})
	// Distinct fingerprints past the group cap count in overflow instead of
	// opening groups; tracked groups keep exact counts.
	for i := 0; i < fingerprintTrackLimit+3; i++ {
		fp := "update t set a=? where id=" + strconv.Itoa(i)
		agg.ConsumeTransaction(model.Transaction{
			TxnKey:    "t",
			Tables:    map[string]int{"s.t": 1},
			TotalRows: 1,
			RetainedStatements: []model.RetainedStatement{
				{Fingerprint: fp, Sample: "s", Rows: 1, Tables: map[string]int{"s.t": 1}},
			},
		})
	}
	snap := agg.Snapshot()
	if len(snap.StatementFingerprints) != fingerprintTrackLimit {
		t.Fatalf("groups = %d, want cap %d", len(snap.StatementFingerprints), fingerprintTrackLimit)
	}
	if snap.FingerprintOverflow != 3 {
		t.Fatalf("overflow = %d, want 3", snap.FingerprintOverflow)
	}
	// The transient field must not survive into ranked transactions.
	for _, txn := range snap.Transactions {
		if txn.RetainedStatements != nil {
			t.Fatal("RetainedStatements leaked into a listed transaction")
		}
	}
}
