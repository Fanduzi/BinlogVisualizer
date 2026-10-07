package analyzer

import (
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestPercentile95IsNearestRank(t *testing.T) {
	sorted := make([]int64, 20)
	for i := range sorted {
		sorted[i] = int64(i + 1)
	}
	if got := percentile95(sorted); got != 19 {
		t.Fatalf("p95 of 1..20 = %d, want 19", got)
	}
	if got := percentile95([]int64{7}); got != 7 {
		t.Fatalf("p95 of one value = %d", got)
	}
	if got := percentile95([]int64{-10, -3, 4}); got != 4 {
		t.Fatalf("p95 of a short signed set = %d, want the max", got)
	}
}

func TestAnalyzerApplyDelayRanksReplicaAndIgnoresMissingTimestamps(t *testing.T) {
	base := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	peak := time.Date(2026, 3, 15, 14, 5, 10, 0, time.UTC)
	opts := DefaultOptions()
	opts.TopTransactions = 0
	events := make([]model.NormalizedEvent, 0, 80)
	// A row transaction with no commit timestamps must not become a fake 0 delay.
	events = append(events, delayGroup(base, "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1", "shop", "orders", "INSERT", 50, 1000, 0, 0)...)
	for i := 0; i < 20; i++ {
		delayUs := uint64(i + 1)
		when := base.Add(10 * time.Second)
		if i == 19 {
			when = peak
		}
		orig := uint64(when.UnixMicro())
		events = append(events, delayGroup(when, gtidN(i+2), "shop", "orders", "INSERT", 1, int64(2000+i*200), orig, orig+delayUs)...)
	}
	// DDL-only group carries timestamps and is not a Top Tables transaction.
	ddlAt := peak.Add(time.Minute)
	events = append(events, model.NormalizedEvent{
		Timestamp: ddlAt, EventType: "GTID", GTID: gtidN(99),
		BinlogPath: "mysql-bin.000001", PositionStart: 9000, PositionEnd: 9040,
		OriginalCommitUs: 1, ImmediateCommitUs: 9_000_000, ServerFlavor: "mysql",
	}, model.NormalizedEvent{
		Timestamp: ddlAt, EventType: "DDL", Schema: "shop", Table: "orders",
		QuerySQL:   "ALTER TABLE shop.orders ADD COLUMN note INT",
		BinlogPath: "mysql-bin.000001", PositionStart: 9040, PositionEnd: 9100, ServerFlavor: "mysql",
	})

	result, err := New(opts).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	delay := result.Diagnostics.ApplyDelay
	if delay == nil || delay.Origin != model.ApplyDelayReplica {
		t.Fatalf("delay=%+v, want a replica", delay)
	}
	if delay.Max != 20*time.Microsecond || delay.P95 != 19*time.Microsecond {
		t.Fatalf("max=%s p95=%s", delay.Max, delay.P95)
	}
	wantPeak := time.Date(2026, 3, 15, 14, 5, 0, 0, time.UTC)
	if !delay.PeakMinute.Equal(wantPeak) {
		t.Fatalf("peak minute=%s, want %s", delay.PeakMinute, wantPeak)
	}
	if len(delay.Transactions) != 20 || delay.Transactions[0].GTID != gtidN(21) || delay.Transactions[0].Delay != 20*time.Microsecond {
		t.Fatalf("top=%+v", delay.Transactions[0])
	}
	if delay.Transactions[0].TxnStartPos != 2000+19*200 {
		t.Fatalf("txn start=%s:%d", delay.Transactions[0].TxnStartPath, delay.Transactions[0].TxnStartPos)
	}
	if delay.Transactions[0].Tables["shop.orders"] != 1 {
		t.Fatalf("tables=%v", delay.Transactions[0].Tables)
	}
	for _, finding := range result.Diagnostics.Findings {
		if finding.Kind == "replica_apply_delay" {
			t.Fatalf("delay became a finding: %+v", finding)
		}
	}
}

func TestAnalyzerApplyDelaySourceOmitsTheRanking(t *testing.T) {
	base := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	orig := uint64(base.UnixMicro())
	events := delayGroup(base, gtidN(1), "shop", "orders", "INSERT", 2, 100, orig, orig)
	events = append(events, delayGroup(base.Add(time.Second), gtidN(2), "shop", "orders", "UPDATE", 1, 400, orig+1_000_000, orig+1_000_000)...)
	result, err := New(DefaultOptions()).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	delay := result.Diagnostics.ApplyDelay
	if delay == nil || delay.Origin != model.ApplyDelaySource || delay.Max != 0 || delay.P95 != 0 || len(delay.Transactions) != 0 {
		t.Fatalf("source delay=%+v", delay)
	}
}

func TestAnalyzerApplyDelayUnavailableWhenTimestampsAreAbsent(t *testing.T) {
	base := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	result, err := New(DefaultOptions()).Analyze(delayGroup(base, "", "shop", "orders", "INSERT", 1, 100, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if result.Diagnostics.ApplyDelay != nil {
		t.Fatalf("missing timestamps produced %+v", result.Diagnostics.ApplyDelay)
	}
}

func TestAnalyzerApplyDelayFollowsTableDMLTimePositionAndGTIDFilters(t *testing.T) {
	base := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	orig := uint64(base.UnixMicro())
	// audit is the slowest. orders is next. A delete is the only DELETE.
	// A second orders row is inside a later minute so a time window can drop it.
	events := delayGroup(base, gtidN(1), "shop", "audit", "INSERT", 1, 100, orig, orig+5_000)
	events = append(events, delayGroup(base.Add(time.Second), gtidN(2), "shop", "orders", "INSERT", 3, 500, orig, orig+3_000)...)
	events = append(events, delayGroup(base.Add(2*time.Second), gtidN(3), "shop", "orders", "DELETE", 4, 900, orig, orig+1_000)...)
	late := base.Add(2 * time.Hour)
	events = append(events, delayGroup(late, gtidN(4), "shop", "orders", "INSERT", 8, 1300, orig, orig+9_000)...)
	// Mixed transaction: catalog rows are dropped by an orders include, orders rows stay.
	mixed := []model.NormalizedEvent{
		{Timestamp: base.Add(3 * time.Second), EventType: "GTID", GTID: gtidN(5), BinlogPath: "mysql-bin.000001", PositionStart: 1700, PositionEnd: 1740, OriginalCommitUs: orig, ImmediateCommitUs: orig + 2_000, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 1740, PositionEnd: 1780, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 2, BinlogPath: "mysql-bin.000001", PositionStart: 1780, PositionEnd: 1820, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "ROWS", Schema: "shop", Table: "catalog", Operation: "INSERT", RowCount: 9, BinlogPath: "mysql-bin.000001", PositionStart: 1820, PositionEnd: 1860, ServerFlavor: "mysql"},
		{Timestamp: base.Add(3 * time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 1860, PositionEnd: 1900, ServerFlavor: "mysql"},
	}
	events = append(events, mixed...)

	all, err := New(DefaultOptions()).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if all.Diagnostics.ApplyDelay == nil || all.Diagnostics.ApplyDelay.Max != 9*time.Millisecond {
		t.Fatalf("unfiltered max=%v", all.Diagnostics.ApplyDelay)
	}

	orders := DefaultOptions()
	orders.IncludeTables = []string{"shop.orders"}
	filtered, err := New(orders).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	delay := filtered.Diagnostics.ApplyDelay
	if delay == nil || delay.Max != 9*time.Millisecond {
		t.Fatalf("include orders max=%v", delay)
	}
	for _, txn := range delay.Transactions {
		if txn.GTID == gtidN(1) {
			t.Fatalf("audit transaction survived include orders: %+v", txn)
		}
		if _, ok := txn.Tables["shop.catalog"]; ok {
			t.Fatalf("catalog rows survived include orders: %+v", txn)
		}
	}
	var mixedSeen bool
	for _, txn := range delay.Transactions {
		if txn.GTID == gtidN(5) {
			mixedSeen = true
			if txn.Tables["shop.orders"] != 2 || len(txn.Tables) != 1 {
				t.Fatalf("mixed tables=%v", txn.Tables)
			}
		}
	}
	if !mixedSeen {
		t.Fatal("mixed orders transaction was dropped")
	}

	onlyDelete := DefaultOptions()
	onlyDelete.IncludeDML = []string{"DELETE"}
	deleted, err := New(onlyDelete).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Diagnostics.ApplyDelay == nil || len(deleted.Diagnostics.ApplyDelay.Transactions) != 1 || deleted.Diagnostics.ApplyDelay.Transactions[0].GTID != gtidN(3) || deleted.Diagnostics.ApplyDelay.Max != time.Millisecond {
		t.Fatalf("dml delete=%+v", deleted.Diagnostics.ApplyDelay)
	}

	end := base.Add(time.Minute)
	windowed := DefaultOptions()
	windowed.End = &end
	window, err := New(windowed).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if window.Diagnostics.ApplyDelay == nil || window.Diagnostics.ApplyDelay.Max != 5*time.Millisecond {
		t.Fatalf("time window kept the late 9ms transaction: %+v", window.Diagnostics.ApplyDelay)
	}

	startPos := int64(500)
	positioned := DefaultOptions()
	positioned.StartPosition = &startPos
	positioned.End = &end
	position, err := New(positioned).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if position.Diagnostics.ApplyDelay == nil || position.Diagnostics.ApplyDelay.Transactions[0].GTID != gtidN(2) {
		t.Fatalf("position window top=%+v", position.Diagnostics.ApplyDelay)
	}

	selector, err := ParseGTIDSelector(nil, []string{gtidN(1)})
	if err != nil {
		t.Fatal(err)
	}
	gtidOpts := DefaultOptions()
	gtidOpts.GTIDSelector = selector
	gtidOpts.End = &end
	excluded, err := New(gtidOpts).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if excluded.Diagnostics.ApplyDelay == nil || excluded.Diagnostics.ApplyDelay.Max != 3*time.Millisecond {
		t.Fatalf("exclude gtid max=%v", excluded.Diagnostics.ApplyDelay)
	}

	schema := DefaultOptions()
	schema.ExcludeSchemas = []string{"shop"}
	none, err := New(schema).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	if none.Diagnostics.ApplyDelay != nil || none.Summary.TotalRows != 0 {
		t.Fatalf("schema exclude still counted delay=%+v rows=%d", none.Diagnostics.ApplyDelay, none.Summary.TotalRows)
	}
}

func TestAnalyzerApplyDelayKeepsNegativeSkew(t *testing.T) {
	base := time.Date(2026, 3, 15, 14, 0, 0, 0, time.UTC)
	orig := uint64(base.UnixMicro())
	events := delayGroup(base, gtidN(1), "shop", "orders", "INSERT", 1, 100, orig, orig-50)
	result, err := New(DefaultOptions()).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	delay := result.Diagnostics.ApplyDelay
	if delay == nil || delay.Origin != model.ApplyDelayReplica || delay.Max != -50*time.Microsecond {
		t.Fatalf("negative delay=%+v", delay)
	}
}

func delayGroup(base time.Time, gtid, schema, table, op string, rows int, start int64, orig, imm uint64) []model.NormalizedEvent {
	const path = "mysql-bin.000001"
	return []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid, BinlogPath: path, PositionStart: start, PositionEnd: start + 40, OriginalCommitUs: orig, ImmediateCommitUs: imm, ServerFlavor: "mysql"},
		{Timestamp: base, EventType: "BEGIN", BinlogPath: path, PositionStart: start + 40, PositionEnd: start + 80, ServerFlavor: "mysql"},
		{Timestamp: base, EventType: "ROWS", Schema: schema, Table: table, Operation: op, RowCount: rows, BinlogPath: path, PositionStart: start + 80, PositionEnd: start + 120, ServerFlavor: "mysql"},
		{Timestamp: base, EventType: "XID", BinlogPath: path, PositionStart: start + 120, PositionEnd: start + 140, ServerFlavor: "mysql"},
	}
}

func gtidN(n int) string {
	return "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:" + itoa(n)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
