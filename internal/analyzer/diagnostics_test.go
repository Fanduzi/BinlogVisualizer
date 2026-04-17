// Package analyzer verifies diagnostic finding builders and evidence helpers for DBA-oriented outputs.
// input: alerts, minute buckets, completed transactions, and DDL timeline events.
// output: regression coverage for finding mapping, evidence enrichment, and deterministic hot-interval summaries.
// pos: focused diagnostics contract tests ahead of Analyzer integration wiring.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import (
	"strconv"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestHotIntervalEvidenceIncludesBucketAndDDLRefs(t *testing.T) {
	minute := time.Date(2026, 4, 15, 11, 45, 0, 0, time.UTC)
	ddl := model.DDLEvent{
		Timestamp:  minute.Add(25 * time.Second),
		Operation:  "ALTER TABLE",
		Schema:     "app",
		Table:      "orders",
		Statement:  "ALTER TABLE app.orders ADD COLUMN note TEXT",
		BinlogPath: "mysql-bin.000010",
	}

	got := HotIntervalEvidence(
		model.MinuteBucket{Minute: minute, TotalRows: 300, TxnCount: 8, EventCount: 12, BinlogBytes: 8192, DDLCount: 1},
		[]model.DDLEvent{ddl},
	)

	want := []string{
		"rows=300",
		"txns=8",
		"events=12",
		"binlog_bytes=8192",
		"ddl=ALTER TABLE app.orders @ mysql-bin.000010",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d evidence refs, got %d", len(want), len(got))
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("evidence %d: expected %q, got %q", index, want[index], got[index])
		}
	}
}

func TestBuildFindingsFromAlertsEnrichesTransactionAndMinuteAlerts(t *testing.T) {
	minute := time.Date(2026, 4, 15, 12, 10, 0, 0, time.UTC)
	txns := []model.Transaction{{
		TxnKey:          "txn-9",
		TotalRows:       1200,
		EventCount:      12,
		Duration:        5 * time.Second,
		BinlogBytes:     4096,
		BinlogPathStart: "mysql-bin.000009",
		BinlogPathEnd:   "mysql-bin.000009",
		PositionStart:   100,
		PositionEnd:     220,
	}}
	minutes := []model.MinuteBucket{{
		Minute:      minute,
		TotalRows:   300,
		TxnCount:    8,
		EventCount:  12,
		BinlogBytes: 8192,
		DDLCount:    1,
	}}
	ddlEvents := []model.DDLEvent{{
		Timestamp:  minute.Add(30 * time.Second),
		Operation:  "ALTER TABLE",
		Schema:     "app",
		Table:      "orders",
		Statement:  "ALTER TABLE app.orders ADD COLUMN note TEXT",
		BinlogPath: "mysql-bin.000010",
	}}

	got := BuildFindingsFromAlerts([]model.Alert{
		{Type: "large_transaction", Severity: "warning", Message: "large txn", TxnKey: "txn-9"},
		{Type: "spike", Severity: "critical", Message: "write spike", Minute: minute},
	}, minutes, txns, ddlEvents)

	if len(got) != 2 {
		t.Fatalf("expected 2 findings, got %d", len(got))
	}

	if got[0].Kind != "large_transaction" || got[0].TxnKey != "txn-9" {
		t.Fatalf("expected first finding to map transaction alert, got %+v", got[0])
	}
	wantTxnEvidence := []string{
		"rows=1200",
		"events=12",
		"duration_ms=5000",
		"binlog_bytes=4096",
		"binlog_span=mysql-bin.000009:100-mysql-bin.000009:220",
	}
	for index := range wantTxnEvidence {
		if got[0].EvidenceRefs[index] != wantTxnEvidence[index] {
			t.Fatalf("txn evidence %d: expected %q, got %q", index, wantTxnEvidence[index], got[0].EvidenceRefs[index])
		}
	}

	if got[1].Kind != "spike" || !got[1].Minute.Equal(minute) {
		t.Fatalf("expected second finding to map spike minute, got %+v", got[1])
	}
	if len(got[1].EvidenceRefs) == 0 || got[1].EvidenceRefs[len(got[1].EvidenceRefs)-1] != "ddl=ALTER TABLE app.orders @ mysql-bin.000010" {
		t.Fatalf("expected DDL evidence on spike finding, got %v", got[1].EvidenceRefs)
	}
}

func BenchmarkBuildFindingsFromAlertsIncidentScale(b *testing.B) {
	base := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)
	txns := make([]model.Transaction, 0, 5000)
	minutes := make([]model.MinuteBucket, 0, 5000)
	alerts := make([]model.Alert, 0, 10000)
	ddlEvents := make([]model.DDLEvent, 0, 5000)

	for index := 0; index < 5000; index++ {
		minute := base.Add(time.Duration(index) * time.Minute)
		txnKey := "txn-" + strconv.Itoa(index)
		txns = append(txns, model.Transaction{
			TxnKey:          txnKey,
			TotalRows:       1000 + index%100,
			EventCount:      10 + index%5,
			Duration:        time.Duration(index%120) * time.Second,
			BinlogBytes:     int64(4096 + index),
			BinlogPathStart: "mysql-bin.000100",
			BinlogPathEnd:   "mysql-bin.000101",
			PositionStart:   int64(index * 100),
			PositionEnd:     int64(index*100 + 99),
		})
		minutes = append(minutes, model.MinuteBucket{
			Minute:      minute,
			TotalRows:   500 + index%250,
			TxnCount:    3 + index%10,
			EventCount:  8 + index%4,
			BinlogBytes: int64(2048 + index),
			DDLCount:    1,
		})
		ddlEvents = append(ddlEvents, model.DDLEvent{
			Timestamp:  minute.Add(30 * time.Second),
			Operation:  "ALTER TABLE",
			Schema:     "shop",
			Table:      "orders",
			BinlogPath: "mysql-bin.000100",
		})
		alerts = append(alerts,
			model.Alert{Type: "large_transaction", Severity: "warning", Message: "large txn", TxnKey: txnKey},
			model.Alert{Type: "spike", Severity: "critical", Message: "write spike", Minute: minute},
		)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		findings := BuildFindingsFromAlerts(alerts, minutes, txns, ddlEvents)
		if len(findings) != len(alerts) {
			b.Fatalf("expected %d findings, got %d", len(alerts), len(findings))
		}
	}
}

func BenchmarkDiagnosticTopSelectorsIncidentScale(b *testing.B) {
	txns := make([]model.Transaction, 0, 5000)
	minutes := make([]model.MinuteBucket, 0, 5000)
	base := time.Date(2026, 4, 17, 12, 0, 0, 0, time.UTC)

	for index := 0; index < 5000; index++ {
		txns = append(txns, model.Transaction{
			TxnKey:      "txn-" + strconv.Itoa(index),
			TotalRows:   1000 + index%1000,
			EventCount:  8 + index%7,
			Duration:    time.Duration(index%300) * time.Second,
			BinlogBytes: int64(4096 + index*3),
			Tables: map[string]int{
				"shop.orders":                     100 + index%50,
				"shop.orders_archive_" + strconv.Itoa(index%32): 10 + index%9,
			},
			Operations: map[string]int{
				"UPDATE": 50 + index%11,
				"INSERT": 10 + index%5,
			},
		})
		minutes = append(minutes, model.MinuteBucket{
			Minute:      base.Add(time.Duration(index) * time.Minute),
			TotalRows:   500 + index%250,
			TxnCount:    3 + index%10,
			EventCount:  8 + index%4,
			BinlogBytes: int64(2048 + index),
			DDLCount:    index % 2,
		})
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		largest, longest := SelectDiagnosticTransactions(txns, 5)
		if len(largest) != 5 || len(longest) != 5 {
			b.Fatalf("expected 5 diagnostic transactions, got largest=%d longest=%d", len(largest), len(longest))
		}
		hot := SelectHotIntervals(minutes, 5)
		if len(hot) != 5 {
			b.Fatalf("expected 5 hot intervals, got %d", len(hot))
		}
		wide := SelectWidestTransactions(txns, 5)
		if len(wide) != 5 {
			b.Fatalf("expected 5 widest transactions, got %d", len(wide))
		}
	}
}

func TestSelectDiagnosticTransactionsRanksLargestAndLongestSeparately(t *testing.T) {
	txns := []model.Transaction{
		{
			TxnKey:          "txn-rows",
			TotalRows:       2000,
			EventCount:      20,
			Duration:        2 * time.Second,
			BinlogBytes:     7000,
			BinlogPathStart: "mysql-bin.000001",
			BinlogPathEnd:   "mysql-bin.000001",
			PositionStart:   100,
			PositionEnd:     200,
			Tables:          map[string]int{"shop.orders": 1800, "shop.payments": 200},
		},
		{
			TxnKey:          "txn-long",
			TotalRows:       50,
			EventCount:      5,
			Duration:        35 * time.Second,
			BinlogBytes:     400,
			BinlogPathStart: "mysql-bin.000002",
			BinlogPathEnd:   "mysql-bin.000002",
			PositionStart:   220,
			PositionEnd:     260,
			Tables:          map[string]int{"shop.accounts": 50},
		},
		{
			TxnKey:          "txn-mid",
			TotalRows:       500,
			EventCount:      9,
			Duration:        5 * time.Second,
			BinlogBytes:     1500,
			BinlogPathStart: "mysql-bin.000003",
			BinlogPathEnd:   "mysql-bin.000003",
			PositionStart:   300,
			PositionEnd:     360,
			Tables:          map[string]int{"shop.inventory": 500},
		},
	}

	largest, longest := SelectDiagnosticTransactions(txns, 2)

	if len(largest) != 2 || len(longest) != 2 {
		t.Fatalf("expected 2 largest and 2 longest txns, got largest=%d longest=%d", len(largest), len(longest))
	}
	if largest[0].TxnKey != "txn-rows" || largest[1].TxnKey != "txn-mid" {
		t.Fatalf("unexpected largest ordering: %#v", largest)
	}
	if longest[0].TxnKey != "txn-long" || longest[1].TxnKey != "txn-mid" {
		t.Fatalf("unexpected longest ordering: %#v", longest)
	}
}

func TestSelectHotIntervalsReturnsBusiestMinutes(t *testing.T) {
	minutes := []model.MinuteBucket{
		{Minute: time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC), TotalRows: 100, TxnCount: 3},
		{Minute: time.Date(2026, 4, 15, 10, 1, 0, 0, time.UTC), TotalRows: 900, TxnCount: 8, DDLCount: 1},
		{Minute: time.Date(2026, 4, 15, 10, 2, 0, 0, time.UTC), TotalRows: 500, TxnCount: 4},
	}

	got := SelectHotIntervals(minutes, 2)
	if len(got) != 2 {
		t.Fatalf("expected 2 hot intervals, got %d", len(got))
	}
	if !got[0].Minute.Equal(time.Date(2026, 4, 15, 10, 1, 0, 0, time.UTC)) || got[0].TotalRows != 900 {
		t.Fatalf("expected hottest minute first, got %#v", got[0])
	}
	if !got[1].Minute.Equal(time.Date(2026, 4, 15, 10, 2, 0, 0, time.UTC)) || got[1].TotalRows != 500 {
		t.Fatalf("expected second hottest minute second, got %#v", got[1])
	}
}

func TestBuildFileSegmentsReturnsSegmentsFromMinuteBuckets(t *testing.T) {
	minutes := []model.MinuteBucket{
		{Minute: time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC), TotalRows: 100, EventCount: 15, BinlogBytes: 2048},
		{Minute: time.Date(2026, 4, 15, 10, 1, 0, 0, time.UTC), TotalRows: 300, EventCount: 40, BinlogBytes: 8192},
		{Minute: time.Date(2026, 4, 15, 10, 2, 0, 0, time.UTC), TotalRows: 50, EventCount: 8, BinlogBytes: 512},
	}

	segments := BuildFileSegments(minutes, 1)

	if len(segments) != 3 {
		t.Fatalf("expected 3 segments with segmentSize=1, got %d", len(segments))
	}
	seg := segments[0]
	if !seg.StartTime.Equal(time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected segment start 10:00, got %v", seg.StartTime)
	}
	if !seg.EndTime.Equal(time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected segment end 10:00, got %v", seg.EndTime)
	}
	if seg.Rows != 100 || seg.Events != 15 || seg.BinlogBytes != 2048 {
		t.Fatalf("expected rows=100 events=15 binlog_bytes=2048, got rows=%d events=%d binlog_bytes=%d",
			seg.Rows, seg.Events, seg.BinlogBytes)
	}
}

func TestBuildFileSegmentsGroupsBucketsIntoLargerSegments(t *testing.T) {
	minutes := []model.MinuteBucket{
		{Minute: time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC), TotalRows: 100, EventCount: 15, BinlogBytes: 2048},
		{Minute: time.Date(2026, 4, 15, 10, 1, 0, 0, time.UTC), TotalRows: 300, EventCount: 40, BinlogBytes: 8192},
		{Minute: time.Date(2026, 4, 15, 10, 2, 0, 0, time.UTC), TotalRows: 50, EventCount: 8, BinlogBytes: 512},
		{Minute: time.Date(2026, 4, 15, 10, 3, 0, 0, time.UTC), TotalRows: 200, EventCount: 25, BinlogBytes: 4096},
	}

	segments := BuildFileSegments(minutes, 2)

	if len(segments) != 2 {
		t.Fatalf("expected 2 segments with segmentSize=2, got %d", len(segments))
	}
	// First segment: 10:00-10:01
	if segments[0].Rows != 400 || segments[0].Events != 55 || segments[0].BinlogBytes != 10240 {
		t.Fatalf("expected first segment rows=400 events=55 binlog_bytes=10240, got %+v", segments[0])
	}
	// Second segment: 10:02-10:03
	if segments[1].Rows != 250 || segments[1].Events != 33 || segments[1].BinlogBytes != 4608 {
		t.Fatalf("expected second segment rows=250 events=33 binlog_bytes=4608, got %+v", segments[1])
	}
}

func TestBuildFileSegmentsReturnsEmptyForNoMinutes(t *testing.T) {
	segments := BuildFileSegments(nil, 1)
	if len(segments) != 0 {
		t.Fatalf("expected 0 segments for nil minutes, got %d", len(segments))
	}
}

func TestSelectWidestTransactionsRanksByTableCount(t *testing.T) {
	txns := []model.Transaction{
		{
			TxnKey:   "txn-narrow",
			Tables:   map[string]int{"shop.orders": 100},
			Duration: time.Second,
		},
		{
			TxnKey:   "txn-wide",
			Tables:   map[string]int{"shop.orders": 100, "shop.payments": 50, "shop.users": 30, "shop.log": 10},
			Duration: 2 * time.Second,
		},
		{
			TxnKey:   "txn-mid",
			Tables:   map[string]int{"shop.orders": 100, "shop.inventory": 200},
			Duration: 3 * time.Second,
		},
	}

	widest := SelectWidestTransactions(txns, 2)

	if len(widest) != 2 {
		t.Fatalf("expected 2 widest transactions, got %d", len(widest))
	}
	if widest[0].TxnKey != "txn-wide" {
		t.Fatalf("expected widest first, got %s", widest[0].TxnKey)
	}
	if widest[1].TxnKey != "txn-mid" {
		t.Fatalf("expected second widest, got %s", widest[1].TxnKey)
	}
}

func TestSelectWidestTransactionsReturnsNilForEmpty(t *testing.T) {
	widest := SelectWidestTransactions(nil, 5)
	if widest != nil {
		t.Fatalf("expected nil for empty input, got %v", widest)
	}
}
