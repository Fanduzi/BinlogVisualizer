//go:build cgo

// Package detailstore exercises the DuckDB adapter: schema, batching, hydration, and benchmarks.
// input: in-process DuckDB files under t.TempDir().
// output: adapter guarantees the analyzer relies on through the detail-store interface.
// pos: adapter tests live with the package; cross-store contract tests stay in internal/analyzer.
// note: if this file changes, keep this header and README.md synchronized.
package detailstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestDuckDBStoreInitializesSchema(t *testing.T) {
	store := newTestDuckDBStore(t, DefaultBatchFlushRows)

	for _, table := range []string{
		"transactions",
		"transaction_sql_contexts",
		"transaction_tables",
		"transaction_operations",
		"minute_buckets",
		"minute_table_rows",
		"alerts",
	} {
		if !store.tableExists(table) {
			t.Fatalf("expected table %q to exist", table)
		}
	}
}

func TestDuckDBStoreDoesNotDefineUnboundedMirrors(t *testing.T) {
	storeType := reflect.TypeOf(DuckDBStore{})
	for _, fieldName := range []string{"persistedTxns", "persistedTxnIndex", "persistedMinutes"} {
		if _, ok := storeType.FieldByName(fieldName); ok {
			t.Fatalf("DuckDBStore must not retain unbounded in-memory mirror field %q", fieldName)
		}
	}
}

func TestDuckDBStoreFlushesBatchesAtThreshold(t *testing.T) {
	store := newTestDuckDBStore(t, 2)
	base := time.Date(2026, 3, 16, 10, 0, 0, 0, time.UTC)

	if err := store.RecordTransactions([]Transaction{
		newPersistedTransaction("txn-1", base, 5),
	}); err != nil {
		t.Fatalf("RecordTransactions returned error: %v", err)
	}
	if got := store.mustCountRows(t, "transactions"); got != 0 {
		t.Fatalf("expected no persisted transaction rows before threshold, got %d", got)
	}

	if err := store.RecordTransactions([]Transaction{
		newPersistedTransaction("txn-2", base.Add(time.Second), 7),
	}); err != nil {
		t.Fatalf("RecordTransactions returned error: %v", err)
	}
	if got := store.mustCountRows(t, "transactions"); got != 2 {
		t.Fatalf("expected 2 persisted transaction rows after threshold flush, got %d", got)
	}
}

func TestDuckDBStoreResolvesBoundedQuerySQLForRequestedTransactions(t *testing.T) {
	store := newTestDuckDBStore(t, DefaultBatchFlushRows)
	base := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)

	if err := store.RecordTransactions([]Transaction{
		{
			TxnKey:             "txn-1",
			StartTime:          base,
			EndTime:            base.Add(time.Second),
			DurationMS:         1000,
			TotalRows:          9,
			EventCount:         1,
			QuerySummary:       "UPDATE users SET name = ? WHERE id = ?",
			QuerySQL:           "UPDATE users SET name = 'alice' WHERE id = 7",
			QueryTruncated:     false,
			QueryOriginalBytes: 43,
			TableRows:          map[string]int{"testdb.users": 9},
			Operations:         map[string]int{"UPDATE": 9},
		},
	}); err != nil {
		t.Fatalf("RecordTransactions returned error: %v", err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if got := store.mustCountRows(t, "transaction_sql_contexts"); got != 1 {
		t.Fatalf("expected 1 persisted SQL context row, got %d", got)
	}

	txns, err := store.QueryTopTransactions(1)
	if err != nil {
		t.Fatalf("QueryTopTransactions returned error: %v", err)
	}
	if len(txns) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txns))
	}
	if txns[0].QueryContext == nil || txns[0].QueryContext.SQL != "" {
		t.Fatalf("expected QueryTopTransactions to omit eager SQL hydration, got %#v", txns[0].QueryContext)
	}

	sqlByTxn, err := store.ResolveTransactionQuerySQL([]string{"txn-1"})
	if err != nil {
		t.Fatalf("ResolveTransactionQuerySQL returned error: %v", err)
	}
	if sqlByTxn["txn-1"] != "UPDATE users SET name = 'alice' WHERE id = 7" {
		t.Fatalf("expected resolved bounded query SQL, got %#v", sqlByTxn)
	}
}

func TestDuckDBStoreQueryAllTransactionsDoesNotHydrateFullSQL(t *testing.T) {
	store := newTestDuckDBStore(t, DefaultBatchFlushRows)
	base := time.Date(2026, 3, 16, 12, 30, 0, 0, time.UTC)

	if err := store.RecordTransactions([]Transaction{
		{
			TxnKey:             "txn-1",
			StartTime:          base,
			EndTime:            base.Add(time.Second),
			DurationMS:         1000,
			TotalRows:          9,
			EventCount:         1,
			QuerySummary:       "UPDATE users SET name = ? WHERE id = ?",
			QuerySQL:           "UPDATE users SET name = 'alice' WHERE id = 7",
			QueryTruncated:     false,
			QueryOriginalBytes: 43,
			TableRows:          map[string]int{"testdb.users": 9},
			Operations:         map[string]int{"UPDATE": 9},
		},
	}); err != nil {
		t.Fatalf("RecordTransactions returned error: %v", err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if got := store.mustCountRows(t, "transaction_sql_contexts"); got != 1 {
		t.Fatalf("expected 1 persisted SQL context row, got %d", got)
	}

	txns, err := store.QueryAllTransactions()
	if err != nil {
		t.Fatalf("QueryAllTransactions returned error: %v", err)
	}
	if len(txns) != 1 {
		t.Fatalf("expected 1 transaction, got %d", len(txns))
	}
	if txns[0].QueryContext == nil {
		t.Fatal("expected query context metadata to remain available")
	}
	if txns[0].QueryContext.SQL != "" {
		t.Fatalf("expected QueryAllTransactions to omit full SQL hydration, got %#v", txns[0].QueryContext)
	}
}

func TestDuckDBStoreQueryAllTransactionsReturnsCountError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "analysis.duckdb")
	store, err := NewDuckDBStore(path, DefaultBatchFlushRows)
	if err != nil {
		if errors.Is(err, ErrDuckDBRequiresCGO) {
			t.Skip(err.Error())
		}
		t.Fatalf("NewDuckDBStore returned error: %v", err)
	}
	// Close the store so subsequent queries fail
	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	_, err = store.QueryAllTransactions()
	if err == nil {
		t.Fatal("expected QueryAllTransactions to return error on closed store, got nil")
	}
}

func newTestDuckDBStore(t testing.TB, batchRows int) *DuckDBStore {
	t.Helper()

	path := filepath.Join(t.TempDir(), "analysis.duckdb")
	store, err := NewDuckDBStore(path, batchRows)
	if err != nil {
		if errors.Is(err, ErrDuckDBRequiresCGO) {
			t.Skip(err.Error())
		}
		t.Fatalf("NewDuckDBStore returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	})
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected DuckDB file to exist at %s: %v", path, err)
	}
	if _, err := os.Stat(path + ".querysql.jsonl"); !os.IsNotExist(err) {
		t.Fatalf("expected no SQL context sidecar file, got err=%v", err)
	}
	return store
}

func newPersistedTransaction(txnKey string, start time.Time, totalRows int) Transaction {
	return Transaction{
		TxnKey:             txnKey,
		StartTime:          start,
		EndTime:            start.Add(time.Second),
		DurationMS:         int64(time.Second / time.Millisecond),
		TotalRows:          int64(totalRows),
		EventCount:         1,
		QuerySummary:       "",
		QueryTruncated:     false,
		QueryOriginalBytes: 0,
		TableRows:          map[string]int{"shop.orders": totalRows},
		Operations:         map[string]int{"INSERT": totalRows},
	}
}

func BenchmarkDuckDBStoreQueryTopTransactionsLargeDataset(b *testing.B) {
	store := newTestDuckDBStore(b, DefaultBatchFlushRows)
	base := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)
	fixtures := make([]Transaction, 0, 4000)
	for i := 0; i < 4000; i++ {
		fixtures = append(fixtures, Transaction{
			TxnKey:             fmt.Sprintf("txn-%06d", i),
			StartTime:          base.Add(time.Duration(i) * time.Second),
			EndTime:            base.Add(time.Duration(i)*time.Second + 2*time.Second),
			DurationMS:         2000,
			TotalRows:          int64(4000 - i),
			EventCount:         3,
			QuerySummary:       "UPDATE shop.orders SET status = ? WHERE id = ?",
			QuerySQL:           "UPDATE shop.orders SET status = 'done' WHERE id = 7",
			QueryTruncated:     false,
			QueryOriginalBytes: 48,
			TableRows: map[string]int{
				"shop.orders": 5,
				fmt.Sprintf("shop.orders_archive_%02d", i%16): 3,
			},
			Operations: map[string]int{
				"UPDATE": 6,
				"INSERT": 2,
			},
		})
	}
	if err := store.RecordTransactions(fixtures); err != nil {
		b.Fatalf("RecordTransactions returned error: %v", err)
	}
	if err := store.Flush(); err != nil {
		b.Fatalf("Flush returned error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txns, err := store.QueryTopTransactions(10)
		if err != nil {
			b.Fatalf("QueryTopTransactions returned error: %v", err)
		}
		if len(txns) != 10 {
			b.Fatalf("expected 10 transactions, got %d", len(txns))
		}
	}
}

func BenchmarkDuckDBStoreQueryAllTransactionsLargeDataset(b *testing.B) {
	store := newTestDuckDBStore(b, DefaultBatchFlushRows)
	base := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)
	fixtures := make([]Transaction, 0, 4000)
	for i := 0; i < 4000; i++ {
		fixtures = append(fixtures, Transaction{
			TxnKey:             fmt.Sprintf("txn-%06d", i),
			StartTime:          base.Add(time.Duration(i) * time.Second),
			EndTime:            base.Add(time.Duration(i)*time.Second + 2*time.Second),
			DurationMS:         2000,
			TotalRows:          int64(4000 - i),
			EventCount:         3,
			QuerySummary:       "UPDATE shop.orders SET status = ? WHERE id = ?",
			QuerySQL:           "UPDATE shop.orders SET status = 'done' WHERE id = 7",
			QueryTruncated:     false,
			QueryOriginalBytes: 48,
			TableRows: map[string]int{
				"shop.orders": 5,
				fmt.Sprintf("shop.orders_archive_%02d", i%16): 3,
			},
			Operations: map[string]int{
				"UPDATE": 6,
				"INSERT": 2,
			},
		})
	}
	if err := store.RecordTransactions(fixtures); err != nil {
		b.Fatalf("RecordTransactions returned error: %v", err)
	}
	if err := store.Flush(); err != nil {
		b.Fatalf("Flush returned error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txns, err := store.QueryAllTransactions()
		if err != nil {
			b.Fatalf("QueryAllTransactions returned error: %v", err)
		}
		if len(txns) != len(fixtures) {
			b.Fatalf("expected %d transactions, got %d", len(fixtures), len(txns))
		}
	}
}

func BenchmarkDuckDBStoreRecordTransactions(b *testing.B) {
	base := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)
	fixtures := make([]Transaction, 0, 1000)
	for i := 0; i < 1000; i++ {
		fixtures = append(fixtures, Transaction{
			TxnKey:             fmt.Sprintf("txn-%06d", i),
			StartTime:          base.Add(time.Duration(i) * time.Second),
			EndTime:            base.Add(time.Duration(i)*time.Second + 2*time.Second),
			DurationMS:         2000,
			TotalRows:          int64(1000 - i),
			EventCount:         3,
			QuerySummary:       "UPDATE shop.orders SET status = ? WHERE id = ?",
			QuerySQL:           "UPDATE shop.orders SET status = 'done' WHERE id = 7",
			QueryTruncated:     false,
			QueryOriginalBytes: 48,
			TableRows: map[string]int{
				"shop.orders": 5,
				fmt.Sprintf("shop.orders_archive_%02d", i%16): 3,
			},
			Operations: map[string]int{
				"UPDATE": 6,
				"INSERT": 2,
			},
		})
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		store := newTestDuckDBStore(b, len(fixtures)+1)
		if err := store.RecordTransactions(fixtures); err != nil {
			b.Fatalf("RecordTransactions returned error: %v", err)
		}
	}
}

func BenchmarkDuckDBStoreRecordTransactionsBatchSizing(b *testing.B) {
	base := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)
	fixtures := make([]Transaction, 0, 10000)
	for i := 0; i < 10000; i++ {
		fixtures = append(fixtures, Transaction{
			TxnKey:             fmt.Sprintf("txn-%06d", i),
			StartTime:          base.Add(time.Duration(i) * time.Second),
			EndTime:            base.Add(time.Duration(i)*time.Second + 2*time.Second),
			DurationMS:         2000,
			TotalRows:          int64(10000 - i),
			EventCount:         3,
			QuerySummary:       "UPDATE shop.orders SET status = ? WHERE id = ?",
			QuerySQL:           "UPDATE shop.orders SET status = 'done' WHERE id = 7",
			QueryTruncated:     false,
			QueryOriginalBytes: 48,
			TableRows: map[string]int{
				"shop.orders":                            5,
				fmt.Sprintf("shop.lineitems_%02d", i%16): 3,
			},
			Operations: map[string]int{
				"UPDATE": 6,
				"INSERT": 2,
			},
		})
	}

	for _, batchSize := range []int{1000, 5000, 10000, 20000} {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				store := newTestDuckDBStore(b, batchSize)
				if err := store.RecordTransactions(fixtures); err != nil {
					b.Fatalf("RecordTransactions returned error: %v", err)
				}
				if err := store.Flush(); err != nil {
					b.Fatalf("Flush returned error: %v", err)
				}
			}
		})
	}
}

func BenchmarkDuckDBStoreRecordMinuteBuckets(b *testing.B) {
	base := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)
	fixtures := make([]model.MinuteBucket, 0, 1000)
	for i := 0; i < 1000; i++ {
		tableRows := make(map[string]int, 32)
		for tableIndex := 0; tableIndex < 32; tableIndex++ {
			tableRows[fmt.Sprintf("shop.orders_%02d", tableIndex)] = i + tableIndex + 1
		}
		fixtures = append(fixtures, model.MinuteBucket{
			Minute:      base.Add(time.Duration(i) * time.Minute),
			TotalRows:   1000 + i,
			TxnCount:    10 + i%7,
			EventCount:  20 + i%11,
			BinlogBytes: int64(4096 + i),
			DDLCount:    i % 3,
			TableRows:   tableRows,
		})
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		store := newTestDuckDBStore(b, len(fixtures)+1)
		if err := store.RecordMinuteBuckets(fixtures); err != nil {
			b.Fatalf("RecordMinuteBuckets returned error: %v", err)
		}
	}
}

func BenchmarkDuckDBStoreQueryMinuteBuckets(b *testing.B) {
	store := newTestDuckDBStore(b, DefaultBatchFlushRows)
	base := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)
	fixtures := make([]model.MinuteBucket, 0, 1000)
	for i := 0; i < 1000; i++ {
		tableRows := make(map[string]int, 32)
		for tableIndex := 0; tableIndex < 32; tableIndex++ {
			tableRows[fmt.Sprintf("shop.orders_%02d", tableIndex)] = i + tableIndex + 1
		}
		fixtures = append(fixtures, model.MinuteBucket{
			Minute:      base.Add(time.Duration(i) * time.Minute),
			TotalRows:   1000 + i,
			TxnCount:    10 + i%7,
			EventCount:  20 + i%11,
			BinlogBytes: int64(4096 + i),
			DDLCount:    i % 3,
			TableRows:   tableRows,
		})
	}
	if err := store.RecordMinuteBuckets(fixtures); err != nil {
		b.Fatalf("RecordMinuteBuckets returned error: %v", err)
	}
	if err := store.Flush(); err != nil {
		b.Fatalf("Flush returned error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buckets, err := store.QueryMinuteBuckets()
		if err != nil {
			b.Fatalf("QueryMinuteBuckets returned error: %v", err)
		}
		if len(buckets) != len(fixtures) {
			b.Fatalf("expected %d minute buckets, got %d", len(fixtures), len(buckets))
		}
	}
}

func TestDuckDBStoreQueryAllTransactionsHydratesMapsLazily(t *testing.T) {
	store := newTestDuckDBStore(t, DefaultBatchFlushRows)
	base := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)

	if err := store.RecordTransactions([]Transaction{
		{
			TxnKey:     "txn-with-maps",
			StartTime:  base,
			EndTime:    base.Add(time.Second),
			DurationMS: 1000,
			TotalRows:  10,
			EventCount: 2,
			TableRows:  map[string]int{"shop.orders": 7, "shop.users": 3},
			Operations: map[string]int{"INSERT": 7, "UPDATE": 3},
		},
		{
			TxnKey:     "txn-no-maps",
			StartTime:  base.Add(2 * time.Second),
			EndTime:    base.Add(3 * time.Second),
			DurationMS: 1000,
			TotalRows:  0,
			EventCount: 1,
			TableRows:  nil,
			Operations: nil,
		},
	}); err != nil {
		t.Fatalf("RecordTransactions returned error: %v", err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	txns, err := store.QueryAllTransactions()
	if err != nil {
		t.Fatalf("QueryAllTransactions returned error: %v", err)
	}
	if len(txns) != 2 {
		t.Fatalf("expected 2 transactions, got %d", len(txns))
	}

	withMaps := txns[0]
	if withMaps.TxnKey != "txn-with-maps" {
		t.Fatalf("unexpected first txn: %s", withMaps.TxnKey)
	}
	if len(withMaps.Tables) != 2 || withMaps.Tables["shop.orders"] != 7 {
		t.Fatalf("expected Tables map with shop.orders=7, got %v", withMaps.Tables)
	}
	if len(withMaps.Operations) != 2 || withMaps.Operations["INSERT"] != 7 {
		t.Fatalf("expected Operations map with INSERT=7, got %v", withMaps.Operations)
	}

	noMaps := txns[1]
	if noMaps.TxnKey != "txn-no-maps" {
		t.Fatalf("unexpected second txn: %s", noMaps.TxnKey)
	}
	// nil or empty map both acceptable; must not panic on range/len
	_ = len(noMaps.Tables)
	_ = len(noMaps.Operations)
	for k, v := range noMaps.Tables {
		t.Fatalf("expected no table entries, got %s=%d", k, v)
	}
	for k, v := range noMaps.Operations {
		t.Fatalf("expected no operation entries, got %s=%d", k, v)
	}
}

func TestDuckDBStoreQueryTopTransactionsStillHydratesOnlySelectedKeys(t *testing.T) {
	store := newTestDuckDBStore(t, DefaultBatchFlushRows)
	base := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)

	fixtures := []Transaction{
		{TxnKey: "txn-1", StartTime: base, EndTime: base.Add(time.Second), DurationMS: 1000, TotalRows: 50, EventCount: 3,
			TableRows: map[string]int{"shop.orders": 50}, Operations: map[string]int{"INSERT": 50}},
		{TxnKey: "txn-2", StartTime: base.Add(2 * time.Second), EndTime: base.Add(3 * time.Second), DurationMS: 1000, TotalRows: 30, EventCount: 2,
			TableRows: map[string]int{"shop.users": 30}, Operations: map[string]int{"UPDATE": 30}},
		{TxnKey: "txn-3", StartTime: base.Add(4 * time.Second), EndTime: base.Add(5 * time.Second), DurationMS: 1000, TotalRows: 10, EventCount: 1,
			TableRows: map[string]int{"shop.products": 10}, Operations: map[string]int{"DELETE": 10}},
	}
	if err := store.RecordTransactions(fixtures); err != nil {
		t.Fatalf("RecordTransactions returned error: %v", err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}

	txns, err := store.QueryTopTransactions(2)
	if err != nil {
		t.Fatalf("QueryTopTransactions returned error: %v", err)
	}
	if len(txns) != 2 {
		t.Fatalf("expected 2 top transactions, got %d", len(txns))
	}
	// Top 2 by rows: txn-1 (50), txn-2 (30)
	if txns[0].TxnKey != "txn-1" {
		t.Fatalf("expected first top txn to be txn-1, got %s", txns[0].TxnKey)
	}
	if txns[0].Tables["shop.orders"] != 50 {
		t.Fatalf("expected txn-1 Tables[shop.orders]=50, got %d", txns[0].Tables["shop.orders"])
	}
	if txns[1].TxnKey != "txn-2" {
		t.Fatalf("expected second top txn to be txn-2, got %s", txns[1].TxnKey)
	}
	if txns[1].Tables["shop.users"] != 30 {
		t.Fatalf("expected txn-2 Tables[shop.users]=30, got %d", txns[1].Tables["shop.users"])
	}
}

func TestDuckDBStoreRoundTripsAlertGTID(t *testing.T) {
	store := newTestDuckDBStore(t, 1)
	alerts := []model.Alert{
		{Type: "large_transaction", Severity: "warning", Message: "m1", TxnKey: "txn-1", GTID: "4d8275bc-c221-11f1-a25e-822b383dbcd0:7", Details: map[string]any{"rows": float64(5)}},
		{Type: "spike", Severity: "info", Message: "m2", Minute: time.Date(2026, 3, 16, 10, 0, 0, 0, time.UTC), Details: map[string]any{}},
	}
	if err := store.RecordAlerts(alerts); err != nil {
		t.Fatalf("RecordAlerts: %v", err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	got, err := store.QueryAlerts()
	if err != nil {
		t.Fatalf("QueryAlerts: %v", err)
	}
	gtids := map[string]string{}
	for _, alert := range got {
		gtids[alert.Type] = alert.GTID
	}
	if gtids["large_transaction"] != "4d8275bc-c221-11f1-a25e-822b383dbcd0:7" {
		t.Fatalf("large_transaction GTID = %q", gtids["large_transaction"])
	}
	if gtids["spike"] != "" {
		t.Fatalf("spike GTID = %q, want empty", gtids["spike"])
	}
}

func (s *DuckDBStore) tableExists(table string) bool {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?`, table).Scan(&count)
	return err == nil && count == 1
}

func (s *DuckDBStore) mustCountRows(t interface{ Fatalf(string, ...any) }, table string) int {
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
		t.Fatalf("count rows for %s: %v", table, err)
	}
	return count
}
