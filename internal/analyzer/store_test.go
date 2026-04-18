// Package analyzer validates DuckDB-backed result persistence and finalize assembly.
// input: temporary DuckDB paths, analyzer.Options, and normalized event sequences that exercise persistence thresholds.
// output: regression coverage for schema initialization, batch flushing, query ordering, and DuckDB-backed Finalize semantics.
// pos: module-level persistence test suite for the analyzer's internal result store layer.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
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

	if err := store.RecordTransactions([]persistedTransaction{
		newPersistedTransaction("txn-1", base, 5),
	}); err != nil {
		t.Fatalf("RecordTransactions returned error: %v", err)
	}
	if got := store.mustCountRows(t, "transactions"); got != 0 {
		t.Fatalf("expected no persisted transaction rows before threshold, got %d", got)
	}

	if err := store.RecordTransactions([]persistedTransaction{
		newPersistedTransaction("txn-2", base.Add(time.Second), 7),
	}); err != nil {
		t.Fatalf("RecordTransactions returned error: %v", err)
	}
	if got := store.mustCountRows(t, "transactions"); got != 2 {
		t.Fatalf("expected 2 persisted transaction rows after threshold flush, got %d", got)
	}
}

func TestAnalyzerFinalizeReadsBackPersistedResultsAndAppliesTopN(t *testing.T) {
	store := newTestDuckDBStore(t, 2)
	a := NewWithStore(Options{
		TopTables:       2,
		TopTransactions: 2,
		DetectSpikes:    true,
		SpikeWindow:     2,
		SpikeFactor:     3,
		SpikeMinRows:    10,
		LargeTxnRows:    50,
	}, store)
	base := time.Date(2026, 3, 16, 10, 0, 0, 0, time.UTC)

	events := analyzerPersistenceFixture(base)
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatalf("Consume returned error: %v", err)
		}
	}

	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("Finalize returned error: %v", err)
	}

	if len(result.Transactions) != 2 {
		t.Fatalf("expected 2 top transactions, got %d", len(result.Transactions))
	}
	if result.Transactions[0].TxnKey != "txn-4" || result.Transactions[1].TxnKey != "txn-3" {
		t.Fatalf("unexpected top transaction order: %#v", result.Transactions)
	}
	if len(result.Minutes) != 4 {
		t.Fatalf("expected 4 persisted minute buckets, got %d", len(result.Minutes))
	}
	if len(result.Alerts) == 0 {
		t.Fatal("expected alerts read back from DuckDB")
	}
	if result.Alerts[0].Type != "large_transaction" {
		t.Fatalf("expected first alert to be large_transaction, got %s", result.Alerts[0].Type)
	}
	if got := store.mustCountRows(t, "alerts"); got != 0 {
		t.Fatalf("expected finalize to avoid persisting alerts, got %d stored rows", got)
	}
	if got := store.mustCountRows(t, "minute_table_rows"); got == 0 {
		t.Fatal("expected minute_table_rows to be persisted")
	}
}

func TestDuckDBStoreResolvesBoundedQuerySQLForRequestedTransactions(t *testing.T) {
	store := newTestDuckDBStore(t, DefaultBatchFlushRows)
	base := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)

	if err := store.RecordTransactions([]persistedTransaction{
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

	if err := store.RecordTransactions([]persistedTransaction{
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

func newTestDuckDBStore(t interface {
	Helper()
	TempDir() string
	Cleanup(func())
	Fatalf(string, ...any)
}, batchRows int) *DuckDBStore {
	t.Helper()

	path := filepath.Join(t.TempDir(), "analysis.duckdb")
	store, err := NewDuckDBStore(path, batchRows)
	if err != nil {
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

func newPersistedTransaction(txnKey string, start time.Time, totalRows int) persistedTransaction {
	return persistedTransaction{
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
	fixtures := make([]persistedTransaction, 0, 4000)
	for i := 0; i < 4000; i++ {
		fixtures = append(fixtures, persistedTransaction{
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
	fixtures := make([]persistedTransaction, 0, 4000)
	for i := 0; i < 4000; i++ {
		fixtures = append(fixtures, persistedTransaction{
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
	fixtures := make([]persistedTransaction, 0, 1000)
	for i := 0; i < 1000; i++ {
		fixtures = append(fixtures, persistedTransaction{
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

	if err := store.RecordTransactions([]persistedTransaction{
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

	fixtures := []persistedTransaction{
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

func TestNilMapTransactionIsConsumerSafe(t *testing.T) {
	base := time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC)
	txns := []model.Transaction{
		{TxnKey: "empty-txn", StartTime: base, Tables: nil, Operations: nil},
		{TxnKey: "with-txn", StartTime: base, Tables: map[string]int{"a": 1}, Operations: map[string]int{"INSERT": 1}},
	}

	// These consumers must not panic on nil maps
	t.Run("BuildPatterns", func(t *testing.T) {
		patterns := BuildPatterns(txns)
		if len(patterns) == 0 {
			t.Fatal("expected at least one pattern")
		}
	})

	t.Run("SelectWidestTransactions", func(t *testing.T) {
		wide := SelectWidestTransactions(txns, 5)
		if len(wide) == 0 {
			t.Fatal("expected at least one widest transaction")
		}
	})

	t.Run("SelectDiagnosticTransactions", func(t *testing.T) {
		byRows, byDur := SelectDiagnosticTransactions(txns, 5)
		if len(byRows) == 0 || len(byDur) == 0 {
			t.Fatal("expected diagnostic transactions")
		}
	})
}

func analyzerPersistenceFixture(base time.Time) []model.NormalizedEvent {
	return []model.NormalizedEvent{
		{Timestamp: base, EventType: "BEGIN"},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 10},
		{Timestamp: base.Add(2 * time.Second), EventType: "XID"},

		{Timestamp: base.Add(time.Minute), EventType: "BEGIN"},
		{Timestamp: base.Add(time.Minute + time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 12},
		{Timestamp: base.Add(time.Minute + 2*time.Second), EventType: "XID"},

		{Timestamp: base.Add(2 * time.Minute), EventType: "BEGIN"},
		{Timestamp: base.Add(2*time.Minute + time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 60},
		{Timestamp: base.Add(2*time.Minute + 2*time.Second), EventType: "XID"},

		{Timestamp: base.Add(3 * time.Minute), EventType: "BEGIN"},
		{Timestamp: base.Add(3*time.Minute + time.Second), EventType: "ROWS", Schema: "shop", Table: "orders", Operation: "INSERT", RowCount: 80},
		{Timestamp: base.Add(3*time.Minute + 2*time.Second), EventType: "XID"},
	}
}
