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
