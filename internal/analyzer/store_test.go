//go:build cgo

// Package analyzer validates DuckDB-backed result persistence and finalize assembly.
// input: temporary DuckDB paths, provenance/completeness/replay/SQL-bearing transactions, analyzer.Options, and persistence-threshold events.
// output: regression coverage for transaction-evidence parity across store backends, consumer safety on nil maps, and DuckDB-backed Finalize semantics.
// pos: module-level persistence test suite for the analyzer's internal result store layer.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"binlogviz/internal/detailstore"
	"binlogviz/internal/model"
)

func TestAnalyzerFinalizeReadsBackPersistedResultsAndAppliesTopN(t *testing.T) {
	store, err := detailstore.NewDuckDBStore(filepath.Join(t.TempDir(), "analysis.duckdb"), 2)
	if err != nil {
		if errors.Is(err, detailstore.ErrDuckDBRequiresCGO) {
			t.Skip("DuckDB store requires CGO")
		}
		t.Fatalf("NewDuckDBStore returned error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
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
	if alerts, qerr := store.QueryAlerts(); qerr != nil || len(alerts) != 0 {
		t.Fatalf("expected finalize to avoid persisting alerts, got %d stored rows (err %v)", len(alerts), qerr)
	}
	hasMinuteTables := false
	if buckets, qerr := store.QueryMinuteBuckets(); qerr == nil {
		for _, bucket := range buckets {
			if len(bucket.TableRows) > 0 {
				hasMinuteTables = true
			}
		}
	}
	if !hasMinuteTables {
		t.Fatal("expected minute_table_rows to be persisted")
	}
}

func TestDetailStoresPreserveTransactionEvidence(t *testing.T) {
	base := time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	transaction := detailstore.Transaction{
		TxnKey:              "txn-51",
		ServerID:            7,
		ServerVersion:       "11.8.3-MariaDB-log",
		ServerFlavor:        "mariadb",
		GTID:                "0-7-1848",
		ThreadID:            1875,
		XID:                 "3928",
		ActorUser:           "alice",
		ActorHost:           "db.local",
		XAXID:               "X'6276742d3537',X'',1",
		StartTime:           base,
		EndTime:             base.Add(time.Second),
		DurationMS:          1000,
		TotalRows:           2,
		EventCount:          1,
		BinlogBytes:         100,
		BinlogPathStart:     "mariadb-bin.000001",
		BinlogPathEnd:       "mariadb-bin.000001",
		PositionStart:       120,
		PositionEnd:         180,
		Completeness:        model.TransactionPartialStart,
		FullBinlogPathStart: "mariadb-bin.000001",
		FullBinlogPathEnd:   "mariadb-bin.000001",
		FullPositionStart:   100,
		FullPositionEnd:     200,
		FullBinlogBytes:     100,
		QuerySummary:        "UPDATE shop.orders SET status = ?",
		QuerySQL:            "UPDATE shop.orders SET status = 'done'",
		QueryTruncated:      true,
		QueryOriginalBytes:  5000,
		TableRows:           map[string]int{"shop.orders": 2},
		Operations:          map[string]int{"UPDATE": 2},
	}

	stores := map[string]analysisStore{
		"in-memory": newInMemoryStore(),
		"duckdb":    newContractDuckDBStore(t),
	}
	for name, store := range stores {
		t.Run(name, func(t *testing.T) {
			if err := store.RecordTransactions([]detailstore.Transaction{transaction}); err != nil {
				t.Fatalf("RecordTransactions: %v", err)
			}
			if err := store.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			txns, err := store.QueryTopTransactions(1)
			if err != nil {
				t.Fatalf("QueryTopTransactions: %v", err)
			}
			if len(txns) != 1 {
				t.Fatalf("expected one transaction, got %+v", txns)
			}
			txn := txns[0]
			if txn.ServerID != 7 || txn.ServerVersion != "11.8.3-MariaDB-log" || txn.ServerFlavor != "mariadb" ||
				txn.GTID != "0-7-1848" || txn.ThreadID != 1875 || txn.XID != "3928" ||
				txn.ActorUser != "alice" || txn.ActorHost != "db.local" || txn.XAXID != "X'6276742d3537',X'',1" {
				t.Fatalf("provenance/XA evidence was not preserved: %+v", txn)
			}
			if txn.Completeness != model.TransactionPartialStart || txn.FullReplaySpan == nil || txn.FullReplaySpan.PositionStart != 100 || txn.FullReplaySpan.PositionEnd != 200 {
				t.Fatalf("completeness/replay evidence was not preserved: %+v", txn)
			}
			if txn.BinlogPathStart != "mariadb-bin.000001" || txn.PositionStart != 120 || txn.PositionEnd != 180 || txn.BinlogBytes != 100 || txn.Tables["shop.orders"] != 2 || txn.Operations["UPDATE"] != 2 {
				t.Fatalf("retained transaction evidence was not preserved: %+v", txn)
			}
			if txn.QuerySummary != "UPDATE shop.orders SET status = ?" || txn.QueryContext == nil || txn.QueryContext.SQL != "" || !txn.QueryContext.Truncated || txn.QueryContext.OriginalBytes != 5000 {
				t.Fatalf("bounded SQL metadata was not preserved: %+v", txn)
			}
			sqlByTxn, err := store.ResolveTransactionQuerySQL([]string{"txn-51"})
			if err != nil {
				t.Fatalf("ResolveTransactionQuerySQL: %v", err)
			}
			if sqlByTxn["txn-51"] != "UPDATE shop.orders SET status = 'done'" {
				t.Fatalf("bounded SQL body was not preserved: %+v", sqlByTxn)
			}
		})
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

// #282 / ADR 0011: an alert's GTID survives the DuckDB alerts table, the same
// as the in-memory store.

// newContractDuckDBStore opens the DuckDB leg of the cross-store contract test.
func newContractDuckDBStore(t testing.TB) analysisStore {
	t.Helper()
	store, err := detailstore.NewDuckDBStore(filepath.Join(t.TempDir(), "analysis.duckdb"), detailstore.DefaultBatchFlushRows)
	if err != nil {
		if errors.Is(err, detailstore.ErrDuckDBRequiresCGO) {
			t.Skip("DuckDB store requires CGO")
		}
		t.Fatalf("NewDuckDBStore returned error: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}
