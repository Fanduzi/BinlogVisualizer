// Package analyzer verifies optional detail-store mode selection.
// input: analyzer Options values and no-op detail store calls.
// output: assertions for mode defaults and no-op store behavior.
// pos: regression coverage for making DuckDB optional in analyze.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import "testing"

func TestDefaultOptionsUsesNoDetailStore(t *testing.T) {
	opts := DefaultOptions()
	if opts.DetailStoreMode != DetailStoreNone {
		t.Fatalf("DetailStoreMode = %q, want %q", opts.DetailStoreMode, DetailStoreNone)
	}
}

func TestNoopDetailStoreAcceptsAnalyzerWrites(t *testing.T) {
	store := noopDetailStore{}
	if err := store.Reset(); err != nil {
		t.Fatalf("Reset returned error: %v", err)
	}
	if err := store.RecordTransactions(nil); err != nil {
		t.Fatalf("RecordTransactions returned error: %v", err)
	}
	if err := store.RecordMinuteBuckets(nil); err != nil {
		t.Fatalf("RecordMinuteBuckets returned error: %v", err)
	}
	if err := store.RecordAlerts(nil); err != nil {
		t.Fatalf("RecordAlerts returned error: %v", err)
	}
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

func TestNoopDetailStoreResolveSQLReturnsEmptyMap(t *testing.T) {
	store := noopDetailStore{}
	result, err := store.ResolveTransactionQuerySQL([]string{"txn-1", "txn-2"})
	if err != nil {
		t.Fatalf("ResolveTransactionQuerySQL returned error: %v", err)
	}
	if result == nil {
		t.Fatal("ResolveTransactionQuerySQL returned nil map, want non-nil")
	}
	if len(result) != 0 {
		t.Fatalf("ResolveTransactionQuerySQL returned %d entries, want 0", len(result))
	}
}
