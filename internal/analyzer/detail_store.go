// Package analyzer defines optional transaction detail persistence backends.
// input: completed transactions, minute buckets, alerts, and transaction keys needing SQL hydration.
// output: no-op or DuckDB-backed detail persistence for optional drilldown use.
// pos: storage boundary that keeps default report generation independent of DuckDB.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import "binlogviz/internal/model"

// DetailStoreMode selects the optional detail-store backend.
type DetailStoreMode string

const (
	DetailStoreNone   DetailStoreMode = "none"
	DetailStoreDuckDB DetailStoreMode = "duckdb"
)

// detailStore is the narrowed store interface the Analyzer needs for write-path persistence
// and optional SQL hydration. Report assembly reads from ReportAggregator instead.
type detailStore interface {
	Reset() error
	RecordTransactions([]persistedTransaction) error
	RecordMinuteBuckets([]model.MinuteBucket) error
	RecordAlerts([]model.Alert) error
	Flush() error
	ResolveTransactionQuerySQL(txnKeys []string) (map[string]string, error)
	Close() error
}

// noopDetailStore accepts all writes and returns empty reads.
type noopDetailStore struct{}

func (noopDetailStore) Reset() error                                        { return nil }
func (noopDetailStore) RecordTransactions([]persistedTransaction) error     { return nil }
func (noopDetailStore) RecordMinuteBuckets([]model.MinuteBucket) error      { return nil }
func (noopDetailStore) RecordAlerts([]model.Alert) error                    { return nil }
func (noopDetailStore) Flush() error                                        { return nil }
func (noopDetailStore) ResolveTransactionQuerySQL([]string) (map[string]string, error) {
	return map[string]string{}, nil
}
func (noopDetailStore) Close() error { return nil }
