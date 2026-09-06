//go:build !cgo

// Package analyzer stubs DuckDB when CGO is disabled.
// input: NewDuckDBStore requests from analyze --detail-store duckdb.
// output: ErrDuckDBRequiresCGO and a DuckDBStore type so command code still compiles.
// pos: !cgo adapter so default analyze builds without go-duckdb.
// note: if this file changes, keep this header and module README.md synchronized.
package analyzer

import "binlogviz/internal/model"

// DuckDBStore is unavailable in !cgo builds.
type DuckDBStore struct{}

// NewDuckDBStore cannot open DuckDB without CGO.
func NewDuckDBStore(path string, batchRows int) (*DuckDBStore, error) {
	return nil, ErrDuckDBRequiresCGO
}

func (s *DuckDBStore) Path() string { return "" }

func (s *DuckDBStore) Reset() error { return ErrDuckDBRequiresCGO }

func (s *DuckDBStore) Close() error { return nil }

func (s *DuckDBStore) RecordTransactions([]persistedTransaction) error {
	return ErrDuckDBRequiresCGO
}

func (s *DuckDBStore) RecordMinuteBuckets([]model.MinuteBucket) error {
	return ErrDuckDBRequiresCGO
}

func (s *DuckDBStore) RecordAlerts([]model.Alert) error { return ErrDuckDBRequiresCGO }

func (s *DuckDBStore) Flush() error { return ErrDuckDBRequiresCGO }

func (s *DuckDBStore) QueryAllTransactions() ([]model.Transaction, error) {
	return nil, ErrDuckDBRequiresCGO
}

func (s *DuckDBStore) QueryTopTransactions(int) ([]model.Transaction, error) {
	return nil, ErrDuckDBRequiresCGO
}

func (s *DuckDBStore) ResolveTransactionQuerySQL([]string) (map[string]string, error) {
	return nil, ErrDuckDBRequiresCGO
}

func (s *DuckDBStore) QueryMinuteBuckets() ([]model.MinuteBucket, error) {
	return nil, ErrDuckDBRequiresCGO
}

func (s *DuckDBStore) QueryAlerts() ([]model.Alert, error) {
	return nil, ErrDuckDBRequiresCGO
}
