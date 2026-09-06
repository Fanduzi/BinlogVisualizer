// Package analyzer verifies the DuckDB constructor contract with and without CGO.
// input: NewDuckDBStore against a temp path.
// output: success when CGO is on; ErrDuckDBRequiresCGO when CGO is off.
// pos: fail-loud coverage for the !cgo stub so skipped DuckDB tests are not the only signal.
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestNewDuckDBStoreCGOContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "analysis.duckdb")
	store, err := NewDuckDBStore(path, DefaultBatchFlushRows)
	if err == nil {
		t.Cleanup(func() { _ = store.Close() })
		return
	}
	if !errors.Is(err, ErrDuckDBRequiresCGO) {
		t.Fatalf("NewDuckDBStore: %v, want success or ErrDuckDBRequiresCGO", err)
	}
}
