// Package analyzer verifies statement fingerprints against real MySQL 8.0
// fixtures that carry Rows_query_log_event text.
// input: testdata/mysql-8.0.46-dml-minimal.binlog (Rows_query on) and
// testdata/mysql-8.0.46-check-table.binlog (no Rows_query).
// output: the same normalized statement ranking the CLI surfaces on text,
// JSON, and CSV — proving the capture path works on real binlog bytes.
// pos: fixture-level regression for fingerprint.go plus the builder seam in
// transactions.go (ADR 0026).
// note: if this file changes, update this header and README.md.
package analyzer

import (
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/binlog"
)

func TestDMLFixtureProducesStatementFingerprints(t *testing.T) {
	path := filepath.Join("..", "binlog", "testdata", "mysql-8.0.46-dml-minimal.binlog")
	p := binlog.NewParser()
	a := New(DefaultOptions())
	if err := p.ParseFiles([]string{path}, func(raw binlog.RawEvent) error {
		ev, err := binlog.NormalizeRawEvent(raw)
		if err != nil {
			return err
		}
		if ev == nil {
			return nil
		}
		return a.Consume(*ev)
	}); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("finalize fixture: %v", err)
	}
	if !result.SQLContextAvailable {
		t.Fatal("dml fixture must carry Rows_query text")
	}
	fps := result.StatementFingerprints
	if len(fps) == 0 {
		t.Fatal("dml fixture must produce statement fingerprints")
	}
	var sawInsert, sawUpdate, sawDelete bool
	var totalStmts, totalRows int
	for _, fp := range fps {
		totalStmts += fp.Statements
		totalRows += fp.Rows
		switch {
		case strings.HasPrefix(fp.Fingerprint, "insert "):
			sawInsert = true
		case strings.HasPrefix(fp.Fingerprint, "update "):
			sawUpdate = true
		case strings.HasPrefix(fp.Fingerprint, "delete "):
			sawDelete = true
		}
		if strings.Contains(fp.Fingerprint, "'") {
			t.Fatalf("literal leaked into fingerprint: %q", fp.Fingerprint)
		}
		if fp.Tables["shop.orders"] == 0 {
			t.Fatalf("fingerprint missing shop.orders attribution: %+v", fp)
		}
	}
	if !sawInsert || !sawUpdate || !sawDelete {
		t.Fatalf("expected insert+update+delete fingerprints, got %d groups", len(fps))
	}
	if totalRows != result.Summary.TotalRows {
		t.Fatalf("fingerprint rows %d != workload rows %d — fingerprints must cover every retained row", totalRows, result.Summary.TotalRows)
	}
}

func TestCheckTableFixtureHasNoFingerprints(t *testing.T) {
	path := filepath.Join("..", "binlog", "testdata", "mysql-8.0.46-check-table.binlog")
	p := binlog.NewParser()
	a := New(DefaultOptions())
	if err := p.ParseFiles([]string{path}, func(raw binlog.RawEvent) error {
		ev, err := binlog.NormalizeRawEvent(raw)
		if err != nil {
			return err
		}
		if ev == nil {
			return nil
		}
		return a.Consume(*ev)
	}); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	result, err := a.Finalize()
	if err != nil {
		t.Fatalf("finalize fixture: %v", err)
	}
	if result.SQLContextAvailable {
		t.Fatal("check-table fixture must not carry Rows_query text")
	}
	if len(result.StatementFingerprints) != 0 {
		t.Fatalf("expected no fingerprints, got %+v", result.StatementFingerprints)
	}
}
