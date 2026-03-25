// Package binlog verifies parser construction and helper behavior for progress offsets.
// input: parser constructors and helper functions such as clampProgressOffset and maxInt64.
// output: regression coverage that parser setup and progress helper semantics stay correct.
// pos: focused unit-test layer for parser helpers that support command-level progress reporting.
// note: if this file changes, update this header and README.md.
package binlog

import "testing"

func TestNewParserReturnsImplementation(t *testing.T) {
	if NewParser() == nil {
		t.Fatal("expected parser")
	}
}

func TestClampProgressOffsetClampsToFileSize(t *testing.T) {
	got := clampProgressOffset(120, 100)
	if got != 100 {
		t.Fatalf("expected 100, got %d", got)
	}
}

func TestClampProgressOffsetPreservesValidOffset(t *testing.T) {
	got := clampProgressOffset(40, 100)
	if got != 40 {
		t.Fatalf("expected 40, got %d", got)
	}
}

func TestMaxInt64IgnoresRegression(t *testing.T) {
	got := maxInt64(40, 30)
	if got != 40 {
		t.Fatalf("expected 40, got %d", got)
	}
}
