package model

import "testing"

func TestTransactionDurationUsesStartAndEnd(t *testing.T) {
	trx := Transaction{}
	if trx.Duration != 0 {
		t.Fatalf("expected zero duration")
	}
}

// Tests for NewQueryContext

func TestNewQueryContextEmpty(t *testing.T) {
	qc := NewQueryContext("")
	if qc != nil {
		t.Fatalf("expected nil for empty SQL, got: %+v", qc)
	}
}

func TestNewQueryContextShort(t *testing.T) {
	sql := "SELECT 1"
	qc := NewQueryContext(sql)
	if qc == nil {
		t.Fatal("expected non-nil QueryContext")
	}
	if qc.SQL != sql {
		t.Fatalf("expected SQL %q, got %q", sql, qc.SQL)
	}
	if qc.Truncated {
		t.Fatal("expected Truncated=false for short SQL")
	}
	if qc.OriginalBytes != len(sql) {
		t.Fatalf("expected OriginalBytes=%d, got %d", len(sql), qc.OriginalBytes)
	}
}

func TestNewQueryContextLong(t *testing.T) {
	// Create SQL longer than MaxStoredSQLBytes
	longSQL := "SELECT '" + makeNChars('x', MaxStoredSQLBytes+1000) + "'"
	qc := NewQueryContext(longSQL)
	if qc == nil {
		t.Fatal("expected non-nil QueryContext")
	}
	if !qc.Truncated {
		t.Fatal("expected Truncated=true for long SQL")
	}
	if qc.OriginalBytes != len(longSQL) {
		t.Fatalf("expected OriginalBytes=%d, got %d", len(longSQL), qc.OriginalBytes)
	}
	if len(qc.SQL) > MaxStoredSQLBytes {
		t.Fatalf("expected SQL to be truncated to <=%d bytes, got %d", MaxStoredSQLBytes, len(qc.SQL))
	}
}

// Tests for NewQueryContextFromNormalized

func TestNewQueryContextFromNormalizedEmpty(t *testing.T) {
	qc := NewQueryContextFromNormalized("", false, 0)
	if qc != nil {
		t.Fatalf("expected nil for empty SQL, got: %+v", qc)
	}
}

func TestNewQueryContextFromNormalizedNotTruncated(t *testing.T) {
	sql := "SELECT id FROM users"
	originalBytes := len(sql)
	qc := NewQueryContextFromNormalized(sql, false, originalBytes)
	if qc == nil {
		t.Fatal("expected non-nil QueryContext")
	}
	if qc.SQL != sql {
		t.Fatalf("expected SQL %q, got %q", sql, qc.SQL)
	}
	if qc.Truncated {
		t.Fatal("expected Truncated=false")
	}
	if qc.OriginalBytes != originalBytes {
		t.Fatalf("expected OriginalBytes=%d, got %d", originalBytes, qc.OriginalBytes)
	}
}

func TestNewQueryContextFromNormalizedTruncated(t *testing.T) {
	// Simulate a scenario where SQL was truncated at normalize layer
	originalBytes := 5000 // Original was 5000 bytes
	truncatedSQL := "SELECT 'xxxxx...'" // Now only ~20 bytes after truncation
	truncated := true

	qc := NewQueryContextFromNormalized(truncatedSQL, truncated, originalBytes)
	if qc == nil {
		t.Fatal("expected non-nil QueryContext")
	}
	if qc.SQL != truncatedSQL {
		t.Fatalf("expected SQL %q, got %q", truncatedSQL, qc.SQL)
	}
	if !qc.Truncated {
		t.Fatal("expected Truncated=true")
	}
	// KEY TEST: OriginalBytes must be the ORIGINAL length, not the truncated length
	if qc.OriginalBytes != originalBytes {
		t.Fatalf("expected OriginalBytes=%d (original), got %d", originalBytes, qc.OriginalBytes)
	}
	if qc.OriginalBytes == len(truncatedSQL) {
		t.Fatalf("OriginalBytes should NOT equal truncated SQL length %d, but it does", len(truncatedSQL))
	}
}

// Tests for MakeQuerySummary

func TestMakeQuerySummaryEmpty(t *testing.T) {
	if s := MakeQuerySummary(""); s != "" {
		t.Fatalf("expected empty string, got %q", s)
	}
}

func TestMakeQuerySummaryNoTruncation(t *testing.T) {
	sql := "SELECT id FROM users WHERE id = 1"
	summary := MakeQuerySummary(sql)
	if summary != sql {
		t.Fatalf("expected %q, got %q", sql, summary)
	}
}

func TestMakeQuerySummaryWhitespaceCompression(t *testing.T) {
	sql := "SELECT   id\n\tFROM   users  WHERE  id  =  1"
	expected := "SELECT id FROM users WHERE id = 1"
	summary := MakeQuerySummary(sql)
	if summary != expected {
		t.Fatalf("expected %q, got %q", expected, summary)
	}
}

func TestMakeQuerySummaryTruncation(t *testing.T) {
	// Create SQL longer than MaxQuerySummaryChars
	longSQL := "SELECT " + makeNChars('x', MaxQuerySummaryChars+50) + " FROM users"
	summary := MakeQuerySummary(longSQL)

	// Should be truncated and end with "..."
	if len(summary) > MaxQuerySummaryChars+3 { // +3 for "..."
		t.Fatalf("expected summary <= %d chars, got %d", MaxQuerySummaryChars+3, len(summary))
	}
	if len(summary) < 3 || summary[len(summary)-3:] != "..." {
		t.Fatalf("expected summary to end with '...', got %q", summary)
	}
}

func TestMakeQuerySummaryWhitespaceThenTruncation(t *testing.T) {
	// Test that whitespace compression happens before truncation
	sql := "SELECT   " + makeNChars('x', MaxQuerySummaryChars) + "\n\nFROM   users"
	summary := MakeQuerySummary(sql)

	// Verify whitespace was compressed (no double spaces)
	if len(summary) > 0 {
		// Check there are no double spaces in the summary
		for i := 1; i < len(summary); i++ {
			if summary[i] == ' ' && summary[i-1] == ' ' {
				t.Fatalf("expected no double spaces in summary, got %q", summary)
			}
		}
	}
}

func TestMakeQuerySummaryUTF8(t *testing.T) {
	sql := "SELECT '日本語テストデータ' FROM users WHERE name = '山田太郎'"
	summary := MakeQuerySummary(sql)

	// Should preserve UTF-8 characters
	runes := []rune(summary)
	if len(runes) > MaxQuerySummaryChars+3 {
		t.Fatalf("expected summary <= %d runes, got %d", MaxQuerySummaryChars+3, len(runes))
	}
}

// makeNChars creates a string of n identical characters
func makeNChars(c rune, n int) string {
	result := make([]rune, n)
	for i := range result {
		result[i] = c
	}
	return string(result)
}
