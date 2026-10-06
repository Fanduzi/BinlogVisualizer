package model

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// TruncationMarker is appended wherever stored or displayed SQL is cut.
// shown and original are byte counts of the SQL, not of the marker.
func TruncationMarker(shown, original int) string {
	if shown < 0 {
		shown = 0
	}
	if original < shown {
		original = shown
	}
	return fmt.Sprintf(" … [truncated: %d of %d bytes]", shown, original)
}

// DisplayStoredSQL returns sql unchanged, or sql plus a truncation marker when
// the stored statement was cut at MaxStoredSQLBytes.
func DisplayStoredSQL(sql string, truncated bool, originalBytes int) string {
	if sql == "" || !truncated {
		return sql
	}
	if originalBytes < len(sql) {
		originalBytes = len(sql)
	}
	return sql + TruncationMarker(len(sql), originalBytes)
}

// NewQueryContext creates a QueryContext with proper truncation.
// If sql exceeds MaxStoredSQLBytes, it is truncated and Truncated is set to true.
// Returns nil if sql is empty.
//
// Note: For the main binlog processing path, use NewQueryContextFromNormalized instead,
// as truncation already happened in the normalize layer.
func NewQueryContext(sql string) *QueryContext {
	if sql == "" {
		return nil
	}

	originalBytes := len(sql)
	truncated := false

	// Truncate if exceeds max stored bytes
	if originalBytes > MaxStoredSQLBytes {
		// Truncate to MaxStoredSQLBytes, ensuring we don't cut in middle of UTF-8 char
		sql = safeTruncateBytes(sql, MaxStoredSQLBytes)
		truncated = true
	}

	return &QueryContext{
		SQL:           sql,
		Truncated:     truncated,
		OriginalBytes: originalBytes,
	}
}

// NewQueryContextFromNormalized creates a QueryContext from already-normalized values.
// Use this when SQL has already been truncated at the normalize layer.
// Returns nil if sql is empty.
func NewQueryContextFromNormalized(sql string, truncated bool, originalBytes int) *QueryContext {
	if sql == "" {
		return nil
	}

	return &QueryContext{
		SQL:           sql,
		Truncated:     truncated,
		OriginalBytes: originalBytes,
	}
}

// MakeQuerySummary creates a bounded summary from SQL.
// The summary is whitespace-compressed and limited to MaxQuerySummaryChars
// of SQL. A cut, including a stored SQL that was already shorter than the
// original, appends TruncationMarker with the original byte length.
func MakeQuerySummary(sql string) string {
	return FormatQuerySummary(sql, len(sql))
}

// FormatQuerySummary is MakeQuerySummary with the pre-storage byte length.
// originalBytes is the SQL size before the 4096-byte store cap. query_truncated
// still means that store cap; this marker is the visible cut.
func FormatQuerySummary(sql string, originalBytes int) string {
	compressed := compressWhitespace(sql)
	if compressed == "" {
		return ""
	}
	if originalBytes < len(sql) {
		originalBytes = len(sql)
	}
	body := compressed
	displayCut := utf8.RuneCountInString(compressed) > MaxQuerySummaryChars
	if displayCut {
		body = safeTruncateRunes(compressed, MaxQuerySummaryChars)
	}
	storageCut := originalBytes > len(sql)
	if !displayCut && !storageCut {
		return body
	}
	orig := originalBytes
	if !storageCut {
		orig = len(compressed)
	}
	return body + TruncationMarker(len(body), orig)
}

// compressWhitespace replaces runs of whitespace with single space.
func compressWhitespace(s string) string {
	var result strings.Builder
	result.Grow(len(s))

	inWhitespace := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if !inWhitespace {
				result.WriteByte(' ')
				inWhitespace = true
			}
		} else {
			result.WriteRune(r)
			inWhitespace = false
		}
	}

	return strings.TrimSpace(result.String())
}

// safeTruncateBytes truncates to maxBytes without cutting UTF-8 characters.
func safeTruncateBytes(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}

	// Find the last valid UTF-8 boundary at or before maxBytes
	for maxBytes > 0 {
		if utf8.ValidString(s[:maxBytes]) {
			return s[:maxBytes]
		}
		maxBytes--
	}
	return ""
}

// safeTruncateRunes truncates to maxRunes without cutting UTF-8 characters.
func safeTruncateRunes(s string, maxRunes int) string {
	if utf8.RuneCountInString(s) <= maxRunes {
		return s
	}

	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes])
}
