package model

import (
	"strings"
	"unicode/utf8"
)

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
// The summary is:
// - Whitespace compressed
// - Trimmed of leading/trailing whitespace
// - Limited to MaxQuerySummaryChars
// - Suffixed with "..." if truncated
func MakeQuerySummary(sql string) string {
	if sql == "" {
		return ""
	}

	// Compress whitespace and trim
	summary := compressWhitespace(sql)

	// Truncate to max chars
	if utf8.RuneCountInString(summary) > MaxQuerySummaryChars {
		summary = safeTruncateRunes(summary, MaxQuerySummaryChars-3) + "..."
	}

	return summary
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
