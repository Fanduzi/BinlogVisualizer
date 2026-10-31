// Package model defines the statement-fingerprint contract for analyze results.
// input: normalized Rows_query statement aggregates produced during streaming analysis.
// output: stable per-fingerprint metrics suitable for text/json/csv rendering.
// pos: shared result-model layer between analyzer aggregation and report rendering.
package model

// StatementFingerprint summarizes one literal-normalized write-statement text.
// It is the statement-text counterpart of PatternStats: a pattern groups
// transaction shapes (table set + operations + batch size), while a fingerprint
// groups statement texts regardless of which transaction carried them.
type StatementFingerprint struct {
	// Fingerprint is the normalized statement text: literals replaced by ?,
	// comments stripped, whitespace folded, IN/VALUES groups collapsed to (?),
	// keywords case-folded, identifiers preserved.
	Fingerprint string
	// Sample is a bounded excerpt of the first-seen original statement
	// (FormatQuerySummary rules). Empty when --sql-context off suppressed it.
	Sample     string
	Statements int
	Rows       int
	// ShareOfRows is Rows over the workload's total retained rows.
	ShareOfRows float64
	// Tables counts retained written rows per "schema.table", like PatternStats.Tables.
	Tables map[string]int
}

// RetainedStatement is transient pipeline evidence: one retained Rows_query
// statement occurrence recorded while its row events were counted. It is folded
// into StatementFingerprint groups by report aggregation and cleared before the
// transaction lands in AnalysisResult.Transactions; it is never rendered.
type RetainedStatement struct {
	Fingerprint string
	Sample      string
	Tables      map[string]int
	Rows        int
}
