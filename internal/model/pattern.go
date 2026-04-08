// Package model defines the pattern-level workload summary contract for analyze results.
// input: transaction-shape-derived aggregates produced after transaction reconstruction.
// output: stable per-pattern metrics suitable for text/json rendering and later compare/trend extension.
// pos: shared result-model layer between analyzer aggregation and report rendering.
package model

// PatternStats summarizes one repeated write transaction shape.
type PatternStats struct {
	PatternKey          string
	Label               string
	TotalRows           int
	TxnCount            int
	EventCount          int
	ShareOfRows         float64
	ShareOfTransactions float64
	AvgRowsPerTxn       float64
	Tables              map[string]int
	Operations          map[string]int
	SampleQuerySummary  string
}
