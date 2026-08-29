// Package analyzer defines configurable thresholds, filters, and detail-store behavior for binlog analysis.
// input: CLI or caller-selected analyzer options for time windows, limits, alerts, filters, and detail storage.
// output: Options and DefaultOptions values consumed by Analyzer construction and command mapping, plus filter-presence checks.
// pos: analyzer configuration boundary shared by CLI, tests, and streaming analysis setup.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import "time"

// Options configures the analyzer behavior.
type Options struct {
	// Detail store mode: none (default) or duckdb.
	DetailStoreMode DetailStoreMode

	// Time window filtering (future - Task 9)
	Start *time.Time
	End   *time.Time

	// Report limits (future - CLI flags). TopTables is retained for option
	// compatibility; table presentation limits are applied after aggregation.
	TopTables       int // 0 = unlimited display
	TopTransactions int // 0 = unlimited
	TopMinutes      int // 0 = unlimited

	// Alert thresholds (future - Task 10/11)
	LargeTxnRows     int           // alert if transaction has more rows
	LargeTxnDuration time.Duration // alert if transaction exceeds duration
	DetectSpikes     bool          // enable spike detection
	SpikeWindow      int           // minutes for rolling baseline
	SpikeFactor      float64       // multiplier for spike detection
	SpikeMinRows     int           // minimum rows to consider a spike

	// Schema/table filtering
	IncludeSchemas []string // only analyze these schemas (empty = all)
	ExcludeSchemas []string // skip these schemas
	IncludeTables  []string // only analyze these tables (empty = all)
	ExcludeTables  []string // skip these tables
}

// DefaultOptions returns Options with sensible defaults.
func DefaultOptions() Options {
	return Options{
		DetailStoreMode:  DetailStoreNone,
		TopTables:        20,
		TopTransactions:  20,
		TopMinutes:       60, // last 60 minutes
		LargeTxnRows:     1000,
		LargeTxnDuration: 30 * time.Second,
		DetectSpikes:     false, // disabled by default
		SpikeWindow:      5,
		SpikeFactor:      5.0,
		SpikeMinRows:     100,
	}
}

// HasObjectFilters reports whether schema or table filtering is configured.
func (o Options) HasObjectFilters() bool {
	return len(o.IncludeSchemas) > 0 ||
		len(o.ExcludeSchemas) > 0 ||
		len(o.IncludeTables) > 0 ||
		len(o.ExcludeTables) > 0
}
