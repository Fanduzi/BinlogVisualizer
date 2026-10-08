// Package analyzer defines configurable thresholds, filters, and detail-store behavior for binlog analysis.
// input: CLI or caller-selected analyzer options for workload identity, time/position windows, GTID selectors, limits, alerts, filters, flashback collection, optional schema SQL, AllowUnverifiedGenerated, and detail storage.
// output: Options and DefaultOptions values consumed by Analyzer construction and command mapping, plus identity and selector/filter-presence checks.
// pos: analyzer configuration boundary shared by CLI, tests, and streaming analysis setup.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"fmt"
	"strings"
	"time"
)

// Options configures the analyzer behavior.
type Options struct {
	// WorkloadID is an explicit operator-provided identity persisted in report JSON.
	WorkloadID string

	// Detail store mode: none (default) or duckdb.
	DetailStoreMode DetailStoreMode

	// Time window filtering (future - Task 9)
	Start *time.Time
	End   *time.Time
	// Position filtering is a half-open [StartPosition, StopPosition) window.
	StartPosition *int64
	StopPosition  *int64
	GTIDSelector  *GTIDSelector

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
	IncludeTables  []string // only analyze these objects (empty = all); TABLE or SCHEMA.TABLE, including view, event, routine, and trigger names
	ExcludeTables  []string // skip these objects; TABLE or SCHEMA.TABLE, including view, event, routine, and trigger names
	// IncludeDML limits counted ROW images to these kinds (INSERT, UPDATE, DELETE).
	// Empty means every kind. Canonical uppercase, stable order.
	IncludeDML []string
	// CaptureRowImages keeps bounded cell values on listed transactions.
	// Off unless the operator asked to see rows and did not disable SQL context.
	CaptureRowImages bool
	// Flashback collects every selected row image for undo SQL.
	// Off unless the flashback command is running. Analyze output ignores it.
	Flashback bool
	// SchemaSQL is CREATE/ALTER text from --schema-file. Flashback learns
	// generated columns from it before binlog DDL. Analyze ignores it.
	SchemaSQL string
	// SchemaFileDB is the database for unqualified names in SchemaSQL when
	// the file has no USE and no mysqldump Database header. Analyze ignores it.
	SchemaFileDB string
	// AllowUnverifiedGenerated omits a schema-file generated column whose
	// expression cannot be evaluated, when the logged values do not contradict it.
	// A contradiction still refuses the script. Analyze ignores it.
	AllowUnverifiedGenerated bool
}

// HasPositionSelectors reports whether an exact binlog position bound is active.
func (o Options) HasPositionSelectors() bool {
	return o.StartPosition != nil || o.StopPosition != nil
}

// HasGTIDSelectors reports whether transaction-group GTID filtering is active.
func (o Options) HasGTIDSelectors() bool {
	return o.GTIDSelector != nil
}

// HasSelectionFilters reports whether time or position selection is active.
func (o Options) HasSelectionFilters() bool {
	return o.Start != nil || o.End != nil || o.HasPositionSelectors() || o.HasGTIDSelectors()
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

// HasDMLFilter reports whether analysis is limited to chosen DML kinds.
func (o Options) HasDMLFilter() bool {
	return len(o.IncludeDML) > 0
}

// ParseDMLKinds validates a --dml list. Empty means no filter.
// Accepted tokens are insert, update, and delete, in any combination.
// The result is uppercase and ordered INSERT, UPDATE, DELETE.
func ParseDMLKinds(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	seen := map[string]bool{}
	for _, raw := range values {
		for _, part := range strings.Split(raw, ",") {
			part = strings.ToUpper(strings.TrimSpace(part))
			if part == "" {
				continue
			}
			switch part {
			case "INSERT", "UPDATE", "DELETE":
				seen[part] = true
			default:
				return nil, fmt.Errorf("invalid --dml %q (allowed: insert, update, delete)", strings.TrimSpace(part))
			}
		}
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("invalid --dml %q (allowed: insert, update, delete)", strings.Join(values, ","))
	}
	out := make([]string, 0, len(seen))
	for _, op := range []string{"INSERT", "UPDATE", "DELETE"} {
		if seen[op] {
			out = append(out, op)
		}
	}
	return out, nil
}

// HasObjectFilters reports whether schema or table filtering is configured.
func (o Options) HasObjectFilters() bool {
	return len(o.IncludeSchemas) > 0 ||
		len(o.ExcludeSchemas) > 0 ||
		len(o.IncludeTables) > 0 ||
		len(o.ExcludeTables) > 0
}
