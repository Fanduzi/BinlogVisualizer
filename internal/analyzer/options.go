// Package analyzer defines configurable thresholds, filters, and detail-store behavior for binlog analysis.
// input: CLI or caller-selected analyzer options for workload identity, time/position windows, GTID selectors, limits, alerts, filters, flashback collection, optional schema SQL, and detail storage.
// output: Options and DefaultOptions values consumed by Analyzer construction and command mapping, plus identity and selector/filter-presence checks.
// pos: analyzer configuration boundary shared by CLI, tests, and streaming analysis setup.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"binlogviz/internal/mysqlexpr"
	"binlogviz/internal/splitout"
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
	// ThreadIDs keeps transaction groups whose first QUERY-carried binlog
	// thread id is in the set (--thread-id, ADR 0027). Sorted, deduplicated,
	// nonzero. Empty means no thread selector.
	ThreadIDs []uint32

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
	// FlashbackConflict is the --conflict policy: "" or "refuse" refuses a
	// script that would overwrite later changes to the same rows; "guard"
	// writes it with every undo matched against the logged after image.
	FlashbackConflict string
	// FlashbackPlan prints the JSON plan instead of SQL (--plan).
	FlashbackPlan bool
	// FlashbackConflictLimit bounds the conflicts listed in the plan and the
	// exit 7 details: 0 lists the default 20, a negative value lists all.
	FlashbackConflictLimit int
	// Redo collects every selected row image for forward (redo) SQL.
	// Off unless the redo command is running. Analyze output ignores it.
	// Flashback and Redo are mutually exclusive.
	Redo bool
	// RowWhere is the compiled --where row predicate (ADR 0024): kept row
	// events still run every check; only rows whose matched image is
	// provably selected are staged. nil means no row predicate.
	RowWhere *mysqlexpr.WhereExpr
	// SchemaSQL is CREATE/ALTER text from --schema-file. Flashback learns
	// generated columns from it before binlog DDL. Redo also resolves
	// MINIMAL-metadata names and type facts from it. Analyze ignores it.
	SchemaSQL string
	// SchemaFileDB is the database for unqualified names in SchemaSQL when
	// the file has no USE and no mysqldump Database header. Analyze ignores it.
	SchemaFileDB string
	// Split, when set, sends the flashback or redo script to per-table or
	// statement-bounded files in Split.Dir instead of stdout (ADR 0025).
	// The analyzer ignores it; the command layer's output finish reads it.
	Split *splitout.Spec
}

// HasPositionSelectors reports whether an exact binlog position bound is active.
func (o Options) HasPositionSelectors() bool {
	return o.StartPosition != nil || o.StopPosition != nil
}

// HasGTIDSelectors reports whether transaction-group GTID filtering is active.
func (o Options) HasGTIDSelectors() bool {
	return o.GTIDSelector != nil
}

// HasThreadIDSelector reports whether the --thread-id group filter is active.
func (o Options) HasThreadIDSelector() bool {
	return len(o.ThreadIDs) > 0
}

// HasSelectionFilters reports whether time, position, or group selection is active.
func (o Options) HasSelectionFilters() bool {
	return o.Start != nil || o.End != nil || o.HasPositionSelectors() || o.HasGTIDSelectors() || o.HasThreadIDSelector()
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

// ParseThreadIDs validates a --thread-id list (ADR 0027). Empty means no
// selector. Each token is a positive decimal uint32 connection id; 0 is the
// unattributed marker and would never match, so it is a usage error like any
// other unparseable token. The result is sorted and deduplicated.
func ParseThreadIDs(values []string) ([]uint32, error) {
	if len(values) == 0 {
		return nil, nil
	}
	seen := map[uint32]bool{}
	for _, raw := range values {
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			n, err := strconv.ParseUint(part, 10, 32)
			if err != nil || n == 0 {
				return nil, fmt.Errorf("invalid --thread-id %q (allowed: positive connection ids)", strings.TrimSpace(part))
			}
			seen[uint32(n)] = true
		}
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("invalid --thread-id %q (allowed: positive connection ids)", strings.Join(values, ","))
	}
	out := make([]uint32, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// HasObjectFilters reports whether schema or table filtering is configured.
func (o Options) HasObjectFilters() bool {
	return len(o.IncludeSchemas) > 0 ||
		len(o.ExcludeSchemas) > 0 ||
		len(o.IncludeTables) > 0 ||
		len(o.ExcludeTables) > 0
}
