package flashback

import (
	"os"
	"runtime"
	"runtime/debug"
	"strconv"

	"binlogviz/internal/i18n"
)

// ResourceError is an enforced ceiling (ADR 0012). Reason is spill_limit or
// memory_limit. The CLI maps it to exit 10, never exit 2.
type ResourceError struct {
	Reason string
	Limit  int64
	Used   int64
}

func (e *ResourceError) Error() string {
	if e == nil {
		return ""
	}
	return i18n.Tf("error.flashbackResource", map[string]any{
		"Reason": e.Reason,
		"Limit":  e.Limit,
		"Used":   e.Used,
	})
}

func spillLimitFromEnv() int64 {
	v := os.Getenv("BINLOGVIZ_SPILL_BYTES")
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

// reviewBudget fails when the row spill plus the review spill pass the ceiling.
func (a *Planner) reviewBudget(reviewBytes int64) error {
	return a.overBudget(reviewBytes)
}

func (a *Planner) overBudget(extra int64) error {
	if a == nil || a.spillLimit <= 0 {
		return nil
	}
	var row int64
	if a.flashSpill != nil {
		row = a.flashSpill.size
	}
	used := row + extra
	if used > a.spillLimit {
		return &ResourceError{Reason: "spill_limit", Limit: a.spillLimit, Used: used}
	}
	return nil
}

// checkMemory fails when HeapAlloc stays above 90% of the ceiling after one GC.
// The ceiling is the planner limit, or GOMEMLIMIT when that is finite.
func (a *Planner) checkMemory() error {
	if a == nil {
		return nil
	}
	limit := a.memLimit
	if limit == 0 {
		limit = debug.SetMemoryLimit(-1)
	}
	if limit <= 0 || limit > 1<<62 {
		return nil
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if int64(m.HeapAlloc) < limit*9/10 {
		return nil
	}
	runtime.GC()
	runtime.ReadMemStats(&m)
	used := int64(m.HeapAlloc)
	if used < limit*9/10 {
		return nil
	}
	return &ResourceError{Reason: "memory_limit", Limit: limit, Used: used}
}
