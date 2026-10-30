// Package redo enforces the same resource ceilings as undo: the staged
// script counts as spill, the live heap as memory.
// input: BINLOGVIZ_SPILL_BYTES and BINLOGVIZ_MEMORY_BYTES, the staging file size, HeapAlloc.
// output: ResourceError (exit 10, reason spill_limit or memory_limit) or a usage error for a set-but-unusable ceiling.
// pos: redo-side copy of flashback's ceiling plumbing; both env names and messages stay shared vocabulary (ADR 0022).
// note: if this file changes, update this header and module README.md.
package redo

import (
	"os"
	"runtime"
	"runtime/debug"
	"strconv"

	"binlogviz/internal/clierr"
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
	return i18n.Tf("error.redoResource", map[string]any{
		"Reason": e.Reason,
		"Limit":  e.Limit,
		"Used":   e.Used,
	})
}

// ValidateResourceEnv rejects a set-but-unusable ceiling before any binlog is
// read. Unset means no spill ceiling, and the memory ceiling stays GOMEMLIMIT.
func ValidateResourceEnv() error {
	if _, err := parseByteCeiling("BINLOGVIZ_SPILL_BYTES"); err != nil {
		return err
	}
	_, err := parseByteCeiling("BINLOGVIZ_MEMORY_BYTES")
	return err
}

func spillLimitFromEnv() (int64, error) {
	return parseByteCeiling("BINLOGVIZ_SPILL_BYTES")
}

func memoryLimitFromEnv() (int64, error) {
	return parseByteCeiling("BINLOGVIZ_MEMORY_BYTES")
}

// parseByteCeiling returns 0 when name is unset. A set value must be a
// positive base-10 integer: empty, 0, negative, and unit suffixes are usage
// errors.
func parseByteCeiling(name string) (int64, error) {
	v, ok := os.LookupEnv(name)
	if !ok {
		return 0, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		shown := v
		if shown == "" {
			shown = "empty"
		}
		return 0, clierr.Errorf(clierr.KindUsage, "%s", i18n.Tf("error.flashbackByteEnv", map[string]any{
			"Name":  name,
			"Value": shown,
		}))
	}
	return n, nil
}

func (a *Planner) overBudget() error {
	if a == nil || a.spillLimit <= 0 || a.spill == nil {
		return nil
	}
	if a.spill.size > a.spillLimit {
		return &ResourceError{Reason: "spill_limit", Limit: a.spillLimit, Used: a.spill.size}
	}
	return nil
}

// checkMemory fails when HeapAlloc stays above 90% of the ceiling after one GC.
// The ceiling is BINLOGVIZ_MEMORY_BYTES when that is set, otherwise a finite
// GOMEMLIMIT (ADR 0013). A value above GOMEMLIMIT does not raise the runtime limit.
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
