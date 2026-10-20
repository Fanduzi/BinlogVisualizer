// Package analyzer matches schema/table include and exclude filters against event identities.
// input: analyzer.Options include/exclude schema and table lists, including SCHEMA.TABLE tokens.
// output: EventFilter.Allow decisions used by streaming analysis before aggregation, and MayHoldTable / MayHoldSchema, the case-folding superset the flashback DDL refusal reads (ADR 0015).
// pos: analyzer object-filter helper shared by Consume and tests.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"strings"

	"binlogviz/internal/flashback"
)

// EventFilter decides whether a schema/table combination should be analyzed.
// Empty include lists mean "allow all"; non-empty means only those listed.
// Exclude lists always take priority over include lists.
type EventFilter struct {
	includeSchemas map[string]struct{}
	excludeSchemas map[string]struct{}
	includeTables  []tableSelector
	excludeTables  []tableSelector
	includeDML     map[string]struct{}
}

type tableSelector struct {
	schema string // empty matches any schema
	table  string
}

// newEventFilter constructs an EventFilter from Options.
func newEventFilter(opts Options) *EventFilter {
	toSet := func(ss []string) map[string]struct{} {
		m := make(map[string]struct{}, len(ss))
		for _, s := range ss {
			m[s] = struct{}{}
		}
		return m
	}
	return &EventFilter{
		includeSchemas: toSet(opts.IncludeSchemas),
		excludeSchemas: toSet(opts.ExcludeSchemas),
		includeTables:  parseTableSelectors(opts.IncludeTables),
		excludeTables:  parseTableSelectors(opts.ExcludeTables),
		includeDML:     toSet(opts.IncludeDML),
	}
}

func parseTableSelectors(values []string) []tableSelector {
	out := make([]tableSelector, 0, len(values))
	for _, value := range values {
		if sel, ok := parseTableSelector(value); ok {
			out = append(out, sel)
		}
	}
	return out
}

func parseTableSelector(raw string) (tableSelector, bool) {
	raw = strings.TrimSpace(strings.ReplaceAll(raw, "`", ""))
	if raw == "" {
		return tableSelector{}, false
	}
	schema, table, dotted := strings.Cut(raw, ".")
	if dotted {
		schema = strings.TrimSpace(schema)
		table = strings.TrimSpace(table)
		if schema != "" && table != "" && !strings.Contains(table, ".") {
			return tableSelector{schema: schema, table: table}, true
		}
	}
	return tableSelector{table: raw}, true
}

func (sel tableSelector) matches(schema, table string) bool {
	if sel.table != table {
		return false
	}
	return sel.schema == "" || sel.schema == schema
}

func anyTableSelector(selectors []tableSelector, schema, table string) bool {
	for _, sel := range selectors {
		if sel.matches(schema, table) {
			return true
		}
	}
	return false
}

// AllowOperation reports whether a ROW operation should be counted.
// An empty DML filter allows INSERT, UPDATE, and DELETE.
func (f *EventFilter) AllowOperation(operation string) bool {
	if f == nil || len(f.includeDML) == 0 {
		return true
	}
	_, ok := f.includeDML[operation]
	return ok
}

// Allow returns true if the given schema+table should be processed.
func (f *EventFilter) Allow(schema, table string) bool {
	if _, excluded := f.excludeSchemas[schema]; excluded {
		return false
	}
	if len(f.includeSchemas) > 0 {
		if _, ok := f.includeSchemas[schema]; !ok {
			return false
		}
	}
	if len(f.excludeTables) > 0 && anyTableSelector(f.excludeTables, schema, table) {
		return false
	}
	if len(f.includeTables) > 0 && !anyTableSelector(f.includeTables, schema, table) {
		return false
	}
	return true
}

// flashSelection is the filter as the flashback planner reads it, or nil
// when no table or schema filter is set (ADR 0015).
func (a *Analyzer) flashSelection() flashback.Selection {
	if !a.opts.HasObjectFilters() || a.filter == nil {
		return nil
	}
	return a.filter
}

// MayHoldTable reports whether schema.table, as a DDL names it, could be a
// table whose rows the filter selects. It leans to true: an include list
// matches with letter case folded, an exclude list only on the exact name,
// and an empty schema (an unqualified name with no session schema) matches
// any schema.
func (f *EventFilter) MayHoldTable(schema, table string) bool {
	if schema != "" {
		if _, excluded := f.excludeSchemas[schema]; excluded {
			return false
		}
		if len(f.includeSchemas) > 0 && !foldMember(f.includeSchemas, schema) {
			return false
		}
		for _, sel := range f.excludeTables {
			if sel.table == table && (sel.schema == "" || sel.schema == schema) {
				return false
			}
		}
	} else {
		for _, sel := range f.excludeTables {
			if sel.schema == "" && sel.table == table {
				return false
			}
		}
	}
	if len(f.includeTables) == 0 {
		return true
	}
	for _, sel := range f.includeTables {
		if strings.EqualFold(sel.table, table) && (sel.schema == "" || schema == "" || strings.EqualFold(sel.schema, schema)) {
			return true
		}
	}
	return false
}

// MayHoldSchema reports whether a DDL on a whole schema (CREATE, ALTER, or
// DROP DATABASE) could touch a selected table. It leans to true the same way.
func (f *EventFilter) MayHoldSchema(schema string) bool {
	if schema == "" {
		return true
	}
	if _, excluded := f.excludeSchemas[schema]; excluded {
		return false
	}
	if len(f.includeSchemas) > 0 && !foldMember(f.includeSchemas, schema) {
		return false
	}
	if len(f.includeTables) == 0 {
		return true
	}
	for _, sel := range f.includeTables {
		if sel.schema == "" || strings.EqualFold(sel.schema, schema) {
			return true
		}
	}
	return false
}

func foldMember(set map[string]struct{}, name string) bool {
	for have := range set {
		if strings.EqualFold(have, name) {
			return true
		}
	}
	return false
}
