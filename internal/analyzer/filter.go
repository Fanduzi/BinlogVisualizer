// Package analyzer matches schema/table include and exclude filters against event identities.
// input: analyzer.Options include/exclude schema and table lists, including SCHEMA.TABLE tokens.
// output: EventFilter.Allow decisions used by streaming analysis before aggregation.
// pos: analyzer object-filter helper shared by Consume and tests.
// note: if this file changes, update this header and module README.md.
package analyzer

import "strings"

// EventFilter decides whether a schema/table combination should be analyzed.
// Empty include lists mean "allow all"; non-empty means only those listed.
// Exclude lists always take priority over include lists.
type EventFilter struct {
	includeSchemas map[string]struct{}
	excludeSchemas map[string]struct{}
	includeTables  []tableSelector
	excludeTables  []tableSelector
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
