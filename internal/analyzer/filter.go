package analyzer

// EventFilter decides whether a schema/table combination should be analyzed.
// Empty include lists mean "allow all"; non-empty means only those listed.
// Exclude lists always take priority over include lists.
type EventFilter struct {
	includeSchemas map[string]struct{}
	excludeSchemas map[string]struct{}
	includeTables  map[string]struct{}
	excludeTables  map[string]struct{}
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
		includeTables:  toSet(opts.IncludeTables),
		excludeTables:  toSet(opts.ExcludeTables),
	}
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
	if _, excluded := f.excludeTables[table]; excluded {
		return false
	}
	if len(f.includeTables) > 0 {
		if _, ok := f.includeTables[table]; !ok {
			return false
		}
	}
	return true
}
