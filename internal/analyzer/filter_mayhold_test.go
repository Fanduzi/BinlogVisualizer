package analyzer

import "testing"

// MayHoldTable and MayHoldSchema are the flashback DDL refusal's view of the
// filter (ADR 0015). They may say yes where Allow says no, never the reverse.
func TestFilterMayHoldLeansToRefuse(t *testing.T) {
	cases := []struct {
		name          string
		opts          Options
		schema, table string
		want          bool
	}{
		{"include exact", Options{IncludeTables: []string{"rn.t1"}}, "rn", "t1", true},
		{"include other", Options{IncludeTables: []string{"rn.t1"}}, "rn", "t2", false},
		{"include folded", Options{IncludeTables: []string{"rn.t1"}}, "RN", "T1", true},
		{"include unknown schema", Options{IncludeTables: []string{"rn.t1"}}, "", "t1", true},
		{"include bare table any schema", Options{IncludeTables: []string{"t1"}}, "other", "t1", true},
		{"exclude exact", Options{ExcludeTables: []string{"rn.t2"}}, "rn", "t2", false},
		{"exclude does not fold", Options{ExcludeTables: []string{"rn.t2"}}, "rn", "T2", true},
		{"exclude schema-qualified, schema unknown", Options{ExcludeTables: []string{"rn.t2"}}, "", "t2", true},
		{"exclude bare, schema unknown", Options{ExcludeTables: []string{"t2"}}, "", "t2", false},
		{"include schema folded", Options{IncludeSchemas: []string{"rn"}}, "Rn", "x", true},
		{"include schema other", Options{IncludeSchemas: []string{"rn"}}, "qa", "x", false},
		{"exclude schema", Options{ExcludeSchemas: []string{"qa"}}, "qa", "x", false},
	}
	for _, tc := range cases {
		f := newEventFilter(tc.opts)
		if got := f.MayHoldTable(tc.schema, tc.table); got != tc.want {
			t.Errorf("%s: MayHoldTable(%q,%q) = %v", tc.name, tc.schema, tc.table, got)
		}
		if f.Allow(tc.schema, tc.table) && !f.MayHoldTable(tc.schema, tc.table) {
			t.Errorf("%s: Allow is true but MayHoldTable is false", tc.name)
		}
	}
	schemas := []struct {
		opts   Options
		schema string
		want   bool
	}{
		{Options{IncludeTables: []string{"rn.t1"}}, "rn", true},
		{Options{IncludeTables: []string{"rn.t1"}}, "shop", false},
		{Options{IncludeTables: []string{"t1"}}, "shop", true},
		{Options{IncludeSchemas: []string{"rn"}}, "RN", true},
		{Options{ExcludeSchemas: []string{"shop"}}, "shop", false},
		{Options{ExcludeTables: []string{"shop.a"}}, "shop", true},
		{Options{IncludeTables: []string{"rn.t1"}}, "", true},
	}
	for _, tc := range schemas {
		if got := newEventFilter(tc.opts).MayHoldSchema(tc.schema); got != tc.want {
			t.Errorf("%+v: MayHoldSchema(%q) = %v", tc.opts, tc.schema, got)
		}
	}
}
