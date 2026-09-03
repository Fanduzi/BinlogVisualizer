// Package analyzer verifies schema/table include and exclude filter matching.
// input: analyzer.Options filter lists including TABLE and SCHEMA.TABLE tokens.
// output: assertions that EventFilter.Allow matches qualified and unqualified table identities.
// pos: regression coverage for object-filter parsing used by streaming analysis.
// note: if this file changes, keep internal/analyzer/README.md synchronized.
package analyzer

import "testing"

func TestEventFilter_Allow(t *testing.T) {
	tests := []struct {
		name          string
		opts          Options
		schema, table string
		want          bool
	}{
		{
			name:   "no filter allows everything",
			opts:   Options{},
			schema: "mydb", table: "orders",
			want: true,
		},
		{
			name:   "include-schema match",
			opts:   Options{IncludeSchemas: []string{"mydb"}},
			schema: "mydb", table: "orders",
			want: true,
		},
		{
			name:   "include-schema no match",
			opts:   Options{IncludeSchemas: []string{"mydb"}},
			schema: "other", table: "orders",
			want: false,
		},
		{
			name:   "exclude-schema",
			opts:   Options{ExcludeSchemas: []string{"sys"}},
			schema: "sys", table: "whatever",
			want: false,
		},
		{
			name:   "exclude-schema other allowed",
			opts:   Options{ExcludeSchemas: []string{"sys"}},
			schema: "mydb", table: "orders",
			want: true,
		},
		{
			name:   "include-table match",
			opts:   Options{IncludeTables: []string{"orders"}},
			schema: "mydb", table: "orders",
			want: true,
		},
		{
			name:   "include-table no match",
			opts:   Options{IncludeTables: []string{"orders"}},
			schema: "mydb", table: "users",
			want: false,
		},
		{
			name:   "exclude-table",
			opts:   Options{ExcludeTables: []string{"audit_log"}},
			schema: "mydb", table: "audit_log",
			want: false,
		},
		{
			name:   "exclude overrides include schema",
			opts:   Options{IncludeSchemas: []string{"mydb"}, ExcludeSchemas: []string{"mydb"}},
			schema: "mydb", table: "orders",
			want: false,
		},
		{
			name:   "include schema + exclude table",
			opts:   Options{IncludeSchemas: []string{"mydb"}, ExcludeTables: []string{"audit_log"}},
			schema: "mydb", table: "audit_log",
			want: false,
		},
		{
			name:   "include schema + include table both match",
			opts:   Options{IncludeSchemas: []string{"mydb"}, IncludeTables: []string{"orders"}},
			schema: "mydb", table: "orders",
			want: true,
		},
		{
			name:   "include schema match but include table no match",
			opts:   Options{IncludeSchemas: []string{"mydb"}, IncludeTables: []string{"orders"}},
			schema: "mydb", table: "users",
			want: false,
		},
		{
			name:   "include-table schema.table match",
			opts:   Options{IncludeTables: []string{"dogfood.orders"}},
			schema: "dogfood", table: "orders",
			want: true,
		},
		{
			name:   "include-table schema.table rejects other schema",
			opts:   Options{IncludeTables: []string{"dogfood.orders"}},
			schema: "other", table: "orders",
			want: false,
		},
		{
			name:   "include-table schema.table rejects other table",
			opts:   Options{IncludeTables: []string{"dogfood.orders"}},
			schema: "dogfood", table: "payments",
			want: false,
		},
		{
			name:   "include-table quoted schema.table match",
			opts:   Options{IncludeTables: []string{"`dogfood`.`orders`"}},
			schema: "dogfood", table: "orders",
			want: true,
		},
		{
			name:   "exclude-table schema.table keeps other schema",
			opts:   Options{ExcludeTables: []string{"dogfood.orders"}},
			schema: "other", table: "orders",
			want: true,
		},
		{
			name:   "exclude-table schema.table drops that pair",
			opts:   Options{ExcludeTables: []string{"dogfood.orders"}},
			schema: "dogfood", table: "orders",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newEventFilter(tt.opts)
			got := f.Allow(tt.schema, tt.table)
			if got != tt.want {
				t.Errorf("Allow(%q, %q) = %v, want %v", tt.schema, tt.table, got, tt.want)
			}
		})
	}
}
