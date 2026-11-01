package sqlemit_test

import (
	"reflect"
	"testing"

	"binlogviz/internal/model"
	"binlogviz/internal/sqlemit"
)

func TestQuotedIdentifiersStayInsideOneIdentifier(t *testing.T) {
	tests := []struct {
		name, input, want string
	}{
		{"backtick", "order`detail", "`order``detail`"},
		{"hostile text", `name"; DROP TABLE users; --`, "`name\"; DROP TABLE users; --`"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sqlemit.QuoteIdent(tt.input); got != tt.want {
				t.Fatalf("QuoteIdent(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
	if got := sqlemit.QuoteColumnList(nil); got != "" {
		t.Fatalf("empty column list = %q, want empty", got)
	}
	if got := sqlemit.QuoteColumnList([]string{"id", "order`detail"}); got != "`id`, `order``detail`" {
		t.Fatalf("quoted column list = %q", got)
	}
}

func TestAssignColumnsOmitsGeneratedAndPreservesInputs(t *testing.T) {
	columns := []string{"id", "ToTaL", "note"}
	values := []string{"7", "NULL"}
	gen := map[string]struct{}{"total": {}}
	gotColumns, gotValues := sqlemit.AssignColumns(columns, values, gen)
	if !reflect.DeepEqual(gotColumns, []string{"id", "note"}) || !reflect.DeepEqual(gotValues, []string{"7", "NULL"}) {
		t.Fatalf("generated omission = %v %v", gotColumns, gotValues)
	}
	if !reflect.DeepEqual(columns, []string{"id", "ToTaL", "note"}) || !reflect.DeepEqual(values, []string{"7", "NULL"}) {
		t.Fatalf("inputs changed: %v %v", columns, values)
	}

	allColumns, allValues := sqlemit.AssignColumns(columns, values, nil)
	if !reflect.DeepEqual(allColumns, []string{"id", "ToTaL", "note"}) || !reflect.DeepEqual(allValues, []string{"7", "NULL", "NULL"}) {
		t.Fatalf("without generated map = %v %v", allColumns, allValues)
	}
}

func TestOmittedGeneratedNullNeedsLoggedNull(t *testing.T) {
	gen := map[string]struct{}{"total": {}}
	tests := []struct {
		name   string
		values []string
		gen    map[string]struct{}
		want   bool
	}{
		{"generated NULL", []string{"7", "NULL"}, gen, true},
		{"generated value", []string{"7", "8"}, gen, false},
		{"missing generated value", []string{"7"}, gen, false},
		{"no generated columns", []string{"7", "NULL"}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sqlemit.OmittedGeneratedNull([]string{"id", "TOTAL"}, tt.values, tt.gen); got != tt.want {
				t.Fatalf("OmittedGeneratedNull = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestKeyMatchSkipsInvalidAndUnloggedCompositeColumns(t *testing.T) {
	row := model.FlashRow{Columns: []string{"first", "second", "missing"}, PK: []int{1, -1, 0, 9, 2}}
	if got := sqlemit.KeyMatch(row, []string{"NULL", "-2"}); got != "`second` <=> -2 AND `first` <=> NULL" {
		t.Fatalf("composite key match = %q", got)
	}
}

func TestWhereImageUsesAvailableKeysOrSafeFallback(t *testing.T) {
	head := "DELETE FROM `s`.`items`"
	keyed := model.FlashRow{Schema: "s", Table: "items", Columns: []string{"id", "value"}, PK: []int{0}}
	noPK := keyed
	noPK.NoPK = true
	nilPK := keyed
	nilPK.PK = nil
	invalidPK := keyed
	invalidPK.PK = []int{-1, 9}
	tests := []struct {
		name       string
		row        model.FlashRow
		image      []string
		cond       string
		allColumns bool
		want       string
		ok         bool
	}{
		{"primary key", keyed, []string{"42", "'hi'"}, "", false, "DELETE FROM `s`.`items` WHERE `id` <=> 42;", true},
		{"conflict guard", keyed, []string{"42", "'hi'"}, "", true, "DELETE FROM `s`.`items` WHERE `id` <=> 42 AND `value` <=> 'hi';", true},
		{"additional condition", keyed, []string{"42", "'hi'"}, "`active` = 1", false, "DELETE FROM `s`.`items` WHERE `id` <=> 42 AND `active` = 1;", true},
		{"explicit no PK", noPK, []string{"42", "'hi'"}, "", false, "-- no primary key on s.items; this matches every column and LIMIT 1\nDELETE FROM `s`.`items` WHERE `id` <=> 42 AND `value` <=> 'hi' LIMIT 1;", true},
		{"nil PK", nilPK, []string{"42", "'hi'"}, "", false, "-- no primary key on s.items; this matches every column and LIMIT 1\nDELETE FROM `s`.`items` WHERE `id` <=> 42 AND `value` <=> 'hi' LIMIT 1;", true},
		{"empty row", model.FlashRow{}, nil, "", false, "", false},
		{"empty image", keyed, nil, "", false, "", false},
		{"invalid PK", invalidPK, []string{"42", "'hi'"}, "", false, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := sqlemit.WhereImage(tt.row, head, tt.image, tt.cond, tt.allColumns)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("WhereImage = (%q, %t), want (%q, %t)", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestRelaxedModesOnlyDropsModesNeededByStoredValue(t *testing.T) {
	row := model.FlashRow{Op: "UPDATE", Columns: []string{"id", "Total"}, Before: []string{"7", "NULL"}, NonStrict: true, ZeroDate: true}
	gen := map[string]struct{}{"total": {}}
	want := []string{"STRICT_ALL_TABLES", "STRICT_TRANS_TABLES", "NO_ZERO_DATE", "NO_ZERO_IN_DATE", "ERROR_FOR_DIVISION_BY_ZERO", "TRADITIONAL"}
	if got := sqlemit.RelaxedModes(row, gen); !reflect.DeepEqual(got, want) {
		t.Fatalf("UPDATE drops = %v, want %v", got, want)
	}
	row.Op = "INSERT"
	want = []string{"STRICT_ALL_TABLES", "STRICT_TRANS_TABLES", "NO_ZERO_DATE", "NO_ZERO_IN_DATE", "TRADITIONAL"}
	if got := sqlemit.RelaxedModes(row, gen); !reflect.DeepEqual(got, want) {
		t.Fatalf("INSERT drops = %v, want %v", got, want)
	}
	if got := sqlemit.RelaxedModes(model.FlashRow{Op: "UPDATE"}, gen); len(got) != 0 {
		t.Fatalf("clean row drops = %v, want none", got)
	}
}

func TestDroppedModeExprAndJoinConds(t *testing.T) {
	tests := []struct {
		name  string
		drops []string
		want  string
	}{
		{"none", nil, "TRIM(BOTH ',' FROM CONCAT(',', @@SESSION.sql_mode, ','))"},
		{"one", []string{"NO_ZERO_DATE"}, "TRIM(BOTH ',' FROM REPLACE(CONCAT(',', @@SESSION.sql_mode, ','), ',NO_ZERO_DATE,', ','))"},
		{"two", []string{"NO_ZERO_DATE", "TRADITIONAL"}, "TRIM(BOTH ',' FROM REPLACE(REPLACE(CONCAT(',', @@SESSION.sql_mode, ','), ',NO_ZERO_DATE,', ','), ',TRADITIONAL,', ','))"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sqlemit.DroppedModeExpr(tt.drops); got != tt.want {
				t.Fatalf("DroppedModeExpr = %q, want %q", got, tt.want)
			}
		})
	}
	for _, tt := range []struct{ left, right, want string }{
		{"", "", ""},
		{"a", "", "a"},
		{"", "b", "b"},
		{"a", "b", "a AND b"},
	} {
		if got := sqlemit.JoinConds(tt.left, tt.right); got != tt.want {
			t.Fatalf("JoinConds(%q, %q) = %q, want %q", tt.left, tt.right, got, tt.want)
		}
	}
}
