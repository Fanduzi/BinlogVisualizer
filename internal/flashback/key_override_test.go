package flashback

import (
	"testing"

	"binlogviz/internal/model"
	"binlogviz/internal/mysqlexpr"
	"binlogviz/internal/schemacatalog"
)

// The planner observes CREATE/ALTER in the parsed files, while the capture only
// saw --schema-file. When a later ALTER changes the definition's key, the rows
// must lose it, or the script would address a row by a key the definition no
// longer owns (ADR 0030).
func TestApplyDefinitionKeyFollowsThePlannerCatalog(t *testing.T) {
	row := model.FlashRow{
		Schema: "shop", Table: "t", Op: "DELETE",
		Columns: []string{"id", "note"}, Before: []string{"3", "'c'"},
		PK: []int{1}, PKFromSchema: true,
	}
	withKey := schemacatalog.Resolution{
		Kind: schemacatalog.Exact,
		Cols: []mysqlexpr.SchemaCol{
			{Name: "id"},
			{Name: "note", Primary: true},
		},
	}
	got := applyDefinitionKey([]model.FlashRow{row}, withKey)
	if len(got[0].PK) != 1 || got[0].PK[0] != 1 || got[0].NoPK || !got[0].PKFromSchema {
		t.Fatalf("a definition key must stay the address: %+v", got[0])
	}

	dropped, ok := mysqlexpr.ParseSchemaColumns("`id` int NOT NULL, `note` varchar(8) NOT NULL")
	if !ok {
		t.Fatal("parse the key-less columns")
	}
	noKey := schemacatalog.Resolution{Kind: schemacatalog.Exact, Cols: dropped}
	got = applyDefinitionKey([]model.FlashRow{row}, noKey)
	if !got[0].NoPK || len(got[0].PK) != 0 || got[0].PKFromSchema {
		t.Fatalf("a dropped key must fall back to matching every column: %+v", got[0])
	}
	if len(got[0].Columns) != 2 || got[0].Columns[1] != "note" {
		t.Fatalf("the labels are not the address and must stay: %+v", got[0].Columns)
	}

	// A binlog-sourced key is never overridden by the definition.
	binlogRow := model.FlashRow{Schema: "shop", Table: "t", Columns: []string{"id", "note"}, PK: []int{0}}
	if got := applyDefinitionKey([]model.FlashRow{binlogRow}, noKey); len(got[0].PK) != 1 || got[0].NoPK {
		t.Fatalf("a binlog key must be left alone: %+v", got[0])
	}
}
