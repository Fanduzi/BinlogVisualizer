package report

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

// The binlog answers first; the definition only fills the gap it leaves, and
// says so when it did (ADR 0031).
func TestPrimaryKeyViewUsesDefinitionOnlyWhereTheBinlogCannot(t *testing.T) {
	forceEnglishReportLocale(t)
	rows := []model.TableStats{{Schema: "shop", Table: "t", UpdateRows: 3, DeleteRows: 1}}

	view := primaryKeyView(rows, func(string, string) ([]string, bool) { return []string{"id"}, true })
	if len(view.risks) != 0 {
		t.Fatalf("a definition key must not be a risk: %+v", view.risks)
	}
	if !strings.Contains(view.summaryLine, "--schema-file") {
		t.Fatalf("the summary must name the source: %q", view.summaryLine)
	}
	// The JSON note and the HTML section carry the same sentence.
	if !strings.Contains(view.note, "--schema-file") {
		t.Fatalf("the note must name the source too: %q", view.note)
	}

	view = primaryKeyView(rows, func(string, string) ([]string, bool) { return nil, true })
	if len(view.risks) != 1 || view.risks[0].update != 3 || view.risks[0].delete != 1 {
		t.Fatalf("a definition-declared no-key table is a risk with its counts: %+v", view.risks)
	}

	known := []model.TableStats{{Schema: "shop", Table: "t", UpdateRows: 1, KeyStatus: model.KeyStatusHasPK}}
	if view := primaryKeyView(known, func(string, string) ([]string, bool) { return nil, true }); len(view.risks) != 0 || strings.Contains(view.summaryLine, "--schema-file") {
		t.Fatalf("binlog metadata must win where it speaks: %+v", view)
	}

	mixed := []model.TableStats{{Schema: "shop", Table: "a", UpdateRows: 1}, {Schema: "shop", Table: "b", UpdateRows: 1}}
	partial := func(_, table string) ([]string, bool) { return []string{"id"}, table == "a" }
	if view := primaryKeyView(mixed, partial); view.note == "" {
		t.Fatalf("partial coverage must keep the unknown note: %+v", view)
	}
}

// A definition that covers the table but not this row says both counts instead
// of falling back to @1..@N silently (ADR 0031).
func TestColumnNamesNoteReportsDefinitionWidthMismatch(t *testing.T) {
	forceEnglishReportLocale(t)
	result := productTextFixture()
	result.Transactions = []model.Transaction{{
		TxnKey: "t",
		RowImages: []model.RowImage{{
			Schema: "shop", Table: "orders", Op: "DELETE",
			Names: model.RowNamesPositional, Columns: []string{"@1", "@2"},
			NamesFileCols: 13, NamesRowCols: 2,
		}},
	}}
	note := columnNamesNote(result)
	for _, want := range []string{"13 columns", "2"} {
		if !strings.Contains(note, want) {
			t.Fatalf("note %q is missing %q", note, want)
		}
	}
}
