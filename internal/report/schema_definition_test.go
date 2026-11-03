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
	// The no-key section must say the definition is the source, not only count
	// the rows (ADR 0031).
	if !strings.Contains(view.note, "--schema-file") {
		t.Fatalf("the no-key section must name the source: %q", view.note)
	}
	if !strings.Contains(view.summaryLine, "--schema-file") {
		t.Fatalf("the summary must name the source too: %q", view.summaryLine)
	}
	if len(view.risks) != 1 || !strings.Contains(view.risks[0].name(), "--schema-file") {
		t.Fatalf("the row itself must say the definition answered: %+v", view.risks)
	}

	known := []model.TableStats{{Schema: "shop", Table: "t", UpdateRows: 1, KeyStatus: model.KeyStatusHasPK}}
	if view := primaryKeyView(known, func(string, string) ([]string, bool) { return nil, true }); len(view.risks) != 0 || strings.Contains(view.summaryLine, "--schema-file") {
		t.Fatalf("binlog metadata must win where it speaks: %+v", view)
	}

	// A mixed run: one table's key comes from binlog metadata, another's from the
	// definition. Neither source sentence may claim both.
	mixedSources := []model.TableStats{
		{Schema: "shop", Table: "full", UpdateRows: 1, KeyStatus: model.KeyStatusHasPK},
		{Schema: "shop", Table: "minimal", UpdateRows: 1},
	}
	view = primaryKeyView(mixedSources, func(_, table string) ([]string, bool) { return []string{"id"}, table == "minimal" })
	if len(view.risks) != 0 {
		t.Fatalf("no risk expected: %+v", view.risks)
	}
	if strings.Contains(view.summaryLine, "--schema-file") || strings.Contains(view.note, "--schema-file") {
		t.Fatalf("a mixed run must not claim one source: line %q note %q", view.summaryLine, view.note)
	}

	// A definition with one keyed table and one without: the summary may say the
	// file is the source (it answered both), but the no-key section must mark the
	// table that has no key and must not claim the file declares none for all.
	oneKeyOneNot := []model.TableStats{
		{Schema: "shop", Table: "orders", InsertRows: 2, UpdateRows: 1},
		{Schema: "shop", Table: "heap", UpdateRows: 1, DeleteRows: 1},
	}
	view = primaryKeyView(oneKeyOneNot, func(_, table string) ([]string, bool) {
		if table == "orders" {
			return []string{"id"}, true
		}
		return nil, true
	})
	if len(view.risks) != 1 || view.risks[0].table != "heap" || !strings.Contains(view.risks[0].name(), "--schema-file") {
		t.Fatalf("only the keyless table is a risk, and it is marked: %+v", view.risks)
	}
	if !strings.Contains(view.summaryLine, "primary key from --schema-file") || strings.Contains(view.summaryLine, "no primary key") {
		t.Fatalf("the summary must be true for a mixed definition: %q", view.summaryLine)
	}
	if !strings.Contains(view.note, "marked --schema-file") {
		t.Fatalf("the section must name the marked tables: %q", view.note)
	}

	// Mixed metadata with a keyless definition: the binlog answered one table, so
	// the run claims no single source, but the keyless row is still explained.
	view = primaryKeyView(mixedSources, func(_, table string) ([]string, bool) { return nil, table == "minimal" })
	if len(view.risks) != 1 || !strings.Contains(view.risks[0].name(), "--schema-file") {
		t.Fatalf("the keyless definition row must be marked: %+v", view.risks)
	}
	if strings.Contains(view.summaryLine, "--schema-file") || !strings.Contains(view.note, "marked --schema-file") {
		t.Fatalf("a mixed run explains the row, not the run: line %q note %q", view.summaryLine, view.note)
	}

	// Partial coverage with a keyless covered table and an uncovered one: the
	// summary keeps the unknown warning and claims no source, while the section
	// marks the table the definition answered (ADR 0031).
	partialWithRisk := []model.TableStats{
		{Schema: "shop", Table: "heap", UpdateRows: 2, DeleteRows: 1},
		{Schema: "shop", Table: "log", UpdateRows: 1},
	}
	view = primaryKeyView(partialWithRisk, func(_, table string) ([]string, bool) { return nil, table == "heap" })
	if len(view.risks) != 1 || view.risks[0].table != "heap" || !strings.Contains(view.risks[0].name(), "--schema-file") {
		t.Fatalf("the covered keyless table is the risk and is marked: %+v", view.risks)
	}
	if strings.Contains(view.summaryLine, "--schema-file") {
		t.Fatalf("partial coverage must not claim a source: %q", view.summaryLine)
	}
	if !strings.Contains(view.summaryLine, "unknown") {
		t.Fatalf("partial coverage keeps the unknown warning in the summary: %q", view.summaryLine)
	}
	if !strings.Contains(view.note, "unknown") || !strings.Contains(view.note, "marked --schema-file") {
		t.Fatalf("the section note must carry both facts: %q", view.note)
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
