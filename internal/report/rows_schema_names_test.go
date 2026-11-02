package report

import (
	"strings"
	"testing"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// The summary must say where the labels came from: a definition-named image is
// not "unavailable" (ADR 0030).
func TestColumnNamesNoteDistinguishesSchemaFile(t *testing.T) {
	forceEnglishReportLocale(t)

	withSchema := productTextFixture()
	withSchema.Transactions = []model.Transaction{{
		TxnKey:    "txn-1",
		RowImages: []model.RowImage{{Schema: "shop", Table: "orders", Op: "DELETE", Names: model.RowNamesPositional, NamesFrom: model.RowNamesFromSchema, Columns: []string{"id", "status"}}},
	}}
	if note := columnNamesNote(withSchema); !strings.Contains(note, "--schema-file") {
		t.Fatalf("definition-named rows must name their source, got %q", note)
	}

	withoutSchema := productTextFixture()
	withoutSchema.Transactions = []model.Transaction{{
		TxnKey:    "txn-1",
		RowImages: []model.RowImage{{Schema: "shop", Table: "orders", Op: "DELETE", Names: model.RowNamesPositional, Columns: []string{"@1", "@2"}}},
	}}
	want := i18n.T("report.text.columnNamesMissing")
	if note := columnNamesNote(withoutSchema); note != want {
		t.Fatalf("an unnamed row must keep the missing note, got %q", note)
	}
}
