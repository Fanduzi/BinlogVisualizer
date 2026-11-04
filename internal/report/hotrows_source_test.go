package report

import (
	"strings"
	"testing"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

// The Hot Rows section names the source of the keys, and the JSON carries the
// same fact (ADR 0032).
func TestHotRowsNameTheDefinitionSource(t *testing.T) {
	forceEnglishReportLocale(t)
	result := productTextFixture()
	result.HotRows = model.HotRowReport{
		Rows: []model.HotRow{{
			Schema: "shop", Table: "t", PrimaryKey: "id=7", Touches: 2, Transactions: 1,
			KeySource: model.HotRowKeySourceSchemaFile,
		}},
		KeySource:  model.HotRowKeySourceSchemaFile,
		TrackLimit: 8192,
	}

	out, err := RenderText(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, i18n.T("report.text.hotRowsFromSchema")) {
		t.Fatalf("the section must name the source:\n%s", out)
	}

	md, err := RenderMarkdown(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(md, i18n.T("report.text.hotRowsFromSchema")) {
		t.Fatalf("the Markdown section must name the source:\n%s", md)
	}

	if !strings.Contains(out, "id=7 [--schema-file]") {
		t.Fatalf("the ranked row must be marked:\n%s", out)
	}
	if !strings.Contains(md, "id=7 [--schema-file]") {
		t.Fatalf("the Markdown row must be marked:\n%s", md)
	}

	opts := DefaultOptions()
	converted := convertToJSON(result, opts, newView(result, opts))
	if converted.HotRowsKeySource != model.HotRowKeySourceSchemaFile {
		t.Fatalf("JSON must carry the source, got %q", converted.HotRowsKeySource)
	}
	if len(converted.HotRows) != 1 || converted.HotRows[0].KeySource != model.HotRowKeySourceSchemaFile {
		t.Fatalf("the JSON row must carry its source: %+v", converted.HotRows)
	}

	// A mixed window: no run-wide claim, only the definition row is marked.
	result.HotRows.KeySource = ""
	result.HotRows.Rows = append(result.HotRows.Rows, model.HotRow{
		Schema: "shop", Table: "full", PrimaryKey: "id=9", Touches: 1, Transactions: 1,
	})
	out, err = RenderText(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, i18n.T("report.text.hotRowsFromSchema")) {
		t.Fatalf("a mixed window must not claim one source:\n%s", out)
	}
	if !strings.Contains(out, "id=7 [--schema-file]") || strings.Contains(out, "id=9 [--schema-file]") {
		t.Fatalf("only the definition row is marked:\n%s", out)
	}
	if got := convertToJSON(result, opts, newView(result, opts)); got.HotRowsKeySource != "" {
		t.Fatalf("a mixed window must omit the run-wide field: %q", got.HotRowsKeySource)
	}

	// Without a definition the line is absent and the field is omitted.
	result.HotRows.KeySource = ""
	out, err = RenderText(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, i18n.T("report.text.hotRowsFromSchema")) {
		t.Fatalf("no source line without a definition:\n%s", out)
	}
	if got := convertToJSON(result, opts, newView(result, opts)); got.HotRowsKeySource != "" {
		t.Fatalf("no source field without a definition: %q", got.HotRowsKeySource)
	}
}

// A table whose definition declares no key has no key source to name.
func TestTableKeySourceNeedsADeclaredKey(t *testing.T) {
	def := func(schema, table string) ([]string, bool) {
		switch table {
		case "counters":
			return []string{"id"}, true
		case "heap":
			return nil, true
		}
		return nil, false
	}
	withKey := model.TableStats{Schema: "shop", Table: "counters", KeyStatus: model.KeyStatusUnknown}
	if got := tableKeySource(withKey, def); got != "schema_file" {
		t.Fatalf("a declared key is named: %q", got)
	}
	noKey := model.TableStats{Schema: "shop", Table: "heap", KeyStatus: model.KeyStatusUnknown}
	if got := tableKeySource(noKey, def); got != "" {
		t.Fatalf("a table the definition declares without a key has no key source: %q", got)
	}
}
