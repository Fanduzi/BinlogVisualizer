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
		Rows:       []model.HotRow{{Schema: "shop", Table: "t", PrimaryKey: "id=7", Touches: 2, Transactions: 1}},
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

	opts := DefaultOptions()
	converted := convertToJSON(result, opts, newView(result, opts))
	if converted.HotRowsKeySource != model.HotRowKeySourceSchemaFile {
		t.Fatalf("JSON must carry the source, got %q", converted.HotRowsKeySource)
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
