package report

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

var generatedTime = regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC`)

func TestWriteMatchesRenderers(t *testing.T) {
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 4,
			TotalRows:         9,
			StartTime:         time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC),
			EndTime:           time.Date(2026, 3, 9, 10, 5, 0, 0, time.UTC),
			Duration:          5 * time.Minute,
		},
		Transactions: []model.Transaction{{
			TxnKey:       "listed",
			TotalRows:    2,
			QuerySummary: "UPDATE t SET a = 1",
		}},
		Diagnostics: model.Diagnostics{
			LargestTransactions: []model.Transaction{
				{TxnKey: "big", TotalRows: 8, Tables: map[string]int{"shop.t": 8}},
			},
			LongestTransactions: []model.Transaction{
				{TxnKey: "slow", TotalRows: 1, Duration: time.Second},
			},
			WidestTransactions: []model.Transaction{
				{TxnKey: "wide", TotalRows: 1, Tables: map[string]int{"a": 1, "b": 1}},
			},
		},
	}
	opts := Options{TopN: 10, SQLContextMode: SQLContextSummary}
	checks := []struct {
		format string
		render func() (string, error)
	}{
		{"text", func() (string, error) { return RenderTextWithOptions(result, opts) }},
		{"markdown", func() (string, error) { return RenderMarkdownWithOptions(result, opts) }},
		{"html", func() (string, error) { return RenderHTMLWithOptions(result, opts) }},
	}
	for _, check := range checks {
		want, err := check.render()
		if err != nil {
			t.Fatalf("%s render: %v", check.format, err)
		}
		var got bytes.Buffer
		if err := Write(&got, result, opts, check.format); err != nil {
			t.Fatalf("%s write: %v", check.format, err)
		}
		gotText, wantText := got.String(), want
		if check.format == "html" {
			gotText = generatedTime.ReplaceAllString(gotText, "TIME")
			wantText = generatedTime.ReplaceAllString(wantText, "TIME")
		}
		if gotText != wantText {
			t.Fatalf("%s Write differs from the renderer", check.format)
		}
	}
	var direct, via bytes.Buffer
	if err := RenderJSONToWithOptions(result, &direct, opts); err != nil {
		t.Fatal(err)
	}
	if err := Write(&via, result, opts, "json"); err != nil {
		t.Fatal(err)
	}
	if direct.String() != via.String() {
		t.Fatal("json Write differs from RenderJSONTo")
	}
}

func TestWriteUnknownFormatWritesNothing(t *testing.T) {
	var buf bytes.Buffer
	err := Write(&buf, model.AnalysisResult{Summary: model.WorkloadSummary{TotalRows: 1}}, Options{}, "yaml")
	if err == nil || !strings.Contains(err.Error(), `unknown format "yaml"`) {
		t.Fatalf("err = %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("wrote %q", buf.String())
	}
	err = Write(&buf, model.AnalysisResult{}, Options{}, "md")
	if err == nil || !strings.Contains(err.Error(), `unknown format "md"`) {
		t.Fatalf("md err = %v", err)
	}
}
