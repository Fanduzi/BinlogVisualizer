// Package report verifies UTC presentation across human and machine analyze report formats.
// input: analysis timestamps carrying an explicit non-UTC location.
// output: regression coverage for UTC-labelled text, HTML, Markdown, and RFC3339 JSON timestamps.
// pos: cross-renderer timestamp presentation contract for analyze reports.
// note: if this file changes, update this header and module README.md.
package report

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

var humanDateTime = regexp.MustCompile(`\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?`)

func timezoneReportFixture() model.AnalysisResult {
	local := time.FixedZone("UTC+8", 8*60*60)
	start := time.Date(2026, 8, 29, 19, 59, 13, 0, local)
	minute := time.Date(2026, 8, 29, 19, 59, 0, 0, local)

	return model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 1,
			TotalRows:         1,
			TotalEvents:       1,
			StartTime:         start,
			EndTime:           start.Add(time.Minute),
			Duration:          time.Minute,
		},
		Timeseries: model.Timeseries{
			TPSSeries:  []model.TimeseriesPoint{{Minute: minute, Value: 1}},
			RowsSeries: []model.TimeseriesPoint{{Minute: minute, Value: 1}},
		},
		Tables: []model.TableStats{{
			Schema:     "shop",
			Table:      "orders",
			TotalRows:  1,
			EventCount: 1,
			Activity:   []model.TableActivityPoint{{Minute: minute, Rows: 1}},
		}},
		Minutes: []model.MinuteBucket{{Minute: minute, TotalRows: 1, TxnCount: 1}},
		Alerts: []model.Alert{{
			Type: "spike", Severity: "warning", Message: "spike", Minute: start,
			Details: map[string]any{
				"minute": start,
				"nested": map[string]any{"minutes": []any{start}},
			},
		}},
		Diagnostics: model.Diagnostics{
			DDLEvents: []model.DDLEvent{{Timestamp: start, Operation: "ALTER", Schema: "shop", Table: "orders"}},
			HotIntervals: []model.MinuteBucket{{
				Minute: minute, TotalRows: 1, TxnCount: 1,
			}},
			FileCoverage: model.FileCoverage{Selected: []model.FileCoverageItem{{
				BinlogPath: "mysql-bin.000001", Reason: "selected", Size: 1,
				FirstEventAt: start, LastEventAt: start.Add(time.Minute),
			}}},
			FileSegments: []model.FileSegment{{
				StartTime: minute, EndTime: minute.Add(time.Minute), Rows: 1, Events: 1,
			}},
			Findings: []model.Finding{{Kind: "spike", Severity: "warning", Message: "spike", Minute: minute}},
		},
		PatternDrilldowns: []model.PatternDrilldown{{
			PatternKey: "shop.orders", Label: "shop.orders", WhySelected: "peak",
			BusiestMinutes: []model.PatternPeakMinute{{Minute: minute, TotalRows: 1, TxnCount: 1}},
		}},
	}
}

func TestHumanAnalyzeReportsLabelBinlogTimestampsUTC(t *testing.T) {
	forceEnglishReportLocale(t)
	result := timezoneReportFixture()
	opts := Options{Details: true}

	renderers := map[string]struct {
		render      func() (string, error)
		minuteStyle bool
	}{
		"text":     {render: func() (string, error) { return RenderTextWithOptions(result, opts) }, minuteStyle: true},
		"html":     {render: func() (string, error) { return RenderHTMLWithOptions(result, opts) }, minuteStyle: true},
		"markdown": {render: func() (string, error) { return RenderMarkdownWithOptions(result, opts) }},
	}
	for name, renderer := range renderers {
		t.Run(name, func(t *testing.T) {
			out, err := renderer.render()
			if err != nil {
				t.Fatalf("render %s: %v", name, err)
			}
			for _, want := range []string{
				"Timestamps",
				"UTC (binlog)",
				"2026-08-29 11:59:13 UTC",
			} {
				if !strings.Contains(out, want) {
					t.Fatalf("%s output missing %q", name, want)
				}
			}
			if strings.Contains(out, "2026-08-29 19:59") {
				t.Fatalf("%s output rendered the timestamp in UTC+8", name)
			}
			for _, timestamp := range humanDateTime.FindAllString(out, -1) {
				if !strings.Contains(out, timestamp+" UTC") {
					t.Fatalf("%s output contains an unlabelled timestamp %q", name, timestamp)
				}
			}
			if renderer.minuteStyle && !strings.Contains(out, "2026-08-29 11:59 UTC") {
				t.Fatalf("%s output did not label minute-precision timestamps UTC", name)
			}
		})
	}
}

func TestHumanAnalyzeTimestampBasisIsLocalized(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("zh-CN")
	t.Cleanup(func() {
		i18n.ResetForTesting()
		_ = i18n.Init("en")
	})

	for name, render := range map[string]func() (string, error){
		"text":     func() (string, error) { return RenderText(timezoneReportFixture()) },
		"html":     func() (string, error) { return RenderHTML(timezoneReportFixture()) },
		"markdown": func() (string, error) { return RenderMarkdown(timezoneReportFixture()) },
	} {
		t.Run(name, func(t *testing.T) {
			out, err := render()
			if err != nil {
				t.Fatalf("render %s: %v", name, err)
			}
			for _, want := range []string{"时间戳", "UTC（binlog）"} {
				if !strings.Contains(out, want) {
					t.Fatalf("%s output missing %q", name, want)
				}
			}
		})
	}
}

func TestAnalyzeJSONTimestampsRemainRFC3339UTC(t *testing.T) {
	out, err := RenderJSON(timezoneReportFixture())
	if err != nil {
		t.Fatalf("render JSON: %v", err)
	}
	for _, want := range []string{
		`"report_version": 3`,
		`"start_time": "2026-08-29T11:59:13Z"`,
		`"minute": "2026-08-29T11:59:00Z"`,
		`"timestamp": "2026-08-29T11:59:13Z"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("JSON output missing %q", want)
		}
	}
	if strings.Contains(out, " UTC") {
		t.Fatal("JSON output used a human display timestamp")
	}
	details := parseJSONMap(t, out)["alerts"].([]any)[0].(map[string]any)["details"].(map[string]any)
	if details["minute"] != "2026-08-29T11:59:13Z" {
		t.Fatal("JSON alert detail timestamp was not normalized to RFC3339 UTC")
	}
	nested := details["nested"].(map[string]any)["minutes"].([]any)
	if len(nested) != 1 || nested[0] != "2026-08-29T11:59:13Z" || strings.Contains(out, "+08:00") {
		t.Fatal("JSON alert timestamps were not normalized to RFC3339 UTC")
	}
}
