// Package report renders self-contained HTML reports from bounded analysis results.
// input: analyzer-produced AnalysisResult values plus optional SQL context presentation controls.
// output: single self-contained HTML file with embedded ECharts, dark OLED theme, inline CSS.
// pos: HTML renderer for the CLI output path after analyzer Finalize.
// note: if this file changes, update this header and module README.md.
package report

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"strings"
	"time"

	"binlogviz/internal/model"
)

//go:embed static/echarts.min.js
var echartsFS embed.FS

// RenderHTML renders an AnalysisResult as a self-contained HTML report.
func RenderHTML(result model.AnalysisResult) (string, error) {
	return RenderHTMLWithOptions(result, DefaultOptions())
}

// RenderHTMLWithOptions renders an AnalysisResult as HTML with explicit presentation controls.
func RenderHTMLWithOptions(result model.AnalysisResult, opts Options) (string, error) {
	echartJS, err := echartsFS.ReadFile("static/echarts.min.js")
	if err != nil {
		return "", fmt.Errorf("read echarts: %w", err)
	}

	tmpl, err := template.New("report").Funcs(template.FuncMap{
		"safeJS": func(s string) template.JS { return template.JS(s) }, //nolint:gosec
		"json":   jsonMarshal,
	}).Parse(htmlReportTemplate)
	if err != nil {
		return "", fmt.Errorf("parse html template: %w", err)
	}

	data := buildHTMLData(result, opts, string(echartJS))

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("render html template: %w", err)
	}
	return buf.String(), nil
}

func jsonMarshal(v any) (template.JS, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return template.JS(b), nil //nolint:gosec
}

type htmlReportData struct {
	GeneratedAt   string
	SourceFiles   string
	StartTime     string
	EndTime       string
	Duration      string
	TotalTxns     int
	TotalRows     int
	TotalEvents   int
	Tables        []htmlTableRow
	Alerts        []htmlAlert
	HasAlerts     bool
	MinuteLabels  template.JS
	MinuteRows    template.JS
	MinuteTxns    template.JS
	TableBarNames template.JS
	TableBarRows  template.JS
	OpsPie        template.JS
	EChartsJS     template.JS
}

type htmlTableRow struct {
	Schema  string
	Table   string
	Total   int
	Inserts int
	Updates int
	Deletes int
	Txns    int
}

type htmlAlert struct {
	Severity string
	Message  string
	Badge    string
}

func buildHTMLData(result model.AnalysisResult, _ Options, echartsJS string) htmlReportData {
	d := htmlReportData{
		GeneratedAt: time.Now().UTC().Format("2006-01-02 15:04:05 UTC"),
		TotalTxns:   result.Summary.TotalTransactions,
		TotalRows:   result.Summary.TotalRows,
		TotalEvents: result.Summary.TotalEvents,
		EChartsJS:   template.JS(echartsJS), //nolint:gosec
	}

	if !result.Summary.StartTime.IsZero() {
		d.StartTime = result.Summary.StartTime.Format("2006-01-02 15:04:05")
		d.EndTime = result.Summary.EndTime.Format("2006-01-02 15:04:05")
		d.Duration = result.Summary.Duration.String()
	}

	// Tables
	for _, t := range result.Tables {
		d.Tables = append(d.Tables, htmlTableRow{
			Schema:  t.Schema,
			Table:   t.Table,
			Total:   t.TotalRows,
			Inserts: t.InsertRows,
			Updates: t.UpdateRows,
			Deletes: t.DeleteRows,
			Txns:    t.TxnCount,
		})
	}

	// Alerts
	for _, a := range result.Alerts {
		badge := "INFO"
		switch a.Severity {
		case "warning":
			badge = "WARN"
		case "critical":
			badge = "CRIT"
		}
		d.Alerts = append(d.Alerts, htmlAlert{
			Severity: a.Severity,
			Message:  a.Message,
			Badge:    badge,
		})
	}
	d.HasAlerts = len(d.Alerts) > 0

	// Chart data — minute timeline
	labels := make([]string, 0, len(result.Minutes))
	rows := make([]int, 0, len(result.Minutes))
	txns := make([]int, 0, len(result.Minutes))
	for _, m := range result.Minutes {
		labels = append(labels, m.Minute.Format("15:04"))
		rows = append(rows, m.TotalRows)
		txns = append(txns, m.TxnCount)
	}
	d.MinuteLabels = mustJSON(labels)
	d.MinuteRows = mustJSON(rows)
	d.MinuteTxns = mustJSON(txns)

	// Chart data — top 10 tables bar
	top := result.Tables
	if len(top) > 10 {
		top = top[:10]
	}
	barNames := make([]string, 0, len(top))
	barRows := make([]int, 0, len(top))
	var totalInserts, totalUpdates, totalDeletes int
	for _, t := range result.Tables {
		totalInserts += t.InsertRows
		totalUpdates += t.UpdateRows
		totalDeletes += t.DeleteRows
	}
	for _, t := range top {
		barNames = append(barNames, t.Schema+"."+t.Table)
		barRows = append(barRows, t.TotalRows)
	}
	d.TableBarNames = mustJSON(barNames)
	d.TableBarRows = mustJSON(barRows)

	// Ops pie
	pie := []map[string]any{
		{"name": "INSERT", "value": totalInserts},
		{"name": "UPDATE", "value": totalUpdates},
		{"name": "DELETE", "value": totalDeletes},
	}
	d.OpsPie = mustJSON(pie)

	return d
}

func mustJSON(v any) template.JS {
	b, err := json.Marshal(v)
	if err != nil {
		return template.JS("[]") //nolint:gosec
	}
	return template.JS(b) //nolint:gosec
}

// RenderHTMLToStdout writes the HTML report to stdout.
func RenderHTMLToStdout(result model.AnalysisResult, opts Options) error {
	text, err := RenderHTMLWithOptions(result, opts)
	if err != nil {
		return err
	}
	_, err = fmt.Print(text)
	return err
}

// fmtInt formats an integer with thousands separators.
func fmtIntHTML(n int) string {
	s := fmt.Sprintf("%d", n)
	if n < 1000 {
		return s
	}
	var result strings.Builder
	for i, ch := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteByte(',')
		}
		result.WriteRune(ch)
	}
	return result.String()
}
