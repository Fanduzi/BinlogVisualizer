package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestRankedMinuteTablesOrdersByRowsThenName(t *testing.T) {
	got := rankedMinuteTables(map[string]int{
		"shop.zeta":   10,
		"shop.alpha":  10,
		"shop.orders": 30,
		"shop.empty":  0,
	})
	if len(got) != 3 || got[0].name != "shop.orders" || got[0].rows != 30 || got[1].name != "shop.alpha" || got[2].name != "shop.zeta" {
		t.Fatalf("tables = %+v", got)
	}
}

func TestRenderTextBusiestMinutesNamesDrivingTables(t *testing.T) {
	forceEnglishReportLocale(t)
	spike := time.Date(2026, 3, 15, 14, 5, 0, 0, time.UTC)
	background := spike.Add(-5 * time.Minute)
	result := model.AnalysisResult{
		Summary: model.WorkloadSummary{
			TotalTransactions: 6,
			TotalRows:         110,
			StartTime:         background,
			EndTime:           spike,
			Duration:          5 * time.Minute,
		},
		Tables: []model.TableStats{
			{Schema: "shop", Table: "catalog", TotalRows: 80},
			{Schema: "shop", Table: "orders", TotalRows: 30},
		},
		Diagnostics: model.Diagnostics{
			HotIntervals: []model.MinuteBucket{
				{
					Minute:    spike,
					TotalRows: 30,
					TxnCount:  15,
					TableRows: map[string]int{"shop.orders": 30},
				},
				{
					Minute:    background,
					TotalRows: 20,
					TxnCount:  1,
					TableRows: map[string]int{"shop.catalog": 20, "shop.orders": 0},
				},
				{Minute: spike.Add(time.Minute), TotalRows: 0, TxnCount: 0},
			},
		},
	}

	out, err := RenderTextWithOptions(result, Options{TopN: 10, TopTables: 10})
	if err != nil {
		t.Fatal(err)
	}
	section := sectionBetweenReport(out, "=== Busiest Minutes ===", "===")
	if section == "" {
		t.Fatalf("missing busiest minutes:\n%s", out)
	}
	if !strings.Contains(section, "Rows in that minute, by table.") {
		t.Fatalf("missing lead:\n%s", section)
	}
	ordersAt := strings.Index(section, "2026-03-15 14:05:00 UTC  rows=30  txns=15")
	catalogAt := strings.Index(section, "2026-03-15 14:00:00 UTC  rows=20  txns=1")
	if ordersAt < 0 || catalogAt < 0 || ordersAt > catalogAt {
		t.Fatalf("expected the 14:05 spike ahead of the catalog minute:\n%s", section)
	}
	if !strings.Contains(section, "shop.orders  30") || !strings.Contains(section, "shop.catalog  20") {
		t.Fatalf("driving tables missing:\n%s", section)
	}
	if strings.Contains(section, "14:06") {
		t.Fatalf("zero-row minute was listed:\n%s", section)
	}
	if strings.Contains(out, "[critical] Write spike") || strings.Contains(out, "[warning] Write spike") {
		t.Fatalf("busiest minutes became a spike finding:\n%s", out)
	}

	limited, err := RenderTextWithOptions(result, Options{TopN: 1, TopTables: 1})
	if err != nil {
		t.Fatal(err)
	}
	limitedSection := sectionBetweenReport(limited, "=== Busiest Minutes ===", "===")
	if strings.Contains(limitedSection, "shop.catalog") || strings.Contains(limitedSection, "14:00") {
		t.Fatalf("top 1 should keep only the peak minute:\n%s", limitedSection)
	}
}

func TestRenderTextShowMinutesIncludesDrivingTables(t *testing.T) {
	forceEnglishReportLocale(t)
	minute := time.Date(2026, 3, 15, 14, 5, 0, 0, time.UTC)
	result := model.AnalysisResult{
		Minutes: []model.MinuteBucket{{
			Minute:    minute,
			TotalRows: 30,
			TxnCount:  15,
			TableRows: map[string]int{"shop.orders": 28, "shop.catalog": 2},
		}},
	}
	out, err := RenderTextWithOptions(result, Options{TopN: 10, TopTables: 10, ShowMinutes: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "shop.orders 28, shop.catalog 2") {
		t.Fatalf("minute details omitted driving tables:\n%s", out)
	}
}

func TestRenderMarkdownAndHTMLNameMinuteTables(t *testing.T) {
	forceEnglishReportLocale(t)
	minute := time.Date(2026, 3, 15, 14, 5, 0, 0, time.UTC)
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{
			HotIntervals: []model.MinuteBucket{{
				Minute:    minute,
				TotalRows: 30,
				TxnCount:  15,
				TableRows: map[string]int{"shop.catalog": 2, "shop.orders": 28, "shop.extra": 1},
			}},
		},
		Minutes: []model.MinuteBucket{{
			Minute:    minute,
			TotalRows: 30,
			TxnCount:  15,
			TableRows: map[string]int{"shop.orders": 28, "shop.catalog": 2},
		}},
	}

	md, err := RenderMarkdownWithOptions(result, Options{TopN: 2, TopTables: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(md, "## Busiest Minutes") || !strings.Contains(md, "shop.orders 28, shop.catalog 2") {
		t.Fatalf("markdown missing ranked minute tables:\n%s", md)
	}
	if !strings.Contains(md, "… and 1 more tables") {
		t.Fatalf("markdown should cap driving tables at top N:\n%s", md)
	}
	if !strings.Contains(md, "| Driving tables |") {
		t.Fatalf("chronological minute table lost the driving-tables column:\n%s", md)
	}

	html, err := RenderHTMLWithOptions(result, Options{TopN: 2, TopTables: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(html, "shop.orders 28") || !strings.Contains(html, "shop.catalog 2") {
		t.Fatalf("html hot interval missing driving tables:\n%s", html)
	}
	if !strings.Contains(html, "… and 1 more tables") {
		t.Fatalf("html should cap driving tables at top N")
	}
}

func sectionBetweenReport(text, start, nextHeading string) string {
	at := strings.Index(text, start)
	if at < 0 {
		return ""
	}
	rest := text[at+len(start):]
	if end := strings.Index(rest, nextHeading); end >= 0 {
		rest = rest[:end]
	}
	return rest
}
