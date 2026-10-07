package binlogviz

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBusiestMinuteNamesTheTableThatDroveIt(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-busiest-minute.binlog")

	text, stderr, err := executeAnalyzeLikeMain(t, path)
	if err != nil {
		t.Fatalf("text analyze: %v\n%s", err, stderr)
	}
	if !strings.Contains(text, "1  shop.catalog") || strings.Index(text, "shop.catalog") > strings.Index(text, "shop.orders") {
		t.Fatalf("window top table should stay shop.catalog ahead of shop.orders:\n%s", text)
	}
	section := sectionBetween(text, "=== Busiest Minutes ===", "===")
	if section == "" {
		t.Fatalf("missing busiest minutes:\n%s", text)
	}
	if !strings.Contains(section, "Rows in that minute, by table.") {
		t.Fatalf("missing lead:\n%s", section)
	}
	peak := strings.Index(section, "2026-03-15 14:05:00 UTC  rows=32  txns=16")
	orders := strings.Index(section, "shop.orders  30")
	catalogSpike := strings.Index(section, "shop.catalog  2")
	background := strings.Index(section, "2026-03-15 14:00:00 UTC  rows=20  txns=1")
	if peak < 0 || orders < 0 || catalogSpike < 0 || background < 0 || !(peak < orders && orders < catalogSpike && catalogSpike < background) {
		t.Fatalf("14:05 should lead with shop.orders 30, then shop.catalog 2, ahead of the catalog minutes:\n%s", section)
	}
	if strings.Contains(text, "[warning] Write spike") || strings.Contains(text, "[critical] Write spike") {
		t.Fatalf("busiest minutes must stay evidence, not a spike finding:\n%s", text)
	}

	minutes, _, err := executeAnalyzeLikeMain(t, path, "--show-minutes")
	if err != nil {
		t.Fatalf("show-minutes: %v", err)
	}
	if !strings.Contains(minutes, "shop.orders 30, shop.catalog 2") {
		t.Fatalf("minute details omitted the 14:05 tables:\n%s", minutes)
	}

	md, _, err := executeAnalyzeLikeMain(t, path, "--format", "markdown")
	if err != nil {
		t.Fatalf("markdown: %v", err)
	}
	if !strings.Contains(md, "## Busiest Minutes") || !strings.Contains(md, "shop.orders 30, shop.catalog 2") {
		t.Fatalf("markdown missing the spike minute's tables:\n%s", md)
	}

	html, _, err := executeAnalyzeLikeMain(t, path, "--format", "html", "--output", "-")
	if err != nil {
		t.Fatalf("html: %v", err)
	}
	if !strings.Contains(html, "shop.orders 30") || !strings.Contains(html, "shop.catalog 2") {
		t.Fatal("html hot interval missing the spike minute's tables")
	}

	raw, _, err := executeAnalyzeLikeMain(t, path, "--format", "json")
	if err != nil {
		t.Fatalf("json: %v", err)
	}
	var doc struct {
		Tables []struct {
			Schema    string `json:"schema"`
			Table     string `json:"table"`
			TotalRows int    `json:"total_rows"`
		} `json:"tables"`
		Diagnostics struct {
			HotIntervals []struct {
				Minute    string         `json:"minute"`
				TotalRows int            `json:"total_rows"`
				TxnCount  int            `json:"txn_count"`
				TableRows map[string]int `json:"table_rows"`
			} `json:"hot_intervals"`
			LargestTransactions []struct {
				TotalRows int            `json:"total_rows"`
				Tables    map[string]int `json:"tables"`
			} `json:"largest_transactions"`
		} `json:"diagnostics"`
	}
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("json: %v", err)
	}
	if len(doc.Tables) < 2 || doc.Tables[0].Schema != "shop" || doc.Tables[0].Table != "catalog" || doc.Tables[0].TotalRows != 82 {
		t.Fatalf("top table = %+v, want shop.catalog 82", doc.Tables)
	}
	if doc.Tables[1].Table != "orders" || doc.Tables[1].TotalRows != 30 {
		t.Fatalf("second table = %+v, want shop.orders 30", doc.Tables[1])
	}
	if len(doc.Diagnostics.HotIntervals) == 0 {
		t.Fatal("no hot intervals")
	}
	peakMinute := doc.Diagnostics.HotIntervals[0]
	if peakMinute.Minute != "2026-03-15T14:05:00Z" || peakMinute.TotalRows != 32 || peakMinute.TxnCount != 16 {
		t.Fatalf("peak minute = %+v", peakMinute)
	}
	if peakMinute.TableRows["shop.orders"] != 30 || peakMinute.TableRows["shop.catalog"] != 2 {
		t.Fatalf("peak table_rows = %+v", peakMinute.TableRows)
	}
	if len(doc.Diagnostics.LargestTransactions) == 0 || doc.Diagnostics.LargestTransactions[0].Tables["shop.catalog"] != 20 {
		t.Fatalf("largest txn should stay a 20-row catalog batch, got %+v", doc.Diagnostics.LargestTransactions)
	}

	ordersOnly, _, err := executeAnalyzeLikeMain(t, path, "--include-table", "shop.orders")
	if err != nil {
		t.Fatalf("include-table: %v", err)
	}
	ordersSection := sectionBetween(ordersOnly, "=== Busiest Minutes ===", "===")
	if !strings.Contains(ordersSection, "shop.orders  30") || strings.Contains(ordersSection, "shop.catalog") {
		t.Fatalf("include-table shop.orders should keep only that table:\n%s", ordersSection)
	}

	catalogOnly, _, err := executeAnalyzeLikeMain(t, path, "--exclude-table", "shop.orders")
	if err != nil {
		t.Fatalf("exclude-table: %v", err)
	}
	catalogSection := sectionBetween(catalogOnly, "=== Busiest Minutes ===", "===")
	if strings.Contains(catalogSection, "shop.orders") || !strings.Contains(catalogSection, "shop.catalog  20") {
		t.Fatalf("exclude-table shop.orders should leave the catalog minutes:\n%s", catalogSection)
	}
	if strings.Contains(catalogSection, "rows=32") {
		t.Fatalf("excluded orders should not leave the 32-row spike minute:\n%s", catalogSection)
	}
}
