package report

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

func TestRenderApplyDelayReplicaSourceAndUnavailable(t *testing.T) {
	forceEnglishReportLocale(t)
	peak := time.Date(2026, 3, 15, 14, 5, 0, 0, time.UTC)
	original := time.Date(2026, 3, 15, 14, 5, 1, 12_000, time.UTC)
	immediate := original.Add(1500 * time.Microsecond)
	replica := model.AnalysisResult{
		Summary: model.WorkloadSummary{StartTime: peak, EndTime: peak.Add(time.Minute)},
		Transactions: []model.Transaction{{
			TxnKey:       "txn-1",
			QuerySummary: "INSERT INTO shop.secret VALUES (1)",
			TotalRows:    4,
		}},
		Diagnostics: model.Diagnostics{
			HotIntervals: []model.MinuteBucket{{Minute: peak, TotalRows: 4, TxnCount: 1, TableRows: map[string]int{"shop.orders": 4}}},
			ApplyDelay: &model.ApplyDelay{
				Origin:     model.ApplyDelayReplica,
				Max:        1500 * time.Microsecond,
				P95:        800 * time.Microsecond,
				PeakMinute: peak,
				Transactions: []model.ApplyDelayTxn{
					{
						GTID:            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:2",
						TxnStartPath:    "mysql-bin.000001",
						TxnStartPos:     154,
						OriginalCommit:  original,
						ImmediateCommit: immediate,
						Delay:           1500 * time.Microsecond,
						Tables:          map[string]int{"shop.orders": 3, "shop.catalog": 1},
					},
					{
						TxnStartPath:    "mysql-bin.000001",
						TxnStartPos:     400,
						OriginalCommit:  original,
						ImmediateCommit: original.Add(200 * time.Microsecond),
						Delay:           200 * time.Microsecond,
						Tables:          map[string]int{"shop.orders": 1},
					},
				},
			},
		},
	}

	text, err := RenderTextWithOptions(replica, Options{TopN: 10, SQLContextMode: SQLContextOff})
	if err != nil {
		t.Fatal(err)
	}
	section := sectionBetweenReport(text, "=== Replica Apply Delay ===", "===")
	if section == "" {
		t.Fatalf("missing section:\n%s", text)
	}
	for _, want := range []string{
		"assumes the source and replica clocks agree",
		"Replica: at least one transaction committed here after it committed on the source.",
		"max 1.5ms  p95 800µs  peak minute 2026-03-15 14:05:00 UTC",
		"gtid=bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:2  mysql-bin.000001:154",
		"original=2026-03-15 14:05:01.000012 UTC",
		"immediate=2026-03-15 14:05:01.001512 UTC",
		"delay=1.5ms",
		"shop.orders  3",
		"shop.catalog  1",
		"mysql-bin.000001:400",
	} {
		if !strings.Contains(section, want) {
			t.Fatalf("section missing %q:\n%s", want, section)
		}
	}
	if strings.Contains(section, "secret") || strings.Contains(section, "INSERT INTO") {
		t.Fatalf("sql context leaked into the delay section:\n%s", section)
	}
	if strings.Index(text, "=== Busiest Minutes ===") > strings.Index(text, "=== Replica Apply Delay ===") {
		t.Fatalf("delay section is before busiest minutes:\n%s", text)
	}
	limited, err := RenderTextWithOptions(replica, Options{TopN: 1})
	if err != nil {
		t.Fatal(err)
	}
	limitedSection := sectionBetweenReport(limited, "=== Replica Apply Delay ===", "===")
	if strings.Contains(limitedSection, "mysql-bin.000001:400") || !strings.Contains(limitedSection, "max 1.5ms") {
		t.Fatalf("--top 1 changed the stats or kept the second row:\n%s", limitedSection)
	}

	source := replica
	source.Diagnostics.ApplyDelay = &model.ApplyDelay{
		Origin:     model.ApplyDelaySource,
		PeakMinute: peak,
	}
	sourceText, err := RenderText(source)
	if err != nil {
		t.Fatal(err)
	}
	sourceSection := sectionBetweenReport(sourceText, "=== Replica Apply Delay ===", "===")
	if !strings.Contains(sourceSection, "Source: original and immediate commit timestamps are equal.") || strings.Contains(sourceSection, "gtid=") || strings.Contains(sourceSection, "max ") {
		t.Fatalf("source section=\n%s", sourceSection)
	}

	unavailable := replica
	unavailable.Diagnostics.ApplyDelay = nil
	unavailableText, err := RenderText(unavailable)
	if err != nil {
		t.Fatal(err)
	}
	unavailableSection := sectionBetweenReport(unavailableText, "=== Replica Apply Delay ===", "===")
	if strings.TrimSpace(unavailableSection) != "commit timestamps unavailable" {
		t.Fatalf("unavailable section=%q", unavailableSection)
	}
	if strings.Contains(unavailableSection, "clocks agree") {
		t.Fatalf("unavailable section grew a clock note:\n%s", unavailableSection)
	}

	js, err := RenderJSONWithOptions(replica, Options{TopN: 1, SQLContextMode: SQLContextOff})
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(js), &payload); err != nil {
		t.Fatal(err)
	}
	raw, ok := payload["replica_apply_delay"].(map[string]any)
	if !ok {
		t.Fatalf("missing replica_apply_delay: %s", js)
	}
	if raw["origin"] != "replica" || raw["max_delay_us"] != float64(1500) || raw["p95_delay_us"] != float64(800) || raw["peak_minute"] != "2026-03-15T14:05:00Z" {
		t.Fatalf("summary=%v", raw)
	}
	txns, _ := raw["transactions"].([]any)
	if len(txns) != 1 {
		t.Fatalf("top 1 json txns=%d", len(txns))
	}
	first := txns[0].(map[string]any)
	if first["gtid"] != "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:2" || first["txn_start_file"] != "mysql-bin.000001" || first["txn_start_pos"] != float64(154) || first["delay_us"] != float64(1500) {
		t.Fatalf("txn=%v", first)
	}
	if _, ok := first["query_summary"]; ok {
		t.Fatalf("query leaked into delay json: %v", first)
	}
	if first["original_commit_time"] == "" || first["immediate_commit_time"] == "" {
		t.Fatalf("times=%v", first)
	}

	sourceJSON, err := RenderJSON(source)
	if err != nil {
		t.Fatal(err)
	}
	var sourcePayload map[string]any
	if err := json.Unmarshal([]byte(sourceJSON), &sourcePayload); err != nil {
		t.Fatal(err)
	}
	sourceDelay, _ := sourcePayload["replica_apply_delay"].(map[string]any)
	if sourceDelay["origin"] != "source" || sourceDelay["max_delay_us"] != float64(0) || sourceDelay["p95_delay_us"] != float64(0) {
		t.Fatalf("source delay=%v", sourceDelay)
	}
	if _, ok := sourceDelay["transactions"]; ok {
		t.Fatalf("source json included a transaction table: %v", sourceDelay)
	}
	unavailableJSON, err := RenderJSON(unavailable)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(unavailableJSON, "replica_apply_delay") || strings.Contains(unavailableJSON, "max_delay_us") {
		t.Fatalf("unavailable json leaked delay fields:\n%s", unavailableJSON)
	}

	md, err := RenderMarkdownWithOptions(replica, Options{TopN: 10, SQLContextMode: SQLContextOff})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(md, "## Replica Apply Delay") || !strings.Contains(md, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:2") || strings.Contains(md, "secret") {
		t.Fatalf("markdown=\n%s", md)
	}
	html, err := RenderHTMLWithOptions(replica, Options{TopN: 10, SQLContextMode: SQLContextOff})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(html, `id="section-replica-delay"`) || !strings.Contains(html, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:2") || strings.Contains(html, "secret") {
		t.Fatal("html delay section missing the transaction or leaked SQL")
	}
	activity := strings.Index(html, `id="section-activity"`)
	delayAt := strings.Index(html, `id="section-replica-delay"`)
	objects := strings.Index(html, `id="section-objects"`)
	if activity < 0 || delayAt < 0 || objects < 0 || !(activity < delayAt && delayAt < objects) {
		t.Fatalf("section order activity=%d delay=%d objects=%d", activity, delayAt, objects)
	}
}

func TestRenderApplyDelayChineseUnavailableAndReplica(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("zh-CN")
	t.Cleanup(func() {
		i18n.ResetForTesting()
		_ = i18n.Init("en")
	})
	text, err := RenderText(model.AnalysisResult{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "=== 副本应用延迟 ===") || !strings.Contains(text, "提交时间戳不可用") {
		t.Fatalf("zh unavailable=\n%s", text)
	}
	if strings.Contains(text, "commit timestamps unavailable") {
		t.Fatalf("english unavailable line survived zh-CN:\n%s", text)
	}
}
