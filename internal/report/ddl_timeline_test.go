package report

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
)

func TestDDLTimelinePrintsGTIDAndStopPositionInEveryFormat(t *testing.T) {
	forceEnglishReportLocale(t)
	event := model.DDLEvent{
		Timestamp:     time.Date(2026, 10, 7, 7, 33, 6, 0, time.UTC),
		Operation:     "DROP TABLE",
		Schema:        "shop",
		Table:         "orders",
		Statement:     "DROP TABLE `shop`.`orders`",
		BinlogPath:    "mysql-bin.000001",
		PositionStart: 600,
		PositionEnd:   734,
		GTID:          "4d8275bc-c221-11f1-a25e-822b383dbcd0:2",
		TxnStartPath:  "mysql-bin.000001",
		TxnStartPos:   523,
		ServerID:      1,
		ThreadID:      8,
		ActorUser:     "root",
		ActorHost:     "localhost",
	}
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{DDLEvents: []model.DDLEvent{event}, ServerVersion: "8.0.46"},
	}
	text, err := RenderText(result)
	if err != nil {
		t.Fatal(err)
	}
	md, err := RenderMarkdown(result)
	if err != nil {
		t.Fatal(err)
	}
	html, err := RenderHTML(result)
	if err != nil {
		t.Fatal(err)
	}
	js, err := RenderJSON(result)
	if err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{text, md, html} {
		for _, want := range []string{
			"gtid=4d8275bc-c221-11f1-a25e-822b383dbcd0:2",
			"server_id=1",
			"thread_id=8",
			"user@host=root@localhost",
			"mysql-bin.000001:523",
			"mysqlbinlog --stop-position=523 mysql-bin.000001",
			"BinlogServer stop_gtid=4d8275bc-c221-11f1-a25e-822b383dbcd0:2",
			"stop before this DDL (replays earlier events, excludes this DDL)",
			"mysql-bin.000001:600-734",
		} {
			if !strings.Contains(out, want) {
				t.Fatalf("missing %q\n%s", want, out)
			}
		}
		if strings.Contains(out, "GTID unavailable") {
			t.Fatalf("named GTID printed the unavailable note:\n%s", out)
		}
	}
	for _, want := range []string{
		`"gtid": "4d8275bc-c221-11f1-a25e-822b383dbcd0:2"`,
		`"stop_gtid": "4d8275bc-c221-11f1-a25e-822b383dbcd0:2"`,
		`"txn_start_pos": 523`,
		`"mysqlbinlog_stop": "mysqlbinlog --stop-position=523 mysql-bin.000001"`,
		`"server_id": 1`,
		`"thread_id": 8`,
		`"user": "root"`,
		`"host": "localhost"`,
		"stop before this DDL (replays earlier events, excludes this DDL)",
	} {
		if !strings.Contains(js, want) {
			t.Fatalf("json missing %q\n%s", want, js)
		}
	}
	if strings.Contains(js, "gtid_note") {
		t.Fatalf("json included gtid_note for a named GTID:\n%s", js)
	}
	parsed := parseJSONMap(t, js)
	ddl := parsed["diagnostics"].(map[string]any)["ddl_events"].([]any)[0].(map[string]any)
	if ddl["gtid"] != event.GTID || ddl["stop_gtid"] != event.GTID || ddl["txn_start_pos"].(float64) != 523 {
		t.Fatalf("json ddl = %#v", ddl)
	}
	if _, ok := ddl["gtid_note"]; ok {
		t.Fatalf("gtid_note should be omitted when gtid is present: %#v", ddl)
	}
}

func TestDDLTimelineOmitsEmptyGTIDAndSaysUnavailable(t *testing.T) {
	forceEnglishReportLocale(t)
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{DDLEvents: []model.DDLEvent{{
			Timestamp:     time.Date(2026, 10, 7, 7, 0, 0, 0, time.UTC),
			Operation:     "DROP TABLE",
			Schema:        "shop",
			Table:         "orders",
			Statement:     "DROP TABLE shop.orders",
			BinlogPath:    "mysql-bin.000001",
			PositionStart: 219,
			PositionEnd:   300,
			TxnStartPath:  "mysql-bin.000001",
			TxnStartPos:   219,
		}}},
	}
	text, err := RenderText(result)
	if err != nil {
		t.Fatal(err)
	}
	js, err := RenderJSON(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "GTID unavailable") || strings.Contains(text, "gtid=") {
		t.Fatalf("text should name a missing GTID without an empty value:\n%s", text)
	}
	if !strings.Contains(text, "mysqlbinlog --stop-position=219 mysql-bin.000001") {
		t.Fatalf("position hint missing:\n%s", text)
	}
	if strings.Contains(text, "BinlogServer") {
		t.Fatalf("no GTID must not print stop_gtid:\n%s", text)
	}
	ddl := parseJSONMap(t, js)["diagnostics"].(map[string]any)["ddl_events"].([]any)[0].(map[string]any)
	if _, ok := ddl["gtid"]; ok {
		t.Fatalf("gtid field present: %#v", ddl)
	}
	if _, ok := ddl["stop_gtid"]; ok {
		t.Fatalf("stop_gtid field present: %#v", ddl)
	}
	if ddl["gtid_note"] != "GTID unavailable" || ddl["txn_start_pos"].(float64) != 219 {
		t.Fatalf("json ddl = %#v", ddl)
	}
}

func TestDDLTimelineChineseNamesMissingGTID(t *testing.T) {
	i18n.ResetForTesting()
	i18n.MustInit("zh-CN")
	t.Cleanup(func() {
		i18n.ResetForTesting()
		i18n.MustInit("en")
	})
	result := model.AnalysisResult{
		Diagnostics: model.Diagnostics{DDLEvents: []model.DDLEvent{{
			Timestamp:    time.Date(2026, 10, 7, 7, 0, 0, 0, time.UTC),
			Operation:    "DROP TABLE",
			Schema:       "shop",
			Table:        "orders",
			BinlogPath:   "mysql-bin.000001",
			TxnStartPath: "mysql-bin.000001",
			TxnStartPos:  40,
		}}},
	}
	text, err := RenderText(result)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "GTID 不可用") || !strings.Contains(text, "事务起点") || !strings.Contains(text, "停在这条 DDL 之前") {
		t.Fatalf("zh-CN timeline:\n%s", text)
	}
}
