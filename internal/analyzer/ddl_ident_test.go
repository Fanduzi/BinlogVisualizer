package analyzer

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/compare"
	"binlogviz/internal/flashback"
	"binlogviz/internal/i18n"
	"binlogviz/internal/model"
	"binlogviz/internal/report"
	"binlogviz/internal/trend"
)

// Quoted names decode once. CREATE and DROP of `we“ird` are the same table.
func TestAnalyzeQuotedNamesDecodeOnce(t *testing.T) {
	base := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "DDL", Schema: "qa410a", PositionStart: 10, PositionEnd: 20, QuerySQL: "CREATE TABLE `qa410a`.`do.t` (id INT)"},
		{Timestamp: base.Add(time.Second), EventType: "DDL", Schema: "qa410a", PositionStart: 30, PositionEnd: 40, QuerySQL: "CREATE TABLE `sp ace` (id INT)"},
		{Timestamp: base.Add(2 * time.Second), EventType: "DDL", Schema: "qa", PositionStart: 50, PositionEnd: 60, QuerySQL: "CREATE TABLE `d``b`.`y.z` (id INT)"},
		{Timestamp: base.Add(3 * time.Second), EventType: "DDL", Schema: "qa", PositionStart: 70, PositionEnd: 80, QuerySQL: "CREATE TABLE `we``ird` (id INT)"},
		{Timestamp: base.Add(4 * time.Second), EventType: "DDL", Schema: "qa", PositionStart: 90, PositionEnd: 100, QuerySQL: "DROP TABLE `we``ird`"},
	}
	result, err := New(DefaultOptions()).Analyze(events)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	got := map[string]int{}
	for _, table := range result.Tables {
		got[table.Schema+"."+table.Table] = table.DDLCount
	}
	if got["qa410a.do.t"] != 1 || got["qa410a.sp ace"] != 1 || got["d`b.y.z"] != 1 || got["qa.we`ird"] != 2 {
		t.Fatalf("tables = %v", got)
	}
	if got["qa410a.do"] != 0 || got["qa410a.sp"] != 0 || got["d`b.y"] != 0 || got["qa.we``ird"] != 0 {
		t.Fatalf("name was split or left doubled: %v", got)
	}
	for name := range got {
		if strings.Contains(name, "``") {
			t.Fatalf("doubled backtick kept in %q (%v)", name, got)
		}
	}
}

func TestAnalyzeVersionedAndANSIDropsNameEachTable(t *testing.T) {
	base := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{
			Timestamp: base, EventType: "DDL", Schema: "qa410a",
			ServerVersion: "8.0.36", ServerFlavor: "mysql",
			QuerySQL: "DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `td 2``x`,`td1`",
		},
		{
			Timestamp: base.Add(time.Second), EventType: "DDL", Schema: "qa410a",
			SQLMode:  4,
			QuerySQL: `DROP TABLE IF EXISTS "t9","nope2"`,
		},
	}
	result, err := New(DefaultOptions()).Analyze(events)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	got := map[string]int{}
	for _, table := range result.Tables {
		got[table.Schema+"."+table.Table] = table.DDLCount
	}
	for _, name := range []string{"qa410a.td 2`x", "qa410a.td1", "qa410a.t9", "qa410a.nope2"} {
		if got[name] != 1 {
			t.Fatalf("missing %s in %v", name, got)
		}
	}
}

// Text, Markdown, HTML, JSON, compare, and trend count statements.
// Per-table rows stay in the timeline.
func TestAnalyzeDDLCountAgreesAcrossFormats(t *testing.T) {
	base := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{
			Timestamp: base, EventType: "DDL", Schema: "shop", BinlogPath: "bin.000001",
			PositionStart: 100, PositionEnd: 180,
			QuerySQL: "CREATE TABLE shop.a (id INT)",
		},
		{
			Timestamp: base.Add(time.Second), EventType: "DDL", Schema: "shop", BinlogPath: "bin.000001",
			PositionStart: 200, PositionEnd: 280,
			QuerySQL: "DROP TABLE shop.b, shop.c, shop.d",
		},
	}
	result, err := New(DefaultOptions()).Analyze(events)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	const phrase = "4 targets in 2 statements"
	opts := report.DefaultOptions()
	text, err := report.RenderTextWithOptions(*result, opts)
	if err != nil {
		t.Fatal(err)
	}
	md, err := report.RenderMarkdownWithOptions(*result, opts)
	if err != nil {
		t.Fatal(err)
	}
	html, err := report.RenderHTMLWithOptions(*result, opts)
	if err != nil {
		t.Fatal(err)
	}
	js, err := report.RenderJSONWithOptions(*result, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{text, md, html} {
		if !strings.Contains(out, phrase) {
			t.Fatalf("missing %q\n%s", phrase, out)
		}
	}
	if !strings.Contains(text, "DDL Timeline: "+phrase+" (CREATE TABLE, DROP TABLE)") {
		t.Fatalf("text timeline:\n%s", text)
	}
	idx := strings.Index(html, phrase)
	before := html[:idx]
	marker := `card-value">`
	at := strings.LastIndex(before, marker)
	if at < 0 {
		t.Fatalf("card value missing before %q", phrase)
	}
	val := before[at+len(marker):]
	if cut := strings.Index(val, "<"); cut >= 0 {
		val = val[:cut]
	}
	if val != "2" {
		t.Fatalf("DDL card value %q, want the statement count 2", val)
	}

	var doc struct {
		Timeseries struct {
			DDL []struct {
				Value float64 `json:"value"`
			} `json:"ddl_event_series"`
		} `json:"timeseries"`
		Diagnostics struct {
			DDLEvents []struct {
				Operation string `json:"operation"`
			} `json:"ddl_events"`
		} `json:"diagnostics"`
	}
	if err := json.Unmarshal([]byte(js), &doc); err != nil {
		t.Fatal(err)
	}
	var sum float64
	points := make([]compare.InputTimeseriesPoint, len(doc.Timeseries.DDL))
	for i, p := range doc.Timeseries.DDL {
		sum += p.Value
		points[i] = compare.InputTimeseriesPoint{Minute: "2026-10-20T09:00:00Z", Value: p.Value}
	}
	if sum != 2 || len(doc.Diagnostics.DDLEvents) != 4 {
		t.Fatalf("series sum %v events %d, want 2 statements and 4 targets", sum, len(doc.Diagnostics.DDLEvents))
	}

	current := compare.InputReport{
		Timeseries:  compare.InputTimeseries{DDLEventSeries: points},
		Diagnostics: compare.InputDiagnostics{DDLEvents: make([]compare.InputDDLEvent, len(doc.Diagnostics.DDLEvents))},
	}
	baseline := current
	baseline.Timeseries.DDLEventSeries = nil
	baseline.Diagnostics.DDLEvents = nil
	delta := compare.BuildCompareResult(current, baseline)
	if delta.DiagnosticsDelta.EventMixDelta.DDLDelta != 2 {
		t.Fatalf("DDLDelta=%d, want the statement count", delta.DiagnosticsDelta.EventMixDelta.DDLDelta)
	}

	other := current
	other.Timeseries.DDLEventSeries = []compare.InputTimeseriesPoint{{Minute: "2026-10-20T10:00:00Z", Value: 9}}
	other.Diagnostics.DDLEvents = make([]compare.InputDDLEvent, 1)
	snap := func(r compare.InputReport, name, start string) compare.InputReport {
		r.Snapshot = &compare.InputSnapshot{
			Name:   name,
			Window: compare.InputSnapshotWindow{StartTime: start},
		}
		return r
	}
	tr, err := trend.BuildResult(trend.BuildOptions{
		Points: []trend.BuildInput{
			{Report: snap(current, "a", "2026-10-20T09:00:00Z")},
			{Report: snap(other, "b", "2026-10-20T10:00:00Z")},
		},
	})
	if err != nil {
		t.Fatalf("trend: %v", err)
	}
	got := tr.DiagnosticsTrends.DDLTrends
	if len(got) != 2 || got[0].Value != 2 || got[1].Value != 9 {
		t.Fatalf("DDL trends = %+v, want series sums 2 and 9", got)
	}
}

func TestDDLTargetPhraseUsesReportLanguage(t *testing.T) {
	i18n.ResetForTesting()
	if err := i18n.Init("zh-CN"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		i18n.ResetForTesting()
		_ = i18n.Init("en")
	})
	base := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{
			Timestamp: base, EventType: "DDL", Schema: "shop", BinlogPath: "bin.000001",
			PositionStart: 100, PositionEnd: 180,
			QuerySQL: "CREATE TABLE shop.a (id INT)",
		},
		{
			Timestamp: base.Add(time.Second), EventType: "DDL", Schema: "shop", BinlogPath: "bin.000001",
			PositionStart: 200, PositionEnd: 280,
			QuerySQL: "DROP TABLE shop.b, shop.c, shop.d",
		},
	}
	result, err := New(DefaultOptions()).Analyze(events)
	if err != nil {
		t.Fatal(err)
	}
	const phrase = "4 个对象，共 2 条语句"
	text, err := report.RenderTextWithOptions(*result, report.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	md, err := report.RenderMarkdownWithOptions(*result, report.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	html, err := report.RenderHTMLWithOptions(*result, report.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{text, md, html} {
		if !strings.Contains(out, phrase) {
			t.Fatalf("missing %q\n%s", phrase, out)
		}
	}
}

func TestDigitLeadingNamesAreTables(t *testing.T) {
	cases := []struct {
		sql, table string
	}{
		{"ALTER TABLE 2fa_codes ADD COLUMN c INT", "2fa_codes"},
		{"TRUNCATE 2fa_codes", "2fa_codes"},
		{"CREATE INDEX i ON 2fa_codes (c INT)", "2fa_codes"},
		{"RENAME TABLE 2fa_codes TO x", "2fa_codes"},
		{"DROP TABLE 2fa_codes", "2fa_codes"},
		{"CREATE INDEX `ON` ON t (c INT)", "t"},
	}
	for _, tc := range cases {
		stmt, ok := ParseDDLStatement(tc.sql)
		if !ok || stmt.Table != tc.table {
			t.Errorf("%s: ok=%v table=%q, want %s", tc.sql, ok, stmt.Table, tc.table)
		}
	}
	if stmt, ok := ParseDDLStatement("ALTER TABLE 1e5 ADD COLUMN c INT"); ok && stmt.Table == "1e5" {
		t.Fatal("1e5 was read as a table")
	}
}

func TestUserStatementsKeepAccount(t *testing.T) {
	cases := []struct {
		sql, table string
	}{
		{"CREATE USER 'repl'@'%'", `'repl'@'%'`},
		{"CREATE USER IF NOT EXISTS 'repl'@'%'", `'repl'@'%'`},
		{"ALTER USER 'u'@'localhost' IDENTIFIED BY 'x'", `'u'@'localhost'`},
		{"DROP USER IF EXISTS 'u'@'localhost'", `'u'@'localhost'`},
	}
	for _, tc := range cases {
		ev, ok := DDLEventFromNormalizedEvent(model.NormalizedEvent{EventType: "QUERY", QuerySQL: tc.sql})
		if !ok || ev.Table != tc.table || ev.Object != "user" {
			t.Errorf("%s: ok=%v table=%q object=%q", tc.sql, ok, ev.Table, ev.Object)
		}
	}
	grant, ok := ParseDDLStatement("GRANT SELECT ON t TO 'repl'@'%'")
	if !ok || grant.Operation != "GRANT" || grant.Object != "privilege" || strings.Contains(grant.Table, "repl") {
		t.Fatalf("grant changed: %+v", grant)
	}
}

func TestDDLEventUsesEventDialect(t *testing.T) {
	ansi, ok := DDLEventFromNormalizedEvent(model.NormalizedEvent{
		EventType: "DDL",
		SQLMode:   4,
		QuerySQL:  `DROP TABLE "t9"`,
	})
	if !ok || ansi.Table != "t9" {
		t.Fatalf("ANSI drop: ok=%v table=%q", ok, ansi.Table)
	}
	base := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	result, err := New(DefaultOptions()).Analyze([]model.NormalizedEvent{{
		Timestamp: base, EventType: "DDL", Schema: "qa",
		ServerVersion: "8.0.36", ServerFlavor: "mysql",
		QuerySQL: "DROP TABLE a /*!50001 , t2 */",
	}})
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for _, ev := range result.Diagnostics.DDLEvents {
		tables = append(tables, ev.Table)
	}
	if len(tables) != 2 || tables[0] != "a" || tables[1] != "t2" {
		t.Fatalf("versioned drop tables = %v", tables)
	}
}

func TestFlashbackUserRefusalNamesAccount(t *testing.T) {
	opts := DefaultOptions()
	opts.Flashback = true
	a := New(opts)
	result, err := a.Analyze([]model.NormalizedEvent{{
		EventType: "DDL",
		QuerySQL:  "CREATE USER 'u'@'localhost' IDENTIFIED BY 'x'",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Diagnostics.DDLEvents) != 1 || result.Diagnostics.DDLEvents[0].Table != `'u'@'localhost'` {
		t.Fatalf("ddl event %+v", result.Diagnostics.DDLEvents)
	}
	_, err = a.Planner().Prepare()
	if err == nil {
		t.Fatal("expected a DDL refusal")
	}
	reason, table, ok := flashback.RefusalDetails(err)
	if !ok || reason != "ddl_in_range" || table != `'u'@'localhost'` {
		t.Fatalf("reason %q table %q err %v", reason, table, err)
	}
	if strings.HasPrefix(err.Error(), "selected range") {
		t.Fatalf("account missing from message: %v", err)
	}
}
