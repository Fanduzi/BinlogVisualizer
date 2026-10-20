package binlogviz

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

// #282: --diagnostics caps analyze JSON, and a transaction alert carries its GTID.
func TestAnalyzeDiagnosticsBudgetAndAlertGTID(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-dml-full.binlog")

	full, stderr, err := executeAnalyzeLikeMain(t, path, "--format", "json", "-q")
	if err != nil {
		t.Fatalf("full: %v\n%s", err, stderr)
	}
	explicit, _, err := executeAnalyzeLikeMain(t, path, "--format", "json", "-q", "--diagnostics", "full")
	if err != nil {
		t.Fatal(err)
	}
	if explicit != full {
		t.Fatal("--diagnostics full must match the default JSON")
	}

	fullDoc := decodeObject(t, full)
	if _, ok := fullDoc["diagnostics"].(map[string]any); !ok {
		t.Fatal("default JSON must include diagnostics")
	}
	if fullDoc["report_version"].(float64) != 4 {
		t.Fatalf("report_version = %v", fullDoc["report_version"])
	}

	summary, _, err := executeAnalyzeLikeMain(t, path, "--format", "json", "-q", "--diagnostics", "summary")
	if err != nil {
		t.Fatal(err)
	}
	summaryDoc := decodeObject(t, summary)
	if summaryDoc["report_version"].(float64) != 4 {
		t.Fatalf("summary report_version = %v", summaryDoc["report_version"])
	}
	assertJSONEqualExcept(t, fullDoc, summaryDoc, "diagnostics")
	sumDiag := objectField(t, summaryDoc, "diagnostics")
	fullDiag := objectField(t, fullDoc, "diagnostics")
	for _, key := range []string{"largest_transactions", "longest_transactions", "widest_transactions", "file_segments", "hot_intervals", "findings"} {
		list, ok := sumDiag[key].([]any)
		if !ok || len(list) != 0 {
			t.Fatalf("summary %s = %#v, want []", key, sumDiag[key])
		}
	}
	for _, key := range []string{"largest_transactions", "longest_transactions", "widest_transactions", "file_segments", "hot_intervals"} {
		if fullList, _ := fullDiag[key].([]any); len(fullList) == 0 {
			t.Fatalf("fixture %s was already empty; it does not prove summary drops copies", key)
		}
	}
	if _, ok := sumDiag["largest_byte_transactions"]; ok {
		t.Fatalf("summary must omit empty largest_byte_transactions, got %#v", sumDiag["largest_byte_transactions"])
	}
	for _, key := range []string{"file_coverage", "counted_event_bytes", "input_format_guess", "ddl_events", "ignored_query_dml_events"} {
		if !sameJSON(sumDiag[key], fullDiag[key]) {
			t.Fatalf("summary %s changed: got %#v want %#v", key, sumDiag[key], fullDiag[key])
		}
	}
	if _, ok := fullDiag["ddl_events_omitted"]; ok {
		t.Fatalf("full must omit ddl_events_omitted, got %#v", fullDiag["ddl_events_omitted"])
	}
	if sumDiag["ddl_events_omitted"] != float64(0) {
		t.Fatalf("summary ddl_events_omitted = %#v, want 0 for this fixture", sumDiag["ddl_events_omitted"])
	}
	if len(summary) >= len(full) {
		t.Fatalf("summary JSON is not smaller: summary=%d full=%d", len(summary), len(full))
	}

	off, _, err := executeAnalyzeLikeMain(t, path, "--format", "json", "-q", "--diagnostics", "off")
	if err != nil {
		t.Fatal(err)
	}
	offDoc := decodeObject(t, off)
	if _, ok := offDoc["diagnostics"]; ok {
		t.Fatal("--diagnostics off must omit diagnostics")
	}
	assertJSONEqualExcept(t, fullDoc, offDoc, "diagnostics")
	if offDoc["report_version"].(float64) != 4 {
		t.Fatalf("off report_version = %v", offDoc["report_version"])
	}

	alerted, _, err := executeAnalyzeLikeMain(t, path, "--format", "json", "-q", "--large-trx-rows", "1")
	if err != nil {
		t.Fatal(err)
	}
	alertDoc := decodeObject(t, alerted)
	gtidByTxn := map[string]string{}
	for _, item := range alertDoc["transactions"].([]any) {
		txn := item.(map[string]any)
		gtid, _ := txn["gtid"].(string)
		gtidByTxn[txn["txn_key"].(string)] = gtid
	}
	var sawGTID bool
	for _, item := range alertDoc["alerts"].([]any) {
		alert := item.(map[string]any)
		if alert["type"] != "large_transaction" {
			continue
		}
		txnKey := alert["txn_key"].(string)
		message := alert["message"].(string)
		if !strings.Contains(message, txnKey) {
			t.Fatalf("message %q dropped %s", message, txnKey)
		}
		gtid := gtidByTxn[txnKey]
		if gtid == "" {
			t.Fatalf("fixture transaction %s has no gtid", txnKey)
		}
		if alert["gtid"] != gtid {
			t.Fatalf("alert gtid = %#v, want %s (%s)", alert["gtid"], gtid, txnKey)
		}
		sawGTID = true
	}
	if !sawGTID {
		t.Fatal("expected a large_transaction alert with a gtid")
	}

	noPK, _, err := executeAnalyzeLikeMain(t, mustFixturePath(t, "mysql-8.0.46-no-pk-full.binlog"), "--format", "json", "-q")
	if err != nil {
		t.Fatal(err)
	}
	noPKDoc := decodeObject(t, noPK)
	var sawNoPK bool
	for _, item := range noPKDoc["alerts"].([]any) {
		alert := item.(map[string]any)
		if alert["type"] != "no_primary_key" {
			continue
		}
		sawNoPK = true
		if _, ok := alert["gtid"]; ok {
			t.Fatalf("no_primary_key alert must omit gtid: %#v", alert)
		}
		if _, ok := alert["txn_key"]; ok {
			t.Fatalf("no_primary_key alert must omit txn_key: %#v", alert)
		}
	}
	if !sawNoPK {
		t.Fatal("expected a no_primary_key alert")
	}
}

func TestAnalyzeDiagnosticsFlagLeavesHumanReportsUnchanged(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-dml-full.binlog")
	for _, format := range []string{"text", "markdown", "html"} {
		args := []string{path, "--format", format, "-q", "--large-trx-rows", "1"}
		if format == "html" {
			args = append(args, "--output", "-")
		}
		base, stderr, err := executeAnalyzeLikeMain(t, args...)
		if err != nil {
			t.Fatalf("%s: %v\n%s", format, err, stderr)
		}
		for _, mode := range []string{"full", "summary", "off"} {
			got, _, err := executeAnalyzeLikeMain(t, append(append([]string{}, args...), "--diagnostics", mode)...)
			if err != nil {
				t.Fatalf("%s %s: %v", format, mode, err)
			}
			if stripGeneratedAt(got) != stripGeneratedAt(base) {
				t.Fatalf("--diagnostics %s changed %s output", mode, format)
			}
		}
	}
}

func TestAnalyzeDiagnosticsFlagSameGTIDInZhCN(t *testing.T) {
	path := mustFixturePath(t, "mysql-8.0.46-dml-full.binlog")
	en := alertGTIDs(t, "en", path)
	zh := alertGTIDs(t, "zh-CN", path)
	if len(en) == 0 || len(en) != len(zh) {
		t.Fatalf("en=%v zh=%v", en, zh)
	}
	for i := range en {
		if en[i].gtid != zh[i].gtid || en[i].txnKey != zh[i].txnKey {
			t.Fatalf("identity changed with language: en=%+v zh=%+v", en[i], zh[i])
		}
		if en[i].message == zh[i].message {
			t.Fatalf("message was not translated: %q", en[i].message)
		}
		if !strings.Contains(zh[i].message, zh[i].txnKey) {
			t.Fatalf("zh message %q dropped %s", zh[i].message, zh[i].txnKey)
		}
	}
}

func TestAnalyzeOpenDMLAlertCarriesGTID(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fixture := openBeginDMLCommandPrefix(t, mustFixturePath(t, "mysql-8.0.46-open-begin-dml.binlog"))
	stdout, stderr, err := executeAnalyzeLikeMain(t, fixture, "--format", "json", "-q")
	if err != nil {
		t.Fatalf("prefix: %v\n%s", err, stderr)
	}
	doc := decodeObject(t, stdout)
	group := objectField(t, doc, "diagnostics")["open_dml_groups"].([]any)[0].(map[string]any)
	var matched bool
	for _, item := range doc["alerts"].([]any) {
		alert := item.(map[string]any)
		if alert["type"] != "open_dml_group" {
			continue
		}
		if alert["gtid"] != group["gtid"] || alert["txn_key"] == "" {
			t.Fatalf("open_dml alert = %#v, group gtid = %#v", alert, group["gtid"])
		}
		matched = true
	}
	if !matched {
		t.Fatal("expected an open_dml_group alert")
	}
}

func TestAnalyzeRejectsBadDiagnosticsMode(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, stdout, stderr := runRoot(t, "analyze", "dummy.binlog", "--diagnostics", "nope")
	if code != 3 {
		t.Fatalf("exit %d, want 3; stdout=%q stderr=%s", code, stdout, stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q", stdout)
	}
	if !strings.Contains(stderr, `invalid --diagnostics "nope" (allowed: full, summary, off)`) {
		t.Fatalf("stderr = %s", stderr)
	}
}

func TestAnalyzeRejectsExplicitEmptyDiagnostics(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	code, stdout, stderr := runRoot(t, "analyze", "dummy.binlog", "--diagnostics", "")
	if code != 3 {
		t.Fatalf("exit %d, want 3; stdout=%q stderr=%s", code, stdout, stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout = %q", stdout)
	}
	if !strings.Contains(stderr, `invalid --diagnostics "" (allowed: full, summary, off)`) {
		t.Fatalf("stderr = %s", stderr)
	}
}

type alertIdentity struct {
	txnKey  string
	gtid    string
	message string
}

func alertGTIDs(t *testing.T, lang, path string) []alertIdentity {
	t.Helper()
	t.Cleanup(func() { langFlag = "" })
	stdout, stderr, err := executeAnalyzeLikeMain(t, "--lang", lang, path, "--format", "json", "-q", "--large-trx-rows", "1")
	if err != nil {
		t.Fatalf("%s: %v\n%s", lang, err, stderr)
	}
	doc := decodeObject(t, stdout)
	var out []alertIdentity
	for _, item := range doc["alerts"].([]any) {
		alert := item.(map[string]any)
		if alert["type"] != "large_transaction" {
			continue
		}
		out = append(out, alertIdentity{
			txnKey:  alert["txn_key"].(string),
			gtid:    alert["gtid"].(string),
			message: alert["message"].(string),
		})
	}
	return out
}

func decodeObject(t *testing.T, raw string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("json: %v\n%s", err, raw)
	}
	return doc
}

func objectField(t *testing.T, doc map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := doc[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %#v", key, doc[key])
	}
	return value
}

func assertJSONEqualExcept(t *testing.T, left, right map[string]any, skip string) {
	t.Helper()
	for key, value := range left {
		if key == skip {
			continue
		}
		if !sameJSON(value, right[key]) {
			t.Fatalf("%s changed under --diagnostics", key)
		}
	}
}

func sameJSON(left, right any) bool {
	lb, _ := json.Marshal(left)
	rb, _ := json.Marshal(right)
	return string(lb) == string(rb)
}

var generatedAtPattern = regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC`)

func stripGeneratedAt(s string) string {
	return generatedAtPattern.ReplaceAllString(s, "TIME")
}
