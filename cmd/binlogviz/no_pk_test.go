package binlogviz

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

func TestNoPrimaryKeyOnMySQL80FullAndMinimal(t *testing.T) {
	forceEnglishRuntimeOutput(t)

	fullPath := mustFixturePath(t, "mysql-8.0.46-no-pk-full.binlog")
	stdout, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--format", "json", "--top-transactions", "0")
	if err != nil {
		t.Fatalf("full analyze: %v\n%s", err, stderr)
	}
	full := decodePKReport(t, stdout)
	assertPKTable(t, full, "orders", "has_pk", 2, 1, 0)
	assertPKTable(t, full, "prefixed", "has_pk", 1, 1, 0)
	assertPKTable(t, full, "heap", "no_pk", 3, 2, 1)
	assertPKTable(t, full, "log", "no_pk", 2, 1, 0)
	assertPKTable(t, full, "scratch", "no_pk", 2, 0, 0)
	if full.PrimaryKeyNote != "" {
		t.Fatalf("FULL note should be empty, got %q", full.PrimaryKeyNote)
	}
	if len(full.Alerts) != 2 || full.Alerts[0].Type != "no_primary_key" || full.Alerts[1].Type != "no_primary_key" {
		t.Fatalf("alerts=%v", full.Alerts)
	}
	if full.Alerts[0].Details["table"] != "shop.heap" || full.Alerts[1].Details["table"] != "shop.log" {
		t.Fatalf("alert order=%v", full.Alerts)
	}
	for _, alert := range full.Alerts {
		if alert.Details["table"] == "shop.scratch" || alert.Details["table"] == "shop.orders" {
			t.Fatalf("insert-only or pk table alerted: %+v", alert)
		}
	}

	text, _, err := executeAnalyzeLikeMain(t, fullPath)
	if err != nil {
		t.Fatalf("full text: %v", err)
	}
	section := sectionBetween(text, "=== No Primary Key ===", "===")
	if section == "" {
		t.Fatalf("missing no-primary-key section:\n%s", text)
	}
	if strings.Index(section, "shop.heap") > strings.Index(section, "shop.log") && strings.Contains(section, "shop.log") {
		t.Fatalf("heap should rank ahead of log:\n%s", section)
	}
	if !strings.Contains(section, "shop.heap") || !strings.Contains(section, "shop.log") {
		t.Fatalf("section missing lag tables:\n%s", section)
	}
	if strings.Contains(rankedPKLines(section), "shop.scratch") || strings.Contains(rankedPKLines(section), "shop.orders") {
		t.Fatalf("insert-only or pk table was ranked:\n%s", section)
	}
	if !strings.Contains(section, "INSERT-only, not a lag risk") || !strings.Contains(section, "shop.scratch (2 INSERT)") {
		t.Fatalf("insert-only mention missing:\n%s", section)
	}
	if strings.Contains(text, "primary key presence unknown") {
		t.Fatalf("FULL text claimed key presence is unknown:\n%s", text)
	}

	md, _, err := executeAnalyzeLikeMain(t, fullPath, "--format", "markdown")
	if err != nil {
		t.Fatalf("markdown: %v", err)
	}
	if !strings.Contains(md, "## No Primary Key") || !strings.Contains(md, "shop.heap") || !strings.Contains(md, "shop.scratch (2 INSERT)") {
		t.Fatalf("markdown missing no-pk section:\n%s", md)
	}
	html, _, err := executeAnalyzeLikeMain(t, fullPath, "--format", "html", "--output", "-")
	if err != nil {
		t.Fatalf("html: %v", err)
	}
	if !strings.Contains(html, `id="no-primary-key-table"`) || !strings.Contains(html, "shop.heap") || !strings.Contains(html, "shop.scratch (2 INSERT)") {
		t.Fatal("html missing no-pk table")
	}
	if !strings.Contains(html, "no_primary_key") && !strings.Contains(html, "has no primary key") {
		t.Fatal("html missing the no-pk alert")
	}

	minimalPath := mustFixturePath(t, "mysql-8.0.46-no-pk-minimal.binlog")
	minimalOut, stderr, err := executeAnalyzeLikeMain(t, minimalPath, "--format", "json", "--top-transactions", "0")
	if err != nil {
		t.Fatalf("minimal analyze: %v\n%s", err, stderr)
	}
	minimal := decodePKReport(t, minimalOut)
	if minimal.PrimaryKeyNote != "primary key presence unknown (binlog_row_metadata is not FULL)" {
		t.Fatalf("minimal note=%q", minimal.PrimaryKeyNote)
	}
	if strings.Contains(minimalOut, `"no_pk"`) || strings.Contains(minimalOut, `"has_pk"`) {
		t.Fatalf("MINIMAL JSON guessed key presence:\n%s", minimalOut)
	}
	for _, name := range []string{"orders", "prefixed", "heap", "log", "scratch"} {
		table := findPKTable(t, minimal, name)
		if table.KeyStatus != "unknown" {
			t.Fatalf("MINIMAL %s status=%s", name, table.KeyStatus)
		}
	}
	for _, alert := range minimal.Alerts {
		if alert.Type == "no_primary_key" {
			t.Fatalf("MINIMAL raised a no-pk alert: %+v", alert)
		}
	}
	minimalText, _, err := executeAnalyzeLikeMain(t, minimalPath)
	if err != nil {
		t.Fatalf("minimal text: %v", err)
	}
	if strings.Count(minimalText, "primary key presence unknown (binlog_row_metadata is not FULL)") != 1 {
		t.Fatalf("MINIMAL text should grow by one unknown line:\n%s", minimalText)
	}
	if strings.Contains(minimalText, "=== No Primary Key ===") || strings.Contains(minimalText, "no primary key") {
		t.Fatalf("MINIMAL text claimed a missing primary key:\n%s", minimalText)
	}

	pkOnly := mustFixturePath(t, "mysql-8.0.46-dml-full.binlog")
	pkText, _, err := executeAnalyzeLikeMain(t, pkOnly)
	if err != nil {
		t.Fatalf("dml full text: %v", err)
	}
	if strings.Count(pkText, "primary key present on every table") != 1 || strings.Contains(pkText, "=== No Primary Key ===") {
		t.Fatalf("all-pk text:\n%s", pkText)
	}
	pkJSON, _, err := executeAnalyzeLikeMain(t, pkOnly, "--format", "json")
	if err != nil {
		t.Fatalf("dml full json: %v", err)
	}
	orders := findPKTable(t, decodePKReport(t, pkJSON), "orders")
	if orders.KeyStatus != "has_pk" || orders.UpdateRows == 0 || orders.DeleteRows == 0 {
		t.Fatalf("dml full orders=%+v", orders)
	}

	minDML := mustFixturePath(t, "mysql-8.0.46-dml-minimal.binlog")
	minDMLText, _, err := executeAnalyzeLikeMain(t, minDML)
	if err != nil {
		t.Fatalf("dml minimal text: %v", err)
	}
	if strings.Count(minDMLText, "primary key presence unknown (binlog_row_metadata is not FULL)") != 1 || strings.Contains(minDMLText, "=== No Primary Key ===") {
		t.Fatalf("dml minimal text:\n%s", minDMLText)
	}
}

func TestNoPrimaryKeyFiltersMatchTopTables(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	path := mustFixturePath(t, "mysql-8.0.46-no-pk-full.binlog")

	orders, _, err := executeAnalyzeLikeMain(t, path, "--include-table", "shop.orders")
	if err != nil {
		t.Fatalf("include orders: %v", err)
	}
	if strings.Contains(orders, "=== No Primary Key ===") || !strings.Contains(orders, "primary key present on every table") || strings.Contains(orders, "shop.heap") {
		t.Fatalf("include-table orders:\n%s", orders)
	}

	heap, _, err := executeAnalyzeLikeMain(t, path, "--include-table", "shop.heap", "--format", "json")
	if err != nil {
		t.Fatalf("include heap: %v", err)
	}
	heapDoc := decodePKReport(t, heap)
	if len(heapDoc.Tables) != 1 {
		t.Fatalf("include heap tables=%+v", heapDoc.Tables)
	}
	assertPKTable(t, heapDoc, "heap", "no_pk", 3, 2, 1)

	excluded, _, err := executeAnalyzeLikeMain(t, path, "--exclude-table", "shop.heap")
	if err != nil {
		t.Fatalf("exclude heap: %v", err)
	}
	section := sectionBetween(excluded, "=== No Primary Key ===", "===")
	if strings.Contains(section, "shop.heap") || !strings.Contains(rankedPKLines(section), "shop.log") || !strings.Contains(section, "shop.scratch") {
		t.Fatalf("exclude heap section:\n%s", section)
	}

	schema, stderr, err := executeAnalyzeLikeMain(t, path, "--include-schema", "other")
	if err == nil || ExitCode(err) != 2 || schema != "" {
		t.Fatalf("include other schema exit=%d stdout=%q err=%v\n%s", ExitCode(err), schema, err, stderr)
	}

	deletes, _, err := executeAnalyzeLikeMain(t, path, "--dml", "delete", "--format", "json")
	if err != nil {
		t.Fatalf("dml delete: %v", err)
	}
	deleteDoc := decodePKReport(t, deletes)
	assertPKTable(t, deleteDoc, "heap", "no_pk", 0, 0, 1)
	for _, table := range deleteDoc.Tables {
		if table.InsertRows != 0 || table.UpdateRows != 0 || (table.DeleteRows > 0 && table.Table != "heap") {
			t.Fatalf("delete filter table = %+v", table)
		}
	}

	inserts, _, err := executeAnalyzeLikeMain(t, path, "--dml", "insert")
	if err != nil {
		t.Fatalf("dml insert: %v", err)
	}
	if strings.Contains(inserts, "=== No Primary Key ===") {
		t.Fatalf("insert filter ranked a lag risk:\n%s", inserts)
	}
	if !strings.Contains(inserts, "not a replica lag risk") || !strings.Contains(inserts, "shop.scratch") || !strings.Contains(inserts, "shop.heap") {
		t.Fatalf("insert filter should mention no-pk tables without ranking them:\n%s", inserts)
	}

	window, stderr, err := executeAnalyzeLikeMain(t, path, "--end", "2020-01-01T00:00:00Z")
	if err == nil || ExitCode(err) != 2 || window != "" {
		t.Fatalf("time miss exit=%d stdout=%q err=%v\n%s", ExitCode(err), window, err, stderr)
	}

	base, _, err := executeAnalyzeLikeMain(t, path, "--format", "json", "--top-transactions", "0")
	if err != nil {
		t.Fatalf("base json: %v", err)
	}
	baseDoc := decodePKReport(t, base)
	var scratchGTID string
	var deletePos int64
	for _, txn := range baseDoc.Transactions {
		if txn.Tables["shop.scratch"] > 0 && scratchGTID == "" {
			scratchGTID = txn.GTID
		}
		if txn.Operations["DELETE"] > 0 && txn.Tables["shop.heap"] > 0 && txn.PosStart > deletePos {
			deletePos = txn.PosStart
		}
	}
	if scratchGTID == "" || deletePos == 0 {
		t.Fatalf("missing scratch gtid or heap delete position: %+v", baseDoc.Transactions)
	}
	withoutScratch, _, err := executeAnalyzeLikeMain(t, path, "--exclude-gtids", scratchGTID)
	if err != nil {
		t.Fatalf("exclude gtid: %v", err)
	}
	filteredSection := sectionBetween(withoutScratch, "=== No Primary Key ===", "===")
	if strings.Contains(filteredSection, "shop.scratch") || strings.Contains(withoutScratch, "shop.scratch (2 INSERT)") || !strings.Contains(filteredSection, "shop.heap") {
		t.Fatalf("gtid filter:\n%s", withoutScratch)
	}
	stopped, _, err := executeAnalyzeLikeMain(t, path, "--stop-position", strconv.FormatInt(deletePos, 10), "--format", "json")
	if err != nil {
		t.Fatalf("stop position: %v", err)
	}
	stoppedDoc := decodePKReport(t, stopped)
	stoppedHeap := findPKTable(t, stoppedDoc, "heap")
	if stoppedHeap.KeyStatus != "no_pk" || stoppedHeap.DeleteRows != 0 || stoppedHeap.UpdateRows != 2 {
		t.Fatalf("stop before delete heap=%+v", stoppedHeap)
	}
}

type pkReport struct {
	PrimaryKeyNote string    `json:"primary_key_note"`
	Tables         []pkTable `json:"tables"`
	Alerts         []pkAlert `json:"alerts"`
	Transactions   []pkTxn   `json:"transactions"`
}

type pkTable struct {
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	InsertRows int    `json:"insert_rows"`
	UpdateRows int    `json:"update_rows"`
	DeleteRows int    `json:"delete_rows"`
	KeyStatus  string `json:"key_status"`
}

type pkAlert struct {
	Type    string         `json:"type"`
	Details map[string]any `json:"details"`
}

type pkTxn struct {
	GTID       string         `json:"gtid"`
	PosStart   int64          `json:"pos_start"`
	Tables     map[string]int `json:"tables"`
	Operations map[string]int `json:"operations"`
}

func decodePKReport(t *testing.T, stdout string) pkReport {
	t.Helper()
	var doc pkReport
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("json: %v\n%s", err, stdout)
	}
	return doc
}

func assertPKTable(t *testing.T, doc pkReport, table, status string, insert, update, delete int) {
	t.Helper()
	got := findPKTable(t, doc, table)
	if got.KeyStatus != status || got.InsertRows != insert || got.UpdateRows != update || got.DeleteRows != delete {
		t.Fatalf("%s = %+v, want status=%s insert=%d update=%d delete=%d", table, got, status, insert, update, delete)
	}
}

func findPKTable(t *testing.T, doc pkReport, table string) pkTable {
	t.Helper()
	for _, item := range doc.Tables {
		if item.Table == table {
			return item
		}
	}
	t.Fatalf("missing table %s in %+v", table, doc.Tables)
	return pkTable{}
}

func sectionBetween(text, start, nextHeading string) string {
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

func rankedPKLines(section string) string {
	var lines []string
	for _, line := range strings.Split(section, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] != "INSERT-only," && fields[0] != "UPDATE/DELETE" && fields[0] != "#" && fields[0] != "Table" {
			if _, err := strconv.Atoi(fields[0]); err == nil {
				lines = append(lines, line)
			}
		}
	}
	return strings.Join(lines, "\n")
}
