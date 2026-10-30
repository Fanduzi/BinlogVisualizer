package flashback_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/model"
	"binlogviz/internal/splitout"
)

// splitPlan stages events through the analyzer like the command does, then
// writes the split file set into a published sink. It returns the planner
// and the file contents in manifest order.
func splitPlan(t *testing.T, spec splitout.Spec, events ...model.NormalizedEvent) (*analyzer.Analyzer, []string, []string) {
	t.Helper()
	a := analyzer.New(analyzer.Options{Flashback: true})
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	spec.Dir = filepath.Join(root, "out")
	plan := a.Planner()
	if err := plan.PrepareSplit(spec); err != nil {
		t.Fatal(err)
	}
	sink, err := splitout.Open(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Abort()
	if _, err := plan.WriteSplit(sink, spec); err != nil {
		t.Fatal(err)
	}
	if err := sink.Publish(); err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, name := range sink.Manifest() {
		body, err := os.ReadFile(filepath.Join(spec.Dir, name))
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, string(body))
	}
	return a, sink.Manifest(), files
}

var splitBase = time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)

// splitTxn stages one committed transaction touching table with op rows —
// the undo for a DELETE row is an INSERT, and vice versa.
func splitTxn(key, gtid, table, op string, id int64, pos int64) []model.NormalizedEvent {
	row := model.FlashRow{Schema: "shop", Table: table, Op: op, Columns: []string{"id"}, PK: []int{0}}
	switch op {
	case "DELETE":
		row.Before = []string{"1"}
	case "INSERT":
		row.After = []string{"1"}
	case "UPDATE":
		row.Before, row.After = []string{"1"}, []string{"1"}
	}
	_ = id
	return []model.NormalizedEvent{
		{Timestamp: splitBase, EventType: "GTID", GTID: gtid, TxnKey: key, BinlogPath: "mysql-bin.000001", PositionStart: pos},
		{Timestamp: splitBase, EventType: "BEGIN", TxnKey: key, BinlogPath: "mysql-bin.000001", PositionStart: pos + 1},
		{Timestamp: splitBase, EventType: "ROWS", Operation: op, Schema: "shop", Table: table, TxnKey: key, TxnGTID: gtid, BinlogPath: "mysql-bin.000001", PositionStart: pos + 2, FlashRows: []model.FlashRow{row}},
		{Timestamp: splitBase, EventType: "XID", TxnKey: key, BinlogPath: "mysql-bin.000001", PositionStart: pos + 3},
	}
}

func sealed(t *testing.T, body string) {
	t.Helper()
	for _, want := range []string{"binlogviz-trailer-v1", "@binlogviz_sealed", "-- binlogviz split file:"} {
		if !strings.Contains(body, want) {
			t.Fatalf("file missing %q:\n%s", want, body)
		}
	}
}

func TestSplitPerTableSealsEveryFile(t *testing.T) {
	// Each per-table file is a complete sealed script: its own digest,
	// procedure, XA, and trailer — verify applies to it unchanged (ADR 0025).
	var events []model.NormalizedEvent
	events = append(events, splitTxn("k1", "uuid:1", "wide", "DELETE", 1, 100)...)
	events = append(events, splitTxn("k2", "uuid:2", "gen", "DELETE", 2, 200)...)
	_, names, files := splitPlan(t, splitout.Spec{PerTable: true}, events...)
	if len(files) != 2 || len(names) != 2 {
		t.Fatalf("files = %d names = %v", len(files), names)
	}
	// Backward-first-seen order: the newer transaction's table publishes first.
	if names[0] != "shop.gen.sql" || names[1] != "shop.wide.sql" {
		t.Fatalf("manifest order = %v", names)
	}
	for i, f := range files {
		sealed(t, f)
		if !scriptHas(f, "INSERT INTO `shop`.`") {
			t.Fatalf("file %d has no undo insert", i)
		}
	}
	if !scriptHas(files[0], "`gen`") || strings.Contains(files[0], "`shop`.`wide`") {
		t.Fatalf("gen file content wrong:\n%s", files[0])
	}
	if !scriptHas(files[1], "`wide`") || strings.Contains(files[1], "`shop`.`gen`") {
		t.Fatalf("wide file content wrong:\n%s", files[1])
	}
}

func TestSplitCountChunksAndWarns(t *testing.T) {
	// One transaction's undo can only land in consecutive files under
	// --max-statements-per-file; the warning names it (ADR 0025).
	var events []model.NormalizedEvent
	events = append(events,
		model.NormalizedEvent{Timestamp: splitBase, EventType: "GTID", GTID: "uuid:9", TxnKey: "k1", BinlogPath: "mysql-bin.000001", PositionStart: 100},
		model.NormalizedEvent{Timestamp: splitBase, EventType: "BEGIN", TxnKey: "k1", BinlogPath: "mysql-bin.000001", PositionStart: 110},
		model.NormalizedEvent{Timestamp: splitBase, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "wide", TxnKey: "k1", TxnGTID: "uuid:9", BinlogPath: "mysql-bin.000001", PositionStart: 120, FlashRows: []model.FlashRow{
			{Schema: "shop", Table: "wide", Op: "DELETE", Columns: []string{"id"}, Before: []string{"1"}, PK: []int{0}},
			{Schema: "shop", Table: "wide", Op: "DELETE", Columns: []string{"id"}, Before: []string{"2"}, PK: []int{0}},
		}},
		model.NormalizedEvent{Timestamp: splitBase, EventType: "XID", TxnKey: "k1", BinlogPath: "mysql-bin.000001", PositionStart: 130})
	a, names, files := splitPlan(t, splitout.Spec{MaxStatements: 1}, events...)
	if len(files) != 2 || names[0] != "part-000001.sql" || names[1] != "part-000002.sql" {
		t.Fatalf("files = %d names = %v", len(files), names)
	}
	for _, f := range files {
		sealed(t, f)
		// Each undo statement is one E/G/T-prefixed digest payload; the
		// executable text is generated by the procedure, not repeated.
		payloads := strings.Count(f, "SET @binlogviz_sql = 'E\\n") +
			strings.Count(f, "SET @binlogviz_sql = 'G\\n") +
			strings.Count(f, "SET @binlogviz_sql = 'T\\n")
		if payloads != 1 {
			t.Fatalf("file does not hold exactly one undo statement:\n%s", f)
		}
	}
	var found bool
	for _, w := range a.Planner().Warnings() {
		if strings.Contains(w, "uuid:9") && strings.Contains(w, "spans multiple output files") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no file-span warning: %#v", a.Planner().Warnings())
	}
}

func TestSplitPerTablePlusCountNumbersEveryFile(t *testing.T) {
	// Composition numbers every file of every table, so a table whose name
	// ends in digits can never collide with another table's chunks (ADR 0025).
	var events []model.NormalizedEvent
	events = append(events,
		model.NormalizedEvent{Timestamp: splitBase, EventType: "GTID", GTID: "uuid:1", TxnKey: "k1", BinlogPath: "mysql-bin.000001", PositionStart: 100},
		model.NormalizedEvent{Timestamp: splitBase, EventType: "BEGIN", TxnKey: "k1", BinlogPath: "mysql-bin.000001", PositionStart: 110},
		model.NormalizedEvent{Timestamp: splitBase, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "wide", TxnKey: "k1", TxnGTID: "uuid:1", BinlogPath: "mysql-bin.000001", PositionStart: 120, FlashRows: []model.FlashRow{
			{Schema: "shop", Table: "wide", Op: "DELETE", Columns: []string{"id"}, Before: []string{"1"}, PK: []int{0}},
			{Schema: "shop", Table: "wide", Op: "DELETE", Columns: []string{"id"}, Before: []string{"2"}, PK: []int{0}},
		}},
		model.NormalizedEvent{Timestamp: splitBase, EventType: "XID", TxnKey: "k1", BinlogPath: "mysql-bin.000001", PositionStart: 130})
	events = append(events, splitTxn("k2", "uuid:2", "gen", "DELETE", 2, 200)...)
	_, names, files := splitPlan(t, splitout.Spec{PerTable: true, MaxStatements: 1}, events...)
	if len(files) != 3 {
		t.Fatalf("files = %d, want 3", len(files))
	}
	for i, want := range []string{"shop.gen-000001.sql", "shop.wide-000001.sql", "shop.wide-000002.sql"} {
		if names[i] != want {
			t.Fatalf("manifest = %v, want %s at %d", names, want, i)
		}
	}
	for _, f := range files {
		sealed(t, f)
	}
}
