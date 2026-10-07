package analyzer

import (
	"strings"
	"testing"
	"time"

	"binlogviz/internal/model"
)

func TestRenderFlashbackReversesTransactionsAndRows(t *testing.T) {
	sql, err := renderFlashbackSQL([]flashGroup{
		{
			gtid: "aaaa:1",
			path: "/var/lib/mysql/mysql-bin.000123",
			pos:  100,
			rows: []model.FlashRow{
				{Schema: "shop", Table: "wide", Op: "INSERT", Columns: []string{"id"}, After: []string{"1"}, PK: []int{0}},
				{Schema: "shop", Table: "wide", Op: "DELETE", Columns: []string{"id"}, Before: []string{"2"}, PK: []int{0}},
			},
		},
		{
			gtid: "",
			path: "mysql-bin.000123",
			pos:  400,
			rows: []model.FlashRow{
				{Schema: "shop", Table: "heap", Op: "INSERT", Columns: []string{"id", "note"}, After: []string{"3", "'a'"}, NoPK: true},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	second := strings.Index(sql, "-- gtid: aaaa:1")
	first := strings.Index(sql, "-- gtid: GTID unavailable")
	if first < 0 || second < 0 || first > second {
		t.Fatalf("transaction order:\n%s", sql)
	}
	if !strings.Contains(sql, "-- binlog: mysql-bin.000123:400") || !strings.Contains(sql, "-- binlog: mysql-bin.000123:100") {
		t.Fatalf("positions:\n%s", sql)
	}
	undoDelete := strings.Index(sql, "INSERT INTO `shop`.`wide` (`id`) VALUES (2);")
	undoInsert := strings.Index(sql, "DELETE FROM `shop`.`wide` WHERE `id` <=> 1;")
	if undoDelete < 0 || undoInsert < 0 || undoDelete > undoInsert {
		t.Fatalf("row order:\n%s", sql)
	}
	if !strings.Contains(sql, "-- no primary key on shop.heap; this matches every column and LIMIT 1") || !strings.Contains(sql, "DELETE FROM `shop`.`heap` WHERE `id` <=> 3 AND `note` <=> 'a' LIMIT 1;") {
		t.Fatalf("no pk:\n%s", sql)
	}
	if !strings.Contains(sql, "SET time_zone = '+00:00';") || !strings.Contains(sql, "SET NAMES utf8mb4;") {
		t.Fatalf("preamble:\n%s", sql)
	}
}

func TestGeneratedColumnsOmittedOrRefused(t *testing.T) {
	const create = "CREATE TABLE shop.gen (id INT PRIMARY KEY, base INT, virt INT AS (base + 1) VIRTUAL, stor INT AS (base * 2) STORED)"
	var learned generatedTables
	learned.note("shop", create)
	cols := learned.columns("shop", "gen")
	if _, ok := cols["virt"]; !ok || len(cols) != 2 {
		t.Fatalf("generated: %#v", cols)
	}
	if _, ok := cols["stor"]; !ok {
		t.Fatalf("stored: %#v", cols)
	}

	deleted := model.FlashRow{
		Schema: "shop", Table: "gen", Op: "DELETE",
		Columns: []string{"id", "base", "virt", "stor"},
		Before:  []string{"2", "20", "21", "40"},
		PK:      []int{0},
	}
	sql, err := renderUndoStatement(deleted, cols)
	if err != nil {
		t.Fatal(err)
	}
	if sql != "INSERT INTO `shop`.`gen` (`id`, `base`) VALUES (2, 20);" {
		t.Fatalf("insert: %s", sql)
	}

	updated := model.FlashRow{
		Schema: "shop", Table: "gen", Op: "UPDATE",
		Columns: []string{"id", "base", "Virt", "stor"},
		Before:  []string{"1", "10", "11", "20"},
		After:   []string{"1", "11", "12", "22"},
		PK:      []int{0},
	}
	sql, err = renderUndoStatement(updated, cols)
	if err != nil {
		t.Fatal(err)
	}
	if sql != "UPDATE `shop`.`gen` SET `id` = 1, `base` = 10 WHERE `id` <=> 1;" {
		t.Fatalf("update: %s", sql)
	}

	heap := model.FlashRow{
		Schema: "shop", Table: "heap", Op: "UPDATE", NoPK: true,
		Columns: []string{"id", "virt", "note"},
		Before:  []string{"1", "2", "'a'"},
		After:   []string{"1", "3", "'b'"},
	}
	sql, err = renderUndoStatement(heap, map[string]struct{}{"virt": {}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "matches every non-generated column") || strings.Contains(sql, "`virt`") {
		t.Fatalf("no-pk where: %s", sql)
	}
	if !strings.Contains(sql, "UPDATE `shop`.`heap` SET `id` = 1, `note` = 'a' WHERE `id` <=> 1 AND `note` <=> 'b' LIMIT 1;") {
		t.Fatalf("no-pk sql: %s", sql)
	}

	keyed := model.FlashRow{
		Schema: "shop", Table: "g", Op: "INSERT",
		Columns: []string{"g", "base"},
		After:   []string{"11", "10"},
		PK:      []int{0},
	}
	sql, err = renderUndoStatement(keyed, map[string]struct{}{"g": {}})
	if err != nil {
		t.Fatal(err)
	}
	if sql != "DELETE FROM `shop`.`g` WHERE `g` <=> 11;" {
		t.Fatalf("generated pk: %s", sql)
	}

	onlyGen := model.FlashRow{
		Schema: "shop", Table: "gen", Op: "DELETE",
		Columns: []string{"virt"}, Before: []string{"1"}, PK: []int{0},
	}
	if _, err = renderUndoStatement(onlyGen, cols); err == nil || !strings.Contains(err.Error(), "shop.gen: cannot undo a row whose columns are all generated") {
		t.Fatalf("all generated: %v", err)
	}

	var bad generatedTables
	bad.note("shop", "CREATE TABLE shop.t AS SELECT 1")
	bad.note("shop", "CREATE TABLE shop.copied LIKE shop.missing")
	bad.note("shop", "ALTER TABLE shop.fresh ADD COLUMN g INT AS (id + 1) VIRTUAL")
	for _, table := range []string{"t", "copied", "fresh"} {
		if !bad.unknown("shop", table) {
			t.Fatalf("%s should be unknown", table)
		}
	}

	learned.note("shop", "ALTER TABLE shop.gen ADD COLUMN extra INT AS (base + 2) VIRTUAL")
	learned.note("shop", "CREATE TABLE shop.gen2 LIKE shop.gen")
	if _, ok := learned.columns("shop", "gen2")["extra"]; !ok {
		t.Fatalf("like: %#v", learned.columns("shop", "gen2"))
	}
	learned.note("shop", "ALTER TABLE `shop`.`gen` DROP COLUMN virt")
	if _, ok := learned.columns("shop", "gen")["virt"]; ok {
		t.Fatal("drop column kept virt")
	}
	learned.note("shop", "DROP TABLE IF EXISTS shop.gen, shop.gen2")
	if learned.columns("shop", "gen") != nil || learned.unknown("shop", "gen") {
		t.Fatal("drop table")
	}

	var always generatedTables
	always.note("shop", "CREATE TABLE shop.g (id INT PRIMARY KEY, v INT GENERATED ALWAYS AS ((id + 1)) STORED)")
	if _, ok := always.columns("shop", "g")["v"]; !ok {
		t.Fatalf("generated always: %#v", always.columns("shop", "g"))
	}
}

func TestFlashbackLearnsExcludedCreateAndWarnsOnSplit(t *testing.T) {
	const gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	selector, err := ParseGTIDSelector(nil, []string{gtid + ":1"})
	if err != nil {
		t.Fatal(err)
	}
	a := New(Options{
		Flashback:     true,
		IncludeTables: []string{"shop.gen"},
		GTIDSelector:  selector,
	})
	base := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	events := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid + ":1", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: base, EventType: "DDL", Schema: "shop", QuerySQL: "CREATE TABLE shop.gen (id INT PRIMARY KEY, base INT, virt INT AS (base + 1) VIRTUAL, stor INT AS (base * 2) STORED)", BinlogPath: "mysql-bin.000001", PositionStart: 140, PositionEnd: 200},
		{Timestamp: base.Add(time.Second), EventType: "GTID", GTID: gtid + ":2", BinlogPath: "mysql-bin.000001", PositionStart: 200, PositionEnd: 240},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 240, PositionEnd: 260},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "wide", BinlogPath: "mysql-bin.000001", PositionStart: 260, PositionEnd: 300, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "wide", Op: "DELETE", Columns: []string{"id"}, Before: []string{"1"}, PK: []int{0},
		}}},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "gen", BinlogPath: "mysql-bin.000001", PositionStart: 300, PositionEnd: 360, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "gen", Op: "DELETE", Columns: []string{"id", "base", "virt", "stor"}, Before: []string{"2", "20", "21", "40"}, PK: []int{0},
		}}},
		{Timestamp: base.Add(time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 360, PositionEnd: 400},
	}
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err := a.FlashbackSQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`gen` (`id`, `base`) VALUES (2, 20);") || strings.Contains(sql, "virt") || strings.Contains(sql, "shop`.`wide") {
		t.Fatalf("sql:\n%s", sql)
	}
	warnings := a.FlashbackWarnings()
	if len(warnings) != 1 || !strings.Contains(warnings[0], gtid+":2") || !strings.Contains(warnings[0], "only partly undone") {
		t.Fatalf("warnings: %#v", warnings)
	}

	dml := New(Options{Flashback: true, IncludeDML: []string{"DELETE"}})
	dmlEvents := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid + ":1", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: base, EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 140, PositionEnd: 160},
		{Timestamp: base, EventType: "ROWS", Operation: "UPDATE", Schema: "shop", Table: "wide", BinlogPath: "mysql-bin.000001", PositionStart: 160, PositionEnd: 200, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "wide", Op: "UPDATE", Columns: []string{"id", "note"}, Before: []string{"1", "'a'"}, After: []string{"1", "'b'"}, PK: []int{0},
		}}},
		{Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "wide", BinlogPath: "mysql-bin.000001", PositionStart: 200, PositionEnd: 240, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "wide", Op: "DELETE", Columns: []string{"id", "note"}, Before: []string{"1", "'a'"}, PK: []int{0},
		}}},
		{Timestamp: base, EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 240, PositionEnd: 280},
	}
	for _, ev := range dmlEvents {
		if err := dml.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := dml.Finalize(); err != nil {
		t.Fatal(err)
	}
	if _, err := dml.FlashbackSQL(); err != nil {
		t.Fatal(err)
	}
	if warnings = dml.FlashbackWarnings(); len(warnings) != 1 {
		t.Fatalf("dml warnings: %#v", warnings)
	}

	plain := New(Options{Flashback: true})
	row := model.NormalizedEvent{
		Timestamp: base, EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "gen",
		BinlogPath: "mysql-bin.000001", PositionStart: 10, PositionEnd: 20,
		FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "gen", Op: "DELETE", Columns: []string{"id", "virt"}, Before: []string{"1", "2"}, PK: []int{0},
		}},
	}
	if err := plain.Consume(row); err != nil {
		t.Fatal(err)
	}
	if _, err := plain.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err = plain.FlashbackSQL()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "`virt`") {
		t.Fatalf("missing create should still emit the column:\n%s", sql)
	}

	refused := New(Options{Flashback: true, GTIDSelector: selector})
	badEvents := []model.NormalizedEvent{
		{Timestamp: base, EventType: "GTID", GTID: gtid + ":1", BinlogPath: "mysql-bin.000001", PositionStart: 100, PositionEnd: 140},
		{Timestamp: base, EventType: "DDL", Schema: "shop", QuerySQL: "CREATE TABLE shop.t AS SELECT 1", BinlogPath: "mysql-bin.000001", PositionStart: 140, PositionEnd: 180},
		{Timestamp: base.Add(time.Second), EventType: "GTID", GTID: gtid + ":2", BinlogPath: "mysql-bin.000001", PositionStart: 180, PositionEnd: 220},
		{Timestamp: base.Add(time.Second), EventType: "BEGIN", BinlogPath: "mysql-bin.000001", PositionStart: 220, PositionEnd: 240},
		{Timestamp: base.Add(time.Second), EventType: "ROWS", Operation: "DELETE", Schema: "shop", Table: "t", BinlogPath: "mysql-bin.000001", PositionStart: 240, PositionEnd: 280, FlashRows: []model.FlashRow{{
			Schema: "shop", Table: "t", Op: "DELETE", Columns: []string{"id"}, Before: []string{"1"}, PK: []int{0},
		}}},
		{Timestamp: base.Add(time.Second), EventType: "XID", BinlogPath: "mysql-bin.000001", PositionStart: 280, PositionEnd: 300},
	}
	for _, ev := range badEvents {
		if err := refused.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := refused.Finalize(); err != nil {
		t.Fatal(err)
	}
	sql, err = refused.FlashbackSQL()
	if err == nil || sql != "" || !strings.Contains(err.Error(), "shop.t: generated columns are unknown") {
		t.Fatalf("sql %q err %v", sql, err)
	}
}
