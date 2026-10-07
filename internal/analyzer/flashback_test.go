package analyzer

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

func TestRenderFlashbackReversesTransactionsAndRows(t *testing.T) {
	sql := renderFlashbackSQL([]flashGroup{
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
