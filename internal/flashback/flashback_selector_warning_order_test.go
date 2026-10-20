package flashback_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/model"
)

// TestFlashbackWarningOrderSameUnderGTIDSelector pins the warning order rule
// (#378 QA F1, docs/gtid-filter-streaming.md): a GTID selector does not move
// warnings. Retained rows reach the planner as they are read, so a table with
// no usable definition is listed when the planner settles it, the same as
// without a selector. An unqualified --schema-file name waits until the
// binlog shows it in a second schema, so q378_a.t2 (settled at its first row)
// comes before q378_a.t1 (settled when q378_b.t1 is read).
func TestFlashbackWarningOrderSameUnderGTIDSelector(t *testing.T) {
	const sid = "24bc7850-2c16-11e6-a073-0242ac110002"
	const create = "CREATE TABLE `t1` (\n  `id` int NOT NULL,\n  `g` int GENERATED ALWAYS AS ((`id` + 1)) VIRTUAL\n);\n"
	at := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	ev := func(typ string, pos int64) model.NormalizedEvent {
		return model.NormalizedEvent{Timestamp: at, EventType: typ, BinlogPath: "mysql-bin.000001", PositionStart: pos, PositionEnd: pos + 10, ServerFlavor: "mysql"}
	}
	rows := func(schema, table string, pos int64) model.NormalizedEvent {
		e := flashEvent(model.FlashRow{Schema: schema, Table: table, Op: "DELETE", Columns: []string{"id", "g"}, Cols: intCols(2), After: []string{"1", "2"}})
		e.PositionStart, e.PositionEnd, e.ServerFlavor = pos, pos+10, "mysql"
		return e
	}
	gtid := ev("GTID", 10)
	gtid.GTID = sid + ":1"
	events := []model.NormalizedEvent{gtid, ev("BEGIN", 20), rows("q378_a", "t1", 30), rows("q378_a", "t2", 40), rows("q378_b", "t1", 50), ev("XID", 60)}

	run := func(selector *analyzer.GTIDSelector) []string {
		a := analyzer.New(analyzer.Options{Flashback: true, SchemaSQL: create, GTIDSelector: selector})
		consumeFlash(t, a, events...)
		return a.Planner().Warnings()
	}
	plain := run(nil)
	include, err := analyzer.ParseGTIDSelector([]string{sid + ":1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	exclude, err := analyzer.ParseGTIDSelector(nil, []string{sid + ":2"})
	if err != nil {
		t.Fatal(err)
	}
	for name, sel := range map[string]*analyzer.GTIDSelector{"include": include, "exclude": exclude} {
		if got := run(sel); !reflect.DeepEqual(got, plain) {
			t.Fatalf("%s: warnings\n%s\nwant the no-selector order\n%s", name, strings.Join(got, "\n"), strings.Join(plain, "\n"))
		}
	}
	var missing []string
	for _, w := range plain {
		if i := strings.Index(w, ": no table definition"); i >= 0 {
			missing = append(missing, w[strings.LastIndex(w[:i], " ")+1:i])
		}
	}
	t.Logf("warnings:\n%s", strings.Join(plain, "\n"))
	if want := []string{"q378_a.t2", "q378_a.t1", "q378_b.t1"}; !reflect.DeepEqual(missing, want) {
		t.Fatalf("missing-definition order %v, want %v", missing, want)
	}
}
