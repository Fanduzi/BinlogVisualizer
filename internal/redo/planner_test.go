package redo

import (
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"binlogviz/internal/model"
)

// intCol/varcharCol are the minimal column facts EmitLiteral needs for the
// two most common types in fixtures (ADR 0022: missing facts refuse, they do
// not guess).
func intCol() model.FlashCol {
	return model.FlashCol{Typ: mysql.MYSQL_TYPE_LONG, HasSign: true, Base: "int"}
}

func varcharCol() model.FlashCol {
	return model.FlashCol{Typ: mysql.MYSQL_TYPE_VAR_STRING, Charset: "utf8mb4", Base: "varchar"}
}

func insertRow() model.RedoRow {
	return model.RedoRow{
		Schema: "shop", Table: "orders", Op: "INSERT",
		Columns: []string{"id", "note"}, Names: model.RowNamesFull,
		Cols:  []model.FlashCol{intCol(), varcharCol()},
		After: []any{int64(1), "a"},
		PK:    []int{0},
	}
}

func ev(etype, key, gtid string, rows ...model.RedoRow) model.NormalizedEvent {
	return model.NormalizedEvent{
		Timestamp: time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC),
		EventType: etype, Schema: "shop", Table: "orders",
		TxnKey: key, TxnGTID: gtid, GTID: gtid,
		RedoRows: rows,
	}
}

// plan drives the planner the same way the analyzer does: Note per retained
// event in stream order, Prepare, then WriteTo (ADR 0022: statements are
// staged until planning completes, so a refusal can never leave a partial
// script behind).
func plan(t *testing.T, opts Options, events ...model.NormalizedEvent) (*Planner, string, error) {
	t.Helper()
	p := New(opts)
	t.Cleanup(p.Close)
	for i := range events {
		p.Note(events[i])
	}
	if _, err := p.Prepare(); err != nil {
		return p, "", err
	}
	var out strings.Builder
	if _, err := p.WriteTo(&out); err != nil {
		return p, out.String(), err
	}
	return p, out.String(), nil
}

// sqlLines returns the emitted script without the session preamble.
func sqlLines(t *testing.T, sql string) []string {
	t.Helper()
	var lines []string
	for _, line := range strings.Split(sql, "\n") {
		if strings.HasPrefix(line, "SET ") {
			continue
		}
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestInsertRowsEmitInsideLoggedGroup(t *testing.T) {
	// A committed group mirrors its logged boundaries: BEGIN before rows,
	// COMMIT at XID — never synthesized around it (ADR 0022).
	_, sql, err := plan(t, Options{},
		ev("GTID", "k1", "uuid:1"),
		ev("BEGIN", "k1", "uuid:1"),
		ev("ROWS", "k1", "uuid:1", insertRow()),
		ev("XID", "k1", "uuid:1"))
	if err != nil {
		t.Fatal(err)
	}
	lines := sqlLines(t, sql)
	want := []string{
		"-- gtid: uuid:1",
		"BEGIN;",
		"INSERT INTO `shop`.`orders` (`id`, `note`) VALUES (1, 'a');",
		"COMMIT;",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("script:\n%s", sql)
	}
}

func TestImplicitCommitWorkIsNotWrapped(t *testing.T) {
	// DDL groups never logged BEGIN: emitting one would fabricate a
	// transaction the source stream does not have (ADR 0022).
	_, sql, err := plan(t, Options{},
		ev("GTID", "k1", "uuid:1"),
		model.NormalizedEvent{EventType: "DDL", TxnKey: "k1", TxnGTID: "uuid:1",
			Schema: "shop", QuerySchema: "shop", QuerySQL: "CREATE TABLE t (id int)"})
	if err != nil {
		t.Fatal(err)
	}
	lines := sqlLines(t, sql)
	for _, line := range lines {
		if line == "BEGIN;" || line == "COMMIT;" {
			t.Fatalf("DDL group wrapped in a fabricated transaction:\n%s", sql)
		}
	}
	if lines[len(lines)-1] != "CREATE TABLE t (id int);" {
		t.Fatalf("DDL not emitted verbatim:\n%s", sql)
	}
}

func TestUncommittedGroupGetsTrailerAndWarning(t *testing.T) {
	// Input cut mid-transaction still needs a replayable tail: trailer
	// COMMIT plus the cut warning (ADR 0022).
	p, sql, err := plan(t, Options{},
		ev("BEGIN", "k1", "uuid:2"),
		ev("ROWS", "k1", "uuid:2", insertRow()))
	if err != nil {
		t.Fatal(err)
	}
	lines := sqlLines(t, sql)
	if lines[len(lines)-1] != "COMMIT;" {
		t.Fatalf("open group missing trailer COMMIT:\n%s", sql)
	}
	var found bool
	for _, w := range p.Warnings() {
		if strings.Contains(w, "still open") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no cut-transaction warning: %#v", p.Warnings())
	}
}

func TestDeleteWithoutPrimaryKeyLimitsOneRow(t *testing.T) {
	// No PK: every before-image column matches with <=> and LIMIT 1 so one
	// logged row deletes exactly one stored row (ADR 0022).
	row := insertRow()
	row.Op = "DELETE"
	row.Before, row.After = row.After, nil
	row.PK, row.NoPK = nil, true
	_, sql, err := plan(t, Options{},
		ev("BEGIN", "k1", ""),
		ev("ROWS", "k1", "", row),
		ev("XID", "k1", ""))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "DELETE FROM `shop`.`orders` WHERE `id` <=> 1 AND `note` <=> 'a' LIMIT 1;") {
		t.Fatalf("no-PK delete:\n%s", sql)
	}
	if !strings.Contains(sql, "no primary key") {
		t.Fatalf("missing no-PK comment:\n%s", sql)
	}
}

func TestUpdateMatchesPrimaryKeyColumns(t *testing.T) {
	// UPDATE replays the full after image and locates the row by its PK
	// before image (ADR 0022).
	row := insertRow()
	row.Op = "UPDATE"
	row.Before = []any{int64(1), "a"}
	row.After = []any{int64(1), "b"}
	_, sql, err := plan(t, Options{},
		ev("BEGIN", "k1", ""),
		ev("ROWS", "k1", "", row),
		ev("XID", "k1", ""))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "UPDATE `shop`.`orders` SET `id` = 1, `note` = 'b' WHERE `id` <=> 1;") {
		t.Fatalf("update:\n%s", sql)
	}
}

func TestSplitSelectionWarns(t *testing.T) {
	// Keeping half of a group makes replay diverge from the source
	// transaction; the script must say so (ADR 0022).
	p := New(Options{})
	t.Cleanup(p.Close)
	p.Note(ev("BEGIN", "k1", "uuid:3"))
	p.NoteSkipped("k1", "uuid:3")
	p.Note(ev("ROWS", "k1", "uuid:3", insertRow()))
	p.Note(ev("XID", "k1", "uuid:3"))
	if _, err := p.Prepare(); err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, w := range p.Warnings() {
		if strings.Contains(w, "uuid:3") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no partial-transaction warning: %#v", p.Warnings())
	}
}

func TestPositionalNamesRefuseWithoutSchema(t *testing.T) {
	// MINIMAL metadata carries no names and no facts to guess them from;
	// without a definition the only honest answer is a refusal (ADR 0022).
	row := insertRow()
	row.Names = model.RowNamesPositional
	row.Columns = []string{"@1", "@2"}
	_, _, err := plan(t, Options{}, ev("ROWS", "k1", "", row))
	if err == nil {
		t.Fatal("MINIMAL row without schema emitted anyway")
	}
	ref, ok := err.(*Refusal)
	if !ok || ref.Code != "names" {
		t.Fatalf("err = %#v", err)
	}
}

func TestSchemaFillsNamesAndFacts(t *testing.T) {
	// With a definition, MINIMAL rows emit named columns after every
	// comparable fact checks out (ADR 0022).
	row := insertRow()
	row.Names = model.RowNamesPositional
	row.Columns = []string{"@1", "@2"}
	row.Cols = []model.FlashCol{{Typ: mysql.MYSQL_TYPE_LONG, Base: "int"}, {Typ: mysql.MYSQL_TYPE_VAR_STRING, Base: "varchar"}}
	_, sql, err := plan(t,
		Options{SchemaFileDB: "shop", SchemaSQL: "CREATE TABLE `orders` (\n" +
			"  `id` int NOT NULL,\n" +
			"  `note` varchar(64) CHARACTER SET utf8mb4 DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n)"},
		ev("ROWS", "k1", "", row))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "INSERT INTO `shop`.`orders` (`id`, `note`) VALUES (1, 'a');") {
		t.Fatalf("schema-filled insert:\n%s", sql)
	}
}

func TestSchemaContradictionRefuses(t *testing.T) {
	// A definition that disagrees with the row's own facts is the wrong
	// table — refuse, never silently relabel (ADR 0022).
	row := insertRow()
	row.Names = model.RowNamesPositional
	row.Columns = []string{"@1", "@2"}
	row.Cols = []model.FlashCol{{Typ: mysql.MYSQL_TYPE_LONG, Base: "int"}, {Typ: mysql.MYSQL_TYPE_VAR_STRING, Base: "varchar"}}
	_, _, err := plan(t,
		Options{SchemaFileDB: "shop", SchemaSQL: "CREATE TABLE `orders` (\n" +
			"  `id` bigint NOT NULL,\n" +
			"  `note` varchar(64) CHARACTER SET utf8mb4 DEFAULT NULL,\n" +
			"  PRIMARY KEY (`id`)\n)"},
		ev("ROWS", "k1", "", row))
	if err == nil {
		t.Fatal("contradicting schema emitted anyway")
	}
	ref, ok := err.(*Refusal)
	if !ok || ref.Code != "schema" {
		t.Fatalf("err = %#v", err)
	}
}

func TestSchemaColumnCountMismatchRefuses(t *testing.T) {
	row := insertRow()
	row.Names = model.RowNamesPositional
	row.Columns = []string{"@1", "@2"}
	row.Cols = []model.FlashCol{{Typ: mysql.MYSQL_TYPE_LONG, Base: "int"}, {Typ: mysql.MYSQL_TYPE_VAR_STRING, Base: "varchar"}}
	_, _, err := plan(t,
		Options{SchemaFileDB: "shop", SchemaSQL: "CREATE TABLE `orders` (`id` int NOT NULL, PRIMARY KEY (`id`))"},
		ev("ROWS", "k1", "", row))
	if err == nil {
		t.Fatal("short schema emitted anyway")
	}
	ref, ok := err.(*Refusal)
	if !ok || ref.Code != "schema" {
		t.Fatalf("err = %#v", err)
	}
}

func TestTruncatedQueryRefuses(t *testing.T) {
	// A truncated QUERY cannot be replayed; refusing beats emitting text
	// that looks complete but is not (ADR 0022).
	_, _, err := plan(t, Options{},
		model.NormalizedEvent{EventType: "UNCLASSIFIED_QUERY", TxnKey: "k1",
			QuerySQL: "ALTER TABLE t /* …", QueryTruncated: true})
	if err == nil {
		t.Fatal("truncated query emitted anyway")
	}
	ref, ok := err.(*Refusal)
	if !ok || ref.Code != "truncated_query" {
		t.Fatalf("err = %#v", err)
	}
}

func TestEmptyGroupStillMirrorsLoggedPair(t *testing.T) {
	// A BEGIN/XID pair the selection emptied still replays its logged
	// boundaries so positions in the script track the source stream.
	_, sql, err := plan(t, Options{},
		ev("BEGIN", "k1", ""),
		ev("XID", "k1", ""))
	if err != nil {
		t.Fatal(err)
	}
	lines := sqlLines(t, sql)
	if len(lines) != 2 || lines[0] != "BEGIN;" || lines[1] != "COMMIT;" {
		t.Fatalf("empty group:\n%s", sql)
	}
}

func TestXAPrepareMarkerEmitsEndPreparePair(t *testing.T) {
	// The server-logged XA PREPARE marker carries no QUERY text; replay
	// needs the END it implies before PREPARE (ADR 0022).
	ev2 := ev("XA_PREPARE", "k1", "")
	ev2.XAXID = "'x1'"
	_, sql, err := plan(t, Options{},
		model.NormalizedEvent{EventType: "XA_START", TxnKey: "k1", QuerySQL: "XA START 'x1'"},
		ev2)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "XA END 'x1';\nXA PREPARE 'x1';") {
		t.Fatalf("xa pair:\n%s", sql)
	}
	if strings.Contains(sql, "BEGIN;") || strings.Contains(sql, "COMMIT;") {
		t.Fatalf("xa group wrapped in BEGIN/COMMIT:\n%s", sql)
	}
}

func TestUnknownTableDefinitionWarns(t *testing.T) {
	// FULL metadata rows emit fine with no definition, but the gap is
	// reported once per table (ADR 0022).
	p, _, err := plan(t, Options{}, ev("ROWS", "k1", "", insertRow()))
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, w := range p.Warnings() {
		if strings.Contains(w, "shop.orders") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no unknown-table warning: %#v", p.Warnings())
	}
}

func TestGTIDCommentCarriesBinlogPosition(t *testing.T) {
	e := ev("GTID", "k1", "uuid:9")
	e.BinlogPath = "mysql-bin.000007"
	e.PositionStart = 1234
	_, sql, err := plan(t, Options{}, e,
		ev("BEGIN", "k1", "uuid:9"), ev("XID", "k1", "uuid:9"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "-- gtid: uuid:9") || !strings.Contains(sql, "-- binlog: mysql-bin.000007:1234") {
		t.Fatalf("comments:\n%s", sql)
	}
}
