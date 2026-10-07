package binlogviz

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"binlogviz/internal/binlog"
)

func TestHotRowsOnMySQL80FullAndMinimal(t *testing.T) {
	forceEnglishRuntimeOutput(t)
	fullPath := mustFixturePath(t, "mysql-8.0.46-hot-rows-full.binlog")
	minimalPath := mustFixturePath(t, "mysql-8.0.46-hot-rows-minimal.binlog")

	stdout, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--format", "json", "--top", "20")
	if err != nil {
		t.Fatalf("full analyze: %v\n%s", err, stderr)
	}
	full := decodeHotRows(t, stdout)
	if full.TrackLimit != 8192 {
		t.Fatalf("track limit=%d", full.TrackLimit)
	}
	hot := hotRowByKey(t, full, "id=7")
	if hot.Schema != "shop" || hot.Table != "counters" || hot.Touches != 7 || hot.Transactions != 6 {
		t.Fatalf("id=7=%+v", hot)
	}
	if hot.FirstTime != "2026-10-06T14:00:01Z" || hot.LastTime != "2026-10-06T14:00:06Z" {
		t.Fatalf("times=%s %s", hot.FirstTime, hot.LastTime)
	}
	if hot.FirstGTID == "" || hot.LastGTID == "" || hot.FirstGTID == hot.LastGTID {
		t.Fatalf("gtids first=%q last=%q", hot.FirstGTID, hot.LastGTID)
	}
	if hot.FirstFile != filepath.Base(fullPath) || hot.LastFile != hot.FirstFile || hot.FirstPos <= 0 || hot.LastPos <= hot.FirstPos {
		t.Fatalf("location first=%s:%d last=%s:%d", hot.FirstFile, hot.FirstPos, hot.LastFile, hot.LastPos)
	}
	assertGTIDAt(t, fullPath, hot.FirstGTID, hot.FirstPos)
	assertGTIDAt(t, fullPath, hot.LastGTID, hot.LastPos)
	if full.HotRows[0].PrimaryKey != "id=7" {
		t.Fatalf("hottest=%+v", full.HotRows[0])
	}
	bolt := hotRowByKey(t, full, "sku=BOLT, wh=1")
	if bolt.Table != "inventory" || bolt.Touches != 3 || bolt.Transactions != 3 {
		t.Fatalf("bolt=%+v", bolt)
	}
	warm := hotRowByKey(t, full, "id=8")
	if warm.Touches != 2 || warm.Transactions != 2 {
		t.Fatalf("id=8=%+v", warm)
	}
	gone := hotRowByKey(t, full, "id=9")
	if gone.Touches != 1 {
		t.Fatalf("delete=%+v", gone)
	}
	for _, row := range full.HotRows {
		if row.Table == "heap" || row.PrimaryKey == "id=4" || strings.HasPrefix(row.PrimaryKey, "@") || strings.Contains(row.PrimaryKey, "label=") {
			t.Fatalf("unexpected row %+v", row)
		}
	}
	if len(full.Unavailable) != 0 {
		t.Fatalf("full metadata should rank keys, gaps=%+v", full.Unavailable)
	}

	text, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--top", "20")
	if err != nil {
		t.Fatalf("text: %v\n%s", err, stderr)
	}
	section := sectionBetween(text, "=== Hot Rows ===", "===")
	if section == "" || !strings.Contains(section, "shop.counters id=7") || !strings.Contains(section, "touches=7") || !strings.Contains(section, "transactions=6") {
		t.Fatalf("text section:\n%s", text)
	}
	if !strings.Contains(section, hot.FirstGTID) || !strings.Contains(section, hot.FirstFile+":") {
		t.Fatalf("text missing txn location:\n%s", section)
	}
	if strings.Contains(section, "shop.heap") || strings.Contains(section, "id=4") {
		t.Fatalf("ranked a no-pk table or an insert:\n%s", section)
	}

	md, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--format", "markdown", "--top", "20")
	if err != nil {
		t.Fatalf("markdown: %v\n%s", err, stderr)
	}
	if !strings.Contains(md, "## Hot Rows") || !strings.Contains(md, "id=7") || !strings.Contains(md, hot.FirstGTID) {
		t.Fatalf("markdown missing hot rows:\n%s", md)
	}
	html, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--format", "html", "--output", "-", "--top", "20")
	if err != nil {
		t.Fatalf("html: %v\n%s", err, stderr)
	}
	if !strings.Contains(html, `id="hot-rows-table"`) || !strings.Contains(html, "id=7") || !strings.Contains(html, hot.LastGTID) {
		t.Fatal("html missing hot rows")
	}

	limited, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--format", "json", "--top", "1")
	if err != nil {
		t.Fatalf("top 1: %v\n%s", err, stderr)
	}
	top1 := decodeHotRows(t, limited)
	if top1.Listed != 1 || top1.Omitted != 4 || top1.HotRows[0].PrimaryKey != "id=7" {
		t.Fatalf("top 1=%+v omitted=%d", top1.HotRows, top1.Omitted)
	}
	rowsFlag, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--format", "json", "--top-rows", "2")
	if err != nil {
		t.Fatalf("top-rows: %v\n%s", err, stderr)
	}
	top2 := decodeHotRows(t, rowsFlag)
	if top2.Listed != 2 || top2.HotRows[1].PrimaryKey != "sku=BOLT, wh=1" {
		t.Fatalf("top-rows=%+v", top2.HotRows)
	}

	filtered, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--format", "json", "--include-table", "shop.inventory", "--top", "20")
	if err != nil {
		t.Fatalf("include-table: %v\n%s", err, stderr)
	}
	inv := decodeHotRows(t, filtered)
	if len(inv.HotRows) != 2 || inv.HotRows[0].Table != "inventory" {
		t.Fatalf("inventory filter=%+v", inv.HotRows)
	}
	for _, row := range inv.HotRows {
		if row.Table == "counters" {
			t.Fatalf("include-table kept counters: %+v", row)
		}
	}

	deleted, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--format", "json", "--dml", "delete", "--top", "20")
	if err != nil {
		t.Fatalf("dml: %v\n%s", err, stderr)
	}
	del := decodeHotRows(t, deleted)
	if len(del.HotRows) != 1 || del.HotRows[0].PrimaryKey != "id=9" || del.HotRows[0].Touches != 1 {
		t.Fatalf("delete filter=%+v", del.HotRows)
	}

	window, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--format", "json", "--top", "20",
		"--start", "2026-10-06T14:00:04Z", "--end", "2026-10-06T14:00:06Z")
	if err != nil {
		t.Fatalf("window: %v\n%s", err, stderr)
	}
	win := decodeHotRows(t, window)
	winHot := hotRowByKey(t, win, "id=7")
	if winHot.Touches != 3 || winHot.Transactions != 3 || winHot.FirstTime != "2026-10-06T14:00:04Z" || winHot.LastTime != "2026-10-06T14:00:06Z" {
		t.Fatalf("window id=7=%+v", winHot)
	}
	if len(win.HotRows) != 1 {
		t.Fatalf("window rows=%+v", win.HotRows)
	}

	one, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--format", "json", "--top", "20", "--include-gtids", hot.FirstGTID)
	if err != nil {
		t.Fatalf("gtid: %v\n%s", err, stderr)
	}
	gtidRows := decodeHotRows(t, one)
	only := hotRowByKey(t, gtidRows, "id=7")
	if only.Touches != 2 || only.Transactions != 1 || only.FirstGTID != hot.FirstGTID || only.LastGTID != hot.FirstGTID {
		t.Fatalf("one gtid=%+v", only)
	}

	off, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--format", "json", "--sql-context", "off", "--top", "20")
	if err != nil {
		t.Fatalf("sql-context off: %v\n%s", err, stderr)
	}
	if strings.Contains(off, "id=7") || strings.Contains(off, "sku=BOLT") {
		t.Fatalf("sql-context off leaked a key:\n%s", off)
	}
	quiet := decodeHotRows(t, off)
	if !quiet.HotRows[0].KeyHidden || quiet.HotRows[0].PrimaryKey != "" || quiet.HotRows[0].Touches != 7 || quiet.HotRows[0].Transactions != 6 {
		t.Fatalf("hidden=%+v", quiet.HotRows[0])
	}
	offText, stderr, err := executeAnalyzeLikeMain(t, fullPath, "--sql-context", "off", "--top", "20")
	if err != nil {
		t.Fatalf("off text: %v\n%s", err, stderr)
	}
	offSection := sectionBetween(offText, "=== Hot Rows ===", "===")
	if strings.Contains(offSection, "id=7") || !strings.Contains(offSection, "touches=7") || !strings.Contains(offSection, "primary key values hidden") {
		t.Fatalf("off text:\n%s", offSection)
	}

	minOut, stderr, err := executeAnalyzeLikeMain(t, minimalPath, "--format", "json", "--top", "20")
	if err != nil {
		t.Fatalf("minimal: %v\n%s", err, stderr)
	}
	if strings.Contains(minOut, `"primary_key"`) || strings.Contains(minOut, "@1") || strings.Contains(minOut, "@2") {
		t.Fatalf("minimal guessed a key:\n%s", minOut)
	}
	minimal := decodeHotRows(t, minOut)
	if len(minimal.HotRows) != 0 {
		t.Fatalf("minimal ranked rows=%+v", minimal.HotRows)
	}
	if !gapHas(minimal, "counters", "metadata") || !gapHas(minimal, "inventory", "metadata") || !gapHas(minimal, "heap", "metadata") {
		t.Fatalf("gaps=%+v", minimal.Unavailable)
	}
	minText, stderr, err := executeAnalyzeLikeMain(t, minimalPath, "--top", "20")
	if err != nil {
		t.Fatalf("minimal text: %v\n%s", err, stderr)
	}
	if !strings.Contains(minText, "hot-row tracking unavailable for shop.counters") || !strings.Contains(minText, "binlog_row_metadata is not FULL") {
		t.Fatalf("minimal text:\n%s", minText)
	}
	minSection := sectionBetween(minText, "=== Hot Rows ===", "===")
	if strings.Contains(minSection, "id=7") || strings.Contains(minSection, "@1") || strings.Contains(minSection, "no primary key") {
		t.Fatalf("minimal text guessed a key or a missing primary key:\n%s", minSection)
	}
}

type hotRowsDoc struct {
	HotRows     []hotRowJSON `json:"hot_rows"`
	Listed      int          `json:"hot_rows_listed"`
	Omitted     int          `json:"hot_rows_omitted"`
	TrackLimit  int          `json:"hot_row_track_limit"`
	Unavailable []struct {
		Schema  string `json:"schema"`
		Table   string `json:"table"`
		Reason  string `json:"reason"`
		Message string `json:"message"`
	} `json:"hot_row_unavailable"`
}

type hotRowJSON struct {
	Schema       string `json:"schema"`
	Table        string `json:"table"`
	PrimaryKey   string `json:"primary_key"`
	KeyHidden    bool   `json:"key_hidden"`
	Touches      int    `json:"touches"`
	Transactions int    `json:"transactions"`
	FirstTime    string `json:"first_time"`
	LastTime     string `json:"last_time"`
	FirstGTID    string `json:"first_gtid"`
	FirstFile    string `json:"first_file"`
	FirstPos     int64  `json:"first_pos"`
	LastGTID     string `json:"last_gtid"`
	LastFile     string `json:"last_file"`
	LastPos      int64  `json:"last_pos"`
}

func decodeHotRows(t *testing.T, stdout string) hotRowsDoc {
	t.Helper()
	var doc hotRowsDoc
	if err := json.Unmarshal([]byte(stdout), &doc); err != nil {
		t.Fatalf("json: %v\n%s", err, stdout)
	}
	return doc
}

func hotRowByKey(t *testing.T, doc hotRowsDoc, key string) hotRowJSON {
	t.Helper()
	for _, row := range doc.HotRows {
		if row.PrimaryKey == key {
			return row
		}
	}
	t.Fatalf("missing %s in %+v", key, doc.HotRows)
	return hotRowJSON{}
}

func gapHas(doc hotRowsDoc, table, reason string) bool {
	for _, gap := range doc.Unavailable {
		if gap.Table == table && gap.Reason == reason && strings.Contains(gap.Message, "binlog_row_metadata is not FULL") {
			return true
		}
	}
	return false
}

func assertGTIDAt(t *testing.T, path, gtid string, pos int64) {
	t.Helper()
	parser := binlog.NewParser()
	found := false
	err := parser.ParseFiles([]string{path}, func(raw binlog.RawEvent) error {
		if raw.EventType == "GTID" && raw.GTID == gtid {
			if raw.PositionStart != pos {
				t.Fatalf("gtid %s starts at %d, report said %d", gtid, raw.PositionStart, pos)
			}
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !found {
		t.Fatalf("gtid %s not in %s", gtid, path)
	}
}
