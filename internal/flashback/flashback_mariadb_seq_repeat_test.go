package flashback_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"binlogviz/internal/analyzer"
	"binlogviz/internal/model"
)

// mariaTxn is one MariaDB transaction: GTID, BEGIN, one INSERT row, XID. The
// row id is unique per transaction, so the undo script names it.
func mariaTxn(pos *int64, gtid string, id int) []model.NormalizedEvent {
	at := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	ev := func(typ string) model.NormalizedEvent {
		e := model.NormalizedEvent{Timestamp: at, EventType: typ, BinlogPath: "mariadb-bin.000001", PositionStart: *pos, PositionEnd: *pos + 10, ServerFlavor: "mariadb"}
		*pos += 10
		return e
	}
	g := ev("GTID")
	g.GTID = gtid
	rows := flashEvent(model.FlashRow{Schema: "q383", Table: "t", Op: "INSERT", Columns: []string{"id"}, Cols: intCols(1), After: []string{fmt.Sprint(id)}})
	rows.Operation = "INSERT"
	rows.PositionStart, rows.PositionEnd, rows.ServerFlavor = *pos, *pos+10, "mariadb"
	*pos += 10
	return []model.NormalizedEvent{g, ev("BEGIN"), rows, ev("XID")}
}

// TestMatchedSetRoundTripKeepsExcludedSameSeqTransaction is the #383 QA
// must-have. Servers 7 and 8 alternate in domain 0 (0-8-1, 0-7-2, ...), and
// a second transaction 0-8-6 reuses sequence number 6 of the matched 0-7-6.
// --exclude-gtids 0-8-6 must leave a matched_gtid_set that, passed back to
// flashback as --include-gtids, does not undo 0-8-6. That holds only because
// the analyzer records the sequence number of every MariaDB transaction it
// reads, unmatched ones included (matchedGTIDSet.observe): without it the
// set takes the shared form 0-7-1..20, 0-8-1..20, which names 0-8-6.
func TestMatchedSetRoundTripKeepsExcludedSameSeqTransaction(t *testing.T) {
	var events []model.NormalizedEvent
	pos := int64(4)
	want := map[int]bool{}
	for seq := 1; seq <= 20; seq++ {
		server := 8 // odd seq: server 8, even seq: server 7
		if seq%2 == 0 {
			server = 7
		}
		id := 1000*server + seq
		events = append(events, mariaTxn(&pos, fmt.Sprintf("0-%d-%d", server, seq), id)...)
		want[id] = true
		if seq == 10 {
			events = append(events, mariaTxn(&pos, "0-8-6", 999006)...) // excluded, same seq as 0-7-6
		}
	}

	exclude, err := analyzer.ParseGTIDSelector(nil, []string{"0-8-6"})
	if err != nil {
		t.Fatal(err)
	}
	a := analyzer.New(analyzer.Options{GTIDSelector: exclude})
	for _, ev := range events {
		if err := a.Consume(ev); err != nil {
			t.Fatal(err)
		}
	}
	result, err := a.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	set := result.Selection.MatchedGTIDSet
	if result.Selection.MatchedTransactions == nil || *result.Selection.MatchedTransactions != 20 {
		t.Fatalf("matched_transactions %v, want 20", result.Selection.MatchedTransactions)
	}

	include, err := analyzer.ParseGTIDSelector(set, nil)
	if err != nil {
		t.Fatalf("matched_gtid_set %v is not --include-gtids input: %v", set, err)
	}
	if hit, _ := include.Match("0-8-6"); hit {
		t.Fatalf("matched_gtid_set %v selects the excluded 0-8-6", set)
	}
	fb := analyzer.New(analyzer.Options{Flashback: true, GTIDSelector: include})
	consumeFlash(t, fb, events...)
	sql, err := fb.Planner().SQL()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "999006") {
		t.Fatalf("flashback --include-gtids %s undoes the excluded 0-8-6:\n%s", strings.Join(set, ","), sql)
	}
	for id := range want {
		if !strings.Contains(sql, fmt.Sprint(id)) {
			t.Fatalf("flashback round trip misses matched row id %d; set %v", id, set)
		}
	}
}
