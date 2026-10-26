package flashback

import (
	"strings"
	"testing"

	"binlogviz/internal/model"
)

func TestFlashbackFilterAttributesVersionedAndANSIDrops(t *testing.T) {
	keep := fakeSelection{schema: "qa410a", table: "keep"}
	rn := fakeSelection{schema: "rn", table: "t1"}
	cases := []struct {
		name   string
		sel    Selection
		ev     model.NormalizedEvent
		refuse bool
		unread bool
		table  string
	}{
		{
			name: "versioned temporary drop of other tables",
			sel:  keep,
			ev: model.NormalizedEvent{
				EventType: "DDL", Schema: "qa410a", QuerySchema: "qa410a",
				ServerVersion: "8.0.36", ServerFlavor: "mysql",
				QuerySQL: "DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `td 2``x`,`td1`",
			},
		},
		{
			name: "ansi quotes drop of other tables",
			sel:  keep,
			ev: model.NormalizedEvent{
				EventType: "DDL", Schema: "qa410a", QuerySchema: "qa410a",
				SQLMode:  4,
				QuerySQL: `DROP TABLE IF EXISTS "t9","nope2"`,
			},
		},
		{
			name: "double quotes without ANSI_QUOTES stay unread",
			sel:  keep,
			ev: model.NormalizedEvent{
				EventType: "DDL", Schema: "qa410a", QuerySchema: "qa410a",
				QuerySQL: `DROP TABLE IF EXISTS "t9","nope2"`,
			},
			refuse: true, unread: true,
		},
		{
			name: "versioned comment without a server version stays unread",
			sel:  keep,
			ev: model.NormalizedEvent{
				EventType: "DDL", Schema: "qa410a", QuerySchema: "qa410a",
				QuerySQL: "DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `td 2``x`,`td1`",
			},
			refuse: true, unread: true,
		},
		{
			name: "future comment does not name the selected table",
			sel:  rn,
			ev: model.NormalizedEvent{
				EventType: "DDL", Schema: "rn", QuerySchema: "rn",
				ServerVersion: "8.0.36",
				QuerySQL:      "DROP TABLE z /*!99999 , t1 */",
			},
		},
		{
			name: "executed comment names the selected table",
			sel:  fakeSelection{schema: "rn", table: "t2"},
			ev: model.NormalizedEvent{
				EventType: "DDL", Schema: "rn", QuerySchema: "rn",
				ServerVersion: "8.0.36",
				QuerySQL:      "DROP TABLE a /*!50001 , t2 */",
			},
			refuse: true,
			table:  "rn.a, rn.t2",
		},
	}
	for _, tc := range cases {
		p := New(Options{Selection: tc.sel})
		p.Note(tc.ev)
		if !tc.refuse {
			if p.flashErr != nil {
				t.Errorf("%s: refused: %v", tc.name, p.flashErr)
			}
			continue
		}
		reason, table, ok := RefusalDetails(p.flashErr)
		if !ok || reason != "ddl_in_range" || table != tc.table {
			t.Errorf("%s: reason %q table %q err %v", tc.name, reason, table, p.flashErr)
			continue
		}
		unread := strings.Contains(p.flashErr.Error(), "could not be read completely")
		if unread != tc.unread {
			t.Errorf("%s: message %v", tc.name, p.flashErr)
		}
	}
}

func TestVersionCommentDoesNotHideExecutedRename(t *testing.T) {
	const mysql = "RENAME TABLE qa412.fo_x TO qa412.fo_y, /*!800369_t TO 9_u, qa412.fo_keep TO qa412.fo_k2, */ qa412.fo_z TO qa412.fo_w"
	const seven = "RENAME TABLE qa412.fo_x TO qa412.fo_y, /*!1000009_t TO 9_u, qa412.fo_keep TO qa412.fo_k2, */ qa412.fo_z TO qa412.fo_w"
	const maria = "RENAME TABLE qa412.fo_x TO qa412.fo_y, /*!1100009_t TO 9_u, qa412.fo_keep TO qa412.fo_k2, */ qa412.fo_z TO qa412.fo_w"
	const mariaM = "RENAME TABLE qa412.fo_x TO qa412.fo_y, /*M!1100009_t TO 9_u, qa412.fo_keep TO qa412.fo_k2, */ qa412.fo_z TO qa412.fo_w"
	cases := []model.NormalizedEvent{
		{EventType: "DDL", Schema: "qa412", QuerySchema: "qa412", ServerVersion: "8.0.36", ServerFlavor: "mysql", QuerySQL: mysql},
		{EventType: "DDL", Schema: "qa412", QuerySchema: "qa412", ServerVersion: "8.4.11", ServerFlavor: "mysql", QuerySQL: mysql},
		{EventType: "DDL", Schema: "qa412", QuerySchema: "qa412", ServerVersion: "8.0.36", ServerFlavor: "mysql", QuerySQL: seven},
		{EventType: "DDL", Schema: "qa412", QuerySchema: "qa412", ServerVersion: "8.4.11", ServerFlavor: "mysql", QuerySQL: seven},
		{EventType: "DDL", Schema: "qa412", QuerySchema: "qa412", ServerVersion: "11.8.9-MariaDB", ServerFlavor: "mariadb", QuerySQL: maria},
		{EventType: "DDL", Schema: "qa412", QuerySchema: "qa412", ServerVersion: "11.8.9-MariaDB", ServerFlavor: "mariadb", QuerySQL: mariaM},
	}
	sel := fakeSelection{schema: "qa412", table: "fo_keep"}
	for _, ev := range cases {
		p := New(Options{Selection: sel})
		p.Note(ev)
		reason, table, ok := RefusalDetails(p.flashErr)
		if !ok || reason != "ddl_in_range" || table != "qa412.fo_keep" {
			t.Errorf("%s %s: reason %q table %q err %v", ev.ServerVersion, ev.QuerySQL, reason, table, p.flashErr)
		}
	}
}

func TestDigitLeadingNameDoesNotRefuseUnrelatedFilter(t *testing.T) {
	ev := model.NormalizedEvent{
		EventType: "DDL", Schema: "qa", QuerySchema: "qa",
		QuerySQL: "ALTER TABLE 2fa_codes ADD COLUMN c INT",
	}
	p := New(Options{Selection: fakeSelection{schema: "qa", table: "other"}})
	p.Note(ev)
	if p.flashErr != nil {
		t.Fatalf("unrelated filter refused: %v", p.flashErr)
	}
	p = New(Options{Selection: fakeSelection{schema: "qa", table: "2fa_codes"}})
	p.Note(ev)
	reason, table, ok := RefusalDetails(p.flashErr)
	if !ok || reason != "ddl_in_range" || table != "qa.2fa_codes" {
		t.Fatalf("reason %q table %q err %v", reason, table, p.flashErr)
	}
}

func TestEscapeControlsPrintsUnicodeControls(t *testing.T) {
	got := escapeControls("a\u202eb\u0085c")
	if strings.ContainsRune(got, '\u202e') || strings.ContainsRune(got, '\u0085') {
		t.Fatalf("raw control in %q", got)
	}
	if !strings.Contains(got, `\u202e`) || !strings.Contains(got, `\u0085`) {
		t.Fatalf("got %q", got)
	}
	if escapeControls("a\tb") != `a\tb` {
		t.Fatalf("tab: %q", escapeControls("a\tb"))
	}
}
