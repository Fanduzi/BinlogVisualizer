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
