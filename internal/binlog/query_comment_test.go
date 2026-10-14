package binlog

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

// MariaDB logs a leading comment verbatim; MySQL 8.0 strips it. Both must
// classify the same statement the same way (#269).
func TestQueryEventLeadingCommentDoesNotHideStatement(t *testing.T) {
	cases := []struct {
		query, want, kind string
	}{
		{"/* c */ CREATE USER 'r6'@'%' IDENTIFIED BY 'x'", "CREATE USER 'r6'@'%' IDENTIFIED BY 'x'", "DDL"},
		{"/* a */ /* b */\n\tDROP TABLE t", "DROP TABLE t", "DDL"},
		{"-- note\nALTER TABLE t ADD c int", "ALTER TABLE t ADD c int", "DDL"},
		{"# note\nGRANT SELECT ON *.* TO u", "GRANT SELECT ON *.* TO u", "DDL"},
		{"/* app */ BEGIN", "BEGIN", "BEGIN"},
		// Executable comments run their body, so they are not stripped.
		{"/*!40101 SET character_set_client = utf8 */", "/*!40101 SET character_set_client = utf8 */", "UNCLASSIFIED_QUERY"},
		{"/*M!100100 CREATE USER u */", "/*M!100100 CREATE USER u */", "UNCLASSIFIED_QUERY"},
		// A comment with nothing after it, or an unterminated one, is left alone.
		{"/* only */", "/* only */", "UNCLASSIFIED_QUERY"},
		{"/* open CREATE USER u", "/* open CREATE USER u", "UNCLASSIFIED_QUERY"},
	}
	for _, c := range cases {
		var raw RawEvent
		raw.EventType = kindQuery
		applyBinlogEventMetadata(&raw, replication.QUERY_EVENT, &replication.QueryEvent{Query: []byte(c.query)}, nil)
		if raw.Query != c.want {
			t.Errorf("%q: Query = %q, want %q", c.query, raw.Query, c.want)
			continue
		}
		ev, err := NormalizeRawEvent(raw)
		if err != nil || ev == nil || ev.EventType != c.kind {
			t.Errorf("%q: normalized %+v err %v, want %s", c.query, ev, err, c.kind)
		}
	}
}
