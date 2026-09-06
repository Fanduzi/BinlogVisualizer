// Package binlog verifies canonical event kinds from go-mysql enums.
// input: replication.EventType constants including GTID_TAGGED_LOG_EVENT whose String() is not CamelCase.
// output: assertions that parser-facing kinds are one name per concept.
// pos: unit tests for the parser adapter seam used by normalize and FormatObserver.
// note: if this file changes, update this header and README.md.
package binlog

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

func TestCanonicalEventTypeMapsGoMysqlEnums(t *testing.T) {
	cases := []struct {
		et   replication.EventType
		want string
	}{
		{replication.QUERY_EVENT, kindQuery},
		{replication.WRITE_ROWS_EVENTv2, kindWriteRows},
		{replication.UPDATE_ROWS_EVENTv1, kindUpdateRows},
		{replication.DELETE_ROWS_EVENTv0, kindDeleteRows},
		{replication.ROWS_QUERY_EVENT, kindRowsQuery},
		{replication.MARIADB_ANNOTATE_ROWS_EVENT, kindRowsQuery},
		{replication.GTID_EVENT, kindGTID},
		{replication.MARIADB_GTID_EVENT, kindGTID},
		{replication.GTID_TAGGED_LOG_EVENT, kindGTID},
		{replication.XID_EVENT, kindXID},
		{replication.XA_PREPARE_LOG_EVENT, kindXAPrepare},
		{replication.TABLE_MAP_EVENT, kindTableMap},
		{replication.FORMAT_DESCRIPTION_EVENT, kindFormatDescription},
		{replication.ROTATE_EVENT, ""},
	}
	for _, tc := range cases {
		if got := canonicalEventType(tc.et); got != tc.want {
			t.Fatalf("canonicalEventType(%s)=%q, want %q", tc.et, got, tc.want)
		}
	}
}

func TestCanonicalEventTypeIgnoresGoMysqlStringSpelling(t *testing.T) {
	if replication.GTID_TAGGED_LOG_EVENT.String() == "GtidTaggedLogEvent" {
		t.Fatal("fixture assumption failed: go-mysql still uses CamelCase for tagged GTID")
	}
	if got := canonicalEventType(replication.GTID_TAGGED_LOG_EVENT); got != kindGTID {
		t.Fatalf("tagged GTID kind=%q, want GTID despite String()=%q", got, replication.GTID_TAGGED_LOG_EVENT.String())
	}
}
