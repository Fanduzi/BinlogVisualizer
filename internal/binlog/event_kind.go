// Package binlog maps go-mysql event enums onto one RawEvent kind name.
// input: replication.EventType values from a parsed binlog header, including partial-update, anonymous GTID, MariaDB compressed rows, and transaction payload.
// output: canonical kind strings consumed by FormatObserver and NormalizeRawEvent, including partial-update as UPDATE_ROWS, anonymous GTID as GTID, and an empty kind for a transaction-payload wrapper.
// pos: parser adapter seam so go-mysql String() names never leak into analysis.
// note: if this file changes, keep this header and README.md synchronized.
package binlog

import "github.com/go-mysql-org/go-mysql/replication"

const (
	kindQuery             = "QUERY"
	kindWriteRows         = "WRITE_ROWS"
	kindUpdateRows        = "UPDATE_ROWS"
	kindDeleteRows        = "DELETE_ROWS"
	kindRowsQuery         = "ROWS_QUERY"
	kindGTID              = "GTID"
	kindXID               = "XID"
	kindXAPrepare         = "XA_PREPARE"
	kindTableMap          = "TABLE_MAP"
	kindFormatDescription = "FORMAT_DESCRIPTION"
)

func canonicalEventType(et replication.EventType) string {
	switch et {
	case replication.QUERY_EVENT:
		return kindQuery
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
		replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		return kindWriteRows
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.PARTIAL_UPDATE_ROWS_EVENT, replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
		return kindUpdateRows
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
		replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		return kindDeleteRows
	case replication.ROWS_QUERY_EVENT, replication.MARIADB_ANNOTATE_ROWS_EVENT:
		return kindRowsQuery
	case replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT, replication.MARIADB_GTID_EVENT,
		replication.ANONYMOUS_GTID_EVENT:
		return kindGTID
	case replication.XID_EVENT:
		return kindXID
	case replication.XA_PREPARE_LOG_EVENT:
		return kindXAPrepare
	case replication.TABLE_MAP_EVENT:
		return kindTableMap
	case replication.FORMAT_DESCRIPTION_EVENT:
		return kindFormatDescription
	default:
		return ""
	}
}
