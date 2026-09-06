// Package binlog maps go-mysql event enums onto one RawEvent kind name.
// input: replication.EventType values from a parsed binlog header.
// output: canonical kind strings consumed by FormatObserver and NormalizeRawEvent.
// pos: parser adapter seam so go-mysql String() names never leak into analysis.
// note: if this file changes, keep this header and README.md synchronized.
package binlog

import "github.com/go-mysql-org/go-mysql/replication"

const (
	KindQuery             = "QUERY"
	KindWriteRows         = "WRITE_ROWS"
	KindUpdateRows        = "UPDATE_ROWS"
	KindDeleteRows        = "DELETE_ROWS"
	KindRowsQuery         = "ROWS_QUERY"
	KindGTID              = "GTID"
	KindXID               = "XID"
	KindXAPrepare         = "XA_PREPARE"
	KindTableMap          = "TABLE_MAP"
	KindFormatDescription = "FORMAT_DESCRIPTION"
)

func canonicalEventType(et replication.EventType) string {
	switch et {
	case replication.QUERY_EVENT:
		return KindQuery
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return KindWriteRows
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return KindUpdateRows
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return KindDeleteRows
	case replication.ROWS_QUERY_EVENT, replication.MARIADB_ANNOTATE_ROWS_EVENT:
		return KindRowsQuery
	case replication.GTID_EVENT, replication.GTID_TAGGED_LOG_EVENT, replication.MARIADB_GTID_EVENT:
		return KindGTID
	case replication.XID_EVENT:
		return KindXID
	case replication.XA_PREPARE_LOG_EVENT:
		return KindXAPrepare
	case replication.TABLE_MAP_EVENT:
		return KindTableMap
	case replication.FORMAT_DESCRIPTION_EVENT:
		return KindFormatDescription
	default:
		return ""
	}
}
