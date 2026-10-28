// Package detailstore persists optional analysis details behind the analyzer's
// detail-store interface; default reports do not open DuckDB.
// input: completed transactions, minute buckets, alerts, and transaction keys needing SQL hydration.
// output: DuckDB-backed detail persistence when CGO is enabled, plus the record DTO the analyzer writes.
// pos: the DuckDB adapter seam of ADR 0012; analyzer sees the interface, cmd injects the store.
// note: if this file changes, keep this header and module README.md synchronized.
package detailstore

import (
	"errors"
	"time"

	"binlogviz/internal/model"
)

// ErrDuckDBRequiresCGO is returned by NewDuckDBStore when the binary was built without CGO.
var ErrDuckDBRequiresCGO = errors.New("duckdb detail store requires cgo")

const (
	DefaultBatchFlushRows  = 10000
	defaultBatchFlushBytes = 4 * 1024 * 1024
)

// Transaction is the persisted row record the analyzer writes through the
// detail-store interface. Field names mirror the DuckDB schema.
type Transaction struct {
	TxnKey              string
	ServerID            uint32
	ServerVersion       string
	ServerFlavor        string
	GTID                string
	ThreadID            uint32
	XID                 string
	ActorUser           string
	ActorHost           string
	XAXID               string
	StartTime           time.Time
	EndTime             time.Time
	DurationMS          int64
	TotalRows           int64
	EventCount          int64
	BinlogBytes         int64
	BinlogPathStart     string
	BinlogPathEnd       string
	PositionStart       int64
	PositionEnd         int64
	Completeness        model.TransactionCompleteness
	FullBinlogPathStart string
	FullBinlogPathEnd   string
	FullPositionStart   int64
	FullPositionEnd     int64
	FullBinlogBytes     int64
	QuerySummary        string
	QuerySQL            string
	QueryTruncated      bool
	QueryOriginalBytes  int64
	TableRows           map[string]int
	Operations          map[string]int
}

type transactionRow struct {
	TxnKey              string
	ServerID            uint32
	ServerVersion       string
	ServerFlavor        string
	GTID                string
	ThreadID            uint32
	XID                 string
	ActorUser           string
	ActorHost           string
	XAXID               string
	StartTime           time.Time
	EndTime             time.Time
	DurationMS          int64
	TotalRows           int64
	EventCount          int64
	BinlogBytes         int64
	BinlogPathStart     string
	BinlogPathEnd       string
	PositionStart       int64
	PositionEnd         int64
	Completeness        string
	FullBinlogPathStart string
	FullBinlogPathEnd   string
	FullPositionStart   int64
	FullPositionEnd     int64
	FullBinlogBytes     int64
	QuerySummary        string
	QueryTruncated      bool
	QueryOriginalBytes  int64
}

type transactionTableRow struct {
	TxnKey   string
	TableKey string
	Rows     int64
}

type transactionOperationRow struct {
	TxnKey    string
	Operation string
	Rows      int64
}

type transactionSQLContextRow struct {
	TxnKey             string
	QuerySQL           string
	QueryTruncated     bool
	QueryOriginalBytes int64
}

type minuteBucketRow struct {
	Minute      time.Time
	TotalRows   int64
	TxnCount    int64
	EventCount  int64
	BinlogBytes int64
	DDLCount    int64
}

type minuteTableRow struct {
	Minute   time.Time
	TableKey string
	Rows     int64
}

type alertRow struct {
	Type        string
	Severity    string
	TxnKey      string
	Minute      time.Time
	Message     string
	DetailsJSON string
	GTID        string
}

func estimateStringBytes(v string) int {
	return len(v)
}

func growSlice[T any](items []T, additional int) []T {
	if additional <= 0 {
		return items
	}
	needed := len(items) + additional
	if cap(items) >= needed {
		return items
	}
	newCap := cap(items) * 2
	if newCap < needed {
		newCap = needed
	}
	if newCap == 0 {
		newCap = additional
	}
	grown := make([]T, len(items), newCap)
	copy(grown, items)
	return grown
}

func zeroTimeToNil(ts time.Time) any {
	if ts.IsZero() {
		return nil
	}
	return ts
}
