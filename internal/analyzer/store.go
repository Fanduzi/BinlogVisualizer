// Package analyzer persists optional analysis details; default reports do not open DuckDB.
// input: completed transactions, minute buckets, alerts, and transaction keys needing SQL hydration.
// output: in-memory detail persistence plus shared DTOs used by the DuckDB adapter when CGO is enabled.
// pos: default-package store that keeps analyze compilable without CGO.
// note: if this file changes, keep this header and module README.md synchronized.
package analyzer

import (
	"errors"
	"sort"
	"time"

	"binlogviz/internal/model"
)

// ErrDuckDBRequiresCGO is returned by NewDuckDBStore when the binary was built without CGO.
var ErrDuckDBRequiresCGO = errors.New("duckdb detail store requires cgo")

const (
	DefaultBatchFlushRows  = 10000
	defaultBatchFlushBytes = 4 * 1024 * 1024
)

type analysisStore interface {
	Reset() error
	RecordTransactions([]persistedTransaction) error
	RecordMinuteBuckets([]model.MinuteBucket) error
	RecordAlerts([]model.Alert) error
	Flush() error
	QueryAllTransactions() ([]model.Transaction, error)
	QueryTopTransactions(limit int) ([]model.Transaction, error)
	ResolveTransactionQuerySQL(txnKeys []string) (map[string]string, error)
	QueryMinuteBuckets() ([]model.MinuteBucket, error)
	QueryAlerts() ([]model.Alert, error)
	Close() error
}

type inMemoryStore struct {
	transactions []persistedTransaction
	minutes      []model.MinuteBucket
	alerts       []model.Alert
}

type persistedTransaction struct {
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

func toPersistedTransactions(transactions []model.Transaction) []persistedTransaction {
	result := make([]persistedTransaction, 0, len(transactions))
	for _, txn := range transactions {
		pt := persistedTransaction{
			TxnKey:          txn.TxnKey,
			ServerID:        txn.ServerID,
			ServerVersion:   txn.ServerVersion,
			ServerFlavor:    txn.ServerFlavor,
			GTID:            txn.GTID,
			ThreadID:        txn.ThreadID,
			XID:             txn.XID,
			ActorUser:       txn.ActorUser,
			ActorHost:       txn.ActorHost,
			XAXID:           txn.XAXID,
			StartTime:       txn.StartTime,
			EndTime:         txn.EndTime,
			DurationMS:      txn.Duration.Milliseconds(),
			TotalRows:       int64(txn.TotalRows),
			EventCount:      int64(txn.EventCount),
			BinlogBytes:     txn.BinlogBytes,
			BinlogPathStart: txn.BinlogPathStart,
			BinlogPathEnd:   txn.BinlogPathEnd,
			PositionStart:   txn.PositionStart,
			PositionEnd:     txn.PositionEnd,
			Completeness:    txn.EffectiveCompleteness(),
			QuerySummary:    txn.QuerySummary,
			TableRows:       txn.Tables,
			Operations:      txn.Operations,
		}
		if txn.FullReplaySpan != nil {
			pt.FullBinlogPathStart = txn.FullReplaySpan.BinlogPathStart
			pt.FullBinlogPathEnd = txn.FullReplaySpan.BinlogPathEnd
			pt.FullPositionStart = txn.FullReplaySpan.PositionStart
			pt.FullPositionEnd = txn.FullReplaySpan.PositionEnd
			pt.FullBinlogBytes = txn.FullReplaySpan.BinlogBytes
		}
		if txn.QueryContext != nil {
			pt.QuerySQL = txn.QueryContext.SQL
			pt.QueryTruncated = txn.QueryContext.Truncated
			pt.QueryOriginalBytes = int64(txn.QueryContext.OriginalBytes)
		}
		result = append(result, pt)
	}
	return result
}

func cloneStringIntMap(src map[string]int) map[string]int {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]int, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func newInMemoryStore() analysisStore {
	return &inMemoryStore{}
}

func (s *inMemoryStore) Reset() error {
	s.transactions = nil
	s.minutes = nil
	s.alerts = nil
	return nil
}

func (s *inMemoryStore) RecordTransactions(transactions []persistedTransaction) error {
	for _, txn := range transactions {
		s.transactions = append(s.transactions, clonePersistedTransaction(txn))
	}
	return nil
}

func (s *inMemoryStore) RecordMinuteBuckets(buckets []model.MinuteBucket) error {
	for _, bucket := range buckets {
		s.minutes = append(s.minutes, cloneMinuteBucket(bucket))
	}
	return nil
}

func (s *inMemoryStore) RecordAlerts(alerts []model.Alert) error {
	s.alerts = make([]model.Alert, len(alerts))
	for i, alert := range alerts {
		s.alerts[i] = cloneAlert(alert)
	}
	return nil
}

func (s *inMemoryStore) Flush() error {
	return nil
}

func (s *inMemoryStore) QueryAllTransactions() ([]model.Transaction, error) {
	txns := buildTransactionsFromPersisted(s.transactions, false)
	sort.Slice(txns, func(i, j int) bool {
		if !txns[i].StartTime.Equal(txns[j].StartTime) {
			return txns[i].StartTime.Before(txns[j].StartTime)
		}
		return txns[i].TxnKey < txns[j].TxnKey
	})
	return txns, nil
}

func (s *inMemoryStore) QueryTopTransactions(limit int) ([]model.Transaction, error) {
	txns := buildTransactionsFromPersisted(s.transactions, false)
	sort.Slice(txns, func(i, j int) bool {
		if txns[i].TotalRows != txns[j].TotalRows {
			return txns[i].TotalRows > txns[j].TotalRows
		}
		return txns[i].TxnKey < txns[j].TxnKey
	})
	if limit > 0 && len(txns) > limit {
		txns = txns[:limit]
	}
	return txns, nil
}

func (s *inMemoryStore) ResolveTransactionQuerySQL(txnKeys []string) (map[string]string, error) {
	if len(txnKeys) == 0 {
		return nil, nil
	}
	wanted := make(map[string]struct{}, len(txnKeys))
	for _, key := range txnKeys {
		if key != "" {
			wanted[key] = struct{}{}
		}
	}
	resolved := make(map[string]string, len(wanted))
	for _, txn := range s.transactions {
		if _, ok := wanted[txn.TxnKey]; ok && txn.QuerySQL != "" {
			resolved[txn.TxnKey] = txn.QuerySQL
			delete(wanted, txn.TxnKey)
			if len(wanted) == 0 {
				break
			}
		}
	}
	return resolved, nil
}

func (s *inMemoryStore) QueryMinuteBuckets() ([]model.MinuteBucket, error) {
	minutes := make([]model.MinuteBucket, len(s.minutes))
	for i, bucket := range s.minutes {
		minutes[i] = cloneMinuteBucket(bucket)
	}
	sort.Slice(minutes, func(i, j int) bool {
		return minutes[i].Minute.Before(minutes[j].Minute)
	})
	return minutes, nil
}

func (s *inMemoryStore) QueryAlerts() ([]model.Alert, error) {
	alerts := make([]model.Alert, len(s.alerts))
	for i, alert := range s.alerts {
		alerts[i] = cloneAlert(alert)
	}
	sort.Slice(alerts, func(i, j int) bool {
		rank := func(a model.Alert) int {
			if a.Type == "large_transaction" {
				return 0
			}
			return 1
		}
		if rank(alerts[i]) != rank(alerts[j]) {
			return rank(alerts[i]) < rank(alerts[j])
		}
		if !alerts[i].Minute.Equal(alerts[j].Minute) {
			if alerts[i].Minute.IsZero() {
				return true
			}
			if alerts[j].Minute.IsZero() {
				return false
			}
			return alerts[i].Minute.Before(alerts[j].Minute)
		}
		if alerts[i].TxnKey != alerts[j].TxnKey {
			return alerts[i].TxnKey < alerts[j].TxnKey
		}
		if alerts[i].Type != alerts[j].Type {
			return alerts[i].Type < alerts[j].Type
		}
		return alerts[i].Message < alerts[j].Message
	})
	return alerts, nil
}

func (s *inMemoryStore) Close() error {
	return nil
}

func clonePersistedTransaction(txn persistedTransaction) persistedTransaction {
	return persistedTransaction{
		TxnKey:              txn.TxnKey,
		ServerID:            txn.ServerID,
		ServerVersion:       txn.ServerVersion,
		ServerFlavor:        txn.ServerFlavor,
		GTID:                txn.GTID,
		ThreadID:            txn.ThreadID,
		XID:                 txn.XID,
		ActorUser:           txn.ActorUser,
		ActorHost:           txn.ActorHost,
		XAXID:               txn.XAXID,
		StartTime:           txn.StartTime,
		EndTime:             txn.EndTime,
		DurationMS:          txn.DurationMS,
		TotalRows:           txn.TotalRows,
		EventCount:          txn.EventCount,
		BinlogBytes:         txn.BinlogBytes,
		BinlogPathStart:     txn.BinlogPathStart,
		BinlogPathEnd:       txn.BinlogPathEnd,
		PositionStart:       txn.PositionStart,
		PositionEnd:         txn.PositionEnd,
		Completeness:        txn.Completeness,
		FullBinlogPathStart: txn.FullBinlogPathStart,
		FullBinlogPathEnd:   txn.FullBinlogPathEnd,
		FullPositionStart:   txn.FullPositionStart,
		FullPositionEnd:     txn.FullPositionEnd,
		FullBinlogBytes:     txn.FullBinlogBytes,
		QuerySummary:        txn.QuerySummary,
		QuerySQL:            txn.QuerySQL,
		QueryTruncated:      txn.QueryTruncated,
		QueryOriginalBytes:  txn.QueryOriginalBytes,
		TableRows:           cloneStringIntMap(txn.TableRows),
		Operations:          cloneStringIntMap(txn.Operations),
	}
}

func cloneMinuteBucket(bucket model.MinuteBucket) model.MinuteBucket {
	return model.MinuteBucket{
		Minute:      bucket.Minute,
		TotalRows:   bucket.TotalRows,
		TxnCount:    bucket.TxnCount,
		EventCount:  bucket.EventCount,
		BinlogBytes: bucket.BinlogBytes,
		DDLCount:    bucket.DDLCount,
		TableRows:   cloneStringIntMap(bucket.TableRows),
	}
}

func cloneAlert(alert model.Alert) model.Alert {
	return model.Alert{
		Type:     alert.Type,
		Severity: alert.Severity,
		Message:  alert.Message,
		TxnKey:   alert.TxnKey,
		Minute:   alert.Minute,
		Details:  cloneStringAnyMap(alert.Details),
	}
}

func cloneStringAnyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func buildTransactionsFromPersisted(src []persistedTransaction, includeSQL bool) []model.Transaction {
	if len(src) == 0 {
		return nil
	}
	txns := make([]model.Transaction, len(src))
	for i, row := range src {
		txns[i] = buildTransactionFromPersisted(row, includeSQL)
	}
	return txns
}

func buildTransactionFromPersisted(row persistedTransaction, includeSQL bool) model.Transaction {
	txn := model.Transaction{
		TxnKey:          row.TxnKey,
		ServerID:        row.ServerID,
		ServerVersion:   row.ServerVersion,
		ServerFlavor:    row.ServerFlavor,
		GTID:            row.GTID,
		ThreadID:        row.ThreadID,
		XID:             row.XID,
		ActorUser:       row.ActorUser,
		ActorHost:       row.ActorHost,
		XAXID:           row.XAXID,
		StartTime:       row.StartTime,
		EndTime:         row.EndTime,
		Duration:        time.Duration(row.DurationMS) * time.Millisecond,
		TotalRows:       int(row.TotalRows),
		EventCount:      int(row.EventCount),
		BinlogBytes:     row.BinlogBytes,
		BinlogPathStart: row.BinlogPathStart,
		BinlogPathEnd:   row.BinlogPathEnd,
		PositionStart:   row.PositionStart,
		PositionEnd:     row.PositionEnd,
		Completeness:    row.Completeness,
		QuerySummary:    row.QuerySummary,
		Tables:          cloneStringIntMap(row.TableRows),
		Operations:      cloneStringIntMap(row.Operations),
	}
	if row.FullBinlogPathStart != "" {
		txn.FullReplaySpan = &model.TransactionReplaySpan{
			BinlogPathStart: row.FullBinlogPathStart,
			BinlogPathEnd:   row.FullBinlogPathEnd,
			PositionStart:   row.FullPositionStart,
			PositionEnd:     row.FullPositionEnd,
			BinlogBytes:     row.FullBinlogBytes,
		}
	}
	if row.QuerySummary != "" || row.QueryTruncated || row.QueryOriginalBytes > 0 {
		sql := ""
		if includeSQL {
			sql = row.QuerySQL
		}
		txn.QueryContext = &model.QueryContext{
			SQL:           sql,
			Truncated:     row.QueryTruncated,
			OriginalBytes: int(row.QueryOriginalBytes),
		}
	}
	return txn
}
