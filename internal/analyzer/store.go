// Package analyzer persists high-cardinality analysis results in DuckDB batches for Finalize-time queries.
// input: completed transaction snapshots, finalized minute buckets, generated alerts, and a DuckDB file path owned by internal callers.
// output: batched DuckDB writes plus query-backed reconstruction of transactions, minutes, alerts, and on-demand top-transaction SQL hydration.
// pos: internal result-store layer that decouples hot in-memory state from persisted finalize-time query workloads.
// note: if this file changes, update this header and module README.md.
package analyzer

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"binlogviz/internal/model"

	duckdb "github.com/marcboeker/go-duckdb"
)

const (
	DefaultBatchFlushRows  = 1000
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
	TxnKey             string
	StartTime          time.Time
	EndTime            time.Time
	DurationMS         int64
	TotalRows          int64
	EventCount         int64
	QuerySummary       string
	QuerySQL           string
	QueryTruncated     bool
	QueryOriginalBytes int64
	TableRows          map[string]int
	Operations         map[string]int
}

type transactionRow struct {
	TxnKey             string
	StartTime          time.Time
	EndTime            time.Time
	DurationMS         int64
	TotalRows          int64
	EventCount         int64
	QuerySummary       string
	QueryTruncated     bool
	QueryOriginalBytes int64
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
	Minute    time.Time
	TotalRows int64
	TxnCount  int64
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

// DuckDBStore persists analysis results in a temporary DuckDB database.
type DuckDBStore struct {
	path string
	db   *sql.DB

	batchRowThreshold  int
	batchByteThreshold int
	bufferedRows       int
	bufferedBytes      int

	transactionsBatch []transactionRow
	txnSQLBatch       []transactionSQLContextRow
	txnTablesBatch    []transactionTableRow
	txnOpsBatch       []transactionOperationRow
	minutesBatch      []minuteBucketRow
	minuteTablesBatch []minuteTableRow
	alertsBatch       []alertRow
}

// Path returns the underlying DuckDB file path.
func (s *DuckDBStore) Path() string {
	return s.path
}

// NewDuckDBStore opens or creates a DuckDB result store at path.
func NewDuckDBStore(path string, batchRows int) (*DuckDBStore, error) {
	if batchRows <= 0 {
		batchRows = DefaultBatchFlushRows
	}
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}

	store := &DuckDBStore{
		path:               path,
		db:                 db,
		batchRowThreshold:  batchRows,
		batchByteThreshold: defaultBatchFlushBytes,
	}
	if err := store.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *DuckDBStore) Reset() error {
	for _, stmt := range []string{
		"DELETE FROM alerts",
		"DELETE FROM minute_table_rows",
		"DELETE FROM minute_buckets",
		"DELETE FROM transaction_sql_contexts",
		"DELETE FROM transaction_operations",
		"DELETE FROM transaction_tables",
		"DELETE FROM transactions",
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	s.transactionsBatch = nil
	s.txnSQLBatch = nil
	s.txnTablesBatch = nil
	s.txnOpsBatch = nil
	s.minutesBatch = nil
	s.minuteTablesBatch = nil
	s.alertsBatch = nil
	s.bufferedRows = 0
	s.bufferedBytes = 0
	return nil
}

func (s *DuckDBStore) Close() error {
	return s.db.Close()
}

func (s *DuckDBStore) RecordTransactions(transactions []persistedTransaction) error {
	for _, txn := range transactions {
		s.transactionsBatch = append(s.transactionsBatch, transactionRow{
			TxnKey:             txn.TxnKey,
			StartTime:          txn.StartTime,
			EndTime:            txn.EndTime,
			DurationMS:         txn.DurationMS,
			TotalRows:          txn.TotalRows,
			EventCount:         txn.EventCount,
			QuerySummary:       txn.QuerySummary,
			QueryTruncated:     txn.QueryTruncated,
			QueryOriginalBytes: txn.QueryOriginalBytes,
		})
		s.bufferTopLevelRow(estimateStringBytes(txn.TxnKey) + estimateStringBytes(txn.QuerySummary) + 48)
		if txn.QuerySQL != "" {
			s.txnSQLBatch = append(s.txnSQLBatch, transactionSQLContextRow{
				TxnKey:             txn.TxnKey,
				QuerySQL:           txn.QuerySQL,
				QueryTruncated:     txn.QueryTruncated,
				QueryOriginalBytes: txn.QueryOriginalBytes,
			})
			s.bufferBytes(estimateStringBytes(txn.TxnKey) + estimateStringBytes(txn.QuerySQL) + 16)
		}

		for tableKey, rows := range txn.TableRows {
			s.txnTablesBatch = append(s.txnTablesBatch, transactionTableRow{
				TxnKey:   txn.TxnKey,
				TableKey: tableKey,
				Rows:     int64(rows),
			})
			s.bufferBytes(estimateStringBytes(txn.TxnKey) + estimateStringBytes(tableKey) + 8)
		}
		for operation, rows := range txn.Operations {
			s.txnOpsBatch = append(s.txnOpsBatch, transactionOperationRow{
				TxnKey:    txn.TxnKey,
				Operation: operation,
				Rows:      int64(rows),
			})
			s.bufferBytes(estimateStringBytes(txn.TxnKey) + estimateStringBytes(operation) + 8)
		}
	}
	return s.flushIfNeeded()
}

func (s *DuckDBStore) RecordMinuteBuckets(buckets []model.MinuteBucket) error {
	for _, bucket := range buckets {
		s.minutesBatch = append(s.minutesBatch, minuteBucketRow{
			Minute:    bucket.Minute,
			TotalRows: int64(bucket.TotalRows),
			TxnCount:  int64(bucket.TxnCount),
		})
		s.bufferTopLevelRow(24)

		tableNames := make([]string, 0, len(bucket.TableRows))
		for tableKey := range bucket.TableRows {
			tableNames = append(tableNames, tableKey)
		}
		sort.Strings(tableNames)
		for _, tableKey := range tableNames {
			s.minuteTablesBatch = append(s.minuteTablesBatch, minuteTableRow{
				Minute:   bucket.Minute,
				TableKey: tableKey,
				Rows:     int64(bucket.TableRows[tableKey]),
			})
			s.bufferBytes(estimateStringBytes(tableKey) + 16)
		}
	}
	return s.flushIfNeeded()
}

func (s *DuckDBStore) RecordAlerts(alerts []model.Alert) error {
	for _, alert := range alerts {
		detailsJSON, err := json.Marshal(alert.Details)
		if err != nil {
			return err
		}
		s.alertsBatch = append(s.alertsBatch, alertRow{
			Type:        alert.Type,
			Severity:    alert.Severity,
			TxnKey:      alert.TxnKey,
			Minute:      alert.Minute,
			Message:     alert.Message,
			DetailsJSON: string(detailsJSON),
		})
		s.bufferTopLevelRow(estimateStringBytes(alert.Type) + estimateStringBytes(alert.Severity) + estimateStringBytes(alert.TxnKey) + estimateStringBytes(alert.Message) + len(detailsJSON) + 16)
	}
	return s.flushIfNeeded()
}

func (s *DuckDBStore) Flush() error {
	if len(s.transactionsBatch) > 0 {
		if err := s.appendRows("transactions", func(app *duckdb.Appender) error {
			for _, row := range s.transactionsBatch {
				if err := app.AppendRow(row.TxnKey, row.StartTime, row.EndTime, row.DurationMS, row.TotalRows, row.EventCount, row.QuerySummary, row.QueryTruncated, row.QueryOriginalBytes); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		s.transactionsBatch = nil
	}
	if len(s.txnTablesBatch) > 0 {
		if err := s.appendRows("transaction_tables", func(app *duckdb.Appender) error {
			for _, row := range s.txnTablesBatch {
				if err := app.AppendRow(row.TxnKey, row.TableKey, row.Rows); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		s.txnTablesBatch = nil
	}
	if len(s.txnSQLBatch) > 0 {
		if err := s.appendRows("transaction_sql_contexts", func(app *duckdb.Appender) error {
			for _, row := range s.txnSQLBatch {
				if err := app.AppendRow(row.TxnKey, row.QuerySQL, row.QueryTruncated, row.QueryOriginalBytes); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		s.txnSQLBatch = nil
	}
	if len(s.txnOpsBatch) > 0 {
		if err := s.appendRows("transaction_operations", func(app *duckdb.Appender) error {
			for _, row := range s.txnOpsBatch {
				if err := app.AppendRow(row.TxnKey, row.Operation, row.Rows); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		s.txnOpsBatch = nil
	}
	if len(s.minutesBatch) > 0 {
		if err := s.appendRows("minute_buckets", func(app *duckdb.Appender) error {
			for _, row := range s.minutesBatch {
				if err := app.AppendRow(row.Minute, row.TotalRows, row.TxnCount); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		s.minutesBatch = nil
	}
	if len(s.minuteTablesBatch) > 0 {
		if err := s.appendRows("minute_table_rows", func(app *duckdb.Appender) error {
			for _, row := range s.minuteTablesBatch {
				if err := app.AppendRow(row.Minute, row.TableKey, row.Rows); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		s.minuteTablesBatch = nil
	}
	if len(s.alertsBatch) > 0 {
		if err := s.appendRows("alerts", func(app *duckdb.Appender) error {
			for _, row := range s.alertsBatch {
				if err := app.AppendRow(row.Type, row.Severity, row.TxnKey, zeroTimeToNil(row.Minute), row.Message, row.DetailsJSON); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		s.alertsBatch = nil
	}

	s.bufferedRows = 0
	s.bufferedBytes = 0
	return nil
}

func (s *DuckDBStore) QueryAllTransactions() ([]model.Transaction, error) {
	baseRows, err := s.queryTransactions(`
SELECT txn_key, start_time, end_time, duration_ms, total_rows, event_count, query_summary, query_truncated, query_original_bytes
FROM transactions
ORDER BY start_time ASC, txn_key ASC`)
	if err != nil {
		return nil, err
	}
	return s.hydrateTransactions(baseRows)
}

func (s *DuckDBStore) QueryTopTransactions(limit int) ([]model.Transaction, error) {
	query := `
SELECT txn_key, start_time, end_time, duration_ms, total_rows, event_count, query_summary, query_truncated, query_original_bytes
FROM transactions
ORDER BY total_rows DESC, txn_key ASC`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	baseRows, err := s.queryTransactions(query)
	if err != nil {
		return nil, err
	}
	return s.hydrateTransactions(baseRows)
}

func (s *DuckDBStore) ResolveTransactionQuerySQL(txnKeys []string) (map[string]string, error) {
	if len(txnKeys) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(txnKeys))
	wanted := make(map[string]struct{}, len(txnKeys))
	for _, key := range txnKeys {
		if key != "" {
			if _, ok := wanted[key]; !ok {
				wanted[key] = struct{}{}
				keys = append(keys, key)
			}
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(keys)), ",")
	args := make([]any, 0, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	rows, err := s.db.Query(`
SELECT txn_key, query_sql
FROM transaction_sql_contexts
WHERE txn_key IN (`+placeholders+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	resolved := make(map[string]string, len(wanted))
	for rows.Next() {
		var txnKey string
		var sqlText string
		if err := rows.Scan(&txnKey, &sqlText); err != nil {
			return nil, err
		}
		resolved[txnKey] = sqlText
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return resolved, nil
}

func (s *DuckDBStore) QueryMinuteBuckets() ([]model.MinuteBucket, error) {
	rows, err := s.db.Query(`
SELECT minute, total_rows, txn_count
FROM minute_buckets
ORDER BY minute ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	buckets := make([]model.MinuteBucket, 0)
	indexByMinute := make(map[time.Time]int)
	for rows.Next() {
		var minute time.Time
		var totalRows, txnCount int64
		if err := rows.Scan(&minute, &totalRows, &txnCount); err != nil {
			return nil, err
		}
		indexByMinute[minute] = len(buckets)
		buckets = append(buckets, model.MinuteBucket{
			Minute:    minute,
			TotalRows: int(totalRows),
			TxnCount:  int(txnCount),
			TableRows: make(map[string]int),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	tableRows, err := s.db.Query(`
SELECT minute, table_key, rows
FROM minute_table_rows
ORDER BY minute ASC, table_key ASC`)
	if err != nil {
		return nil, err
	}
	defer tableRows.Close()

	for tableRows.Next() {
		var minute time.Time
		var tableKey string
		var rowsCount int64
		if err := tableRows.Scan(&minute, &tableKey, &rowsCount); err != nil {
			return nil, err
		}
		idx, ok := indexByMinute[minute]
		if !ok {
			continue
		}
		buckets[idx].TableRows[tableKey] = int(rowsCount)
	}
	if err := tableRows.Err(); err != nil {
		return nil, err
	}

	return buckets, nil
}

func (s *DuckDBStore) QueryAlerts() ([]model.Alert, error) {
	rows, err := s.db.Query(`
SELECT type, severity, txn_key, minute, message, details_json
FROM alerts`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	alerts := make([]model.Alert, 0)
	for rows.Next() {
		var row alertRow
		var minute sql.NullTime
		if err := rows.Scan(&row.Type, &row.Severity, &row.TxnKey, &minute, &row.Message, &row.DetailsJSON); err != nil {
			return nil, err
		}
		alert := model.Alert{
			Type:     row.Type,
			Severity: row.Severity,
			TxnKey:   row.TxnKey,
			Message:  row.Message,
			Details:  map[string]any{},
		}
		if minute.Valid {
			alert.Minute = minute.Time
		}
		if row.DetailsJSON != "" {
			if err := json.Unmarshal([]byte(row.DetailsJSON), &alert.Details); err != nil {
				return nil, err
			}
		}
		alerts = append(alerts, alert)
	}
	if err := rows.Err(); err != nil {
		return nil, err
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

func (s *DuckDBStore) tableExists(table string) bool {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?`, table).Scan(&count)
	return err == nil && count == 1
}

func (s *DuckDBStore) mustCountRows(t interface{ Fatalf(string, ...any) }, table string) int {
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
		t.Fatalf("count rows for %s: %v", table, err)
	}
	return count
}

func (s *DuckDBStore) initSchema() error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS transactions (
			txn_key VARCHAR,
			start_time TIMESTAMP,
			end_time TIMESTAMP,
			duration_ms BIGINT,
			total_rows BIGINT,
			event_count BIGINT,
			query_summary VARCHAR,
			query_truncated BOOLEAN,
			query_original_bytes BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS transaction_tables (
			txn_key VARCHAR,
			table_key VARCHAR,
			rows BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS transaction_sql_contexts (
			txn_key VARCHAR PRIMARY KEY,
			query_sql VARCHAR,
			query_truncated BOOLEAN,
			query_original_bytes BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS transaction_operations (
			txn_key VARCHAR,
			operation VARCHAR,
			rows BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS minute_buckets (
			minute TIMESTAMP,
			total_rows BIGINT,
			txn_count BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS minute_table_rows (
			minute TIMESTAMP,
			table_key VARCHAR,
			rows BIGINT
		)`,
		`CREATE TABLE IF NOT EXISTS alerts (
			type VARCHAR,
			severity VARCHAR,
			txn_key VARCHAR,
			minute TIMESTAMP,
			message VARCHAR,
			details_json VARCHAR
		)`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *DuckDBStore) queryTransactions(query string) ([]transactionRow, error) {
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]transactionRow, 0)
	for rows.Next() {
		var row transactionRow
		if err := rows.Scan(
			&row.TxnKey,
			&row.StartTime,
			&row.EndTime,
			&row.DurationMS,
			&row.TotalRows,
			&row.EventCount,
			&row.QuerySummary,
			&row.QueryTruncated,
			&row.QueryOriginalBytes,
		); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *DuckDBStore) hydrateTransactions(baseRows []transactionRow) ([]model.Transaction, error) {
	if len(baseRows) == 0 {
		return nil, nil
	}

	txns := make([]model.Transaction, len(baseRows))
	indexByTxnKey := make(map[string]int, len(baseRows))
	for i, row := range baseRows {
		txns[i] = model.Transaction{
			TxnKey:       row.TxnKey,
			StartTime:    row.StartTime,
			EndTime:      row.EndTime,
			Duration:     time.Duration(row.DurationMS) * time.Millisecond,
			TotalRows:    int(row.TotalRows),
			EventCount:   int(row.EventCount),
			QuerySummary: row.QuerySummary,
			Tables:       make(map[string]int),
			Operations:   make(map[string]int),
		}
		if row.QuerySummary != "" || row.QueryTruncated || row.QueryOriginalBytes > 0 {
			txns[i].QueryContext = &model.QueryContext{
				SQL:           "",
				Truncated:     row.QueryTruncated,
				OriginalBytes: int(row.QueryOriginalBytes),
			}
		}
		indexByTxnKey[row.TxnKey] = i
	}

	if err := s.fillTransactionMaps(indexByTxnKey, txns); err != nil {
		return nil, err
	}
	return txns, nil
}

func (s *DuckDBStore) fillTransactionMaps(indexByTxnKey map[string]int, txns []model.Transaction) error {
	tableRows, err := s.db.Query(`
SELECT txn_key, table_key, rows
FROM transaction_tables
ORDER BY txn_key ASC, table_key ASC`)
	if err != nil {
		return err
	}
	defer tableRows.Close()

	for tableRows.Next() {
		var txnKey, tableKey string
		var rowsCount int64
		if err := tableRows.Scan(&txnKey, &tableKey, &rowsCount); err != nil {
			return err
		}
		if idx, ok := indexByTxnKey[txnKey]; ok {
			txns[idx].Tables[tableKey] = int(rowsCount)
		}
	}
	if err := tableRows.Err(); err != nil {
		return err
	}

	opRows, err := s.db.Query(`
SELECT txn_key, operation, rows
FROM transaction_operations
ORDER BY txn_key ASC, operation ASC`)
	if err != nil {
		return err
	}
	defer opRows.Close()

	for opRows.Next() {
		var txnKey, operation string
		var rowsCount int64
		if err := opRows.Scan(&txnKey, &operation, &rowsCount); err != nil {
			return err
		}
		if idx, ok := indexByTxnKey[txnKey]; ok {
			txns[idx].Operations[operation] = int(rowsCount)
		}
	}
	return opRows.Err()
}

func (s *DuckDBStore) appendRows(table string, fill func(app *duckdb.Appender) error) error {
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Raw(func(raw any) error {
		driverConn, ok := raw.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type %T", raw)
		}
		appender, err := duckdb.NewAppenderFromConn(driverConn, "", table)
		if err != nil {
			return err
		}
		defer appender.Close()

		if err := fill(appender); err != nil {
			return err
		}
		return appender.Flush()
	})
}

func (s *DuckDBStore) flushIfNeeded() error {
	if s.bufferedRows >= s.batchRowThreshold || s.bufferedBytes >= s.batchByteThreshold {
		return s.Flush()
	}
	return nil
}

func (s *DuckDBStore) bufferTopLevelRow(bytes int) {
	s.bufferedRows++
	s.bufferedBytes += bytes
}

func (s *DuckDBStore) bufferBytes(bytes int) {
	s.bufferedBytes += bytes
}

func estimateStringBytes(v string) int {
	return len(v)
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
			TxnKey:       txn.TxnKey,
			StartTime:    txn.StartTime,
			EndTime:      txn.EndTime,
			DurationMS:   txn.Duration.Milliseconds(),
			TotalRows:    int64(txn.TotalRows),
			EventCount:   int64(txn.EventCount),
			QuerySummary: txn.QuerySummary,
			TableRows:    cloneStringIntMap(txn.Tables),
			Operations:   cloneStringIntMap(txn.Operations),
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

func limitTables(tables []model.TableStats, limit int) []model.TableStats {
	if limit <= 0 || len(tables) <= limit {
		return tables
	}
	limited := make([]model.TableStats, limit)
	copy(limited, tables[:limit])
	return limited
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
		TxnKey:             txn.TxnKey,
		StartTime:          txn.StartTime,
		EndTime:            txn.EndTime,
		DurationMS:         txn.DurationMS,
		TotalRows:          txn.TotalRows,
		EventCount:         txn.EventCount,
		QuerySummary:       txn.QuerySummary,
		QuerySQL:           txn.QuerySQL,
		QueryTruncated:     txn.QueryTruncated,
		QueryOriginalBytes: txn.QueryOriginalBytes,
		TableRows:          cloneStringIntMap(txn.TableRows),
		Operations:         cloneStringIntMap(txn.Operations),
	}
}

func cloneMinuteBucket(bucket model.MinuteBucket) model.MinuteBucket {
	return model.MinuteBucket{
		Minute:    bucket.Minute,
		TotalRows: bucket.TotalRows,
		TxnCount:  bucket.TxnCount,
		TableRows: cloneStringIntMap(bucket.TableRows),
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
		txns[i] = model.Transaction{
			TxnKey:       row.TxnKey,
			StartTime:    row.StartTime,
			EndTime:      row.EndTime,
			Duration:     time.Duration(row.DurationMS) * time.Millisecond,
			TotalRows:    int(row.TotalRows),
			EventCount:   int(row.EventCount),
			QuerySummary: row.QuerySummary,
			Tables:       cloneStringIntMap(row.TableRows),
			Operations:   cloneStringIntMap(row.Operations),
		}
		if row.QuerySummary != "" || row.QueryTruncated || row.QueryOriginalBytes > 0 {
			sql := ""
			if includeSQL {
				sql = row.QuerySQL
			}
			txns[i].QueryContext = &model.QueryContext{
				SQL:           sql,
				Truncated:     row.QueryTruncated,
				OriginalBytes: int(row.QueryOriginalBytes),
			}
		}
	}
	return txns
}
