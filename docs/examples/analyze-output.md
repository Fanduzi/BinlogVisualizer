# BinlogViz Report

## Workload Summary

| Field | Value |
|---|---|
| Total Transactions | 4 |
| Partial Transactions | 0 |
| Unknown Transactions | 0 |
| Total Rows | 4 |
| Total Events | 22 |
| Time Range | 2026-03-15 14:10:26 UTC — 2026-03-15 14:10:26 UTC |
| Timestamps | UTC (binlog) |
| Duration | 0s |
| Format | ROW |
| Primary key | primary key presence unknown (binlog_row_metadata is not FULL) |
| Ignored Query-DML Events | 0 |

## Top Tables

| Schema | Table | Total Rows | Inserts | Updates | Deletes | Transactions |
|---|---|---:|---:|---:|---:|---:|
| testdb | users | 4 | 2 | 1 | 1 | 4 |

## Top Threads (by rows)

| # | thread_id | server_id | schema | rows | events | bytes | txns | share |
|---|---:|---:|---|---:|---:|---:|---:|---:|
| 1 | 3 | 1 | testdb | 4 | 4 | 1,092 | 4 | 100.0% |

## Top Transactions

| # | Txn Key | Completeness | Full Replay | Rows | Bytes | Duration | File:Position | Tables | Operations |
|---|---|---|---|---:|---:|---|---|---|---|
| 1 | txn-4 | complete | yes | 1 | 289 | 0s | cmd/binlogviz/testdata/minimal.binlog:897-1186 | testdb.users | UPDATE |
| 2 | txn-2 | complete | yes | 1 | 269 | 0s | cmd/binlogviz/testdata/minimal.binlog:361-630 | testdb.users | INSERT |
| 3 | txn-3 | complete | yes | 1 | 267 | 0s | cmd/binlogviz/testdata/minimal.binlog:630-897 | testdb.users | INSERT |
| 4 | txn-5 | complete | yes | 1 | 267 | 0s | cmd/binlogviz/testdata/minimal.binlog:1186-1453 | testdb.users | DELETE |

`mysqlbinlog_cmd` for `txn-4`:

```text
mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=897 --stop-position=1186 cmd/binlogviz/testdata/minimal.binlog
```

`txn-4`: server_id=1 thread_id=3 xid=8

`mysqlbinlog_cmd` for `txn-2`:

```text
mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=361 --stop-position=630 cmd/binlogviz/testdata/minimal.binlog
```

`txn-2`: server_id=1 thread_id=3 xid=6

`mysqlbinlog_cmd` for `txn-3`:

```text
mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=630 --stop-position=897 cmd/binlogviz/testdata/minimal.binlog
```

`txn-3`: server_id=1 thread_id=3 xid=7

`mysqlbinlog_cmd` for `txn-5`:

```text
mysqlbinlog --base64-output=DECODE-ROWS -v --start-position=1186 --stop-position=1453 cmd/binlogviz/testdata/minimal.binlog
```

`txn-5`: server_id=1 thread_id=3 xid=9


## Hot Rows

UPDATE and DELETE row images, by primary key.

hot-row tracking unavailable for testdb.users: primary key is not in the binlog (binlog_row_metadata is not FULL)

## Busiest Minutes

Rows in that minute, by table.

| Time | Rows | Transactions | Driving tables |
|---|---:|---:|---|
| 2026-03-15 14:10:00 UTC | 4 | 4 | testdb.users 4 |

## Replica Apply Delay

commit timestamps unavailable

## Minute Activity

| Time | Rows | Transactions | Driving tables |
|---|---:|---:|---|
| 2026-03-15 14:10:00 UTC | 4 | 4 | testdb.users 4 |

## DDL Timeline

| Time | Operation | Object | Statement | File:Position |
|---|---|---|---|---|
| 2026-03-15 14:10:26 UTC | CREATE TABLE | testdb.users | CREATE TABLE users ( id INT PRIMARY KEY, name VARCHAR(100)) | cmd/binlogviz/testdata/minimal.binlog:219-361 |

**CREATE TABLE** `testdb.users`

server_id=1 thread_id=2 GTID unavailable

txn start `cmd/binlogviz/testdata/minimal.binlog:154`

stop before this DDL (replays earlier events, excludes this DDL)

```
mysqlbinlog --stop-position=154 cmd/binlogviz/testdata/minimal.binlog
```

