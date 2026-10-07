# Test Fixtures

## minimal.binlog

A minimal MySQL 5.7 ROW-format binlog file used for end-to-end integration testing.

### Contents

The fixture contains the following operations on `testdb.users` table:
- `CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))`
- `INSERT INTO users VALUES (1, 'alice')`
- `INSERT INTO users VALUES (2, 'bob')`
- `UPDATE users SET name = 'alice_updated' WHERE id = 1`
- `DELETE FROM users WHERE id = 2`

### Regeneration

To regenerate this fixture, run:

```bash
cd internal/binlog/testdata
./create_fixture.sh
```

Requirements:
- Docker

The script:
1. Starts a MySQL 5.7 container with ROW binlog format
2. Creates the test schema and data
3. Extracts the binlog file
4. Cleans up the container

### File Details

- **Size**: ~1.5KB
- **Format**: MySQL 5.7 ROW binlog
- **Server ID**: 1

## mysql-8.0.46-flush-tables.binlog

A MySQL 8.0.46 ROW+GTID dialect fixture used to admit `FLUSH TABLES` as ADMIN.

### Contents

After schema setup in an earlier binlog, this file contains:

- Format Description from MySQL 8.0.46
- One GTID-started non-explicit group whose only work is `FLUSH TABLES`
- The next GTID: `BEGIN`, one `INSERT` ROW image on `testdb.users`, and `XID`

### Regeneration

To regenerate this fixture, run:

```bash
cd internal/binlog/testdata
bash ./create_mysql_8.0.46_flush_tables.sh
```

Requirements:

- Docker
- `mysql:8.0.46` image

The script:

1. Starts MySQL 8.0.46 with ROW binlog format and GTID
2. Creates `testdb.users` in an earlier file
3. Rotates, then writes `FLUSH TABLES` and one `INSERT`
4. Extracts that rotated file
5. Cleans up the container

### File Details

- **Format**: MySQL 8.0.46 ROW binlog with GTID
- **Flavor**: mysql
- **Server version**: 8.0.46
- **Server ID**: 1

## mysql-8.0.46-check-table.binlog and mysql-8.0.46-set-role.binlog

The MySQL 8.0.46 ROW+GTID fixtures `mysql-8.0.46-check-table.binlog` and `mysql-8.0.46-set-role.binlog` admit `CHECK TABLE` and `SET ROLE ALL` as ADMIN. Stock mysqld 8.0.46 does not write `CHECK TABLE` or `SET ROLE` into the binary log. Each file is a real rotated ROW+GTID binlog. The maintenance group was logged as `FLUSH TABLES`, then the query text was rewritten to the target statement. Event size, end position, and CRC32 are recomputed. Format Description flavor stays mysql and version stays 8.0.46. One business `INSERT` follows each maintenance group.

### Contents

After schema setup in an earlier binlog, each file contains:

- Format Description from MySQL 8.0.46
- One GTID-started non-explicit group whose only work is the maintenance statement
  - `mysql-8.0.46-check-table.binlog`: `CHECK TABLE testdb.users`
  - `mysql-8.0.46-set-role.binlog`: `SET ROLE ALL`
- The next GTID: `BEGIN`, one `INSERT` ROW image on `testdb.users`, and `XID`

### Regeneration

```bash
cd internal/binlog/testdata
bash ./create_mysql_8.0.46_check_table_set_role.sh
```

Requirements:

- Docker
- `mysql:8.0.46` image
- python3

The script:

1. Starts MySQL 8.0.46 with ROW binlog format and GTID
2. Creates `testdb.users` in an earlier file
3. Rotates twice, and each time writes `FLUSH TABLES` and one `INSERT`
4. Rewrites that `FLUSH TABLES` query to `CHECK TABLE testdb.users` or `SET ROLE ALL`
5. Extracts the rotated files
6. Cleans up the container

### File Details

- **Format**: MySQL 8.0.46 ROW binlog with GTID
- **Flavor**: mysql
- **Server version**: 8.0.46
- **Server ID**: 1

## mysql-8.0.46-open-begin-dml.binlog

A MySQL 8.0.46 ROW+GTID fixture for an explicit `BEGIN` that wrote row images and never `COMMIT` or `ROLLBACK`, followed by a later business GTID.

### Contents

After schema setup in an earlier binlog, this file contains:

- Format Description from MySQL 8.0.46
- One GTID-started group: explicit `BEGIN`, then two `INSERT` row images on `testdb.users` (`alice`, `alice2`)
- No `XID`, `COMMIT`, or `ROLLBACK` for that group
- A later business GTID: `BEGIN`, one `INSERT` (`bob`), and `XID`

InnoDB writes a transaction at `COMMIT`, so mysqld does not emit this shape on its own. The regen script records a normal 8.0.46 file and drops the first group's `XID` event. Every remaining event, including its CRC32, is unmodified server bytes. The error-line duration is the wall clock from the open group's first event to the later GTID (`4s` in this file). It is not lock-contention evidence. MySQL stamps that GTID event at commit time, two seconds after the group's first event. The last row shares that commit second, so an EOF prefix reports group duration `2s`.

An EOF-open reading is a prefix of this file that stops at the later GTID. Analyze of that prefix exits 0 and JSON keeps `diagnostics.open_dml_groups`. The full file exits 1.

### Regeneration

```bash
cd internal/binlog/testdata
bash ./create_mysql_8.0.46_open_begin_dml.sh
```

Requirements:

- Docker
- python3
- `mysql:8.0.46` image

### File Details

- **Format**: MySQL 8.0.46 ROW binlog with GTID
- **Flavor**: mysql
- **Server version**: 8.0.46
- **Server ID**: 1

## mysql-8.0.46-committed-duration.binlog

A MySQL 8.0.46 ROW+GTID fixture with one committed transaction whose wall duration is multiple seconds, plus one committed sub-second insert.

### Contents

After schema setup in an earlier binlog, this file contains:

- Format Description from MySQL 8.0.46
- One GTID-started group: explicit `BEGIN`, one `INSERT` on `testdb.users` (`alice`), `XID`. `SELECT SLEEP(2)` runs between the insert and `COMMIT` and is not logged
- One following autocommit `INSERT` (`bob`) with `BEGIN` and `XID` in the same second

MySQL writes the GTID event at commit and stamps that event and the `XID` with the commit second. `BEGIN` and the row image keep the statement-start second. In this file that span is `2s` (`1s-10s`). The following insert stays in `<1s`. Nothing in the file is a lock wait, and the regen script does not rewrite timestamps.

### Regeneration

```bash
cd internal/binlog/testdata
bash ./create_mysql_8.0.46_committed_duration.sh
```

Requirements:

- Docker
- `mysql:8.0.46` image

The script sleeps two seconds. The checked-in file's span is 2s. Regenerating still passes while that span stays above 1s and below 10s.

### File Details

- **Format**: MySQL 8.0.46 ROW binlog with GTID
- **Flavor**: mysql
- **Server version**: 8.0.46
- **Server ID**: 1

## mysql80_transaction_payload.binlog

A MySQL 8.0.36 ROW binlog with `binlog_transaction_compression=ON`. Used to prove transaction-payload expand through `ParseFiles`.

### Contents

- Flavor / version: MySQL 8.0.36
- `CREATE TABLE testdb.users (id INT PRIMARY KEY, name VARCHAR(100))` as an independent GTID/QUERY group
- One compressed DML transaction: `INSERT` alice, `UPDATE` to alice_updated, `DELETE`
- The compressed DML is one `TRANSACTION_PAYLOAD` event at file offsets `[509, 725)` (216 bytes). The opening GTID is the previous file event at `[430, 509)`. Inner uncompressed `LogPos` is 0 and is not a file offset. The retained transaction reports the wrapper span once.

### Regeneration

```bash
cd internal/binlog/testdata
./create_mysql80_transaction_payload.sh
```

Requirements:
- Docker
- Image `mysql:8.0.36` (override with `MYSQL_IMAGE`)

The script rotates away bootstrap binlogs, records `SHOW MASTER STATUS`, writes the compressed transaction into that file, then copies it. Do not copy an earlier binlog: MySQL 8.0 bootstrap writes a large compressed payload that is not this fixture.

## mysql-8.0.46-index-ddl.binlog

A MySQL 8.0.46 ROW+GTID file where index DDL names a table that is not the index, and one statement names `idxbug.widgets` while `sessiondb` is selected.

### Contents

- `CREATE TABLE idxbug.orders`, one insert, `CREATE INDEX idx_customer ON idxbug.orders (customer)`
- After `USE idxbug`: `CREATE UNIQUE INDEX` and `CREATE FULLTEXT INDEX` on `orders`, then `DROP INDEX idx_customer ON orders`
- After `USE sessiondb`: `CREATE TABLE idxbug.widgets` and `CREATE INDEX idx_w ON idxbug.widgets (id)`
- A following insert into `idxbug.orders`

The index statements are ordinary `Query` events. The table is the identifier after `ON`. The session database is not the table.

### File Details

- **Format**: MySQL 8.0.46 ROW binlog with GTID
- **Flavor**: mysql
- **Server version**: 8.0.46
- **Server ID**: 1

## mysql-8.0.46-dml-minimal.binlog and mysql-8.0.46-dml-full.binlog

MySQL 8.0.46 ROW+GTID files for a bad DELETE buried in INSERT traffic on `shop.orders`, plus a multi-row UPDATE. The paired `.mysqlbinlog.txt` is `mysqlbinlog -v --base64-output=DECODE-ROWS` for the same file. `minimal` was recorded with `binlog_row_metadata=MINIMAL`. `full` was recorded with `binlog_row_metadata=FULL`.

### Contents

`shop.orders` is `id INT UNSIGNED` primary key, nullable `qty INT`, `price DECIMAL(10,2)`, `note VARCHAR(64)`, `created_at DATETIME(6)`, `updated_at TIMESTAMP(6)`, `payload JSON`, `raw BLOB`. Session `time_zone` is `+00:00`.

- One transaction inserts ids 1, 2, 3 and ids 1000–1039
- The next inserts id 3000000000 (`qty` NULL, `price` 19.99, `note` `it's "bad"`, JSON `{"n":1,"sku":"Z"}`, blob `DEADBEEF0102`) and deletes ids 2 and 3000000000
- The next updates `note='changed', qty=qty+1` for ids 1 and 3

### Regeneration

```bash
cd internal/binlog/testdata
bash ./create_mysql_8.0.46_dml_rows.sh
```

Requires Docker and `mysql:8.0.46`. The script writes both binlogs and refreshes the `.mysqlbinlog.txt` decodes.

## mysql-8.0.46-no-pk-full.binlog and mysql-8.0.46-no-pk-minimal.binlog

MySQL 8.0.46 ROW+GTID files for primary-key presence. `full` was recorded with `binlog_row_metadata=FULL`. `minimal` was recorded with `binlog_row_metadata=MINIMAL`. The paired `.mysqlbinlog.txt` is `mysqlbinlog -v --base64-output=DECODE-ROWS --print-table-metadata`.

### Contents

- `shop.orders`: `id INT UNSIGNED` primary key, plus one UPDATE
- `shop.prefixed`: `PRIMARY KEY (sku(8))`, plus one UPDATE
- `shop.heap`: no primary key, three INSERT rows, two UPDATE rows, one DELETE row
- `shop.log`: no primary key, two INSERT rows, one UPDATE row
- `shop.scratch`: no primary key, two INSERT rows, no UPDATE or DELETE

On the FULL file, `orders` and `prefixed` carry a primary-key field. `heap`, `log`, and `scratch` have column names and no primary-key field. The MINIMAL file has neither column names nor a primary-key field.

### Regeneration

```bash
cd internal/binlog/testdata
bash ./create_mysql_8.0.46_no_pk.sh
```

Requires Docker and `mysql:8.0.46`.

## mysql-8.0.46-hot-rows-full.binlog and mysql-8.0.46-hot-rows-minimal.binlog

MySQL 8.0.46 ROW+GTID files for per-row primary-key ranking. `full` was recorded with `binlog_row_metadata=FULL`. `minimal` was recorded with `binlog_row_metadata=MINIMAL`. Both use `binlog_row_image=FULL`. Event headers for the DML are `2026-10-06 14:00:01` through `14:00:14` via `SET TIMESTAMP`. The paired `.mysqlbinlog.txt` is `mysqlbinlog -v --base64-output=DECODE-ROWS --print-table-metadata`.

### Contents

- `shop.counters`: primary key is `id` (not column `@1` / `label`). `id=7` is updated twice in one transaction at 14:00:01, then once each at 14:00:02 through 14:00:06 (7 touches, 6 transactions). `id=8` is updated once at 14:00:07 and once at 14:00:08. `id=9` is deleted at 14:00:09. An INSERT of `id=4` at 14:00:14 is not a hot-row touch.
- `shop.inventory`: primary key is `(sku, wh)`. `sku=BOLT, wh=1` is updated at 14:00:10, 14:00:11, and 14:00:12. `sku=NUT, wh=2` is updated once in the 14:00:12 transaction.
- `shop.heap`: no primary key, twelve updates in one transaction at 14:00:13. It is not ranked. On the MINIMAL file the key is unknown, so the report says hot-row tracking is unavailable and does not guess from `@1`.

### Regeneration

```bash
cd internal/binlog/testdata
bash ./create_mysql_8.0.46_hot_rows.sh
```

Requires Docker and `mysql:8.0.46`.

## mysql-8.0.46-busiest-minute.binlog

A MySQL 8.0.46 ROW+GTID file whose busiest minute is not the window's hottest table. `binlog_row_metadata=FULL`. Event headers are `2026-03-15 14:00` through `14:03` and `14:05` via `SET TIMESTAMP`.

### Contents

- `shop.catalog`: 20-row inserts at 14:00, 14:01, 14:02, and 14:03, plus 2 rows at 14:05 (82 rows, 5 transactions)
- `shop.orders`: 15 two-row inserts at 14:05 (30 rows)

The window leader is `shop.catalog`. The 14:05 minute is 32 rows: `shop.orders` 30 and `shop.catalog` 2. The largest transaction is a 20-row catalog insert.

### Regeneration

```bash
cd internal/binlog/testdata
bash ./create_mysql_8.0.46_busiest_minute.sh
```

Requires Docker and `mysql:8.0.46`. The checked-in file was recorded on mysqld 8.0.46-0ubuntu0.24.04.4 with the same SQL. A regeneration gets a new GTID UUID; tests assert row counts and timestamps, not that UUID.

## mysql-8.0.46-drop-table.binlog

A MySQL 8.0.46 ROW+GTID fixture for an accidental `DROP TABLE` between two ordinary DML transactions. `GTID_MODE=ON`.

### Contents

After schema setup in an earlier binlog, this file contains:

- One GTID-started insert of `before-drop` into `shop.orders`
- The next GTID: `DROP TABLE shop.orders`
- The next GTID: an insert of `after-drop` into `shop.audit`

`mysql-8.0.46-drop-table.mysqlbinlog.txt` is `mysqlbinlog -v --base64-output=DECODE-ROWS` of this file. `mysql-8.0.46-drop-table.stop.mysqlbinlog.txt` is the same command with `--stop-position` at the DROP transaction's GTID event start. That stop includes `before-drop` and excludes `DROP TABLE` and `after-drop`.

### Regeneration

```bash
cd internal/binlog/testdata
bash ./create_mysql_8.0.46_drop_table.sh
```

Requires Docker and `mysql:8.0.46`. The checked-in file was recorded on mysqld 8.0.46-0ubuntu0.24.04.4 with the same SQL. A regeneration gets a new GTID UUID; tests read the checked-in mysqlbinlog decode instead of hard-coding that UUID.

## mariadb-10.11.14-dml.binlog

A MariaDB 10.11.14 ROW binlog: one transaction inserts two rows into `shop.orders`, updates one, and deletes one. Used to confirm `--show-rows` does not crash. Positional column names are expected.

### Regeneration

```bash
cd internal/binlog/testdata
bash ./create_mariadb_10.11_dml.sh
```

Requires Docker and `mariadb:10.11`.

## mysql-8.0.46-source-apply.binlog and mysql-8.0.46-replica-apply.binlog

A MySQL 8.0.46 source and replica pair. `log_replica_updates` is on for the replica. The replica SQL thread was stopped, the source committed a burst, then the SQL thread was started. Each `*.mysqlbinlog.txt` is `mysqlbinlog -vv --base64-output=DECODE-ROWS`.

The copied file is the closed binlog after schema setup, so it is DML only: 22 transactions, GTIDs `11458b63-c244-11f1-a0d2-822b383dbcd0:7` through `:28`.

On the replica file, read from the dump's `# original_commit_timestamp=` and `# immediate_commit_timestamp=` lines:

- max delay `5426519` µs (`5.426519s`), GTID `:7`, transaction start byte `197`, `shop.audit` 1 row
- p95 delay `5208595` µs (`5.208595s`), nearest rank 21 of 22, GTID `:8` (`shop.orders` and `shop.catalog`)
- peak minute `2026-10-07 11:41:00 UTC` (immediate commit of `:7`)
- the last transaction, `:28`, is the caught-up insert and is `2545` µs

On the source file every pair is equal, so every delay is `0`.

### Regeneration

```bash
cd internal/binlog/testdata
bash ./create_mysql_8.0.46_replica_apply.sh
```

Uses Docker (`mysql:8.0.46`) when the daemon answers. Otherwise two local mysqld 8.0 datadirs. The checked-in files were recorded on mysqld 8.0.46-0ubuntu0.24.04.4. A regeneration gets a new GTID UUID and new delays; update the pins in `cmd/binlogviz/replica_apply_test.go` from the new dump.

## Stage 5 Coverage Notes

- Multi-file command-path coverage reuses `minimal.binlog` twice in ordered input tests and benchmarks to exercise the real parser over more than one file without duplicating fixture assets.
- `Rows_query_log_event` present/absent cases still use controlled parser input in `cmd/binlogviz` tests because producing paired real fixtures with and without `binlog_rows_query_log_events=ON` would require maintaining multiple MySQL fixture-generation modes for a narrow renderer contract.
