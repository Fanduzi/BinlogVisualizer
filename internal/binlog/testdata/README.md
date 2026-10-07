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

## mariadb-10.11.14-dml.binlog

A MariaDB 10.11.14 ROW binlog: one transaction inserts two rows into `shop.orders`, updates one, and deletes one. Used to confirm `--show-rows` does not crash. Positional column names are expected.

### Regeneration

```bash
cd internal/binlog/testdata
bash ./create_mariadb_10.11_dml.sh
```

Requires Docker and `mariadb:10.11`.

## Stage 5 Coverage Notes

- Multi-file command-path coverage reuses `minimal.binlog` twice in ordered input tests and benchmarks to exercise the real parser over more than one file without duplicating fixture assets.
- `Rows_query_log_event` present/absent cases still use controlled parser input in `cmd/binlogviz` tests because producing paired real fixtures with and without `binlog_rows_query_log_events=ON` would require maintaining multiple MySQL fixture-generation modes for a narrow renderer contract.
