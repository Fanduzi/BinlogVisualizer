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

## mysql80_transaction_payload.binlog

A MySQL 8.0.36 ROW binlog with `binlog_transaction_compression=ON`. Used to prove transaction-payload expand through `ParseFiles`.

### Contents

- Flavor / version: MySQL 8.0.36
- `CREATE TABLE testdb.users (id INT PRIMARY KEY, name VARCHAR(100))` as an independent GTID/QUERY group
- One compressed DML transaction: `INSERT` alice, `UPDATE` to alice_updated, `DELETE`

### Regeneration

```bash
cd internal/binlog/testdata
./create_mysql80_transaction_payload.sh
```

Requirements:
- Docker
- Image `mysql:8.0.36` (override with `MYSQL_IMAGE`)

The script rotates away bootstrap binlogs, records `SHOW MASTER STATUS`, writes the compressed transaction into that file, then copies it. Do not copy an earlier binlog: MySQL 8.0 bootstrap writes a large compressed payload that is not this fixture.

## Stage 5 Coverage Notes

- Multi-file command-path coverage reuses `minimal.binlog` twice in ordered input tests and benchmarks to exercise the real parser over more than one file without duplicating fixture assets.
- `Rows_query_log_event` present/absent cases still use controlled parser input in `cmd/binlogviz` tests because producing paired real fixtures with and without `binlog_rows_query_log_events=ON` would require maintaining multiple MySQL fixture-generation modes for a narrow renderer contract.
