#!/bin/bash
# Generate MySQL 8.0.46 ROW+GTID fixtures for primary-key presence.
# One file uses binlog_row_metadata=FULL, the other MINIMAL.
# FULL has a primary-key table, a prefix primary key, two no-primary-key
# tables that receive UPDATE/DELETE, and one no-primary-key INSERT-only table.
# Requires Docker and the mysql:8.0.46 image.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="mysql:8.0.46"
CONTAINER=""

cleanup() {
  if [ -n "$CONTAINER" ]; then
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "Creating $IMAGE container with ROW binlog and GTID..."
CONTAINER=$(docker run -d \
  -e MYSQL_ALLOW_EMPTY_PASSWORD=1 \
  "$IMAGE" \
  --binlog-format=ROW \
  --log-bin=mysql-bin \
  --server-id=1 \
  --gtid-mode=ON \
  --enforce-gtid-consistency=ON \
  --binlog-row-image=FULL \
  --binlog-row-metadata=FULL \
  --binlog-checksum=CRC32)

echo "Waiting for MySQL root to accept SQL..."
ready=0
for _ in $(seq 1 90); do
  if docker exec "$CONTAINER" mysql -uroot -e "SELECT 1" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "MySQL did not become ready" >&2
  exit 1
fi

load_sql() {
  docker exec -i "$CONTAINER" mysql -uroot <<'SQL'
CREATE DATABASE IF NOT EXISTS shop;
DROP TABLE IF EXISTS shop.orders;
DROP TABLE IF EXISTS shop.heap;
DROP TABLE IF EXISTS shop.log;
DROP TABLE IF EXISTS shop.scratch;
DROP TABLE IF EXISTS shop.prefixed;
CREATE TABLE shop.orders (
  id INT UNSIGNED NOT NULL,
  note VARCHAR(32) NULL,
  PRIMARY KEY (id)
);
CREATE TABLE shop.heap (
  id INT NOT NULL,
  note VARCHAR(32) NULL
);
CREATE TABLE shop.log (
  id INT NOT NULL,
  note VARCHAR(32) NULL
);
CREATE TABLE shop.scratch (
  id INT NOT NULL,
  note VARCHAR(32) NULL
);
CREATE TABLE shop.prefixed (
  sku VARCHAR(32) NOT NULL,
  note VARCHAR(32) NULL,
  PRIMARY KEY (sku(8))
);
INSERT INTO shop.orders (id, note) VALUES (1, 'a'), (2, 'b');
INSERT INTO shop.heap (id, note) VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO shop.log (id, note) VALUES (1, 'a'), (2, 'b');
INSERT INTO shop.scratch (id, note) VALUES (1, 'a'), (2, 'b');
INSERT INTO shop.prefixed (sku, note) VALUES ('abcdefghij', 'p');
UPDATE shop.orders SET note = 'a2' WHERE id = 1;
UPDATE shop.heap SET note = 'a2' WHERE id = 1;
UPDATE shop.heap SET note = 'b2' WHERE id = 2;
UPDATE shop.log SET note = 'x' WHERE id = 1;
DELETE FROM shop.heap WHERE id = 3;
UPDATE shop.prefixed SET note = 'p2' WHERE sku = 'abcdefghij';
SQL
}

extract_current() {
  local out="$1"
  local current
  current=$(docker exec "$CONTAINER" mysql -uroot -N -e "SHOW BINARY LOGS" | awk 'END {print $1}')
  docker exec "$CONTAINER" cat "/var/lib/mysql/$current" > "$out"
  chmod 644 "$out"
  if command -v mysqlbinlog >/dev/null 2>&1; then
    mysqlbinlog -v --base64-output=DECODE-ROWS --print-table-metadata "$out" > "${out%.binlog}.mysqlbinlog.txt"
  else
    docker exec "$CONTAINER" mysqlbinlog -v --base64-output=DECODE-ROWS --print-table-metadata "/var/lib/mysql/$current" > "${out%.binlog}.mysqlbinlog.txt"
  fi
}

echo "Recording FULL metadata..."
docker exec "$CONTAINER" mysql -uroot -e "SET GLOBAL binlog_row_metadata=FULL; RESET MASTER"
load_sql
extract_current "$SCRIPT_DIR/mysql-8.0.46-no-pk-full.binlog"

echo "Recording MINIMAL metadata..."
docker exec "$CONTAINER" mysql -uroot -e "SET GLOBAL binlog_row_metadata=MINIMAL; RESET MASTER"
load_sql
extract_current "$SCRIPT_DIR/mysql-8.0.46-no-pk-minimal.binlog"

echo "Wrote no-pk FULL and MINIMAL fixtures"
