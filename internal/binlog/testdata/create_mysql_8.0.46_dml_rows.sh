#!/bin/bash
# Generate MySQL 8.0.46 ROW+GTID fixtures: an accidental DELETE among INSERT
# traffic on shop.orders, plus a multi-row UPDATE. One file uses
# binlog_row_metadata=MINIMAL, the other FULL.
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
  --binlog-row-metadata=MINIMAL \
  --binlog-rows-query-log-events=ON \
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
  docker exec -i "$CONTAINER" mysql -uroot --default-character-set=utf8mb4 <<'SQL'
CREATE DATABASE IF NOT EXISTS shop;
DROP TABLE IF EXISTS shop.orders;
CREATE TABLE shop.orders (
  id INT UNSIGNED NOT NULL,
  qty INT NULL,
  price DECIMAL(10,2) NULL,
  note VARCHAR(64) NULL,
  created_at DATETIME(6) NULL,
  updated_at TIMESTAMP(6) NULL,
  payload JSON NULL,
  raw BLOB NULL,
  PRIMARY KEY (id)
);
SET time_zone = '+00:00';
START TRANSACTION;
INSERT INTO shop.orders VALUES
  (1, 1, 10.00, 'alpha', '2026-10-06 14:00:01.000000', '2026-10-06 14:00:01.000000', JSON_OBJECT('sku', 'A'), x'01'),
  (2, 2, 10.50, 'beta', '2026-10-06 14:00:02.000000', '2026-10-06 14:00:02.000000', JSON_OBJECT('sku', 'B'), NULL),
  (3, -4, 0.00, NULL, '2026-10-06 14:00:03.000000', NULL, NULL, '');
INSERT INTO shop.orders (id, qty, price, note, created_at, updated_at, payload, raw)
SELECT 1000 + n, n, 1.00, CONCAT('row-', n), '2026-10-06 14:00:00', '2026-10-06 14:00:00', JSON_OBJECT('n', n), REPEAT('x', 80)
FROM (
  SELECT u.n + 10 * t.n AS n
  FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) u
  JOIN (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3) t
) seq;
COMMIT;
START TRANSACTION;
INSERT INTO shop.orders VALUES (
  3000000000, NULL, 19.99, 'it''s "bad"',
  '2026-10-06 14:05:01.123456', '2026-10-06 14:05:01.500000',
  JSON_OBJECT('sku', 'Z', 'n', 1), x'DEADBEEF0102'
);
DELETE FROM shop.orders WHERE id IN (2, 3000000000);
COMMIT;
START TRANSACTION;
UPDATE shop.orders SET note = 'changed', qty = qty + 1 WHERE id IN (1, 3);
COMMIT;
SQL
}

extract_current() {
  local out="$1"
  local current
  current=$(docker exec "$CONTAINER" mysql -uroot -N -e "SHOW BINARY LOGS" | awk 'END {print $1}')
  docker exec "$CONTAINER" cat "/var/lib/mysql/$current" > "$out"
  chmod 644 "$out"
  mysqlbinlog -v --base64-output=DECODE-ROWS "$out" > "${out%.binlog}.mysqlbinlog.txt" || \
    docker exec "$CONTAINER" mysqlbinlog -v --base64-output=DECODE-ROWS "/var/lib/mysql/$current" > "${out%.binlog}.mysqlbinlog.txt"
}

echo "Recording MINIMAL metadata..."
docker exec "$CONTAINER" mysql -uroot -e "RESET MASTER"
load_sql
extract_current "$SCRIPT_DIR/mysql-8.0.46-dml-minimal.binlog"

echo "Recording FULL metadata..."
docker exec "$CONTAINER" mysql -uroot -e "SET GLOBAL binlog_row_metadata=FULL; RESET MASTER"
load_sql
extract_current "$SCRIPT_DIR/mysql-8.0.46-dml-full.binlog"

echo "Wrote minimal and full DML fixtures"
