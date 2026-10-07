#!/bin/bash
# Generate MySQL 8.0.46 ROW+GTID fixtures where one primary key is updated
# many times across transactions. shop.counters PK is `id` (not column @1).
# shop.inventory PK is (sku, wh). shop.heap has no primary key.
# One file uses binlog_row_metadata=FULL, the other MINIMAL.
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
DROP TABLE IF EXISTS shop.counters;
DROP TABLE IF EXISTS shop.inventory;
DROP TABLE IF EXISTS shop.heap;
CREATE TABLE shop.counters (
  label VARCHAR(32) NOT NULL,
  id INT UNSIGNED NOT NULL,
  n INT NOT NULL,
  PRIMARY KEY (id)
);
CREATE TABLE shop.inventory (
  sku VARCHAR(16) NOT NULL,
  wh INT NOT NULL,
  qty INT NOT NULL,
  PRIMARY KEY (sku, wh)
);
CREATE TABLE shop.heap (
  id INT NOT NULL,
  note VARCHAR(32) NULL
);
INSERT INTO shop.counters (label, id, n) VALUES
  ('hot', 7, 0),
  ('warm', 8, 0),
  ('gone', 9, 0),
  ('fresh', 1, 0);
INSERT INTO shop.inventory (sku, wh, qty) VALUES ('BOLT', 1, 10), ('NUT', 2, 4);
INSERT INTO shop.heap (id, note) VALUES (1, 'a');

-- id=7: two images in one transaction, then five more transactions. 7 touches, 6 txns.
SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:01');
START TRANSACTION;
UPDATE shop.counters SET n = n + 1 WHERE id = 7;
UPDATE shop.counters SET n = n + 1 WHERE id = 7;
COMMIT;
SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:02');
START TRANSACTION;
UPDATE shop.counters SET n = n + 1 WHERE id = 7;
COMMIT;
SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:03');
START TRANSACTION;
UPDATE shop.counters SET n = n + 1 WHERE id = 7;
COMMIT;
SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:04');
START TRANSACTION;
UPDATE shop.counters SET n = n + 1 WHERE id = 7;
COMMIT;
SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:05');
START TRANSACTION;
UPDATE shop.counters SET n = n + 1 WHERE id = 7;
COMMIT;
SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:06');
START TRANSACTION;
UPDATE shop.counters SET n = n + 1 WHERE id = 7;
COMMIT;

SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:07');
START TRANSACTION;
UPDATE shop.counters SET n = n + 1 WHERE id = 8;
COMMIT;
SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:08');
START TRANSACTION;
UPDATE shop.counters SET n = n + 1 WHERE id = 8;
COMMIT;

SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:09');
START TRANSACTION;
DELETE FROM shop.counters WHERE id = 9;
COMMIT;

SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:10');
START TRANSACTION;
UPDATE shop.inventory SET qty = qty + 1 WHERE sku = 'BOLT' AND wh = 1;
COMMIT;
SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:11');
START TRANSACTION;
UPDATE shop.inventory SET qty = qty + 1 WHERE sku = 'BOLT' AND wh = 1;
COMMIT;
SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:12');
START TRANSACTION;
UPDATE shop.inventory SET qty = qty + 1 WHERE sku = 'BOLT' AND wh = 1;
UPDATE shop.inventory SET qty = qty + 1 WHERE sku = 'NUT' AND wh = 2;
COMMIT;

-- No primary key. More images than id=7 so a guessed @1 ranking would put it first.
SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:13');
START TRANSACTION;
UPDATE shop.heap SET note = 'b' WHERE id = 1;
UPDATE shop.heap SET note = 'c' WHERE id = 1;
UPDATE shop.heap SET note = 'd' WHERE id = 1;
UPDATE shop.heap SET note = 'e' WHERE id = 1;
UPDATE shop.heap SET note = 'f' WHERE id = 1;
UPDATE shop.heap SET note = 'g' WHERE id = 1;
UPDATE shop.heap SET note = 'h' WHERE id = 1;
UPDATE shop.heap SET note = 'i' WHERE id = 1;
UPDATE shop.heap SET note = 'j' WHERE id = 1;
UPDATE shop.heap SET note = 'k' WHERE id = 1;
UPDATE shop.heap SET note = 'l' WHERE id = 1;
UPDATE shop.heap SET note = 'm' WHERE id = 1;
COMMIT;

SET TIMESTAMP = UNIX_TIMESTAMP('2026-10-06 14:00:14');
START TRANSACTION;
INSERT INTO shop.counters (label, id, n) VALUES ('extra', 4, 1);
COMMIT;
SQL
}

extract_closed() {
  local out="$1"
  docker exec "$CONTAINER" mysql -uroot -e "FLUSH LOGS"
  # After FLUSH LOGS the newest file is empty. The file before it holds the workload.
  local previous
  previous=$(docker exec "$CONTAINER" mysql -uroot -N -e "SHOW BINARY LOGS" | awk '{older=prev; prev=$1} END {print older}')
  docker exec "$CONTAINER" cat "/var/lib/mysql/$previous" > "$out"
  chmod 644 "$out"
  if command -v mysqlbinlog >/dev/null 2>&1; then
    mysqlbinlog --print-table-metadata -v --base64-output=DECODE-ROWS "$out" > "${out%.binlog}.mysqlbinlog.txt"
  else
    docker exec "$CONTAINER" mysqlbinlog --print-table-metadata -v --base64-output=DECODE-ROWS "/var/lib/mysql/$previous" > "${out%.binlog}.mysqlbinlog.txt"
  fi
}

echo "Recording MINIMAL metadata..."
docker exec "$CONTAINER" mysql -uroot -e "RESET MASTER"
load_sql
extract_closed "$SCRIPT_DIR/mysql-8.0.46-hot-rows-minimal.binlog"

echo "Recording FULL metadata..."
docker exec "$CONTAINER" mysql -uroot -e "SET GLOBAL binlog_row_metadata=FULL; RESET MASTER"
load_sql
extract_closed "$SCRIPT_DIR/mysql-8.0.46-hot-rows-full.binlog"

echo "Wrote hot-row fixtures"
