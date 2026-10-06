#!/bin/bash
# Generate a MariaDB 10.11 ROW binlog: insert, update, and delete on shop.orders.
# Requires Docker and the mariadb:10.11 image.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="mariadb:10.11"
OUT="$SCRIPT_DIR/mariadb-10.11.14-dml.binlog"
CONTAINER=""

cleanup() {
  if [ -n "$CONTAINER" ]; then
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "Creating $IMAGE container with ROW binlog..."
CONTAINER=$(docker run -d \
  -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=1 \
  "$IMAGE" \
  --binlog-format=ROW \
  --log-bin=mysql-bin \
  --server-id=7 \
  --binlog-checksum=CRC32)

echo "Waiting for MariaDB root to accept SQL..."
ready=0
for _ in $(seq 1 90); do
  if docker exec "$CONTAINER" mariadb -uroot -e "SELECT 1" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "MariaDB did not become ready" >&2
  exit 1
fi

docker exec -i "$CONTAINER" mariadb -uroot <<'SQL'
CREATE DATABASE shop;
CREATE TABLE shop.orders (
  id INT UNSIGNED NOT NULL PRIMARY KEY,
  note VARCHAR(32) NULL
);
BEGIN;
INSERT INTO shop.orders VALUES (1, 'alpha'), (2, 'beta');
UPDATE shop.orders SET note='changed' WHERE id=1;
DELETE FROM shop.orders WHERE id=2;
COMMIT;
SQL

current=$(docker exec "$CONTAINER" mariadb -uroot -N -e "SHOW BINARY LOGS" | awk 'END {print $1}')
docker exec "$CONTAINER" cat "/var/lib/mysql/$current" > "$OUT"
chmod 644 "$OUT"
echo "Wrote $OUT"
