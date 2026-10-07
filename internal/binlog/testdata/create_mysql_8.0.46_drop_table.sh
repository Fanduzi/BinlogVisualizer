#!/bin/bash
# Generate a MySQL 8.0.46 ROW+GTID fixture: ordinary DML, then DROP TABLE,
# then more DML. Also writes the mysqlbinlog decode and the --stop-position
# decode that starts at the DROP transaction's GTID event.
# Requires Docker and the mysql:8.0.46 image.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="mysql:8.0.46"
CONTAINER=""
OUT="$SCRIPT_DIR/mysql-8.0.46-drop-table.binlog"

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

docker exec -i "$CONTAINER" mysql -uroot <<'SQL'
CREATE DATABASE shop;
CREATE TABLE shop.orders (id INT PRIMARY KEY, note VARCHAR(32));
CREATE TABLE shop.audit (id INT PRIMARY KEY, note VARCHAR(32));
RESET MASTER;
INSERT INTO shop.orders VALUES (1, 'before-drop');
DROP TABLE shop.orders;
INSERT INTO shop.audit VALUES (2, 'after-drop');
SQL

current=$(docker exec "$CONTAINER" mysql -uroot -N -e "SHOW BINARY LOGS" | awk 'END {print $1}')
docker exec "$CONTAINER" cat "/var/lib/mysql/$current" > "$OUT"
chmod 644 "$OUT"

if command -v mysqlbinlog >/dev/null 2>&1; then
  mysqlbinlog -v --base64-output=DECODE-ROWS "$OUT" > "${OUT%.binlog}.mysqlbinlog.txt"
else
  docker exec "$CONTAINER" mysqlbinlog -v --base64-output=DECODE-ROWS "/var/lib/mysql/$current" > "${OUT%.binlog}.mysqlbinlog.txt"
fi

# The GTID event start is the "# at N" line before this DROP's GTID_NEXT.
stop_at=$(python3 - "${OUT%.binlog}.mysqlbinlog.txt" <<'PY'
import sys
text = open(sys.argv[1], encoding="utf-8", errors="replace").read().splitlines()
at = 0
gtid = ""
gtid_at = 0
query_at = 0
seen_query = False
for line in text:
    if line.startswith("# at "):
        at = int(line.split()[-1])
        if gtid and not seen_query and at != gtid_at:
            query_at = at
            seen_query = True
        continue
    if "GTID_NEXT=" in line and "AUTOMATIC" not in line:
        gtid = line.split("'", 2)[1]
        gtid_at = at
        seen_query = False
        query_at = 0
        continue
    if "DROP TABLE" in line and gtid and gtid_at and query_at > gtid_at:
        print(gtid_at)
        break
else:
    raise SystemExit("DROP TABLE GTID anchor not found")
PY
)
stop_file="${OUT%.binlog}.stop.mysqlbinlog.txt"
if command -v mysqlbinlog >/dev/null 2>&1; then
  mysqlbinlog -v --base64-output=DECODE-ROWS --stop-position="$stop_at" "$OUT" > "$stop_file"
else
  docker exec "$CONTAINER" mysqlbinlog -v --base64-output=DECODE-ROWS --stop-position="$stop_at" "/var/lib/mysql/$current" > "$stop_file"
fi
echo "DROP txn start=$stop_at"

echo "Wrote $OUT"
