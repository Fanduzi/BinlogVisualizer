#!/bin/bash
# Generate MySQL 8.0.46 ROW+GTID flashback fixtures.
# flashback-full.binlog is binlog_row_image=FULL and binlog_row_metadata=FULL.
# flashback-minimal-image.binlog is the same incident with binlog_row_image=MINIMAL.
# Both files contain only the incident transactions (setup is in the previous file).
#
# Default: Docker and the mysql:8.0.46 image.
# Local server: MYSQL_CMD="sudo mysql" bash create_mysql_8.0.46_flashback.sh
# The local server must already be ROW, GTID, metadata FULL, image FULL.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="mysql:8.0.46"
CONTAINER=""
MYSQL_CMD="${MYSQL_CMD:-}"

cleanup() {
  if [ -n "$CONTAINER" ]; then
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

sql_exec() {
  if [ -n "$MYSQL_CMD" ]; then
    # shellcheck disable=SC2086
    $MYSQL_CMD --default-character-set=utf8mb4 -N --batch
    return
  fi
  docker exec -i "$CONTAINER" mysql -uroot --default-character-set=utf8mb4 -N --batch
}

run_file() {
  sql_exec < "$1"
}

run_inline() {
  sql_exec <<SQL
$1
SQL
}

extract_previous() {
  local out="$1"
  local previous
  if [ -n "$MYSQL_CMD" ]; then
    previous=$(run_inline "SHOW BINARY LOGS" | awk '{older=prev; prev=$1} END {print older}')
    sudo cp "/var/lib/mysql/$previous" "$out"
    sudo chmod 644 "$out"
  else
    previous=$(run_inline "SHOW BINARY LOGS" | awk '{older=prev; prev=$1} END {print older}')
    docker exec "$CONTAINER" cat "/var/lib/mysql/$previous" > "$out"
    chmod 644 "$out"
  fi
  if command -v mysqlbinlog >/dev/null 2>&1; then
    mysqlbinlog -v --base64-output=DECODE-ROWS "$out" > "${out%.binlog}.mysqlbinlog.txt"
  elif [ -z "$MYSQL_CMD" ]; then
    docker exec "$CONTAINER" mysqlbinlog -v --base64-output=DECODE-ROWS "/var/lib/mysql/$previous" > "${out%.binlog}.mysqlbinlog.txt"
  fi
}

record_incident() {
  local out="$1"
  run_inline "RESET MASTER"
  run_file "$SCRIPT_DIR/flashback_setup.sql"
  run_inline "FLUSH LOGS"
  run_file "$SCRIPT_DIR/flashback_incident.sql"
  run_inline "FLUSH LOGS"
  extract_previous "$out"
}

if [ -z "$MYSQL_CMD" ]; then
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
    --binlog-rows-query-log-events=ON \
    --character-set-server=utf8mb4 \
    --collation-server=utf8mb4_0900_ai_ci \
    --binlog-checksum=CRC32)
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
fi

echo "Recording FULL row image..."
run_inline "SET GLOBAL binlog_row_image=FULL; SET GLOBAL binlog_row_metadata=FULL"
record_incident "$SCRIPT_DIR/mysql-8.0.46-flashback-full.binlog"

echo "Recording MINIMAL row image..."
run_inline "SET GLOBAL binlog_row_image=MINIMAL; SET GLOBAL binlog_row_metadata=FULL"
record_incident "$SCRIPT_DIR/mysql-8.0.46-flashback-minimal-image.binlog"

run_inline "SET GLOBAL binlog_row_image=FULL"
echo "Wrote flashback FULL and MINIMAL-image fixtures"
