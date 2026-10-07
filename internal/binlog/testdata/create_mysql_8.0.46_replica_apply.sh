#!/bin/bash
# Generate a MySQL 8.0 source binlog and a replica binlog (log_replica_updates)
# where the replica SQL thread was stopped across a burst of commits.
#
# mysql-8.0.46-source-apply.binlog: original_commit_timestamp == immediate_commit_timestamp.
# mysql-8.0.46-replica-apply.binlog: immediate minus original is the apply delay.
# Each *.mysqlbinlog.txt is mysqlbinlog -vv --base64-output=DECODE-ROWS.
#
# Uses Docker (mysql:8.0.46) when the daemon answers. Otherwise two local
# mysqld 8.0 datadirs, which is how the checked-in files were recorded on
# mysqld 8.0.46-0ubuntu0.24.04.4.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_OUT="$SCRIPT_DIR/mysql-8.0.46-source-apply.binlog"
REPLICA_OUT="$SCRIPT_DIR/mysql-8.0.46-replica-apply.binlog"

cleanup() {
  if [ -n "${SRC_CONTAINER:-}" ]; then
    docker rm -f "$SRC_CONTAINER" >/dev/null 2>&1 || true
  fi
  if [ -n "${REPL_CONTAINER:-}" ]; then
    docker rm -f "$REPL_CONTAINER" >/dev/null 2>&1 || true
  fi
  if [ -n "${SRC_PID:-}" ]; then
    sudo kill "$SRC_PID" >/dev/null 2>&1 || true
  fi
  if [ -n "${REPL_PID:-}" ]; then
    sudo kill "$REPL_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

mysql_exec() {
  local target="$1"
  shift
  if [ "$MODE" = "docker" ]; then
    docker exec -i "$target" mysql -uroot "$@"
  else
    sudo mysql --socket="$target" -uroot "$@"
  fi
}

wait_ready() {
  local target="$1"
  for _ in $(seq 1 90); do
    if mysql_exec "$target" -e "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "MySQL did not become ready: $target" >&2
  return 1
}

if docker info >/dev/null 2>&1; then
  MODE=docker
  NET="bv-repl-$$"
  docker network create "$NET" >/dev/null
  cleanup_net() { docker network rm "$NET" >/dev/null 2>&1 || true; }
  trap 'cleanup; cleanup_net' EXIT
  SRC_CONTAINER=$(docker run -d --network "$NET" --network-alias source \
    -e MYSQL_ALLOW_EMPTY_PASSWORD=1 \
    mysql:8.0.46 \
    --server-id=1 \
    --gtid-mode=ON \
    --enforce-gtid-consistency=ON \
    --log-bin=mysql-bin \
    --binlog-format=ROW \
    --binlog-row-image=FULL \
    --binlog-row-metadata=FULL \
    --binlog-checksum=CRC32 \
    --sync-binlog=1 \
    --mysqlx=0)
  REPL_CONTAINER=$(docker run -d --network "$NET" --network-alias replica \
    -e MYSQL_ALLOW_EMPTY_PASSWORD=1 \
    mysql:8.0.46 \
    --server-id=2 \
    --gtid-mode=ON \
    --enforce-gtid-consistency=ON \
    --log-bin=mysql-bin \
    --binlog-format=ROW \
    --binlog-row-image=FULL \
    --binlog-row-metadata=FULL \
    --binlog-checksum=CRC32 \
    --log-replica-updates=ON \
    --relay-log=relay-bin \
    --read-only=ON \
    --replica-parallel-workers=1 \
    --sync-binlog=1 \
    --mysqlx=0)
  wait_ready "$SRC_CONTAINER"
  wait_ready "$REPL_CONTAINER"
  SRC="$SRC_CONTAINER"
  REPL="$REPL_CONTAINER"
else
  MODE=local
  ROOT=/tmp/bv-replica-apply
  sudo rm -rf "$ROOT"
  sudo mkdir -p "$ROOT/src" "$ROOT/repl"
  sudo chown mysql:mysql "$ROOT" "$ROOT/src" "$ROOT/repl"
  sudo -u mysql mysqld --initialize-insecure --datadir="$ROOT/src" >/tmp/bv-src-init.err 2>&1
  sudo -u mysql mysqld --initialize-insecure --datadir="$ROOT/repl" >/tmp/bv-repl-init.err 2>&1
  start_local() {
    local dir="$1" id="$2" port="$3" extra="$4" err="$5"
    # shellcheck disable=SC2086
    sudo -u mysql mysqld \
      --no-defaults \
      --datadir="$dir" \
      --socket="$dir/mysql.sock" \
      --pid-file="$dir/mysql.pid" \
      --port="$port" \
      --bind-address=127.0.0.1 \
      --mysqlx=0 \
      --server-id="$id" \
      --gtid-mode=ON \
      --enforce-gtid-consistency=ON \
      --log-bin=mysql-bin \
      --binlog-format=ROW \
      --binlog-row-image=FULL \
      --binlog-row-metadata=FULL \
      --binlog-checksum=CRC32 \
      --sync-binlog=1 \
      --innodb-buffer-pool-size=64M \
      --replica-parallel-workers=1 \
      $extra \
      --log-error="$err" &
  }
  start_local "$ROOT/src" 1 33061 "" /tmp/bv-src.err
  SRC_PID=$!
  start_local "$ROOT/repl" 2 33062 "--log-replica-updates=ON --relay-log=relay-bin --read-only=ON" /tmp/bv-repl.err
  REPL_PID=$!
  SRC="$ROOT/src/mysql.sock"
  REPL="$ROOT/repl/mysql.sock"
  wait_ready "$SRC"
  wait_ready "$REPL"
fi

mysql_exec "$SRC" <<'SQL'
CREATE USER 'repl'@'%' IDENTIFIED WITH mysql_native_password BY 'replpass';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
CREATE DATABASE shop;
CREATE TABLE shop.orders (id INT NOT NULL PRIMARY KEY, note VARCHAR(32));
CREATE TABLE shop.catalog (id INT NOT NULL PRIMARY KEY, note VARCHAR(32));
CREATE TABLE shop.audit (id INT NOT NULL PRIMARY KEY, note VARCHAR(32));
SQL

if [ "$MODE" = "docker" ]; then
  HOST=source
  PORT=3306
else
  HOST=127.0.0.1
  PORT=33061
fi

mysql_exec "$REPL" -e "CHANGE REPLICATION SOURCE TO SOURCE_HOST='${HOST}', SOURCE_PORT=${PORT}, SOURCE_USER='repl', SOURCE_PASSWORD='replpass', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1; START REPLICA;"

wait_gtid() {
  local label="$1"
  local src_gtid="" repl_gtid=""
  for _ in $(seq 1 60); do
    src_gtid=$(mysql_exec "$SRC" -N -e "SELECT @@GLOBAL.gtid_executed")
    repl_gtid=$(mysql_exec "$REPL" -N -e "SELECT @@GLOBAL.gtid_executed")
    if [ -n "$src_gtid" ] && [ "$src_gtid" = "$repl_gtid" ]; then
      return 0
    fi
    sleep 0.5
  done
  echo "replica did not catch the source GTID during ${label}" >&2
  echo "source=${src_gtid}" >&2
  echo "replica=${repl_gtid}" >&2
  return 1
}

wait_gtid "schema"

mysql_exec "$SRC" -e "FLUSH BINARY LOGS"
mysql_exec "$REPL" -e "FLUSH BINARY LOGS"
mysql_exec "$REPL" -e "STOP REPLICA SQL_THREAD"

# First commit waits the longest. shop.audit is not shop.orders, so an
# include-table filter drops it. The next statement is one transaction that
# touches two tables. Then 18 single-row inserts, then a 4-row delete.
mysql_exec "$SRC" -e "INSERT INTO shop.audit (id, note) VALUES (1, 'lag-audit')"
sleep 0.2
mysql_exec "$SRC" -e "BEGIN; INSERT INTO shop.orders (id, note) VALUES (1, 'multi'); INSERT INTO shop.catalog (id, note) VALUES (1, 'multi'); COMMIT"
sleep 0.2
for i in $(seq 2 19); do
  mysql_exec "$SRC" -e "INSERT INTO shop.orders (id, note) VALUES (${i}, 'lag')"
  sleep 0.15
done
mysql_exec "$SRC" -e "DELETE FROM shop.orders WHERE id IN (2, 3, 4, 5)"
sleep 2
mysql_exec "$REPL" -e "START REPLICA SQL_THREAD"
wait_gtid "stopped SQL thread"

mysql_exec "$SRC" -e "INSERT INTO shop.orders (id, note) VALUES (1000, 'caught-up')"
wait_gtid "caught-up insert"

mysql_exec "$SRC" -e "FLUSH BINARY LOGS"
mysql_exec "$REPL" -e "FLUSH BINARY LOGS"

copy_closed_binlog() {
  local which="$1"
  local dest="$2"
  local datadir file
  if [ "$MODE" = "docker" ]; then
    file=$(mysql_exec "$which" -N -e "SHOW BINARY LOGS" | awk 'NR>1{prev=cur; cur=$1} END{print prev}')
    docker cp "$which:/var/lib/mysql/$file" "$dest"
  else
    datadir=$(dirname "$which")
    file=$(mysql_exec "$which" -N -e "SHOW BINARY LOGS" | awk 'NR>1{prev=cur; cur=$1} END{print prev}')
    sudo cp "$datadir/$file" "$dest"
    sudo chown "$(id -u):$(id -g)" "$dest"
  fi
  echo "copied $file -> $dest"
}

copy_closed_binlog "$SRC" "$SOURCE_OUT"
copy_closed_binlog "$REPL" "$REPLICA_OUT"

dump_one() {
  local bin="$1"
  if [ "$MODE" = "docker" ]; then
    docker run --rm -v "$SCRIPT_DIR":/out --entrypoint mysqlbinlog mysql:8.0.46 -vv --base64-output=DECODE-ROWS "/out/$(basename "$bin")" > "${bin%.binlog}.mysqlbinlog.txt"
  else
    mysqlbinlog -vv --base64-output=DECODE-ROWS "$bin" > "${bin%.binlog}.mysqlbinlog.txt"
  fi
}
dump_one "$SOURCE_OUT"
dump_one "$REPLICA_OUT"

echo "wrote $SOURCE_OUT and $REPLICA_OUT"
