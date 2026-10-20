#!/bin/bash
# Generate small ROW binlogs for the version matrix and a mysqlbinlog / mariadb-binlog
# decode of each file (mysqlbinlog -vv --base64-output=decode-rows).
#
# Pinned images, recorded with SELECT VERSION() into the fixture name:
#   mysql:5.7.44        MySQL 5.7 has no binlog_row_metadata
#   mysql:8.4.11        8.4 LTS, binlog_row_metadata=FULL
#   mysql:9.7.2         9.x, binlog_row_metadata=FULL
#   mariadb:10.11.19    binlog_row_metadata=FULL when the server accepts it
#   mariadb:11.8.9      same shape on the current MariaDB LTS
#
# Each server writes three files, each the rotated binlog of one step:
#   *-wide-full.binlog     DML only, binlog_row_image=FULL
#   *-wide-minimal.binlog  the same DML, binlog_row_image=MINIMAL
#   *-ddl.binlog           CREATE / ALTER / DROP plus two inserts
# Setup for the wide files is flushed into the previous binlog and is not copied.
#
# Requires Docker, curl, and dpkg-deb. The mysql images omit mysqlbinlog, so the
# script downloads mysql-community-server-core (8.4.11 and 9.7.2) and decodes
# with that binary. 5.7.44 files are decoded by mysqlbinlog 8.4.11, which reads
# older servers. MariaDB files are decoded by that server's mariadb-binlog.
# Regeneration changes GTID UUIDs; tests read the checked-in decode.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cleanup() {
  if [ -n "${CONTAINER:-}" ]; then
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

sql_exec() {
  docker exec -i "$CONTAINER" "$CLIENT" -uroot --default-character-set=utf8mb4 "$@"
}

sql_ok() {
  sql_exec -e "$1" >/dev/null 2>&1
}

reset_logs() {
  if ! sql_ok "RESET MASTER"; then
    sql_exec -e "RESET BINARY LOGS AND GTIDS"
  fi
}

wait_ready() {
  local i comm
  # The image entrypoint serves SQL from a temporary server, then restarts.
  # PID 1 is mysqld or mariadbd only after that restart.
  for i in $(seq 1 180); do
    comm=$(docker exec "$CONTAINER" cat /proc/1/comm 2>/dev/null || true)
    case "$comm" in
      mysqld|mariadbd)
        if sql_ok "SELECT 1"; then
          return 0
        fi
        ;;
    esac
    sleep 1
  done
  echo "$IMAGE did not become ready" >&2
  docker logs "$CONTAINER" 2>&1 | tail -40 >&2
  exit 1
}

# Official mysql images do not ship mysqlbinlog. The community server-core
# package does. mysqlbinlog reads older servers, so 5.7 files are decoded
# with the 8.4.11 binary. MariaDB images ship mariadb-binlog.
CACHE="${BINLOGVIZ_MYSQLBINLOG_CACHE:-/tmp/binlogviz-mysqlbinlog}"

fetch_mysqlbinlog() {
  local ver="$1" suite="$2" deb="$3"
  local dest="$CACHE/$ver/mysqlbinlog"
  if [ -x "$dest" ]; then
    echo "$dest"
    return
  fi
  mkdir -p "$CACHE/$ver"
  curl -fsSL -o "$CACHE/$ver/pkg.deb" \
    "https://repo.mysql.com/apt/ubuntu/pool/${suite}/m/mysql-community/${deb}"
  dpkg-deb -x "$CACHE/$ver/pkg.deb" "$CACHE/$ver/root"
  cp "$CACHE/$ver/root/usr/bin/mysqlbinlog" "$dest"
  chmod +x "$dest"
  echo "$dest"
}

extract_previous() {
  local out="$1" decoder="$2"
  local previous
  previous=$(sql_exec -N -e "SHOW BINARY LOGS" | awk '{older=prev; prev=$1} END {print older}')
  if [ -z "$previous" ]; then
    echo "no previous binary log to extract" >&2
    exit 1
  fi
  docker exec "$CONTAINER" cat "/var/lib/mysql/$previous" > "$out"
  chmod 644 "$out"
  local label
  if [ "$decoder" = mariadb-binlog ]; then
    docker exec "$CONTAINER" mariadb-binlog -vv --base64-output=decode-rows "/var/lib/mysql/$previous" > "${out%.binlog}.mysqlbinlog.txt"
    label="mariadb-binlog"
  else
    "$decoder" -vv --base64-output=decode-rows "$out" > "${out%.binlog}.mysqlbinlog.txt"
    label=$("$decoder" --version | awk '{print $2, $3}')
  fi
  echo "  $out $(wc -c < "$out") bytes  decode $(wc -c < "${out%.binlog}.mysqlbinlog.txt") bytes  ($label)"
}

setup_sql() {
  local stored="$1"
  cat <<SQL
CREATE DATABASE IF NOT EXISTS shop;
DROP TABLE IF EXISTS shop.wide;
DROP TABLE IF EXISTS shop.heap;
CREATE TABLE shop.wide (
  id INT NOT NULL,
  qty INT UNSIGNED NULL,
  price DECIMAL(10,2) NULL,
  created_at DATETIME(6) NULL,
  updated_at TIMESTAMP(6) NULL,
  note TEXT NULL,
  raw BLOB NULL,
  payload JSON NULL,
  color ENUM('red','blue','green') NULL,
  flags SET('a','b','c') NULL,
  base INT NULL,
  virt INT AS (base + 1) VIRTUAL,
  stor INT AS (base * 2) ${stored},
  PRIMARY KEY (id)
);
CREATE TABLE shop.heap (
  id INT NULL,
  note VARCHAR(32) NULL
);
SET time_zone = '+00:00';
INSERT INTO shop.wide (id, qty, price, created_at, updated_at, note, raw, payload, color, flags, base)
VALUES
  (1, 3000000000, 10.00, '2026-10-06 14:00:01.000000', '2026-10-06 14:00:01.000000', 'alpha', x'01', JSON_OBJECT('sku', 'A', 'n', 1), 'red', 'a', 10),
  (2, NULL, 0.00, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
INSERT INTO shop.heap VALUES (1, 'gone'), (2, 'keep');
SQL
}

incident_sql() {
  cat <<'SQL'
SET time_zone = '+00:00';
START TRANSACTION;
UPDATE shop.wide
  SET qty = 7,
      price = -12.34,
      created_at = '2026-10-06 14:05:01.123456',
      updated_at = '2026-10-06 14:05:01.500000',
      note = 'it''s "bad" \\ 雪',
      raw = x'DEADBEEF',
      payload = JSON_OBJECT('sku', 'Q', 'n', 2),
      color = 'blue',
      flags = 'a,b',
      base = 11
  WHERE id = 1;
DELETE FROM shop.wide WHERE id = 2;
INSERT INTO shop.wide (id, qty, price, created_at, updated_at, note, raw, payload, color, flags, base)
VALUES (3, 1, 2.50, '2026-10-07 01:02:03.000456', '2026-10-07 01:02:03.000789', 'batch', x'00FF', JSON_OBJECT('sku', 'Z'), 'green', 'c', 7);
DELETE FROM shop.heap WHERE id = 1;
COMMIT;
SQL
}

ddl_sql() {
  cat <<'SQL'
DROP DATABASE IF EXISTS shop;
CREATE DATABASE shop;
CREATE TABLE shop.orders (
  id INT NOT NULL,
  note VARCHAR(32) NULL,
  PRIMARY KEY (id)
);
INSERT INTO shop.orders VALUES (1, 'before-drop');
ALTER TABLE shop.orders ADD INDEX idx_note (note);
DROP TABLE shop.orders;
CREATE TABLE shop.audit (
  id INT NOT NULL,
  note VARCHAR(32) NULL,
  PRIMARY KEY (id)
);
INSERT INTO shop.audit VALUES (2, 'after-drop');
SQL
}

set_metadata_full() {
  if sql_ok "SET GLOBAL binlog_row_metadata = FULL"; then
    echo "  binlog_row_metadata=FULL"
  else
    echo "  binlog_row_metadata unavailable"
  fi
}

record_server() {
  local image="$1" prefix="$2" stored="$3" client="$4" decoder="$5"
  shift 5
  local server_args=("$@")
  IMAGE="$image"
  CLIENT="$client"
  CONTAINER=""
  echo "Creating $IMAGE ..."
  CONTAINER=$(docker run -d \
    -e MYSQL_ALLOW_EMPTY_PASSWORD=1 \
    -e MARIADB_ALLOW_EMPTY_ROOT_PASSWORD=1 \
    "$IMAGE" \
    "${server_args[@]}")
  wait_ready
  local version expect="${prefix#mysql-}"
  expect="${expect#mariadb-}"
  version=$(sql_exec -N -e "SELECT VERSION()")
  echo "  VERSION()=$version"
  case "$version" in
    "$expect"*) ;;
    *)
      echo "version $version does not match $expect" >&2
      exit 1
      ;;
  esac

  local stem="$SCRIPT_DIR/${prefix}-wide"
  reset_logs
  set_metadata_full
  sql_ok "SET GLOBAL binlog_row_image = FULL" || true
  sql_exec <<<"$(setup_sql "$stored")"
  sql_exec -e "FLUSH LOGS"
  sql_exec <<<"$(incident_sql)"
  sql_exec -e "FLUSH LOGS"
  extract_previous "${stem}-full.binlog" "$decoder"

  sql_exec -e "SET GLOBAL binlog_row_image = MINIMAL"
  sql_exec -e "FLUSH LOGS"
  sql_exec <<<"$(setup_sql "$stored")"
  sql_exec -e "FLUSH LOGS"
  sql_exec <<<"$(incident_sql)"
  sql_exec -e "FLUSH LOGS"
  extract_previous "${stem}-minimal.binlog" "$decoder"
  sql_exec -e "SET GLOBAL binlog_row_image = FULL" || true

  reset_logs
  set_metadata_full
  sql_exec <<<"$(ddl_sql)"
  sql_exec -e "FLUSH LOGS"
  extract_previous "$SCRIPT_DIR/${prefix}-ddl.binlog" "$decoder"

  docker rm -f "$CONTAINER" >/dev/null
  CONTAINER=""
}

mysql_args_common=(
  --binlog-format=ROW
  --log-bin=mysql-bin
  --server-id=1
  --binlog-checksum=CRC32
  --binlog-row-image=FULL
  --gtid-mode=ON
  --enforce-gtid-consistency=ON
  --binlog-rows-query-log-events=ON
)

MB_84=$(fetch_mysqlbinlog 8.4.11 mysql-8.4-lts \
  mysql-community-server-core_8.4.11-1ubuntu24.04_amd64.deb)
MB_97=$(fetch_mysqlbinlog 9.7.2 mysql-9.7-lts \
  mysql-community-server-core_9.7.2-1ubuntu24.04_amd64.deb)
echo "mysqlbinlog $($MB_84 --version)"
echo "mysqlbinlog $($MB_97 --version)"

record_server mysql:5.7.44 mysql-5.7.44 STORED mysql "$MB_84" \
  "${mysql_args_common[@]}" \
  --character-set-server=utf8mb4 \
  --collation-server=utf8mb4_general_ci

record_server mysql:8.4.11 mysql-8.4.11 STORED mysql "$MB_84" \
  "${mysql_args_common[@]}" \
  --binlog-row-metadata=FULL \
  --character-set-server=utf8mb4 \
  --collation-server=utf8mb4_0900_ai_ci

record_server mysql:9.7.2 mysql-9.7.2 STORED mysql "$MB_97" \
  "${mysql_args_common[@]}" \
  --binlog-row-metadata=FULL \
  --character-set-server=utf8mb4 \
  --collation-server=utf8mb4_0900_ai_ci

mariadb_args=(
  --binlog-format=ROW
  --log-bin=mysql-bin
  --server-id=7
  --binlog-checksum=CRC32
  --binlog-row-image=FULL
  --character-set-server=utf8mb4
  --collation-server=utf8mb4_general_ci
)

record_server mariadb:10.11.19 mariadb-10.11.19 PERSISTENT mariadb mariadb-binlog \
  "${mariadb_args[@]}"

record_server mariadb:11.8.9 mariadb-11.8.9 PERSISTENT mariadb mariadb-binlog \
  "${mariadb_args[@]}"

echo "Version matrix fixtures written"
