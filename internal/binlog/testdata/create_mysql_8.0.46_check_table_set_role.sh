#!/bin/bash
# Generate MySQL 8.0.46 ROW+GTID dialect fixtures for CHECK TABLE and SET ROLE ALL.
#
# Stock mysqld 8.0.46 does not write CHECK TABLE or SET ROLE (Sql_cmd_check_table
# and Sql_cmd_set_role never call write_bin_log). Each fixture is a real rotated
# ROW+GTID file whose only maintenance group is the logged statement FLUSH TABLES,
# followed by one business INSERT. This script rewrites that query text to the
# target statement and recomputes event size, end_log_pos, and CRC32. The Format
# Description flavor and version stay mysql 8.0.46.
#
# Requires Docker, the mysql:8.0.46 image, and python3.
# Outputs:
#   mysql-8.0.46-check-table.binlog  — CHECK TABLE testdb.users, then one INSERT
#   mysql-8.0.46-set-role.binlog     — SET ROLE ALL, then one INSERT

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

rewrite_query() {
  local src="$1" dst="$2" old="$3" new="$4"
  python3 - "$src" "$dst" "$old" "$new" <<'PY'
import struct, sys, zlib

src, dst, old, new = sys.argv[1:]
old_b, new_b = old.encode(), new.encode()
data = open(src, "rb").read()
if data[:4] != b"\xfebin":
    sys.exit("not a binlog: " + src)

pos = 4
events = []
while pos < len(data):
    size = struct.unpack_from("<I", data, pos + 9)[0]
    if size < 19 or pos + size > len(data):
        sys.exit("truncated event at %d" % pos)
    events.append(bytearray(data[pos:pos + size]))
    pos += size

def query_span(ev):
    if ev[4] != 2:
        return None
    body = ev[19:-4]
    db_len = body[8]
    status_len = struct.unpack_from("<H", body, 11)[0]
    qoff = 13 + status_len + db_len + 1
    return qoff, body[qoff:]

replaced = 0
for i, ev in enumerate(events):
    found = query_span(ev)
    if not found or found[1] != old_b:
        continue
    qoff = found[0]
    body = ev[19:-4]
    events[i] = bytearray(ev[:19] + body[:qoff] + new_b + b"\x00\x00\x00\x00")
    replaced += 1
if replaced != 1:
    sys.exit("expected one %r query, replaced %d" % (old, replaced))

out = bytearray(b"\xfebin")
file_pos = 4
for ev in events:
    end = file_pos + len(ev)
    struct.pack_into("<I", ev, 9, len(ev))
    struct.pack_into("<I", ev, 13, end)
    struct.pack_into("<I", ev, len(ev) - 4, zlib.crc32(ev[:-4]) & 0xFFFFFFFF)
    out += ev
    file_pos = end
open(dst, "wb").write(out)
PY
}

echo "Creating $IMAGE container with ROW binlog and GTID..."
CONTAINER=$(docker run -d \
  -e MYSQL_ROOT_PASSWORD=test \
  -e MYSQL_DATABASE=testdb \
  "$IMAGE" \
  --binlog-format=ROW \
  --log-bin=mysql-bin \
  --server-id=1 \
  --gtid-mode=ON \
  --enforce-gtid-consistency=ON)

echo "Waiting for MySQL root to accept SQL..."
ready=0
for _ in $(seq 1 90); do
  if docker exec "$CONTAINER" mysql -uroot -ptest -e "SELECT 1" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "MySQL did not become ready" >&2
  exit 1
fi

echo "Creating schema in an earlier binlog..."
docker exec -i "$CONTAINER" mysql -uroot -ptest testdb <<'SQL'
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(100)
);
SQL

write_placeholder_and_insert() {
  local id="$1" name="$2"
  echo "Rotating so the maintenance group and the following INSERT share one file..." >&2
  docker exec "$CONTAINER" mysql -uroot -ptest -e "FLUSH LOGS" >&2
  local current
  current=$(docker exec "$CONTAINER" mysql -uroot -ptest -N -e "SHOW BINARY LOGS" | awk 'END {print $1}')
  if [ -z "$current" ]; then
    echo "could not determine current binlog" >&2
    exit 1
  fi
  echo "Writing FLUSH TABLES then one business INSERT into $current..." >&2
  docker exec -i "$CONTAINER" mysql -uroot -ptest testdb <<SQL
FLUSH TABLES;
INSERT INTO users VALUES (${id}, '${name}');
SQL
  docker exec "$CONTAINER" mysql -uroot -ptest -e "FLUSH LOGS" >&2
  printf '%s' "$current"
}

echo "Server version:"
docker exec "$CONTAINER" mysql -uroot -ptest -N -e "SELECT VERSION()"

SET_ROLE_SRC=$(write_placeholder_and_insert 1 alice)
CHECK_SRC=$(write_placeholder_and_insert 2 bob)

SET_ROLE_RAW="$SCRIPT_DIR/mysql-8.0.46-set-role.binlog.raw"
CHECK_RAW="$SCRIPT_DIR/mysql-8.0.46-check-table.binlog.raw"
docker cp "$CONTAINER:/var/lib/mysql/$SET_ROLE_SRC" "$SET_ROLE_RAW"
docker cp "$CONTAINER:/var/lib/mysql/$CHECK_SRC" "$CHECK_RAW"

rewrite_query "$SET_ROLE_RAW" "$SCRIPT_DIR/mysql-8.0.46-set-role.binlog" "FLUSH TABLES" "SET ROLE ALL"
rewrite_query "$CHECK_RAW" "$SCRIPT_DIR/mysql-8.0.46-check-table.binlog" "FLUSH TABLES" "CHECK TABLE testdb.users"
rm -f "$SET_ROLE_RAW" "$CHECK_RAW"

echo "Binlog file sizes:"
ls -la "$SCRIPT_DIR/mysql-8.0.46-set-role.binlog" "$SCRIPT_DIR/mysql-8.0.46-check-table.binlog"

echo "Done."
echo "To verify:"
echo "  go test ./internal/analyzer ./cmd/binlogviz -count=1 -run 'DialectFixture'"
