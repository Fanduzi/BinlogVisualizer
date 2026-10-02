#!/bin/bash
# Generate a MySQL 8.0.46 ROW+GTID fixture: explicit BEGIN, two row images,
# no COMMIT/ROLLBACK, then a later business GTID.
#
# InnoDB writes a transaction to the binlog at COMMIT, so a running server
# never emits BEGIN+rows and then a later GTID without the first XID. This
# script records a real 8.0.46 file, then drops that first XID event. Every
# remaining event, including its CRC32, is unmodified server bytes. Duration
# in the fixture is the wall clock between those event timestamps.
#
# An EOF-open reading is a prefix of this file that stops at the next GTID.
# Requires Docker, python3, and the mysql:8.0.46 image.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="mysql:8.0.46"
OUT="$SCRIPT_DIR/mysql-8.0.46-open-begin-dml.binlog"
CONTAINER=""

cleanup() {
  if [ -n "$CONTAINER" ]; then
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "Creating $IMAGE container with ROW binlog and GTID..."
CONTAINER=$(docker run -d \
  -e MYSQL_ROOT_PASSWORD=test \
  -e MYSQL_DATABASE=testdb \
  "$IMAGE" \
  --binlog-format=ROW \
  --log-bin=mysql-bin \
  --server-id=1 \
  --gtid-mode=ON \
  --enforce-gtid-consistency=ON \
  --binlog-checksum=CRC32)

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

echo "Rotating so the open group and the later INSERT share one file..."
docker exec "$CONTAINER" mysql -uroot -ptest -e "FLUSH LOGS"
CURRENT=$(docker exec "$CONTAINER" mysql -uroot -ptest -N -e "SHOW BINARY LOGS" | awk 'END {print $1}')
if [ -z "$CURRENT" ]; then
  echo "could not determine current binlog" >&2
  exit 1
fi

echo "Writing BEGIN + two inserts, then a later business INSERT into $CURRENT..."
docker exec -i "$CONTAINER" mysql -uroot -ptest testdb <<'SQL'
BEGIN;
INSERT INTO users VALUES (1, 'alice');
SELECT SLEEP(2);
INSERT INTO users VALUES (2, 'alice2');
COMMIT;
SELECT SLEEP(2);
INSERT INTO users VALUES (3, 'bob');
SQL

docker exec "$CONTAINER" mysql -uroot -ptest -e "FLUSH LOGS"

echo "Server version:"
docker exec "$CONTAINER" mysql -uroot -ptest -N -e "SELECT VERSION()"

echo "Extracting $CURRENT..."
docker cp "$CONTAINER:/var/lib/mysql/$CURRENT" "$OUT"

python3 - "$OUT" <<'PY'
import struct, sys
path = sys.argv[1]
data = open(path, "rb").read()
if data[:4] != b"\xfebin":
    sys.exit("binlog magic missing")
XID, GTID, WRITE_ROWS_V2 = 16, 33, 30
pos, events = 4, []
while pos + 19 <= len(data):
    _, typ, _, size, logpos, _ = struct.unpack_from("<IBIIIH", data, pos)
    if size < 19 or pos + size > len(data):
        sys.exit(f"truncated event at {pos}")
    events.append((pos, size, typ, logpos))
    pos += size
if pos != len(data):
    sys.exit("trailing bytes after last event")
gtids = [e for e in events if e[2] == GTID]
if len(gtids) < 2:
    sys.exit(f"want a later business GTID, got {len(gtids)} GTID events")
second = gtids[1][0]
drop = next((e for e in events if e[2] == XID and e[0] < second), None)
if drop is None:
    sys.exit("no XID before the later GTID")
kept = data[:drop[0]] + data[drop[0] + drop[1]:]
open(path, "wb").write(kept)
# Re-walk the file the parser will see.
pos, gtids, xids_before_second, rows = 4, 0, 0, 0
second_at = None
while pos + 19 <= len(kept):
    _, typ, _, size, _, _ = struct.unpack_from("<IBIIIH", kept, pos)
    if gtids == 1 and typ == XID:
        xids_before_second += 1
    if typ == GTID:
        gtids += 1
        if gtids == 2:
            second_at = pos
    if gtids == 1 and typ == WRITE_ROWS_V2:
        rows += 1
    pos += size
if xids_before_second or rows < 1 or second_at is None:
    sys.exit(f"open group check failed rows={rows} xid={xids_before_second} later={second_at}")
print(f"dropped first XID ({drop[1]} bytes); later GTID at file offset {second_at}")
PY

echo "Binlog file size:"
ls -la "$OUT"
echo "Done! Created mysql-8.0.46-open-begin-dml.binlog"
echo ""
echo "To verify:"
echo "  go test ./internal/analyzer ./cmd/binlogviz -count=1 -run 'OpenBeginDML'"
