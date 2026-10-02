#!/bin/bash
# Generate a MySQL 8.0.46 ROW+GTID fixture with one committed transaction
# whose wall clock crosses a non-<1s duration bucket.
#
# InnoDB writes the group at COMMIT. The GTID event is first in the file and
# shares the XID's commit timestamp. BEGIN and the row image keep the
# statement-start timestamp. SLEEP between the INSERT and COMMIT is what
# separates those seconds. The script does not rewrite timestamps.
#
# Requires Docker and the mysql:8.0.46 image.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="mysql:8.0.46"
OUT="$SCRIPT_DIR/mysql-8.0.46-committed-duration.binlog"
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

echo "Rotating so the timed transaction and the short insert share one file..."
docker exec "$CONTAINER" mysql -uroot -ptest -e "FLUSH LOGS"
CURRENT=$(docker exec "$CONTAINER" mysql -uroot -ptest -N -e "SHOW BINARY LOGS" | awk 'END {print $1}')
if [ -z "$CURRENT" ]; then
  echo "could not determine current binlog" >&2
  exit 1
fi

echo "Writing BEGIN + INSERT + SLEEP(2) + COMMIT, then a sub-second INSERT, into $CURRENT..."
docker exec -i "$CONTAINER" mysql -uroot -ptest testdb <<'SQL'
BEGIN;
INSERT INTO users VALUES (1, 'alice');
SELECT SLEEP(2);
COMMIT;
INSERT INTO users VALUES (2, 'bob');
SQL

docker exec "$CONTAINER" mysql -uroot -ptest -e "FLUSH LOGS"

echo "Server version:"
docker exec "$CONTAINER" mysql -uroot -ptest -N -e "SELECT VERSION()"

echo "Extracting $CURRENT..."
docker cp "$CONTAINER:/var/lib/mysql/$CURRENT" "$OUT"

echo "Binlog file size:"
ls -la "$OUT"
echo "Done! Created mysql-8.0.46-committed-duration.binlog"
echo ""
echo "To verify:"
echo "  go test ./internal/analyzer ./cmd/binlogviz -count=1 -run 'CommittedDuration'"
