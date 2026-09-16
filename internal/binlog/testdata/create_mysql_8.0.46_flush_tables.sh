#!/bin/bash
# Generate a MySQL 8.0.46 ROW+GTID dialect fixture whose only maintenance
# work in one GTID-started group is FLUSH TABLES, followed by a business INSERT.
# Requires Docker and the mysql:8.0.46 image.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="mysql:8.0.46"
OUT="$SCRIPT_DIR/mysql-8.0.46-flush-tables.binlog"
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

echo "Rotating so FLUSH TABLES and the following INSERT share one file..."
docker exec "$CONTAINER" mysql -uroot -ptest -e "FLUSH LOGS"
CURRENT=$(docker exec "$CONTAINER" mysql -uroot -ptest -N -e "SHOW BINARY LOGS" | awk 'END {print $1}')
if [ -z "$CURRENT" ]; then
  echo "could not determine current binlog" >&2
  exit 1
fi

echo "Writing FLUSH TABLES then one business INSERT into $CURRENT..."
docker exec -i "$CONTAINER" mysql -uroot -ptest testdb <<'SQL'
FLUSH TABLES;
INSERT INTO users VALUES (1, 'alice');
SQL

docker exec "$CONTAINER" mysql -uroot -ptest -e "FLUSH LOGS"

echo "Server version:"
docker exec "$CONTAINER" mysql -uroot -ptest -N -e "SELECT VERSION()"

echo "Extracting $CURRENT..."
docker cp "$CONTAINER:/var/lib/mysql/$CURRENT" "$OUT"

echo "Binlog file size:"
ls -la "$OUT"

echo "Done! Created mysql-8.0.46-flush-tables.binlog"
echo ""
echo "To verify:"
echo "  go test ./internal/analyzer -count=1 -run TestFlushTablesDialectFixture"
