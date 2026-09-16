#!/bin/bash
# Generate a MySQL 8.0 ROW binlog with binlog_transaction_compression=ON.
# Requires Docker. Writes mysql80_transaction_payload.binlog next to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="${MYSQL_IMAGE:-mysql:8.0.36}"
OUT="$SCRIPT_DIR/mysql80_transaction_payload.binlog"

echo "Creating MySQL 8.0 container from $IMAGE..."
CONTAINER=$(docker run -d \
  -e MYSQL_ROOT_PASSWORD=test \
  -e MYSQL_DATABASE=testdb \
  "$IMAGE" \
  --server-id=1 \
  --log-bin=mysql-bin \
  --binlog-format=ROW \
  --binlog-transaction-compression=ON \
  --gtid-mode=ON \
  --enforce-gtid-consistency=ON)

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Waiting for MySQL to accept connections..."
for _ in $(seq 1 60); do
  if docker exec "$CONTAINER" mysqladmin ping -uroot -ptest --silent >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
docker exec "$CONTAINER" mysqladmin ping -uroot -ptest --silent >/dev/null

echo "Rotating away bootstrap binlogs..."
docker exec "$CONTAINER" mysql -uroot -ptest -e "FLUSH LOGS"

BINLOG=$(docker exec "$CONTAINER" mysql -uroot -ptest -N -e "SHOW MASTER STATUS" | awk '{print $1}')
if [ -z "$BINLOG" ]; then
  echo "failed to read SHOW MASTER STATUS" >&2
  exit 1
fi
echo "Writing compressed transaction into $BINLOG..."

docker exec -i "$CONTAINER" mysql -uroot -ptest testdb <<'SQL'
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(100)
);
BEGIN;
INSERT INTO users VALUES (1, 'alice');
UPDATE users SET name = 'alice_updated' WHERE id = 1;
DELETE FROM users WHERE id = 1;
COMMIT;
SQL

docker exec "$CONTAINER" mysql -uroot -ptest -e "FLUSH LOGS"
docker exec "$CONTAINER" mysql -uroot -ptest -e "SHOW BINARY LOGS"

echo "Extracting $BINLOG..."
docker cp "$CONTAINER:/var/lib/mysql/$BINLOG" "$OUT"
ls -la "$OUT"
echo "Done. Created mysql80_transaction_payload.binlog from $IMAGE ($BINLOG)"
