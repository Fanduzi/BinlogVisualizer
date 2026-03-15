#!/bin/bash
# Generate a minimal MySQL ROW-format binlog fixture
# Requires Docker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Creating MySQL container..."
CONTAINER=$(docker run -d \
  -e MYSQL_ROOT_PASSWORD=test \
  -e MYSQL_DATABASE=testdb \
  mysql:5.7 \
  --binlog-format=ROW \
  --log-bin=mysql-bin \
  --server-id=1)

echo "Waiting for MySQL to start..."
sleep 20

echo "Creating database and table..."
docker exec -i $CONTAINER mysql -uroot -ptest testdb <<'SQL'
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(100)
);
SQL

echo "Inserting test data..."
docker exec -i $CONTAINER mysql -uroot -ptest testdb <<'SQL'
INSERT INTO users VALUES (1, 'alice');
INSERT INTO users VALUES (2, 'bob');
UPDATE users SET name = 'alice_updated' WHERE id = 1;
DELETE FROM users WHERE id = 2;
SQL

echo "Flushing logs..."
docker exec $CONTAINER mysql -uroot -ptest -e "FLUSH LOGS"

echo "Finding binlog files..."
docker exec $CONTAINER mysql -uroot -ptest -e "SHOW BINARY LOGS"

# Extract the binlog with our data
# mysql-bin.000003 contains the INSERT/UPDATE/DELETE operations
echo "Extracting binlog with test data..."
docker cp $CONTAINER:/var/lib/mysql/mysql-bin.000003 "$SCRIPT_DIR/minimal.binlog"

echo "Binlog file size:"
ls -la "$SCRIPT_DIR/minimal.binlog"

echo "Cleaning up..."
docker rm -f $CONTAINER

echo "Done! Created minimal.binlog"
echo ""
echo "To verify:"
echo "  go test ./internal/binlog -v -run TestReal"
