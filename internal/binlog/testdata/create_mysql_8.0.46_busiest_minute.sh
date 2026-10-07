#!/bin/bash
# Generate a MySQL 8.0.46 ROW+GTID binlog where the busiest minute is not
# the window's hottest table.
#
# shop.catalog gets 20 rows at 14:00, 14:01, 14:02, and 14:03 (80 rows),
# plus 2 rows at 14:05. shop.orders gets 30 rows at 14:05, as 15 two-row
# inserts. The window leader is shop.catalog (82). The 14:05 minute is
# shop.orders (30) plus shop.catalog (2). Event timestamps come from
# SET TIMESTAMP, which MySQL writes into the binlog event header.
#
# Requires Docker and the mysql:8.0.46 image.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="mysql:8.0.46"
CONTAINER=""
OUT="$SCRIPT_DIR/mysql-8.0.46-busiest-minute.binlog"

cleanup() {
  if [ -n "$CONTAINER" ]; then
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

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

docker exec "$CONTAINER" mysql -uroot -e "RESET MASTER"
docker exec -i "$CONTAINER" mysql -uroot <<'SQL'
CREATE DATABASE IF NOT EXISTS shop;
CREATE TABLE shop.catalog (
  id INT NOT NULL PRIMARY KEY,
  note VARCHAR(16)
);
CREATE TABLE shop.orders (
  id INT NOT NULL PRIMARY KEY,
  note VARCHAR(16)
);
RESET MASTER;
SET TIMESTAMP = UNIX_TIMESTAMP('2026-03-15 14:00:00');
INSERT INTO shop.catalog (id, note) VALUES
  (1,'c'),(2,'c'),(3,'c'),(4,'c'),(5,'c'),(6,'c'),(7,'c'),(8,'c'),(9,'c'),(10,'c'),
  (11,'c'),(12,'c'),(13,'c'),(14,'c'),(15,'c'),(16,'c'),(17,'c'),(18,'c'),(19,'c'),(20,'c');
SET TIMESTAMP = UNIX_TIMESTAMP('2026-03-15 14:01:00');
INSERT INTO shop.catalog (id, note) VALUES
  (21,'c'),(22,'c'),(23,'c'),(24,'c'),(25,'c'),(26,'c'),(27,'c'),(28,'c'),(29,'c'),(30,'c'),
  (31,'c'),(32,'c'),(33,'c'),(34,'c'),(35,'c'),(36,'c'),(37,'c'),(38,'c'),(39,'c'),(40,'c');
SET TIMESTAMP = UNIX_TIMESTAMP('2026-03-15 14:02:00');
INSERT INTO shop.catalog (id, note) VALUES
  (41,'c'),(42,'c'),(43,'c'),(44,'c'),(45,'c'),(46,'c'),(47,'c'),(48,'c'),(49,'c'),(50,'c'),
  (51,'c'),(52,'c'),(53,'c'),(54,'c'),(55,'c'),(56,'c'),(57,'c'),(58,'c'),(59,'c'),(60,'c');
SET TIMESTAMP = UNIX_TIMESTAMP('2026-03-15 14:03:00');
INSERT INTO shop.catalog (id, note) VALUES
  (61,'c'),(62,'c'),(63,'c'),(64,'c'),(65,'c'),(66,'c'),(67,'c'),(68,'c'),(69,'c'),(70,'c'),
  (71,'c'),(72,'c'),(73,'c'),(74,'c'),(75,'c'),(76,'c'),(77,'c'),(78,'c'),(79,'c'),(80,'c');
SET TIMESTAMP = UNIX_TIMESTAMP('2026-03-15 14:05:00');
INSERT INTO shop.orders (id, note) VALUES (1,'o'),(2,'o');
INSERT INTO shop.orders (id, note) VALUES (3,'o'),(4,'o');
INSERT INTO shop.orders (id, note) VALUES (5,'o'),(6,'o');
INSERT INTO shop.orders (id, note) VALUES (7,'o'),(8,'o');
INSERT INTO shop.orders (id, note) VALUES (9,'o'),(10,'o');
INSERT INTO shop.orders (id, note) VALUES (11,'o'),(12,'o');
INSERT INTO shop.orders (id, note) VALUES (13,'o'),(14,'o');
INSERT INTO shop.orders (id, note) VALUES (15,'o'),(16,'o');
INSERT INTO shop.orders (id, note) VALUES (17,'o'),(18,'o');
INSERT INTO shop.orders (id, note) VALUES (19,'o'),(20,'o');
INSERT INTO shop.orders (id, note) VALUES (21,'o'),(22,'o');
INSERT INTO shop.orders (id, note) VALUES (23,'o'),(24,'o');
INSERT INTO shop.orders (id, note) VALUES (25,'o'),(26,'o');
INSERT INTO shop.orders (id, note) VALUES (27,'o'),(28,'o');
INSERT INTO shop.orders (id, note) VALUES (29,'o'),(30,'o');
INSERT INTO shop.catalog (id, note) VALUES (81,'c'),(82,'c');
SQL

docker exec "$CONTAINER" mysql -uroot -e "FLUSH BINARY LOGS"
current=$(docker exec "$CONTAINER" mysql -uroot -N -e "SHOW BINARY LOGS" | awk 'NR==1 {print $1}')
docker exec "$CONTAINER" cat "/var/lib/mysql/$current" > "$OUT"
chmod 644 "$OUT"
echo "Wrote $OUT"
