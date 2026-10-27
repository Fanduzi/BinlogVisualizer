#!/usr/bin/env bash
# Turn a MySQL Docker service into a ROW/GTID/FULL server and print its name.
# A GitHub Actions service container cannot pass mysqld arguments, so this
# writes a config and restarts the container. If that server still has no
# binary log, the container is replaced with the same image and the flags.
# Usage: difftest_enable_binlog.sh <host-port> <server-id>
set -euo pipefail

port="$1"
server_id="$2"
password="${DIFFTEST_MYSQL_PASSWORD:-difftest}"

cid="$(docker ps -q --filter "publish=${port}" | head -n 1)"
if [ -z "$cid" ]; then
  echo "no container publishes ${port}" >&2
  exit 1
fi

write_cnf() {
  docker exec -i "$1" sh -c 'cat > /etc/mysql/conf.d/zz-binlogviz-difftest.cnf' <<EOF
[mysqld]
server-id=${server_id}
log_bin=mysql-bin
binlog_format=ROW
gtid_mode=ON
enforce_gtid_consistency=ON
binlog_row_image=FULL
binlog_row_metadata=FULL
binlog_checksum=CRC32
binlog_rows_query_log_events=ON
character-set-server=utf8mb4
collation-server=utf8mb4_0900_ai_ci
EOF
}

ready() {
  mysql --protocol=tcp -h 127.0.0.1 -P "$port" -uroot -p"$password" --connect-timeout=5 -N -e "SELECT 1" >/dev/null 2>&1
}

full() {
  local line
  line="$(mysql --protocol=tcp -h 127.0.0.1 -P "$port" -uroot -p"$password" -N --batch --connect-timeout=5 -e "SELECT @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_row_metadata, @@gtid_mode" 2>/dev/null || true)"
  set -- $line
  [ "${1:-}" = "1" ] && [ "${2:-}" = "ROW" ] && [ "${3:-}" = "FULL" ] && [ "${4:-}" = "FULL" ] && [ "${5:-}" = "ON" ]
}

image="$(docker inspect -f '{{.Config.Image}}' "$cid")"
write_cnf "$cid"
docker restart "$cid" >/dev/null
ok=0
for _ in $(seq 1 40); do
  if ready && full; then
    ok=1
    break
  fi
  sleep 2
done

if [ "$ok" != "1" ]; then
  docker rm -f "$cid" "bv-difftest-${port}" >/dev/null 2>&1 || true
  cid="$(docker run -d --name "bv-difftest-${port}" \
    -e MYSQL_ROOT_PASSWORD="$password" -e MYSQL_ROOT_HOST=% \
    -p "${port}:3306" "$image" \
    --server-id="$server_id" --log-bin=mysql-bin --binlog-format=ROW \
    --gtid-mode=ON --enforce-gtid-consistency=ON \
    --binlog-row-image=FULL --binlog-row-metadata=FULL \
    --binlog-checksum=CRC32 --binlog-rows-query-log-events=ON \
    --character-set-server=utf8mb4 --collation-server=utf8mb4_0900_ai_ci)"
  for _ in $(seq 1 60); do
    if ready && full; then
      ok=1
      break
    fi
    sleep 2
  done
fi

if [ "$ok" != "1" ]; then
  echo "MySQL on port ${port} did not become ROW/GTID/FULL" >&2
  docker logs "$cid" 2>&1 | tail -n 40 >&2 || true
  exit 1
fi
docker inspect -f '{{.Name}}' "$cid" | sed 's#^/##'
