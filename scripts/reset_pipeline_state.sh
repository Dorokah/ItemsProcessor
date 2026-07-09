#!/bin/bash
set -euo pipefail

KAFKA_SERVICE="${KAFKA_SERVICE:-kafka}"
HBASE_SERVICE="${HBASE_SERVICE:-hbase}"
HBASE_CLIENT_SERVICE="${HBASE_CLIENT_SERVICE:-hbase-browser}"
HBASE_TABLE_NAME="${HBASE_TABLE_NAME:-pokemon}"

is_hbase_healthy() {
  docker compose exec -T "$HBASE_SERVICE" sh -c "ps aux | grep -q '[D]proc_master' && ps aux | grep -q '[D]proc_regionserver' && ps aux | grep -q '[D]proc_thrift'" >/dev/null 2>&1
}

ensure_hbase_ready() {
  echo "Checking HBase health..."
  if is_hbase_healthy; then
    echo "HBase is healthy."
    return 0
  fi

  echo "HBase is not healthy. Running init script..."
  bash "$(dirname "${BASH_SOURCE[0]}")/init_hbase.sh"

  echo "Checking HBase health after init..."
  is_hbase_healthy
}

echo "Clearing Kafka topic messages..."
docker compose exec -T "$KAFKA_SERVICE" bash -lc '
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPICS=$(/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list | grep -v "^__" || true)

if [ -z "$TOPICS" ]; then
  echo "No Kafka topics found to clear."
  exit 0
fi

for topic in $TOPICS; do
  echo "Clearing Kafka topic: $topic"
  offsets=$(/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP" --topic "$topic" --time -1 2>/dev/null || true)

  if [ -z "$offsets" ]; then
    echo "  No partitions found for topic $topic."
    continue
  fi

  json="{\"partitions\":["
  first=1
  while IFS=: read -r topic_name partition offset; do
    if [ -z "${topic_name:-}" ] || [ -z "${partition:-}" ] || [ -z "${offset:-}" ]; then
      continue
    fi
    if [ "$first" -eq 0 ]; then
      json="$json,"
    fi
    json="$json{\"topic\":\"$topic_name\",\"partition\":$partition,\"offset\":$offset}"
    first=0
  done <<EOF
$offsets
EOF
  json="$json],\"version\":1}"

  if [ "$first" -eq 1 ]; then
    echo "  No offsets found for topic $topic."
    continue
  fi

  printf "%s\n" "$json" > /tmp/delete-records.json
  /opt/kafka/bin/kafka-delete-records.sh --bootstrap-server "$BOOTSTRAP" --offset-json-file /tmp/delete-records.json
done
'

ensure_hbase_ready

echo "Deleting all HBase rows from table: $HBASE_TABLE_NAME"
docker compose exec -T "$HBASE_CLIENT_SERVICE" python3 -c "
import happybase
import os

host = os.environ.get('HBASE_HOST', 'hbase')
port = int(os.environ.get('HBASE_PORT', '9090'))
table_name = os.environ.get('HBASE_TABLE_NAME', '$HBASE_TABLE_NAME')

connection = happybase.Connection(host=host, port=port, timeout=10000)
connection.open()

if table_name.encode('utf-8') not in connection.tables():
    print(f'HBase table {table_name} does not exist; nothing to clear.')
    connection.close()
    raise SystemExit(0)

table = connection.table(table_name)
deleted = 0
batch = table.batch(batch_size=100)

for row_key, _ in table.scan():
    batch.delete(row_key)
    deleted += 1

batch.send()
connection.close()
print(f'Deleted {deleted} rows from HBase table {table_name}.')
"

echo "Pipeline state reset complete."
