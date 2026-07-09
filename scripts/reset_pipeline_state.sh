#!/usr/bin/env bash
set -euo pipefail

HBASE_TABLE_NAME="${HBASE_TABLE_NAME:-pokemon}"
HBASE_CONTAINER="${HBASE_CONTAINER:-hbase}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
KAFKA_BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}"
KAFKA_TOPICS="${KAFKA_TOPICS:-pokedex-raw pokemon-individual hbase-status}"

echo "Resetting pipeline state..."
echo "HBase table: ${HBASE_TABLE_NAME}"
echo "Kafka topics: ${KAFKA_TOPICS}"

echo
echo "Clearing all rows from HBase table '${HBASE_TABLE_NAME}' without dropping the table..."
docker compose exec -T "${HBASE_CONTAINER}" bash -s -- "${HBASE_TABLE_NAME}" <<'EOF'
set -euo pipefail

TABLE_NAME="$1"
SCRIPT_PATH="/tmp/clear_hbase_table.rb"

cat > "${SCRIPT_PATH}" <<'RUBY'
include Java

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Scan

table_name = TableName.valueOf(ARGV[0])
config = HBaseConfiguration.create
connection = ConnectionFactory.createConnection(config)
admin = connection.getAdmin

begin
  unless admin.tableExists(table_name)
    raise "HBase table '#{ARGV[0]}' does not exist"
  end

  table = connection.getTable(table_name)
  scanner = table.getScanner(Scan.new)
  deleted_rows = 0

  begin
    scanner.each do |result|
      table.delete(Delete.new(result.getRow))
      deleted_rows += 1
    end
  ensure
    scanner.close
    table.close
  end

  puts "Deleted #{deleted_rows} rows from '#{ARGV[0]}'. Table was preserved."
ensure
  admin.close
  connection.close
end
RUBY

hbase shell -n "${SCRIPT_PATH}" "${TABLE_NAME}"
rm -f "${SCRIPT_PATH}"
EOF

echo
echo "Removing Kafka messages from topics without deleting the topics..."
docker compose exec -T "${KAFKA_CONTAINER}" bash -s -- "${KAFKA_BOOTSTRAP_SERVER}" ${KAFKA_TOPICS} <<'EOF'
set -euo pipefail

BOOTSTRAP_SERVER="$1"
shift

find_kafka_tool() {
  local tool_name="$1"
  local path

  path="$(command -v "${tool_name}" || true)"
  if [[ -n "${path}" ]]; then
    echo "${path}"
    return
  fi

  path="$(find /opt /usr -name "${tool_name}" -type f 2>/dev/null | head -n 1 || true)"
  if [[ -n "${path}" ]]; then
    echo "${path}"
    return
  fi

  echo "Could not find ${tool_name} in Kafka container" >&2
  exit 1
}

KAFKA_TOPICS_SH="$(find_kafka_tool kafka-topics.sh)"
KAFKA_GET_OFFSETS_SH="$(find_kafka_tool kafka-get-offsets.sh)"
KAFKA_DELETE_RECORDS_SH="$(find_kafka_tool kafka-delete-records.sh)"

for TOPIC in "$@"; do
  if ! "${KAFKA_TOPICS_SH}" --bootstrap-server "${BOOTSTRAP_SERVER}" --list | grep -Fxq "${TOPIC}"; then
    echo "Skipping missing Kafka topic '${TOPIC}'."
    continue
  fi

  OFFSETS_FILE="/tmp/delete-records-${TOPIC}.json"
  OFFSETS="$("${KAFKA_GET_OFFSETS_SH}" --bootstrap-server "${BOOTSTRAP_SERVER}" --topic "${TOPIC}" --time -1)"

  if [[ -z "${OFFSETS}" ]]; then
    echo "Topic '${TOPIC}' has no partitions to clear."
    continue
  fi

  printf '{"partitions":[' > "${OFFSETS_FILE}"
  FIRST_PARTITION=1
  while IFS=: read -r OFFSET_TOPIC PARTITION OFFSET; do
    if [[ "${FIRST_PARTITION}" -eq 0 ]]; then
      printf ',' >> "${OFFSETS_FILE}"
    fi

    printf '{"topic":"%s","partition":%s,"offset":%s}' "${OFFSET_TOPIC}" "${PARTITION}" "${OFFSET}" >> "${OFFSETS_FILE}"
    FIRST_PARTITION=0
  done <<< "${OFFSETS}"
  printf '],"version":1}\n' >> "${OFFSETS_FILE}"

  "${KAFKA_DELETE_RECORDS_SH}" --bootstrap-server "${BOOTSTRAP_SERVER}" --offset-json-file "${OFFSETS_FILE}"
  rm -f "${OFFSETS_FILE}"
  echo "Cleared messages from Kafka topic '${TOPIC}'."
done
EOF

echo
echo "Pipeline state reset complete."
