#!/usr/bin/env bash
set -euo pipefail

ES_URL="${ES_URL:-http://localhost:9200}"
INDEX_DATE="${1:-$(date -u +%Y.%m.%d)}"

SERVICES=(
  splitter
  hbase-writer
  webhook
  pokemon-surname-generator
  surname-api
  surname-enrichment
  hbase-surname-updater
)

read -r -d '' TEMPLATE_BODY <<'JSON' || true
{
  "index_patterns": ["rabbitprocessor-pipeline-*"],
  "priority": 500,
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    },
    "mappings": {
      "dynamic": true,
      "properties": {
        "@timestamp": { "type": "date" },
        "project": { "type": "keyword" },
        "serviceName": { "type": "keyword" },
        "pipelineName": { "type": "keyword" },
        "requestId": { "type": "keyword" },
        "entityId": { "type": "keyword" },
        "traceId": { "type": "keyword" },
        "statusType": { "type": "keyword" },
        "level": { "type": "keyword" },
        "surname": { "type": "keyword" },
        "frenchName": { "type": "keyword" },
        "publishTopic": { "type": "keyword" },
        "message": {
          "type": "text",
          "fields": {
            "keyword": { "type": "keyword", "ignore_above": 256 }
          }
        },
        "exception": { "type": "text" },
        "processingDuration": { "type": "double" },
        "batchSize": { "type": "integer" },
        "hbaseBatchSize": { "type": "integer" },
        "kafkaProducedCount": { "type": "integer" },
        "hbaseSuccessCount": { "type": "integer" },
        "hbaseFailureCount": { "type": "integer" },
        "failureCount": { "type": "integer" }
      }
    }
  }
}
JSON

read -r -d '' LEGACY_TEMPLATE_BODY <<'JSON' || true
{
  "index_patterns": ["rabbitprocessor-logs-rabbitprocessor-pipeline-*"],
  "priority": 500,
  "template": {
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    }
  }
}
JSON

read -r -d '' INDEX_BODY <<'JSON' || true
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  }
}
JSON

put_json() {
  local path="$1"
  local body="$2"
  curl -sS -X PUT "$ES_URL$path" \
    -H "Content-Type: application/json" \
    -d "$body"
  echo
}

echo "Creating normal index template for rabbitprocessor-pipeline-*"
put_json "/_index_template/rabbitprocessor-pipeline-logs" "$TEMPLATE_BODY"

echo "Replacing legacy rabbitprocessor-logs template without data_stream"
put_json "/_index_template/rabbit" "$LEGACY_TEMPLATE_BODY"

for service in "${SERVICES[@]}"; do
  index="rabbitprocessor-pipeline-${service}-${INDEX_DATE}"
  if curl -fsS -I "$ES_URL/$index" >/dev/null 2>&1; then
    echo "Index already exists: $index"
  else
    echo "Creating index: $index"
    put_json "/$index" "$INDEX_BODY"
  fi
done

echo "Setting existing app log indices to 0 replicas"
curl -sS -X PUT "$ES_URL/rabbitprocessor-pipeline-*/_settings" \
  -H "Content-Type: application/json" \
  -d '{"index":{"number_of_replicas":0}}'
echo

curl -sS -X PUT "$ES_URL/rabbitprocessor-logs-rabbitprocessor-pipeline-*/_settings" \
  -H "Content-Type: application/json" \
  -d '{"index":{"number_of_replicas":0}}'
echo

echo "Current app indices:"
curl -sS "$ES_URL/_cat/indices/rabbitprocessor-pipeline-*,rabbitprocessor-logs-rabbitprocessor-pipeline-*?v&s=index"
echo
