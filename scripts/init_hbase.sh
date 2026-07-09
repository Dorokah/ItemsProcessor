#!/bin/bash
set -euo pipefail

HBASE_SERVICE="${HBASE_SERVICE:-hbase}"
HBASE_HEALTH_RETRIES="${HBASE_HEALTH_RETRIES:-24}"
HBASE_HEALTH_SLEEP_SECONDS="${HBASE_HEALTH_SLEEP_SECONDS:-5}"

is_hbase_healthy() {
  docker compose exec -T "$HBASE_SERVICE" sh -c "ps aux | grep -q '[D]proc_master' && ps aux | grep -q '[D]proc_regionserver' && ps aux | grep -q '[D]proc_thrift'" >/dev/null 2>&1
}

echo "Initializing HBase..."
docker compose restart "$HBASE_SERVICE"

attempt=0
while [ "$attempt" -lt "$HBASE_HEALTH_RETRIES" ]; do
  attempt=$((attempt + 1))

  if is_hbase_healthy; then
    echo "HBase is healthy."
    exit 0
  fi

  echo "Waiting for HBase to become healthy... ($attempt/$HBASE_HEALTH_RETRIES)"
  sleep "$HBASE_HEALTH_SLEEP_SECONDS"
done

echo "Error: HBase did not become healthy after initialization."
docker compose ps "$HBASE_SERVICE" || true
docker compose exec -T "$HBASE_SERVICE" ps aux || true
exit 1
