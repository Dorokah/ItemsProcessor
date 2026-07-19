#!/bin/bash
set -e

# Define root directory path
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Checking if Docker containers are running..."
if ! docker compose ps | grep -q "Up"; then
  echo "Error: Docker containers are not running. Please run 'docker compose up -d' first."
  exit 1
fi

#echo "Resetting Kafka messages and HBase data..."
#bash "$REPO_ROOT/scripts/reset_pipeline_state.sh"

echo "Publishing pokedex.json with the Spring/Reactor producer mode..."
docker compose exec -T splitter-service java -jar /app/app.jar \
  --items.mode=producer \
  --items.pokedex-file=/app/pokedex.json \
  --items.consume-topic=pokedex-raw \
  --server.port=0

echo "Pipeline triggered successfully! Check Grafana dashboard at http://localhost:3000 to view logs and traces."
