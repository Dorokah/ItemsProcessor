#!/bin/bash
set -e

# Define root directory path
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Checking if Docker containers are running..."
if ! docker compose ps | grep -q "Up"; then
  echo "Error: Docker containers are not running. Please run 'docker compose up -d' first."
  exit 1
fi

echo "Resetting pipeline state..."
bash "$REPO_ROOT/scripts/reset_pipeline_state.sh"

echo "Copying pokedex.json to the splitter-service container..."
docker cp "$REPO_ROOT/pokedex.json" splitter-service:/app/pokedex.json

echo "Running pokedex_producer inside splitter-service..."
docker compose exec -t splitter-service python3 src/pokedex_producer.py

echo "Pipeline triggered successfully! Check Grafana dashboard at http://localhost:3000 to view logs and traces."
