#!/usr/bin/env bash
set -e

CONNECT_URL=${CONNECT_URL:-http://kafka-connect:8083}
POSTGRES_HOST=${DEBEZIUM_POSTGRES_HOST:-postgres-dept}
POSTGRES_PORT=${DEBEZIUM_POSTGRES_PORT:-5432}
POSTGRES_USER=${DEBEZIUM_POSTGRES_USER:-postgres}
POSTGRES_PASSWORD=${DEBEZIUM_POSTGRES_PASSWORD:-changeme_strong_password}

CONNECTOR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/connectors"


python3 "$(dirname "${BASH_SOURCE[0]}")/generate_connectors.py" \
  --output-dir "$CONNECTOR_DIR" \
  --postgres-host "$POSTGRES_HOST" \
  --postgres-port "$POSTGRES_PORT" \
  --postgres-user "$POSTGRES_USER" \
  --postgres-password "$POSTGRES_PASSWORD"

for file in "$CONNECTOR_DIR"/*.json; do
  connector_name=$(basename "$file" .json)
  echo "Registering connector $connector_name"
  curl -s -X PUT "$CONNECT_URL/connectors/$connector_name/config" \
    -H "Content-Type: application/json" \
    --data-binary "@$file"
  echo
  sleep 1
done