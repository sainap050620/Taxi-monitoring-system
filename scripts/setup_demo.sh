#!/usr/bin/env bash
# Prepares everything the stack needs before `docker compose up`:
#   1. downloads a sample of the T-Drive taxi dataset (551 taxis, about 8 MB)
#   2. places it where the Kafka producer expects it
#   3. copies the Kibana dashboard so the connector imports it on start
set -euo pipefail
cd "$(dirname "$0")/.."

DATA_URL="https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/06.zip"
PRODUCER_DIR="volumes/kafka-producer_volume"
CONNECTOR_DIR="volumes/kafka_elasticsearch_connector_volume"
DATA_FILE="$PRODUCER_DIR/taxi_log_2008_by_id.zip"

mkdir -p "$PRODUCER_DIR" "$CONNECTOR_DIR"

if [ -f "$DATA_FILE" ]; then
  echo "Dataset already present: $DATA_FILE"
else
  echo "Downloading T-Drive sample from Microsoft Research..."
  curl -fL --progress-bar -o "$DATA_FILE" "$DATA_URL"
  unzip -tq "$DATA_FILE" >/dev/null || { echo "Download is not a valid zip"; rm -f "$DATA_FILE"; exit 1; }
fi

cp kibana/exported_objects.ndjson "$CONNECTOR_DIR/exported_objects.ndjson"

echo
echo "Ready. Start the stack with:  docker compose up -d"
echo "Then open: http://localhost:5601/app/dashboards#/view/taxi-fleet-monitor"
