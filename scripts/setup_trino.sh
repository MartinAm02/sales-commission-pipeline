#!/usr/bin/env bash
set -euo pipefail

echo "Starting Trino with Docker Compose..."
docker compose up -d

echo "Waiting for Trino at http://localhost:8080/v1/info ..."
for attempt in {1..60}; do
  if curl -fsS http://localhost:8080/v1/info >/dev/null; then
    echo "Trino is ready at http://localhost:8080"
    exit 0
  fi

  sleep 2
done

echo "Trino did not become ready within 120 seconds." >&2
docker compose logs trino >&2
exit 1
