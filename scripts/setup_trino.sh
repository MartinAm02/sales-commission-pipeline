#!/usr/bin/env bash
set -euo pipefail

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not available. Rebuild the Codespace with Docker-in-Docker enabled." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose is not available. Rebuild the Codespace and verify Docker-in-Docker." >&2
  exit 1
fi

if ! command -v python >/dev/null 2>&1; then
  echo "Python is not available in this Codespace." >&2
  exit 1
fi

echo "Installing Python dependencies from requirements.txt..."
python -m pip install -r requirements.txt

if ! python -c "import trino" >/dev/null 2>&1; then
  echo "Python package 'trino' is not importable after dependency installation." >&2
  exit 1
fi

echo "Starting Trino with Docker Compose..."
if ! docker compose up -d; then
  echo "Failed to start Trino with Docker Compose." >&2
  docker compose logs trino >&2 || true
  exit 1
fi

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
