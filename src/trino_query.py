"""Run a basic Trino healthcheck query against the local Codespaces service."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import trino
from tabulate import tabulate


HOST = "localhost"
PORT = 8080
USER = "codespace"
QUERY = "SELECT 1"
EXPORT_PATH = Path("exports/trino_healthcheck.json")
MAX_RETRIES = 3
RETRY_SECONDS = 5


def run_select_one() -> list[tuple[Any, ...]]:
    """Connect to Trino and execute the Phase 9A healthcheck query."""
    conn = trino.dbapi.connect(
        host=HOST,
        port=PORT,
        user=USER,
        http_scheme="http",
    )
    cursor = conn.cursor()
    cursor.execute(QUERY)
    return cursor.fetchall()


def write_healthcheck(rows: list[tuple[Any, ...]], path: Path = EXPORT_PATH) -> None:
    """Persist the Trino healthcheck result as frontend-friendly JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "service": "trino",
        "host": HOST,
        "port": PORT,
        "query": QUERY,
        "rows": [{"result": row[0]} for row in rows],
        "success": True,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    """Run the Trino healthcheck with retries."""
    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rows = run_select_one()
            write_healthcheck(rows)
            print("Trino healthcheck succeeded")
            print(tabulate(rows, headers=["result"], tablefmt="github"))
            print(f"Healthcheck export: {EXPORT_PATH}")
            return 0
        except Exception as exc:
            last_error = exc
            print(f"Trino healthcheck attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SECONDS)

    raise RuntimeError(
        "Trino healthcheck failed after retries. Start Trino with "
        "`bash scripts/setup_trino.sh` and verify http://localhost:8080."
    ) from last_error


if __name__ == "__main__":
    raise SystemExit(main())
