"""Run Trino healthchecks and staged real-data queries in Codespaces."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pandas as pd
import trino
from tabulate import tabulate
from trino.dbapi import Connection


HOST = "localhost"
PORT = 8080
USER = "codespace"
HEALTHCHECK_QUERY = "SELECT 1"
REAL_DATA_QUERY = """
SELECT
    category,
    COUNT(*) AS products,
    ROUND(AVG(margin_pct), 4) AS avg_margin
FROM memory.default.products
GROUP BY category
ORDER BY products DESC
"""
PRODUCTS_PATH = Path("data/raw/products.parquet")
HEALTHCHECK_EXPORT_PATH = Path("exports/trino_healthcheck.json")
REAL_DATA_EXPORT_PATH = Path("exports/trino_real_data_results.json")
MAX_RETRIES = 3
RETRY_SECONDS = 5
STAGING_LIMIT = 50


def connect() -> Connection:
    """Create a Trino DB-API connection."""
    return trino.dbapi.connect(
        host=HOST,
        port=PORT,
        user=USER,
        http_scheme="http",
    )


def run_select_one(conn: Connection) -> list[tuple[Any, ...]]:
    """Execute the Phase 9A healthcheck query."""
    cursor = conn.cursor()
    cursor.execute(HEALTHCHECK_QUERY)
    return cursor.fetchall()


def execute_statement(conn: Connection, statement: str) -> None:
    """Execute a Trino SQL statement without returning rows."""
    cursor = conn.cursor()
    cursor.execute(statement)


def quote_sql_string(value: Any) -> str:
    """Format a Python value as a SQL string literal."""
    if value is None or pd.isna(value):
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def load_products_subset(path: Path = PRODUCTS_PATH, limit: int = STAGING_LIMIT) -> pd.DataFrame:
    """Read a bounded subset of project products from Parquet."""
    if not path.exists():
        raise FileNotFoundError(
            "Project products source is missing. Run `python src/generate_sources.py` first. "
            f"Missing: {path}"
        )

    columns = ["product_id", "name", "category", "margin_pct"]
    products = pd.read_parquet(path, columns=columns).head(limit).copy()
    return products.rename(columns={"name": "product_name"})


def stage_products_in_memory(conn: Connection, products: pd.DataFrame) -> None:
    """Stage real project product data into Trino memory for Phase 9B."""
    execute_statement(conn, "CREATE SCHEMA IF NOT EXISTS memory.default")
    execute_statement(conn, "DROP TABLE IF EXISTS memory.default.products")
    execute_statement(
        conn,
        """
        CREATE TABLE memory.default.products (
            product_id VARCHAR,
            product_name VARCHAR,
            category VARCHAR,
            margin_pct DOUBLE
        )
        """,
    )

    rows_sql = []
    for row in products.itertuples(index=False):
        rows_sql.append(
            "("
            f"{quote_sql_string(row.product_id)}, "
            f"{quote_sql_string(row.product_name)}, "
            f"{quote_sql_string(row.category)}, "
            f"{float(row.margin_pct)}"
            ")"
        )

    if rows_sql:
        execute_statement(
            conn,
            "INSERT INTO memory.default.products VALUES " + ", ".join(rows_sql),
        )


def run_real_data_query(conn: Connection) -> list[tuple[Any, ...]]:
    """Run a real-data query over staged project data in Trino memory."""
    cursor = conn.cursor()
    cursor.execute(REAL_DATA_QUERY)
    return cursor.fetchall()


def write_healthcheck(rows: list[tuple[Any, ...]], path: Path = HEALTHCHECK_EXPORT_PATH) -> None:
    """Persist the Trino healthcheck result as frontend-friendly JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "service": "trino",
        "host": HOST,
        "port": PORT,
        "mode": "healthcheck",
        "query": HEALTHCHECK_QUERY,
        "rows": [{"result": row[0]} for row in rows],
        "success": True,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_real_data_results(
    rows: list[tuple[Any, ...]],
    staged_records: int,
    path: Path = REAL_DATA_EXPORT_PATH,
) -> None:
    """Persist real-data Trino query results as frontend-friendly JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "service": "trino",
        "host": HOST,
        "port": PORT,
        "mode": "real-data",
        "strategy": "Trino real query over staged project data",
        "source": str(PRODUCTS_PATH),
        "staged_records": staged_records,
        "query": " ".join(REAL_DATA_QUERY.split()),
        "rows": [
            {
                "category": row[0],
                "products": row[1],
                "avg_margin": float(row[2]),
            }
            for row in rows
        ],
        "success": True,
        "limitation": "Uses Trino memory connector staged from products.parquet; final Delta + SQLite federation is planned for Phase 9C.",
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    """Run the Trino healthcheck and real-data staged query with retries."""
    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = connect()

            healthcheck_rows = run_select_one(conn)
            write_healthcheck(healthcheck_rows)
            print("Trino healthcheck succeeded")
            print(tabulate(healthcheck_rows, headers=["result"], tablefmt="github"))
            print(f"Healthcheck export: {HEALTHCHECK_EXPORT_PATH}")

            products = load_products_subset()
            stage_products_in_memory(conn, products)
            real_data_rows = run_real_data_query(conn)
            write_real_data_results(real_data_rows, staged_records=len(products))
            print("Trino real-data query over staged project data succeeded")
            print(tabulate(real_data_rows, headers=["category", "products", "avg_margin"], tablefmt="github"))
            print(f"Real-data export: {REAL_DATA_EXPORT_PATH}")
            return 0
        except Exception as exc:
            last_error = exc
            print(f"Trino query attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SECONDS)

    raise RuntimeError(
        "Trino query flow failed after retries. Start Trino with "
        "`bash scripts/setup_trino.sh` and verify http://localhost:8080."
    ) from last_error


if __name__ == "__main__":
    raise SystemExit(main())
