"""Run Trino healthchecks and staged real-data queries in Codespaces."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

import pandas as pd
import trino
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
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
FEDERATION_QUERY = """
SELECT
    r.name AS rep_name,
    r.region,
    r.tier,
    r.quota,
    SUM(t.amount) AS total_sales,
    COUNT(t.txn_id) AS num_transactions,
    ROUND(SUM(t.amount) / r.quota * 100, 1) AS quota_attainment_pct
FROM memory.default.sales_enriched t
JOIN memory.default.sales_reps r ON t.rep_id = r.rep_id
GROUP BY r.name, r.region, r.tier, r.quota
ORDER BY total_sales DESC
LIMIT 10
"""
REAL_CONNECTOR_FEDERATION_QUERY = """
SELECT
    r.name AS rep_name,
    r.region,
    r.tier,
    r.quota,
    SUM(t.amount) AS total_sales,
    COUNT(t.txn_id) AS num_transactions,
    ROUND(SUM(t.amount) / r.quota * 100, 1) AS quota_attainment_pct
FROM delta.silver.sales_enriched t
JOIN sqlite.default.sales_reps r ON t.rep_id = r.rep_id
GROUP BY r.name, r.region, r.tier, r.quota
ORDER BY total_sales DESC
LIMIT 10
"""
PRODUCTS_PATH = Path("data/raw/products.parquet")
SILVER_PATH = Path("data/delta/silver/sales_enriched")
SALES_REPS_DB_PATH = Path("data/raw/sales_reps.db")
HEALTHCHECK_EXPORT_PATH = Path("exports/trino_healthcheck.json")
REAL_DATA_EXPORT_PATH = Path("exports/trino_real_data_results.json")
FEDERATED_EXPORT_PATH = Path("exports/trino_federated_results.json")
MAX_RETRIES = 3
RETRY_SECONDS = 5
STAGING_LIMIT = 50
INSERT_CHUNK_SIZE = 500


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


def execute_query(conn: Connection, query: str) -> list[tuple[Any, ...]]:
    """Execute a Trino SQL query and return all rows."""
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def quote_sql_string(value: Any) -> str:
    """Format a Python value as a SQL string literal."""
    if value is None or pd.isna(value):
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_number(value: Any) -> str:
    """Format a Python value as a SQL numeric literal."""
    if value is None or pd.isna(value):
        return "NULL"
    return str(float(value))


def sql_int(value: Any) -> str:
    """Format a Python value as a SQL integer literal."""
    if value is None or pd.isna(value):
        return "NULL"
    return str(int(value))


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
    return execute_query(conn, REAL_DATA_QUERY)


def create_spark_session() -> SparkSession:
    """Create a Spark session configured with Delta Lake support."""
    try:
        builder = (
            SparkSession.builder.appName("SalesCommissionTrinoStaging")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as exc:
        raise RuntimeError(
            "Could not create SparkSession. Verify Java 17 is available in Codespaces "
            "with `source scripts/setup_codespaces_env.sh`."
        ) from exc


def load_silver_sales_enriched(path: Path = SILVER_PATH) -> pd.DataFrame:
    """Read Silver Delta rows needed for the federation-style fallback."""
    if not (path / "_delta_log").exists():
        raise FileNotFoundError(
            "Silver Delta table is missing. Run `python src/transform.py` first. "
            f"Missing: {path}"
        )

    spark = create_spark_session()
    try:
        columns = ["txn_id", "rep_id", "amount"]
        return spark.read.format("delta").load(str(path)).select(*columns).toPandas()
    finally:
        spark.stop()


def load_sales_reps(path: Path = SALES_REPS_DB_PATH) -> pd.DataFrame:
    """Read sales reps from the project SQLite source."""
    if not path.exists():
        raise FileNotFoundError(
            "SQLite sales reps source is missing. Run `python src/generate_sources.py` first. "
            f"Missing: {path}"
        )

    with sqlite3.connect(path) as conn:
        return pd.read_sql_query(
            "SELECT rep_id, name, region, tier, quota FROM sales_reps",
            conn,
        )


def insert_rows(conn: Connection, table: str, rows_sql: list[str]) -> None:
    """Insert rows into a Trino table in chunks."""
    for start in range(0, len(rows_sql), INSERT_CHUNK_SIZE):
        chunk = rows_sql[start : start + INSERT_CHUNK_SIZE]
        execute_statement(conn, f"INSERT INTO {table} VALUES " + ", ".join(chunk))


def stage_sales_enriched_in_memory(conn: Connection, sales_enriched: pd.DataFrame) -> None:
    """Stage Silver sales rows into Trino memory."""
    execute_statement(conn, "DROP TABLE IF EXISTS memory.default.sales_enriched")
    execute_statement(
        conn,
        """
        CREATE TABLE memory.default.sales_enriched (
            txn_id VARCHAR,
            rep_id VARCHAR,
            amount DOUBLE
        )
        """,
    )

    rows_sql = [
        "("
        f"{quote_sql_string(row.txn_id)}, "
        f"{quote_sql_string(row.rep_id)}, "
        f"{sql_number(row.amount)}"
        ")"
        for row in sales_enriched.itertuples(index=False)
    ]
    insert_rows(conn, "memory.default.sales_enriched", rows_sql)


def stage_sales_reps_in_memory(conn: Connection, sales_reps: pd.DataFrame) -> None:
    """Stage SQLite sales reps into Trino memory."""
    execute_statement(conn, "DROP TABLE IF EXISTS memory.default.sales_reps")
    execute_statement(
        conn,
        """
        CREATE TABLE memory.default.sales_reps (
            rep_id VARCHAR,
            name VARCHAR,
            region VARCHAR,
            tier VARCHAR,
            quota INTEGER
        )
        """,
    )

    rows_sql = [
        "("
        f"{quote_sql_string(row.rep_id)}, "
        f"{quote_sql_string(row.name)}, "
        f"{quote_sql_string(row.region)}, "
        f"{quote_sql_string(row.tier)}, "
        f"{sql_int(row.quota)}"
        ")"
        for row in sales_reps.itertuples(index=False)
    ]
    insert_rows(conn, "memory.default.sales_reps", rows_sql)


def catalogs_available(conn: Connection, required: set[str]) -> bool:
    """Return whether all required Trino catalogs are available."""
    catalogs = {str(row[0]).lower() for row in execute_query(conn, "SHOW CATALOGS")}
    return required.issubset(catalogs)


def rows_to_federated_records(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    """Convert federation query rows to JSON records."""
    return [
        {
            "rep_name": row[0],
            "region": row[1],
            "tier": row[2],
            "quota": int(row[3]),
            "total_sales": round(float(row[4]), 2),
            "num_transactions": int(row[5]),
            "quota_attainment_pct": float(row[6]),
        }
        for row in rows
    ]


def write_federated_results(payload: dict[str, Any], path: Path = FEDERATED_EXPORT_PATH) -> None:
    """Persist the Phase 9C federation result payload."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_real_connector_federation(conn: Connection) -> list[tuple[Any, ...]]:
    """Attempt real Delta + SQLite connector federation if catalogs exist."""
    if not catalogs_available(conn, {"delta", "sqlite"}):
        raise RuntimeError("Trino catalogs `delta` and `sqlite` are not both configured.")
    return execute_query(conn, REAL_CONNECTOR_FEDERATION_QUERY)


def run_staged_federation_fallback(conn: Connection) -> tuple[list[tuple[Any, ...]], dict[str, int]]:
    """Run federation-style SQL in Trino over staged Silver Delta and SQLite data."""
    execute_statement(conn, "CREATE SCHEMA IF NOT EXISTS memory.default")
    sales_enriched = load_silver_sales_enriched()
    sales_reps = load_sales_reps()
    stage_sales_enriched_in_memory(conn, sales_enriched)
    stage_sales_reps_in_memory(conn, sales_reps)
    rows = execute_query(conn, FEDERATION_QUERY)
    return rows, {
        "sales_enriched": len(sales_enriched),
        "sales_reps": len(sales_reps),
    }


def run_federation_query(conn: Connection) -> dict[str, Any]:
    """Attempt real federation and fall back to staged memory federation."""
    attempted_real_federation = True

    try:
        rows = run_real_connector_federation(conn)
        payload = {
            "success": True,
            "mode": "real-connector-federation",
            "fallback_used": False,
            "attempted_real_federation": attempted_real_federation,
            "query": " ".join(REAL_CONNECTOR_FEDERATION_QUERY.split()),
            "rows": rows_to_federated_records(rows),
        }
        write_federated_results(payload)
        return payload
    except Exception as real_exc:
        print(f"Real connector federation unavailable: {real_exc}")

        try:
            rows, staged_records = run_staged_federation_fallback(conn)
            payload = {
                "success": True,
                "mode": "federation-style-staged-fallback",
                "fallback_used": True,
                "attempted_real_federation": attempted_real_federation,
                "staged_records": staged_records,
                "query": " ".join(FEDERATION_QUERY.split()),
                "limitation": (
                    "Delta + SQLite Trino connector federation is not configured or validated yet. "
                    "This fallback still executes SQL in Trino over two separate memory tables staged "
                    "from Delta Silver and SQLite project data."
                ),
                "rows": rows_to_federated_records(rows),
            }
            write_federated_results(payload)
            return payload
        except Exception as fallback_exc:
            payload = {
                "success": False,
                "mode": "federation-failed",
                "attempted_real_federation": attempted_real_federation,
                "fallback_used": False,
                "error": str(fallback_exc),
                "real_federation_error": str(real_exc),
            }
            write_federated_results(payload)
            return payload


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

            federated_payload = run_federation_query(conn)
            print("Trino federation query result")
            if federated_payload["success"]:
                federated_rows = [
                    (
                        row["rep_name"],
                        row["region"],
                        row["tier"],
                        row["quota"],
                        row["total_sales"],
                        row["num_transactions"],
                        row["quota_attainment_pct"],
                    )
                    for row in federated_payload["rows"]
                ]
                print(
                    tabulate(
                        federated_rows,
                        headers=[
                            "rep_name",
                            "region",
                            "tier",
                            "quota",
                            "total_sales",
                            "num_transactions",
                            "quota_attainment_pct",
                        ],
                        tablefmt="github",
                    )
                )
            else:
                print(f"Federation failed: {federated_payload['error']}")
            print(f"Federated export: {FEDERATED_EXPORT_PATH}")

            return 0 if federated_payload["success"] else 1
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
