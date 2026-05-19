"""Ingest raw source data into the Delta Lake Bronze layer."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession


RAW_DATA_DIR = Path("data/raw")
DELTA_BRONZE_DIR = Path("data/delta/bronze")
TRANSACTIONS_PATH = RAW_DATA_DIR / "transactions.csv"
SALES_REPS_DB_PATH = RAW_DATA_DIR / "sales_reps.db"
PRODUCTS_PATH = RAW_DATA_DIR / "products.parquet"


def validate_sources() -> None:
    """Validate that all raw source files required by ingestion exist."""
    missing = [
        str(path)
        for path in (TRANSACTIONS_PATH, SALES_REPS_DB_PATH, PRODUCTS_PATH)
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError(
            "Missing raw source files. Run `python src/generate_sources.py` first. "
            f"Missing: {', '.join(missing)}"
        )


def create_spark_session() -> SparkSession:
    """Create a Spark session configured with Delta Lake support."""
    try:
        builder = (
            SparkSession.builder.appName("SalesCommissionIngest")
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
            "Could not create SparkSession with Delta Lake. Verify Java is installed "
            "and compatible with Spark 3.5. Use Java 11 or 17 on Windows."
        ) from exc


def read_transactions(spark: SparkSession) -> DataFrame:
    """Read raw sales transactions from CSV."""
    try:
        return (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(TRANSACTIONS_PATH))
        )
    except Exception as exc:
        raise RuntimeError(f"Could not read transactions CSV at {TRANSACTIONS_PATH}") from exc


def read_sales_reps(spark: SparkSession) -> DataFrame:
    """Read sales reps from SQLite and convert them to a Spark DataFrame."""
    try:
        # Avoid an external SQLite JDBC .jar for local Windows portability.
        with sqlite3.connect(SALES_REPS_DB_PATH) as conn:
            reps = pd.read_sql_query("SELECT rep_id, name, region, tier, quota FROM sales_reps", conn)
        return spark.createDataFrame(reps)
    except (sqlite3.Error, pd.errors.DatabaseError, Exception) as exc:
        raise RuntimeError(f"Could not read sales reps from SQLite at {SALES_REPS_DB_PATH}") from exc


def read_products(spark: SparkSession) -> DataFrame:
    """Read the product catalog from Parquet."""
    try:
        return spark.read.parquet(str(PRODUCTS_PATH))
    except Exception as exc:
        raise RuntimeError(f"Could not read products Parquet at {PRODUCTS_PATH}") from exc


def write_delta(df: DataFrame, path: Path) -> None:
    """Write a Spark DataFrame as a Delta table."""
    try:
        df.write.format("delta").mode("overwrite").save(str(path))
    except Exception as exc:
        raise RuntimeError(f"Could not write Delta table at {path}") from exc


def main() -> None:
    """Run Bronze ingestion for transactions, sales reps, and products."""
    validate_sources()
    spark = create_spark_session()

    try:
        transactions = read_transactions(spark)
        sales_reps = read_sales_reps(spark)
        products = read_products(spark)

        write_delta(transactions, DELTA_BRONZE_DIR / "transactions")
        write_delta(sales_reps, DELTA_BRONZE_DIR / "sales_reps")
        write_delta(products, DELTA_BRONZE_DIR / "products")

        transaction_count = transactions.count()
        sales_rep_count = sales_reps.count()
        product_count = products.count()

        print("Bronze ingestion complete")
        print(f"Transactions: {transaction_count}")
        print(f"Sales reps: {sales_rep_count}")
        print(f"Products: {product_count}")
        print(f"Bronze output directory: {DELTA_BRONZE_DIR}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
