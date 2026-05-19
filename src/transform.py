"""Transform Bronze Delta tables into Silver and Gold medallion layers."""

from __future__ import annotations

from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DELTA_DIR = Path("data/delta")
BRONZE_DIR = DELTA_DIR / "bronze"
SILVER_DIR = DELTA_DIR / "silver"
GOLD_DIR = DELTA_DIR / "gold"


def validate_bronze_tables() -> None:
    """Validate that Bronze Delta tables exist before transforming."""
    required = [
        BRONZE_DIR / "transactions" / "_delta_log",
        BRONZE_DIR / "sales_reps" / "_delta_log",
        BRONZE_DIR / "products" / "_delta_log",
    ]
    missing = [str(path.parent) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Bronze Delta tables are missing. Run `python src/ingest.py` first. "
            f"Missing: {', '.join(missing)}"
        )


def create_spark_session() -> SparkSession:
    """Create a Spark session configured with Delta Lake support."""
    try:
        builder = (
            SparkSession.builder.appName("SalesCommissionTransform")
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


def read_delta(spark: SparkSession, path: Path) -> DataFrame:
    """Read a Delta table from disk."""
    try:
        return spark.read.format("delta").load(str(path))
    except Exception as exc:
        raise RuntimeError(f"Could not read Delta table at {path}") from exc


def build_silver(transactions: DataFrame, sales_reps: DataFrame, products: DataFrame) -> DataFrame:
    """Create the enriched Silver sales table."""
    products_for_join = products.withColumnRenamed("name", "product_name")

    return (
        transactions.filter(F.col("status") == "closed")
        .join(sales_reps, "rep_id", "left")
        .join(products_for_join, "product_id", "left")
        .withColumn(
            "commission_rate",
            F.when(F.col("tier") == "Senior", F.lit(0.08))
            .when(F.col("tier") == "Mid", F.lit(0.06))
            .otherwise(F.lit(0.04)),
        )
        .withColumn(
            "commission_amt",
            F.col("amount") * F.col("commission_rate") * F.col("margin_pct"),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )


def build_gold(silver: DataFrame) -> DataFrame:
    """Create the Gold commission summary by sales representative."""
    return (
        silver.groupBy("rep_id", "name", "region", "tier", "quota")
        .agg(
            F.sum("amount").alias("total_sales"),
            F.sum("commission_amt").alias("total_commission"),
            F.count("txn_id").alias("num_transactions"),
            F.avg("margin_pct").alias("avg_margin"),
        )
        .withColumn("quota_attainment", F.col("total_sales") / F.col("quota"))
    )


def write_delta(df: DataFrame, path: Path) -> None:
    """Write a Spark DataFrame as a Delta table."""
    try:
        df.write.format("delta").mode("overwrite").save(str(path))
    except Exception as exc:
        raise RuntimeError(f"Could not write Delta table at {path}") from exc


def main() -> None:
    """Run Silver and Gold transformations."""
    validate_bronze_tables()
    spark = create_spark_session()

    try:
        transactions = read_delta(spark, BRONZE_DIR / "transactions")
        sales_reps = read_delta(spark, BRONZE_DIR / "sales_reps")
        products = read_delta(spark, BRONZE_DIR / "products")

        silver = build_silver(transactions, sales_reps, products)
        write_delta(silver, SILVER_DIR / "sales_enriched")

        gold = build_gold(silver)
        write_delta(gold, GOLD_DIR / "commissions")

        print("Silver and Gold transformations complete")
        print(f"Bronze transactions: {transactions.count()}")
        print(f"Bronze sales reps: {sales_reps.count()}")
        print(f"Bronze products: {products.count()}")
        print(f"Silver sales_enriched: {silver.count()}")
        print(f"Gold commissions: {gold.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
