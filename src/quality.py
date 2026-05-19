"""Run data quality checks against the Silver Delta layer."""

from __future__ import annotations

import sys
import os
from pathlib import Path
from typing import Any

os.environ.setdefault("GX_SHOW_PROGRESS_BARS", "false")

import great_expectations as gx
import pandas as pd
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession


SILVER_PATH = Path("data/delta/silver/sales_enriched")


def validate_silver_exists(path: Path = SILVER_PATH) -> None:
    """Validate that the Silver Delta table exists before quality checks run."""
    delta_log = path / "_delta_log"
    if not delta_log.exists():
        raise FileNotFoundError(
            "Silver Delta table is missing. Run `python src/transform.py` first. "
            f"Missing: {path}"
        )


def create_spark_session() -> SparkSession:
    """Create a Spark session configured with Delta Lake support."""
    try:
        builder = (
            SparkSession.builder.appName("SalesCommissionQuality")
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
            "Could not create SparkSession with Delta Lake. Verify JAVA_HOME uses "
            "Java 11 or 17 and HADOOP_HOME is configured on Windows."
        ) from exc


def read_silver(spark: SparkSession, path: Path = SILVER_PATH) -> DataFrame:
    """Read the Silver Delta table with Spark."""
    try:
        return spark.read.format("delta").load(str(path))
    except Exception as exc:
        raise RuntimeError(f"Could not read Silver Delta table at {path}") from exc


def to_pandas(df: DataFrame) -> pd.DataFrame:
    """Convert the Silver Spark DataFrame to pandas for Great Expectations."""
    try:
        return df.toPandas()
    except Exception as exc:
        raise RuntimeError("Could not convert Silver DataFrame to pandas") from exc


def build_validator(dataframe: pd.DataFrame) -> Any:
    """Create a stable in-memory Great Expectations validator for GE 0.18.0."""
    try:
        context = gx.get_context()
        datasource = context.sources.add_or_update_pandas(name="silver_source")
        asset = datasource.add_dataframe_asset(name="sales_enriched")
        batch_request = asset.build_batch_request(dataframe=dataframe)

        suite_name = "sales_silver_quality_suite"
        try:
            context.delete_expectation_suite(expectation_suite_name=suite_name)
        except Exception:
            pass
        suite = context.add_expectation_suite(expectation_suite_name=suite_name)

        return context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite,
        )
    except Exception as exc:
        raise RuntimeError("Could not initialize Great Expectations validator") from exc


def add_expectations(validator: Any) -> None:
    """Register all Phase 8 expectations on the validator."""
    validator.expect_column_values_to_not_be_null("rep_id")
    validator.expect_column_values_to_not_be_null("product_id")
    validator.expect_column_values_to_not_be_null("amount")
    validator.expect_column_values_to_not_be_null("commission_amt")
    validator.expect_column_values_to_be_between(
        "commission_rate",
        min_value=0.04,
        max_value=0.08,
    )
    validator.expect_column_values_to_be_between(
        "amount",
        min_value=0,
        max_value=100_000,
    )
    validator.expect_column_values_to_be_in_set(
        "tier",
        value_set=["Junior", "Mid", "Senior"],
    )
    validator.expect_column_pair_values_a_to_be_greater_than_b(
        "amount",
        "commission_amt",
    )
    validator.expect_column_values_to_be_unique("txn_id")
    validator.expect_column_values_to_be_between(
        "commission_amt",
        min_value=0,
        max_value=10_000,
    )


def print_summary(results: dict[str, Any]) -> None:
    """Print a compact quality result summary."""
    statistics = results.get("statistics", {})
    evaluated = statistics.get("evaluated_expectations", 0)
    successful = statistics.get("successful_expectations", 0)
    unsuccessful = statistics.get("unsuccessful_expectations", evaluated - successful)
    success = results.get("success", False)

    print("Data quality summary")
    print(f"Total expectations: {evaluated}")
    print(f"Successful expectations: {successful}")
    print(f"Unsuccessful expectations: {unsuccessful}")
    print(f"Success: {success}")


def main() -> int:
    """Run Great Expectations checks against the Silver layer."""
    validate_silver_exists()
    spark = create_spark_session()

    try:
        silver = read_silver(spark)
        silver_pd = to_pandas(silver)
        validator = build_validator(silver_pd)
        add_expectations(validator)
        results = validator.validate()
        print_summary(results)
        return 0 if results.get("success", False) else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"QUALITY ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
