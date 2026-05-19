"""Generate Excel and JSON outputs from the Gold commissions Delta table."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession


GOLD_PATH = Path("data/delta/gold/commissions")
EXCEL_PATH = Path("data/commissions_report.xlsx")
EXPORTS_DIR = Path("exports")
JSON_PATH = EXPORTS_DIR / "commissions.json"


def validate_gold_exists(path: Path = GOLD_PATH) -> None:
    """Validate that the Gold Delta table exists before reporting."""
    delta_log = path / "_delta_log"
    if not delta_log.exists():
        raise FileNotFoundError(
            "Gold Delta table is missing. Run `python src/transform.py` first. "
            f"Missing: {path}"
        )


def create_spark_session() -> SparkSession:
    """Create a Spark session configured with Delta Lake support."""
    try:
        builder = (
            SparkSession.builder.appName("SalesCommissionReport")
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
            "Could not create SparkSession with Delta Lake. Verify JAVA_HOME points "
            "to Java 11 or 17 and HADOOP_HOME is configured on Windows."
        ) from exc


def read_gold(spark: SparkSession, path: Path = GOLD_PATH) -> DataFrame:
    """Read the Gold commissions Delta table."""
    try:
        return spark.read.format("delta").load(str(path))
    except Exception as exc:
        raise RuntimeError(f"Could not read Gold Delta table at {path}") from exc


def to_pandas(df: DataFrame) -> pd.DataFrame:
    """Convert the Gold Spark DataFrame to pandas for report exports."""
    try:
        return df.toPandas()
    except Exception as exc:
        raise RuntimeError("Could not convert Gold DataFrame to pandas") from exc


def format_commissions(df: pd.DataFrame) -> pd.DataFrame:
    """Format metrics for Excel and frontend-ready JSON output."""
    formatted = df.copy()
    formatted["quota_attainment"] = (formatted["quota_attainment"] * 100).round(1)
    formatted["total_sales"] = formatted["total_sales"].round(2)
    formatted["total_commission"] = formatted["total_commission"].round(2)
    formatted["avg_margin"] = formatted["avg_margin"].round(4)
    return formatted.sort_values("total_sales", ascending=False).reset_index(drop=True)


def build_region_summary(commissions: pd.DataFrame) -> pd.DataFrame:
    """Build the regional summary sheet for the Excel report."""
    summary = (
        commissions.groupby("region", as_index=False)
        .agg(
            reps=("rep_id", "count"),
            total_sales=("total_sales", "sum"),
            total_commission=("total_commission", "sum"),
            num_transactions=("num_transactions", "sum"),
            avg_quota_attainment=("quota_attainment", "mean"),
            avg_margin=("avg_margin", "mean"),
        )
        .sort_values("total_sales", ascending=False)
    )
    summary["total_sales"] = summary["total_sales"].round(2)
    summary["total_commission"] = summary["total_commission"].round(2)
    summary["avg_quota_attainment"] = summary["avg_quota_attainment"].round(1)
    summary["avg_margin"] = summary["avg_margin"].round(4)
    return summary.reset_index(drop=True)


def write_excel(commissions: pd.DataFrame, by_region: pd.DataFrame, path: Path = EXCEL_PATH) -> None:
    """Write the commission report workbook with two sheets."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            commissions.to_excel(writer, sheet_name="Commissions", index=False)
            by_region.to_excel(writer, sheet_name="By Region", index=False)
    except Exception as exc:
        raise RuntimeError(f"Could not write Excel report at {path}") from exc


def write_json(commissions: pd.DataFrame, path: Path = JSON_PATH) -> None:
    """Write frontend-ready commissions JSON records."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        commissions.to_json(path, orient="records", indent=2)
    except Exception as exc:
        raise RuntimeError(f"Could not write JSON export at {path}") from exc


def main() -> int:
    """Generate the Excel report and frontend JSON export."""
    validate_gold_exists()
    spark = create_spark_session()

    try:
        gold = read_gold(spark)
        commissions = format_commissions(to_pandas(gold))
        by_region = build_region_summary(commissions)

        write_excel(commissions, by_region)
        write_json(commissions)

        print("Commission exports generated")
        print(f"Records exported: {len(commissions)}")
        print(f"Excel report: {EXCEL_PATH}")
        print(f"JSON export: {JSON_PATH}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"REPORT ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
