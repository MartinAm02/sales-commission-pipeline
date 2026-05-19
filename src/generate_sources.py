"""Generate synthetic source data for the Sales Commission Pipeline."""

from __future__ import annotations

import random
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


SEED = 42
RAW_DATA_DIR = Path("data/raw")
SALES_REPS_DB_PATH = RAW_DATA_DIR / "sales_reps.db"
TRANSACTIONS_CSV_PATH = RAW_DATA_DIR / "transactions.csv"
PRODUCTS_PARQUET_PATH = RAW_DATA_DIR / "products.parquet"


def ensure_raw_data_dir(path: Path = RAW_DATA_DIR) -> None:
    """Create the raw data directory if it does not already exist."""
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise RuntimeError(f"Could not create raw data directory at {path}") from exc


def build_sales_reps(count: int = 20) -> pd.DataFrame:
    """Build a reproducible sales representative table with quota metadata."""
    return pd.DataFrame(
        {
            "rep_id": [f"REP{i:03d}" for i in range(1, count + 1)],
            "name": [f"Rep {i}" for i in range(1, count + 1)],
            "region": np.random.choice(["NORTE", "SUR", "CENTRO", "OCCIDENTE"], count),
            "tier": np.random.choice(["Junior", "Mid", "Senior"], count, p=[0.4, 0.4, 0.2]),
            "quota": np.random.randint(50_000, 200_000, count),
        }
    )


def write_sales_reps_sqlite(reps: pd.DataFrame, db_path: Path = SALES_REPS_DB_PATH) -> None:
    """Persist sales representatives to a SQLite database."""
    try:
        with sqlite3.connect(db_path) as conn:
            reps.to_sql("sales_reps", conn, if_exists="replace", index=False)
    except sqlite3.Error as exc:
        raise RuntimeError(f"Could not write sales reps SQLite database at {db_path}") from exc


def build_transactions(reps: pd.DataFrame, start_date: date, days: int = 730) -> pd.DataFrame:
    """Build two years of synthetic sales transactions."""
    rep_ids = reps["rep_id"].tolist()
    transactions: list[dict[str, object]] = []

    for day_offset in range(days):
        transaction_date = start_date + timedelta(days=day_offset)
        daily_transactions = random.randint(8, 25)

        for _ in range(daily_transactions):
            transactions.append(
                {
                    "txn_id": f"TXN{len(transactions):06d}",
                    "rep_id": random.choice(rep_ids),
                    "product_id": f"PROD{random.randint(1, 50):03d}",
                    "amount": round(random.uniform(500, 15_000), 2),
                    "date": transaction_date.isoformat(),
                    "status": np.random.choice(["closed", "pending"], p=[0.85, 0.15]),
                }
            )

    return pd.DataFrame(transactions)


def build_products(count: int = 50) -> pd.DataFrame:
    """Build a reproducible product catalog with category and margin metadata."""
    return pd.DataFrame(
        {
            "product_id": [f"PROD{i:03d}" for i in range(1, count + 1)],
            "name": [f"Product {i}" for i in range(1, count + 1)],
            "category": np.random.choice(["Software", "Hardware", "Services"], count),
            "margin_pct": np.random.uniform(0.10, 0.45, count).round(3),
        }
    )


def write_dataframe_outputs(transactions: pd.DataFrame, products: pd.DataFrame) -> None:
    """Persist transaction and product source files to raw storage."""
    try:
        transactions.to_csv(TRANSACTIONS_CSV_PATH, index=False)
        products.to_parquet(PRODUCTS_PARQUET_PATH, index=False)
    except (OSError, ValueError, ImportError) as exc:
        raise RuntimeError("Could not write CSV or Parquet source outputs") from exc


def main() -> None:
    """Generate all synthetic source systems for the pipeline."""
    random.seed(SEED)
    np.random.seed(SEED)
    ensure_raw_data_dir()

    reps = build_sales_reps()
    write_sales_reps_sqlite(reps)

    transactions = build_transactions(reps, start_date=date(2023, 1, 1))
    products = build_products()
    write_dataframe_outputs(transactions, products)

    print("Sources generated: SQLite + CSV + Parquet")
    print(f"Sales reps: {len(reps)}")
    print(f"Transactions: {len(transactions)}")
    print(f"Products: {len(products)}")
    print(f"Raw output directory: {RAW_DATA_DIR}")


if __name__ == "__main__":
    main()
