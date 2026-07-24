#!/usr/bin/env python3
"""Print row counts and a breakdown of CDC operations per Bronze table, to
manually sanity-check a full load or CDC run.

Usage:
    uv run scripts/verify_data.py --catalog dev_orders_lakehouse --dataset bronze
"""

from __future__ import annotations

import argparse

from databricks import sql


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--dataset", default="bronze")
    parser.add_argument(
        "--tables",
        default="customers,orders,order_items,products",
        help="Comma-separated list of tables to verify.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tables = [t.strip() for t in args.tables.split(",") if t.strip()]

    with sql.connect() as connection:  # picks up connection details from env/CLI profile
        with connection.cursor() as cursor:
            for table in tables:
                full_name = f"{args.catalog}.{args.dataset}.{table}"
                cursor.execute(f"SELECT COUNT(*) FROM {full_name}")
                total = cursor.fetchone()[0]
                print(f"{full_name}: {total} rows")

                cursor.execute(
                    f"""
                    SELECT _cdc_operation, COUNT(*)
                    FROM {full_name}
                    WHERE _cdc_operation IS NOT NULL
                    GROUP BY _cdc_operation
                    """
                )
                for operation, count in cursor.fetchall():
                    print(f"    {operation}: {count}")


if __name__ == "__main__":
    main()
