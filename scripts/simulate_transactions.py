#!/usr/bin/env python3
"""Generate synthetic INSERT/UPDATE/DELETE traffic against the source
Postgres database, for exercising the CDC path locally without needing a
live application writing to the database.

Usage:
    uv run scripts/simulate_transactions.py <inserts> <updates> <deletes>

Example:
    uv run scripts/simulate_transactions.py 5 2 1
"""

from __future__ import annotations

import os
import random
import sys
from datetime import datetime, timezone

import psycopg2

CONNECTION_ENV_VAR = "LAKEFLOW_SYNC_PG_DSN"


def get_connection() -> "psycopg2.extensions.connection":
    dsn = os.environ.get(CONNECTION_ENV_VAR)
    if not dsn:
        raise SystemExit(
            f"Set {CONNECTION_ENV_VAR} to a postgres:// connection string before running this script."
        )
    return psycopg2.connect(dsn)


def insert_orders(cursor: "psycopg2.extensions.cursor", count: int) -> list[int]:
    new_ids = []
    for _ in range(count):
        cursor.execute(
            """
            INSERT INTO orders (customer_id, status, total_amount, created_at)
            VALUES (%s, %s, %s, %s)
            RETURNING order_id
            """,
            (
                random.randint(1, 50),
                "pending",
                round(random.uniform(10, 500), 2),
                datetime.now(timezone.utc),
            ),
        )
        new_ids.append(cursor.fetchone()[0])
    return new_ids


def update_orders(cursor: "psycopg2.extensions.cursor", count: int) -> None:
    cursor.execute("SELECT order_id FROM orders ORDER BY random() LIMIT %s", (count,))
    ids = [row[0] for row in cursor.fetchall()]
    for order_id in ids:
        cursor.execute(
            "UPDATE orders SET status = %s WHERE order_id = %s",
            (random.choice(["shipped", "delivered", "cancelled"]), order_id),
        )


def delete_orders(cursor: "psycopg2.extensions.cursor", count: int) -> None:
    cursor.execute("SELECT order_id FROM orders ORDER BY random() LIMIT %s", (count,))
    ids = [row[0] for row in cursor.fetchall()]
    for order_id in ids:
        cursor.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(f"Usage: {sys.argv[0]} <inserts> <updates> <deletes>")

    inserts, updates, deletes = (int(arg) for arg in sys.argv[1:4])

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cursor:
                new_ids = insert_orders(cursor, inserts)
                update_orders(cursor, updates)
                delete_orders(cursor, deletes)
        print(
            f"Simulated {inserts} inserts (new ids: {new_ids}), "
            f"{updates} updates, {deletes} deletes."
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
