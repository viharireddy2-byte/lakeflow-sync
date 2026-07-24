"""Fixtures for the CDC end-to-end integration test.

These tests need a real Postgres with `wal_level=logical` (see
`docker-compose.yml`) and write to a local duckdb file instead of a real
Databricks workspace (via `LAKEFLOW_SYNC_DESTINATION=duckdb`).

Deliberately self-skipping: if `LAKEFLOW_SYNC_PG_DSN` is not set (i.e.
nobody ran `scripts/run_integration_tests.sh`), every test in this package
is skipped rather than failed. This means `uv run pytest` -- the existing
CI quality gate -- continues to pass unchanged on a machine with no Docker
and no Postgres; nothing about current CI behavior changes.
"""

from __future__ import annotations

import os
import uuid
from typing import Iterator

import pytest

PG_DSN = os.environ.get("LAKEFLOW_SYNC_PG_DSN")

pytestmark = pytest.mark.skipif(
    not PG_DSN,
    reason=(
        "LAKEFLOW_SYNC_PG_DSN not set -- integration tests require Postgres "
        "with wal_level=logical. Run scripts/run_integration_tests.sh."
    ),
)


@pytest.fixture()
def pg_connection() -> Iterator["object"]:
    import psycopg2

    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture()
def source_table(pg_connection: "object") -> Iterator[str]:
    """Create a throwaway table, publication, and replication slot for one
    test (matching `cdc_load`'s configured slot/publication names so
    `run_cdc_load()` can be called unmodified), then tear everything down.
    """
    from lakeflow_sync import cdc_load

    table_name = f"it_orders_{uuid.uuid4().hex[:8]}"

    with pg_connection.cursor() as cur:  # type: ignore[attr-defined]
        cur.execute(
            f"""
            CREATE TABLE {table_name} (
                order_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL
            )
            """
        )
        cur.execute(f"ALTER TABLE {table_name} REPLICA IDENTITY FULL")
        cur.execute(
            f"CREATE PUBLICATION {cdc_load.PUBLICATION_NAME} FOR TABLE {table_name}"
        )
        cur.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
            (cdc_load.SLOT_NAME,),
        )

    try:
        yield table_name
    finally:
        with pg_connection.cursor() as cur:  # type: ignore[attr-defined]
            cur.execute(
                "SELECT pg_drop_replication_slot(%s) WHERE EXISTS "
                "(SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)",
                (cdc_load.SLOT_NAME, cdc_load.SLOT_NAME),
            )
            cur.execute(f"DROP PUBLICATION IF EXISTS {cdc_load.PUBLICATION_NAME}")
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
