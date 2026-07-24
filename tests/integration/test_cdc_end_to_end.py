"""End-to-end CDC test: real Postgres -> full_load -> mutate rows -> cdc_load
-> assert the destination reflects an append-only change log.

This is the test flagged as missing from the project: it is the only test
that actually proves the CDC path works against live logical replication,
rather than against a mocked `dlt.sources.pg_replication`.

Destination is duckdb (via `LAKEFLOW_SYNC_DESTINATION=duckdb`), not a real
Databricks workspace -- everything downstream of `dlt.pipeline.run(...)` is
destination-agnostic, so this still exercises the real full_load / cdc_load
code paths, the real replication slot/publication, and the real
`normalize_change_event` transformer.
"""

from __future__ import annotations

import os
import time

import duckdb
import pytest

os.environ.setdefault("LAKEFLOW_SYNC_DESTINATION", "duckdb")

from lakeflow_sync import cdc_load, full_load  # noqa: E402  (env must be set first)


@pytest.fixture()
def duckdb_dataset(tmp_path, monkeypatch) -> str:
    """Point dlt's duckdb destination at a throwaway file for this test."""
    db_path = tmp_path / "lakeflow_sync_it.duckdb"
    monkeypatch.setenv("DESTINATION__DUCKDB__CREDENTIALS__DATABASE", str(db_path))
    return "it_bronze"


def _replication_available() -> bool:
    try:
        cdc_load.get_replication_source()
        return True
    except ImportError:
        return False


@pytest.mark.skipif(
    not _replication_available(),
    reason="dlt.sources.pg_replication is not importable with the installed dlt version",
)
def test_full_load_then_cdc_reflects_inserts_updates_deletes(
    pg_connection, source_table, duckdb_dataset, monkeypatch
) -> None:
    monkeypatch.setenv("LAKEFLOW_SYNC_TABLES", source_table)

    with pg_connection.cursor() as cur:
        cur.execute(f"INSERT INTO {source_table} (order_id, status) VALUES (1, 'pending')")

    # 1. Full load: initial snapshot should land the one existing row.
    full_load_summary = full_load.run_full_load(catalog="ignored_for_duckdb", dataset=duckdb_dataset)
    assert full_load_summary["rows_processed"] >= 1

    # 2. Mutate the source: one insert, one update, one delete.
    with pg_connection.cursor() as cur:
        cur.execute(f"INSERT INTO {source_table} (order_id, status) VALUES (2, 'pending')")
        cur.execute(f"UPDATE {source_table} SET status = 'shipped' WHERE order_id = 1")
        cur.execute(f"DELETE FROM {source_table} WHERE order_id = 2")

    # Give Postgres a moment to flush WAL to the replication slot.
    time.sleep(1)

    # 3. Drain the CDC slot.
    cdc_summary = cdc_load.run_cdc_load(catalog="ignored_for_duckdb", dataset=duckdb_dataset)
    assert cdc_summary["rows_processed"] >= 3  # insert + update + delete events

    # 4. Verify the Bronze table is an append-only log: the update produced a
    #    *new* row rather than overwriting the original, and the delete is a
    #    soft-delete marker rather than a removed row.
    db_path = os.environ["DESTINATION__DUCKDB__CREDENTIALS__DATABASE"]
    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = con.execute(
            f"""
            SELECT order_id, status, _cdc_operation
            FROM {duckdb_dataset}.{source_table}
            ORDER BY order_id, _cdc_operation
            """
        ).fetchall()
    finally:
        con.close()

    operations = {row[2] for row in rows}
    assert "insert" in operations
    assert "update" in operations
    assert "delete" in operations
    # The original order_id=1 row from the update must still be present
    # (append-only), not replaced.
    order_1_rows = [r for r in rows if r[0] == 1]
    assert len(order_1_rows) >= 2  # original insert row + update row
