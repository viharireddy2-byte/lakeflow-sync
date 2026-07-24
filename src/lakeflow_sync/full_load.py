"""Full Load mode: snapshot every configured source table and REPLACE
the corresponding Bronze Delta table.

Used for initial backfill or a full reset of a table. Built on
`dlt.sources.sql_database`, which handles connection pooling, type
reflection, and chunked reads against Postgres out of the box.
"""

from __future__ import annotations

import os
import time
from typing import Any

import dlt
from dlt.sources.sql_database import sql_database

from lakeflow_sync.resilience import run_with_retry
from lakeflow_sync.utils.logger import get_logger, log_with_context
from lakeflow_sync.utils.notifications import notify_run_result

logger = get_logger(__name__)

DEFAULT_TABLES = ["customers", "orders", "order_items", "products"]

# Overridable for integration tests (e.g. "duckdb" against a local file) --
# defaults to the original, always-Databricks behavior, so nothing changes
# for existing deployments.
DESTINATION = os.environ.get("LAKEFLOW_SYNC_DESTINATION", "databricks")


def _tables_to_load() -> list[str]:
    configured = os.environ.get("LAKEFLOW_SYNC_TABLES")
    if configured:
        return [t.strip() for t in configured.split(",") if t.strip()]
    return DEFAULT_TABLES


def run_full_load(catalog: str, dataset: str) -> dict[str, Any]:
    """Snapshot source tables and write them to `catalog.dataset` in Databricks.

    Returns a small run summary dict (used by pipeline_main / tests).
    """
    started = time.monotonic()
    tables = _tables_to_load()
    log_with_context(logger, 20, "Starting full load", catalog=catalog, dataset=dataset, tables=tables)

    pipeline = dlt.pipeline(
        pipeline_name="lakeflow_sync_full_load",
        destination=DESTINATION,
        dataset_name=dataset,
        progress="log",
    )

    source = sql_database().with_resources(*tables)

    try:
        load_info = run_with_retry(
            lambda: pipeline.run(source, write_disposition="replace"),
            op_name="Full load",
        )
    except Exception as exc:  # noqa: BLE001
        duration = time.monotonic() - started
        notify_run_result("full_load", "failure", 0, duration, error=str(exc))
        logger.error("Full load failed: %s", exc)
        raise

    rows_processed = _count_rows_loaded(load_info)
    duration = time.monotonic() - started
    log_with_context(
        logger,
        20,
        "Full load completed",
        rows_processed=rows_processed,
        duration_seconds=round(duration, 2),
    )
    notify_run_result("full_load", "success", rows_processed, duration)

    return {
        "mode": "full_load",
        "tables": tables,
        "rows_processed": rows_processed,
        "duration_seconds": duration,
    }


def _count_rows_loaded(load_info: Any) -> int:
    """Best-effort extraction of a row count from a dlt LoadInfo object."""
    try:
        metrics = load_info.metrics
        total = 0
        for package_metrics in metrics.values():
            for job_list in package_metrics:
                for job in job_list.get("job_metrics", {}).values():
                    total += getattr(job, "rows_count", 0) or 0
        return total
    except Exception:  # noqa: BLE001
        return 0
