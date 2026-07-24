"""CDC mode: read the Postgres logical replication slot and APPEND every
insert/update/delete event to the Bronze Delta tables as an immutable log.

See `lakeflow_sync.pg_stream` for why this is append-only rather than a
merge/mirror of the source state.
"""

from __future__ import annotations

import os
import time
from typing import Any, Iterable

import dlt

from lakeflow_sync.pg_stream.events import normalize_change_event
from lakeflow_sync.pg_stream.source_compat import get_replication_source
from lakeflow_sync.resilience import run_with_retry
from lakeflow_sync.utils.logger import get_logger, log_with_context
from lakeflow_sync.utils.notifications import notify_run_result

logger = get_logger(__name__)

SLOT_NAME = os.environ.get("LAKEFLOW_SYNC_SLOT_NAME", "lakeflow_sync_slot")
PUBLICATION_NAME = os.environ.get("LAKEFLOW_SYNC_PUBLICATION", "lakeflow_sync_pub")

# Overridable for integration tests (e.g. "duckdb" against a local file) --
# defaults to the original, always-Databricks behavior, so nothing changes
# for existing deployments.
DESTINATION = os.environ.get("LAKEFLOW_SYNC_DESTINATION", "databricks")


@dlt.transformer
def normalize_events(events: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """dlt transformer: normalize each raw WAL event before it lands in Delta."""
    for event in events:
        yield normalize_change_event(event)


def run_cdc_load(catalog: str, dataset: str) -> dict[str, Any]:
    """Drain the replication slot once and append normalized events to Databricks.

    Each invocation processes whatever has accumulated on the slot since the
    last run and then exits -- this is what makes it safe to run as a
    scheduled (rather than continuously running) Lakeflow Job task.
    """
    started = time.monotonic()
    log_with_context(
        logger, 20, "Starting CDC load", catalog=catalog, dataset=dataset, slot=SLOT_NAME
    )

    pipeline = dlt.pipeline(
        pipeline_name="lakeflow_sync_cdc_load",
        destination=DESTINATION,
        dataset_name=dataset,
        progress="log",
    )

    replication_source = get_replication_source()
    source = replication_source(
        slot_name=SLOT_NAME,
        pub_name=PUBLICATION_NAME,
        include_columns=None,
        target_batch_size=1000,
    )

    try:
        load_info = run_with_retry(
            lambda: pipeline.run(source | normalize_events, write_disposition="append"),
            op_name="CDC load",
        )
    except Exception as exc:  # noqa: BLE001
        duration = time.monotonic() - started
        notify_run_result("cdc", "failure", 0, duration, error=str(exc))
        logger.error("CDC load failed: %s", exc)
        raise

    rows_processed = _count_rows_loaded(load_info)
    duration = time.monotonic() - started
    log_with_context(
        logger,
        20,
        "CDC load completed",
        rows_processed=rows_processed,
        duration_seconds=round(duration, 2),
    )
    notify_run_result("cdc", "success", rows_processed, duration)

    return {
        "mode": "cdc",
        "slot": SLOT_NAME,
        "rows_processed": rows_processed,
        "duration_seconds": duration,
    }


def _count_rows_loaded(load_info: Any) -> int:
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
