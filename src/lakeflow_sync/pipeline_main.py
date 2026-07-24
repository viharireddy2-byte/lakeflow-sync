"""Main orchestrator / CLI entry point.

Usage
-----
    uv run python -m lakeflow_sync.pipeline_main --mode full_load --catalog dev_orders_lakehouse --dataset bronze
    uv run python -m lakeflow_sync.pipeline_main --mode cdc --catalog dev_orders_lakehouse --dataset bronze

Also wired as the `lakeflow_sync` python_wheel_task entry point in
`resources/jobs/lakeflow_sync_job.yml` when running inside a Databricks
Lakeflow Job.
"""

from __future__ import annotations

import argparse
import sys

from lakeflow_sync.cdc_load import run_cdc_load
from lakeflow_sync.full_load import run_full_load
from lakeflow_sync.utils.logger import get_logger

logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="lakeflow-sync",
        description="Postgres -> Databricks ingestion pipeline (Full Load + CDC).",
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=["full_load", "cdc"],
        help="Which pipeline mode to run.",
    )
    parser.add_argument(
        "--catalog", required=True, help="Unity Catalog catalog name, e.g. dev_orders_lakehouse"
    )
    parser.add_argument(
        "--dataset", default="bronze", help="Schema within the catalog to write to (default: bronze)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and exit without writing any data.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.dry_run:
        logger.info(
            "Dry run: mode=%s catalog=%s dataset=%s -- no data will be written.",
            args.mode,
            args.catalog,
            args.dataset,
        )
        return 0

    if args.mode == "full_load":
        run_full_load(catalog=args.catalog, dataset=args.dataset)
    elif args.mode == "cdc":
        run_cdc_load(catalog=args.catalog, dataset=args.dataset)
    else:  # pragma: no cover - guarded by argparse choices
        raise ValueError(f"Unknown mode: {args.mode}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
