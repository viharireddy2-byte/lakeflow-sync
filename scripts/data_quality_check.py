#!/usr/bin/env python3
"""Standalone CLI wrapper around `lakeflow_sync.quality_checks`, for running
the data quality gate manually against any environment without going
through a full Databricks Job run.

Usage:
    uv run scripts/data_quality_check.py --catalog dev_orders_lakehouse --dataset bronze
"""

from __future__ import annotations

from lakeflow_sync.quality_checks import main

if __name__ == "__main__":
    main()
