"""Lightweight post-load data quality gate.

Enhancement over the baseline blueprint: after a full load or CDC run, this
module runs a handful of cheap sanity checks directly against the Bronze
Delta tables via PySpark, and raises if any of them fail. This turns silent
data problems (e.g. a table that suddenly loaded 0 rows, or a CDC batch
introducing NULLs in a primary key) into a failed, alertable Databricks Job
task rather than something that is only noticed downstream.

This intentionally does NOT try to be a full Great Expectations / dbt-test
replacement -- it is meant as a fast, dependency-light guardrail that runs
in seconds as the last task in the job.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lakeflow_sync.utils.logger import get_logger, log_with_context

logger = get_logger(__name__)


@dataclass
class QualityCheckResult:
    table: str
    check: str
    passed: bool
    details: str = ""


@dataclass
class QualityReport:
    results: list[QualityCheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    def add(self, result: QualityCheckResult) -> None:
        self.results.append(result)


def check_not_empty(spark: Any, full_table_name: str) -> QualityCheckResult:
    count = spark.table(full_table_name).count()
    return QualityCheckResult(
        table=full_table_name,
        check="not_empty",
        passed=count > 0,
        details=f"row_count={count}",
    )


def check_no_null_primary_key(
    spark: Any, full_table_name: str, primary_key: str
) -> QualityCheckResult:
    null_count = spark.table(full_table_name).filter(f"{primary_key} IS NULL").count()
    return QualityCheckResult(
        table=full_table_name,
        check=f"no_null_{primary_key}",
        passed=null_count == 0,
        details=f"null_count={null_count}",
    )


# table -> primary key column, used by check_no_null_primary_key
TABLE_PRIMARY_KEYS = {
    "customers": "customer_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "products": "product_id",
}


def run_quality_checks(catalog: str, dataset: str) -> QualityReport:
    """Run the quality gate against every configured Bronze table.

    Imports PySpark lazily so unit tests that don't need Spark can import
    this module without requiring a Spark session to be available.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    report = QualityReport()

    for table, primary_key in TABLE_PRIMARY_KEYS.items():
        full_table_name = f"{catalog}.{dataset}.{table}"
        try:
            report.add(check_not_empty(spark, full_table_name))
            report.add(check_no_null_primary_key(spark, full_table_name, primary_key))
        except Exception as exc:  # noqa: BLE001
            report.add(
                QualityCheckResult(
                    table=full_table_name, check="table_reachable", passed=False, details=str(exc)
                )
            )

    for result in report.results:
        log_with_context(
            logger,
            20 if result.passed else 40,
            "Quality check result",
            table=result.table,
            check=result.check,
            passed=result.passed,
            details=result.details,
        )

    if not report.passed:
        failed = [r for r in report.results if not r.passed]
        raise AssertionError(f"Data quality gate failed: {failed}")

    return report


def main() -> None:
    """Entry point wired to the `run_quality_checks` python_wheel_task in the job resource."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--dataset", required=True)
    args = parser.parse_args()
    run_quality_checks(args.catalog, args.dataset)


if __name__ == "__main__":
    main()
