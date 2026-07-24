import pytest

from lakeflow_sync.pipeline_main import main, parse_args


def test_parse_args_requires_mode_and_catalog() -> None:
    args = parse_args(["--mode", "full_load", "--catalog", "dev_orders_lakehouse"])
    assert args.mode == "full_load"
    assert args.catalog == "dev_orders_lakehouse"
    assert args.dataset == "bronze"


def test_parse_args_rejects_invalid_mode() -> None:
    with pytest.raises(SystemExit):
        parse_args(["--mode", "bogus", "--catalog", "dev_orders_lakehouse"])


def test_dry_run_does_not_touch_pipelines() -> None:
    exit_code = main(
        ["--mode", "full_load", "--catalog", "dev_orders_lakehouse", "--dry-run"]
    )
    assert exit_code == 0
