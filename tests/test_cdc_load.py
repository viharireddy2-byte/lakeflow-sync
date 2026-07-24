"""Unit tests for cdc_load.py's orchestration logic, with dlt's pipeline and
the replication source mocked out. These don't need real Postgres or
Databricks -- see tests/integration/ for the end-to-end version against a
live replication slot.
"""

from __future__ import annotations

from unittest import mock

import pytest

from lakeflow_sync import cdc_load
from lakeflow_sync.resilience import ReplicationSlotInvalidatedError


def _fake_load_info(rows: int) -> mock.MagicMock:
    load_info = mock.MagicMock()
    load_info.metrics = {
        "pkg1": [{"job_metrics": {"job1": mock.MagicMock(rows_count=rows)}}],
    }
    return load_info


def test_run_cdc_load_success_returns_summary(monkeypatch) -> None:
    mock_pipeline = mock.MagicMock()
    mock_pipeline.run.return_value = _fake_load_info(5)
    monkeypatch.setattr(cdc_load.dlt, "pipeline", mock.MagicMock(return_value=mock_pipeline))
    monkeypatch.setattr(cdc_load, "get_replication_source", mock.MagicMock(return_value=mock.MagicMock()))
    notify = mock.MagicMock()
    monkeypatch.setattr(cdc_load, "notify_run_result", notify)

    result = cdc_load.run_cdc_load(catalog="dev_orders_lakehouse", dataset="bronze")

    assert result["mode"] == "cdc"
    assert result["rows_processed"] == 5
    mock_pipeline.run.assert_called_once()
    _, run_kwargs = mock_pipeline.run.call_args
    assert run_kwargs["write_disposition"] == "append"
    notify.assert_called_once_with("cdc", "success", 5, mock.ANY)


def test_run_cdc_load_retries_transient_failure_then_succeeds(monkeypatch) -> None:
    mock_pipeline = mock.MagicMock()
    mock_pipeline.run.side_effect = [RuntimeError("connection reset by peer"), _fake_load_info(2)]
    monkeypatch.setattr(cdc_load.dlt, "pipeline", mock.MagicMock(return_value=mock_pipeline))
    monkeypatch.setattr(cdc_load, "get_replication_source", mock.MagicMock(return_value=mock.MagicMock()))
    monkeypatch.setattr(cdc_load, "notify_run_result", mock.MagicMock())
    monkeypatch.setattr("lakeflow_sync.resilience.time.sleep", lambda _: None)

    result = cdc_load.run_cdc_load(catalog="dev_orders_lakehouse", dataset="bronze")

    assert result["rows_processed"] == 2
    assert mock_pipeline.run.call_count == 2


def test_run_cdc_load_permanent_failure_notifies_and_raises_without_retry(monkeypatch) -> None:
    mock_pipeline = mock.MagicMock()
    mock_pipeline.run.side_effect = RuntimeError("permission denied for table orders")
    monkeypatch.setattr(cdc_load.dlt, "pipeline", mock.MagicMock(return_value=mock_pipeline))
    monkeypatch.setattr(cdc_load, "get_replication_source", mock.MagicMock(return_value=mock.MagicMock()))
    notify = mock.MagicMock()
    monkeypatch.setattr(cdc_load, "notify_run_result", notify)

    with pytest.raises(RuntimeError, match="permission denied"):
        cdc_load.run_cdc_load(catalog="dev_orders_lakehouse", dataset="bronze")

    assert mock_pipeline.run.call_count == 1
    notify.assert_called_once()
    assert notify.call_args[0][1] == "failure"


def test_run_cdc_load_slot_invalidated_raises_dedicated_error_and_notifies(monkeypatch) -> None:
    mock_pipeline = mock.MagicMock()
    mock_pipeline.run.side_effect = RuntimeError(
        'replication slot "lakeflow_sync_slot" does not exist'
    )
    monkeypatch.setattr(cdc_load.dlt, "pipeline", mock.MagicMock(return_value=mock_pipeline))
    monkeypatch.setattr(cdc_load, "get_replication_source", mock.MagicMock(return_value=mock.MagicMock()))
    notify = mock.MagicMock()
    monkeypatch.setattr(cdc_load, "notify_run_result", notify)

    with pytest.raises(ReplicationSlotInvalidatedError):
        cdc_load.run_cdc_load(catalog="dev_orders_lakehouse", dataset="bronze")

    assert mock_pipeline.run.call_count == 1
    notify.assert_called_once()
    assert notify.call_args[0][1] == "failure"


def test_run_cdc_load_uses_destination_override(monkeypatch) -> None:
    monkeypatch.setattr(cdc_load, "DESTINATION", "duckdb")
    mock_pipeline_factory = mock.MagicMock()
    mock_pipeline_factory.return_value.run.return_value = _fake_load_info(0)
    monkeypatch.setattr(cdc_load.dlt, "pipeline", mock_pipeline_factory)
    monkeypatch.setattr(cdc_load, "get_replication_source", mock.MagicMock(return_value=mock.MagicMock()))
    monkeypatch.setattr(cdc_load, "notify_run_result", mock.MagicMock())

    cdc_load.run_cdc_load(catalog="dev_orders_lakehouse", dataset="bronze")

    _, pipeline_kwargs = mock_pipeline_factory.call_args
    assert pipeline_kwargs["destination"] == "duckdb"
