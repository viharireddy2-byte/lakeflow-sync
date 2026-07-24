import importlib

import pytest

from lakeflow_sync.pg_stream import source_compat


def test_get_replication_source_returns_callable_when_import_succeeded() -> None:
    if source_compat.PG_REPLICATION_IMPORT_ERROR is not None:
        pytest.skip("dlt.sources.pg_replication is not importable in this environment")

    result = source_compat.get_replication_source()
    assert callable(result)


def test_get_replication_source_raises_actionable_error_when_import_failed(monkeypatch) -> None:
    monkeypatch.setattr(source_compat, "_replication_source", None)
    monkeypatch.setattr(
        source_compat, "PG_REPLICATION_IMPORT_ERROR", ImportError("simulated missing module")
    )

    with pytest.raises(ImportError, match="dlt init pg_replication databricks"):
        source_compat.get_replication_source()


def test_module_is_importable_even_if_dlt_pg_replication_is_missing() -> None:
    # Re-importing the module itself must never raise, regardless of whether
    # the underlying dlt import succeeded -- only calling
    # get_replication_source() should raise, and only when the import
    # actually failed.
    importlib.reload(source_compat)
