import os
from unittest import mock

from lakeflow_sync.full_load import DEFAULT_TABLES, _tables_to_load


def test_tables_to_load_defaults_when_env_unset() -> None:
    with mock.patch.dict(os.environ, {}, clear=True):
        assert _tables_to_load() == DEFAULT_TABLES


def test_tables_to_load_respects_env_override() -> None:
    with mock.patch.dict(os.environ, {"LAKEFLOW_SYNC_TABLES": "a, b ,c"}, clear=True):
        assert _tables_to_load() == ["a", "b", "c"]
