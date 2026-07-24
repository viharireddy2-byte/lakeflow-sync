import pytest

from lakeflow_sync.pg_stream.events import CdcOperation, normalize_change_event


def test_normalize_insert_event() -> None:
    raw = {
        "action": "insert",
        "data": {"order_id": 1, "status": "pending"},
        "lsn": "0/1",
        "commit_ts": "2026-07-24T10:00:00Z",
    }
    result = normalize_change_event(raw)

    assert result["order_id"] == 1
    assert result["status"] == "pending"
    assert result["_cdc_operation"] == CdcOperation.INSERT.value
    assert result["_cdc_lsn"] == "0/1"
    assert result["_cdc_committed_at"] == "2026-07-24T10:00:00Z"


def test_normalize_update_event_preserves_new_values() -> None:
    raw = {
        "action": "update",
        "data": {"order_id": 1, "status": "shipped"},
        "lsn": "0/2",
    }
    result = normalize_change_event(raw)

    assert result["status"] == "shipped"
    assert result["_cdc_operation"] == "update"
    # commit_ts was omitted from the raw event -> a timestamp is still generated
    assert result["_cdc_committed_at"] is not None


def test_normalize_delete_event_is_soft_delete_marker() -> None:
    raw = {"action": "delete", "data": {"order_id": 1}, "lsn": "0/3"}
    result = normalize_change_event(raw)

    assert result["_cdc_operation"] == "delete"
    assert result["order_id"] == 1


def test_normalize_unknown_action_raises() -> None:
    with pytest.raises(ValueError, match="Unrecognized CDC action"):
        normalize_change_event({"action": "truncate", "data": {}})
