import pytest

from lakeflow_sync.resilience import (
    ReplicationSlotInvalidatedError,
    classify_error,
    run_with_retry,
)


def test_classify_slot_invalidated() -> None:
    exc = RuntimeError('replication slot "lakeflow_sync_slot" does not exist')
    assert classify_error(exc) == "slot_invalidated"


def test_classify_slot_wal_removed() -> None:
    exc = RuntimeError("requested WAL segment has already been removed")
    assert classify_error(exc) == "slot_invalidated"


def test_classify_permanent_auth_error() -> None:
    exc = RuntimeError("password authentication failed for user lakeflow_sync")
    assert classify_error(exc) == "permanent"


def test_classify_permanent_missing_relation() -> None:
    exc = RuntimeError('relation "orders" does not exist')
    assert classify_error(exc) == "permanent"


def test_classify_unknown_error_defaults_transient() -> None:
    exc = RuntimeError("connection reset by peer")
    assert classify_error(exc) == "transient"


def test_run_with_retry_succeeds_first_try_without_sleeping() -> None:
    calls = []
    sleeps: list[float] = []

    def func() -> str:
        calls.append(1)
        return "ok"

    result = run_with_retry(func, op_name="test", sleep=sleeps.append)

    assert result == "ok"
    assert len(calls) == 1
    assert sleeps == []


def test_run_with_retry_retries_transient_then_succeeds() -> None:
    attempts = {"count": 0}
    sleeps: list[float] = []

    def func() -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("connection reset by peer")
        return "ok"

    result = run_with_retry(
        func, op_name="test", max_attempts=5, base_delay_seconds=0.01, sleep=sleeps.append
    )

    assert result == "ok"
    assert attempts["count"] == 3
    assert len(sleeps) == 2  # slept between attempt 1->2 and 2->3
    assert sleeps == [0.01, 0.02]  # exponential backoff


def test_run_with_retry_gives_up_after_max_attempts() -> None:
    def func() -> str:
        raise RuntimeError("connection reset by peer")

    with pytest.raises(RuntimeError, match="connection reset"):
        run_with_retry(func, op_name="test", max_attempts=2, base_delay_seconds=0.01, sleep=lambda _: None)


def test_run_with_retry_never_retries_permanent_errors() -> None:
    calls = []

    def func() -> str:
        calls.append(1)
        raise RuntimeError("permission denied for table orders")

    with pytest.raises(RuntimeError, match="permission denied"):
        run_with_retry(func, op_name="test", max_attempts=5, sleep=lambda _: None)

    assert len(calls) == 1  # no retries for permanent errors


def test_run_with_retry_wraps_slot_invalidated_and_never_retries() -> None:
    calls = []

    def func() -> str:
        calls.append(1)
        raise RuntimeError('replication slot "lakeflow_sync_slot" does not exist')

    with pytest.raises(ReplicationSlotInvalidatedError):
        run_with_retry(func, op_name="test", max_attempts=5, sleep=lambda _: None)

    assert len(calls) == 1  # no retries -- retrying can't fix a dropped slot
