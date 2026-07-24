"""Standalone demo: exercises the real lakeflow_sync logic with fake data.
No Docker, no Postgres, no Databricks required.

Run with:
    uv run python demo.py
"""

from lakeflow_sync.pg_stream.events import normalize_change_event
from lakeflow_sync.resilience import classify_error, run_with_retry

print("=== 1. CDC event normalization (pg_stream/events.py) ===\n")

raw_events = [
    {"action": "insert", "data": {"order_id": 1, "status": "pending"}, "lsn": "0/16B0001"},
    {"action": "update", "data": {"order_id": 1, "status": "shipped"}, "lsn": "0/16B0002"},
    {"action": "delete", "data": {"order_id": 1}, "lsn": "0/16B0003"},
]

for raw in raw_events:
    normalized = normalize_change_event(raw)
    print(f"raw:        {raw}")
    print(f"normalized: {normalized}\n")

print("=== 2. Failure classification (resilience.py) ===\n")

sample_errors = [
    Exception("connection reset by peer"),
    Exception("password authentication failed for user"),
    Exception('replication slot "lakeflow_sync_slot" does not exist'),
]

for err in sample_errors:
    print(f"error: {err!r:60} -> classified as: {classify_error(err)}")

print("\n=== 3. Retry behavior in action ===\n")

attempts = {"count": 0}


def flaky_call():
    attempts["count"] += 1
    if attempts["count"] < 3:
        raise Exception("connection reset")  # transient -> will be retried
    return "success!"


result = run_with_retry(flaky_call, max_attempts=3, base_delay_seconds=0.1, op_name="demo call")
print(f"Result after {attempts['count']} attempt(s): {result}")
